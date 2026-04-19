#!/usr/bin/env python3
"""Helper executable for portable desktop in-app updates."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import uuid
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ship import update_manager
from src.ship.desktop_update import (
    DESKTOP_UPDATE_MANIFEST_ASSET,
    DesktopUpdatePaths,
    clear_handoff_request,
    clear_success_marker,
    compute_sha256,
    desktop_update_public_key_candidate_paths,
    download_file,
    fetch_json,
    install_stage_label,
    iso_now,
    load_desktop_update_public_keys,
    load_status,
    pid_is_running,
    read_cached_manifest,
    resolve_github_api_base,
    resolve_release_repo,
    save_status,
    validate_desktop_manifest,
    validate_install_plan,
    verify_manifest_signature,
    write_json_atomic,
)

DESKTOP_UPDATER_NO_DIALOG_ENV = "BALUFFO_DESKTOP_UPDATER_NO_DIALOG"
DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV = "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S"

MUTATING_INSTALL_STAGES = frozenset(
    {
        "replacing",
        "migrating",
        "relaunching",
        "verifying",
        "rolling_back",
    }
)
SUCCESS_RECOVERY_STAGES = frozenset({"relaunching", "verifying"})


HELPER_WINDOW_TOKENS = {
    "window_bg": "#17141f",
    "shell_bg": "#1f1a29",
    "panel_bg": "#24202f",
    "panel_border": "#3d3550",
    "title_fg": "#f4edff",
    "text_fg": "#d9d2e8",
    "muted_fg": "#b6accd",
    "accent": "#bb86fc",
    "accent_active": "#9f6dfd",
    "track": "#312a3f",
}
HELPER_WINDOW_SIZE = {"width": 420, "height": 188}
HELPER_BRAND_TEXT = "Baluffo Update"
HELPER_TITLE_TEXT = "Installing the latest portable build"
HELPER_SUPPORT_TEXT = "Baluffo can stay closed while the update finishes."


def _normalize_helper_message(message: str) -> str:
    return str(message or "").strip() or "Preparing update"


def _helper_window_theme_tokens() -> dict[str, str]:
    return dict(HELPER_WINDOW_TOKENS)


def _helper_window_layout(initial_message: str) -> dict[str, Any]:
    return {
        "size": dict(HELPER_WINDOW_SIZE),
        "brandText": HELPER_BRAND_TEXT,
        "titleText": HELPER_TITLE_TEXT,
        "supportText": HELPER_SUPPORT_TEXT,
        "initialMessage": _normalize_helper_message(initial_message),
        "tokens": _helper_window_theme_tokens(),
    }


def _drain_helper_queue(
    progress: HelperProgressWindow,
    *,
    on_message,
    on_close,
) -> bool:
    while True:
        try:
            kind, payload = progress._queue.get_nowait()
        except queue.Empty:
            return False
        if kind == "close":
            on_close()
            progress._closed.set()
            return True
        if kind == "message" and payload:
            on_message(payload)


class HelperProgressWindow:
    """Best-effort native progress window for the one-shot updater helper."""

    def __init__(self) -> None:
        self._queue: queue.SimpleQueue[tuple[str, str]] = queue.SimpleQueue()
        self._closed = threading.Event()

    def start(self, message: str) -> None:
        self.update(_normalize_helper_message(message))

    def update(self, message: str) -> None:
        self._queue.put(("message", _normalize_helper_message(message)))

    def close(self) -> None:
        self._queue.put(("close", ""))
        self._closed.wait(timeout=2.0)

    def run(self, initial_message: str = "Preparing update") -> None:
        if os.name != "nt":
            self._closed.wait()
            return
        try:
            import tkinter as tk
            from tkinter import ttk
        except Exception:  # noqa: BLE001
            self._closed.wait()
            return

        layout = _helper_window_layout(initial_message)
        tokens = layout["tokens"]
        root = tk.Tk()
        root.title(HELPER_BRAND_TEXT)
        root.resizable(False, False)
        root.attributes("-topmost", True)
        root.protocol("WM_DELETE_WINDOW", lambda: None)
        root.configure(bg=tokens["window_bg"])

        style = ttk.Style(root)
        with contextlib.suppress(Exception):
            style.theme_use("clam")
        style.configure(
            "Baluffo.Helper.Horizontal.TProgressbar",
            troughcolor=tokens["track"],
            background=tokens["accent"],
            bordercolor=tokens["panel_border"],
            lightcolor=tokens["accent"],
            darkcolor=tokens["accent_active"],
            thickness=10,
        )

        shell = tk.Frame(
            root,
            bg=tokens["shell_bg"],
            highlightthickness=1,
            highlightbackground=tokens["panel_border"],
            bd=0,
            padx=18,
            pady=16,
        )
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        brand = tk.Label(
            shell,
            text=layout["brandText"],
            bg=tokens["shell_bg"],
            fg=tokens["accent"],
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        )
        brand.pack(fill="x")
        panel = tk.Frame(
            shell,
            bg=tokens["panel_bg"],
            highlightthickness=1,
            highlightbackground=tokens["panel_border"],
            bd=0,
            padx=16,
            pady=14,
        )
        panel.pack(fill="both", expand=True, pady=(10, 0))
        title = tk.Label(
            panel,
            text=layout["titleText"],
            bg=tokens["panel_bg"],
            fg=tokens["title_fg"],
            font=("Segoe UI Semibold", 12),
            anchor="w",
            justify="left",
        )
        title.pack(fill="x")
        message_var = tk.StringVar(value=layout["initialMessage"])
        detail = tk.Label(
            panel,
            textvariable=message_var,
            bg=tokens["panel_bg"],
            fg=tokens["text_fg"],
            font=("Segoe UI", 10),
            anchor="w",
            justify="left",
            wraplength=340,
            pady=0,
        )
        detail.pack(fill="x", pady=(10, 0))
        support = tk.Label(
            panel,
            text=layout["supportText"],
            bg=tokens["panel_bg"],
            fg=tokens["muted_fg"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=340,
        )
        support.pack(fill="x", pady=(8, 0))
        bar = ttk.Progressbar(
            panel,
            mode="indeterminate",
            length=320,
            style="Baluffo.Helper.Horizontal.TProgressbar",
        )
        bar.pack(fill="x", expand=True, pady=(14, 0))
        bar.start(12)
        root.update_idletasks()
        width = max(root.winfo_width() or 0, int(layout["size"]["width"]))
        height = max(root.winfo_height() or 0, int(layout["size"]["height"]))
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        offset_x = max(0, int((screen_width - width) / 2))
        offset_y = max(0, int((screen_height - height) / 3))
        root.geometry(f"{width}x{height}+{offset_x}+{offset_y}")

        def drain() -> None:
            def stop_bar() -> None:
                with contextlib.suppress(Exception):
                    bar.stop()

            should_close = _drain_helper_queue(
                self,
                on_message=message_var.set,
                on_close=stop_bar,
            )
            if should_close:
                root.destroy()
                return
            root.after(120, drain)

        root.after(120, drain)
        with contextlib.suppress(Exception):
            root.mainloop()
        self._closed.set()


class NullProgressWindow:
    def start(self, message: str) -> None:
        return None

    def update(self, message: str) -> None:
        return None

    def close(self) -> None:
        return None


def _append_helper_diagnostics(log_path: Path, event: str, **fields: Any) -> None:
    row = {
        "ts": iso_now(),
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        return


def _helper_diagnostics_path_for_plan(plan_path: Path) -> Path:
    try:
        raw = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return plan_path.parent / "desktop-updater-helper.diagnostics.jsonl"
    helper_path = Path(str(raw.get("helperDiagnosticsPath") or "")).expanduser()
    if str(helper_path).strip():
        return helper_path.resolve()
    updater_dir = Path(str(raw.get("updaterWorkingDir") or "")).expanduser()
    if str(updater_dir).strip():
        return updater_dir.resolve() / "desktop-updater-helper.diagnostics.jsonl"
    return plan_path.parent / "desktop-updater-helper.diagnostics.jsonl"


def _helper_failure_dialog_enabled(*, env: dict[str, str] | None = None) -> bool:
    env_map = env if env is not None else os.environ
    raw = str(env_map.get(DESKTOP_UPDATER_NO_DIALOG_ENV) or "").strip().lower()
    return raw not in {"1", "true", "yes", "on"}


def _helper_relaunch_verify_timeout_s(
    default: float = 90.0, *, env: dict[str, str] | None = None
) -> float:
    env_map = env if env is not None else os.environ
    raw = str(env_map.get(DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV) or "").strip()
    if not raw:
        return float(default)
    try:
        return max(1.0, float(raw))
    except ValueError:
        return float(default)


def _launch_executable(executable_path: Path, *, clear_app_version_override: bool = False) -> None:
    if not executable_path.is_file():
        raise RuntimeError(f"Desktop executable not found: {executable_path}")
    creationflags = (
        int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
    )
    env = None
    if clear_app_version_override:
        env = os.environ.copy()
        env.pop("BALUFFO_APP_VERSION_OVERRIDE", None)
    subprocess.Popen(  # noqa: S603
        [str(executable_path)],
        cwd=str(executable_path.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
        env=env,
    )


def _status_for_stage(
    paths: DesktopUpdatePaths,
    *,
    install_state: str,
    install_stage: str,
    **extra: Any,
) -> dict[str, Any]:
    status = load_status(paths)
    status.update(
        {
            "installState": str(install_state or "").strip().lower() or "idle",
            "installStage": str(install_stage or "").strip().lower() or "idle",
            "installStageLabel": install_stage_label(install_state, install_stage),
            "helperUpdatedAt": iso_now(),
        }
    )
    status.update(extra)
    return save_status(paths, status)


def _find_release_for_target_version(repo: str, target_version: str) -> dict[str, Any]:
    url = f"{resolve_github_api_base()}/repos/{repo}/releases?per_page=10"
    payload = fetch_json(url)
    if not isinstance(payload, list):
        raise RuntimeError("GitHub releases payload was not a list.")
    wanted = str(target_version or "").strip()
    wanted_tags = {wanted, f"v{wanted}"} if wanted else set()
    for release in payload:
        if not isinstance(release, dict):
            continue
        if bool(release.get("draft")) or bool(release.get("prerelease")):
            continue
        tag_name = str(release.get("tag_name") or "").strip()
        release_name = str(release.get("name") or "").strip()
        if wanted and tag_name not in wanted_tags and release_name != wanted:
            continue
        return release
    raise RuntimeError(f"Could not recover desktop manifest for version {wanted}.")


def _recover_manifest_for_install(
    plan: dict[str, Any],
    *,
    install_root: Path,
    ship_root: Path,
    paths: DesktopUpdatePaths,
) -> dict[str, Any]:
    cached_manifest = read_cached_manifest(paths)
    manifest = (
        cached_manifest.get("manifest") if isinstance(cached_manifest.get("manifest"), dict) else {}
    )
    if manifest:
        return manifest
    repo = resolve_release_repo(install_root=install_root, ship_root=ship_root)
    if not repo:
        raise RuntimeError(
            "Verified manifest cache is unavailable and desktop update repo is not configured."
        )
    release = _find_release_for_target_version(repo, str(plan.get("targetVersion") or ""))
    assets = release.get("assets") if isinstance(release.get("assets"), list) else []
    manifest_asset = next(
        (
            asset
            for asset in assets
            if isinstance(asset, dict)
            and str(asset.get("name") or "").strip() == DESKTOP_UPDATE_MANIFEST_ASSET
        ),
        None,
    )
    if manifest_asset is None:
        raise RuntimeError("Recovered release did not publish a desktop manifest asset.")
    manifest_url = str(manifest_asset.get("browser_download_url") or "").strip()
    if not manifest_url:
        raise RuntimeError("Recovered desktop manifest asset is missing its download URL.")
    manifest = fetch_json(manifest_url)
    if not isinstance(manifest, dict):
        raise RuntimeError("Recovered desktop manifest payload is invalid.")
    validate_desktop_manifest(manifest)
    verify_manifest_signature(
        manifest,
        public_keys=load_desktop_update_public_keys(
            candidate_paths=desktop_update_public_key_candidate_paths(ship_root),
        ),
    )
    if str(manifest.get("version") or "").strip() != str(plan.get("targetVersion") or "").strip():
        raise RuntimeError("Recovered desktop manifest does not match the install target version.")
    if str(manifest.get("key_id") or "").strip() != str(plan.get("manifestKeyId") or "").strip():
        raise RuntimeError("Recovered desktop manifest does not match the expected signing key.")
    artifact = (
        manifest.get("portable_artifact")
        if isinstance(manifest.get("portable_artifact"), dict)
        else {}
    )
    expected_hash = str(plan.get("expectedZipSha256") or "").strip().lower()
    manifest_hash = str(artifact.get("sha256") or "").strip().lower()
    if expected_hash and manifest_hash and manifest_hash != expected_hash:
        raise RuntimeError("Recovered desktop manifest does not match the expected ZIP checksum.")
    write_json_atomic(paths.manifest_cache_path, {"cachedAt": iso_now(), "manifest": manifest})
    return manifest


def _ensure_verified_zip_for_install(
    plan: dict[str, Any],
    *,
    manifest: dict[str, Any],
    zip_path: Path,
) -> Path:
    expected_hash = str(plan.get("expectedZipSha256") or "").strip().lower()
    artifact = (
        manifest.get("portable_artifact")
        if isinstance(manifest.get("portable_artifact"), dict)
        else {}
    )
    manifest_hash = str(artifact.get("sha256") or "").strip().lower()
    expected_hash = expected_hash or manifest_hash
    artifact_url = str(artifact.get("url") or "").strip()
    try:
        if (
            zip_path.is_file()
            and expected_hash
            and compute_sha256(zip_path).lower() == expected_hash
        ):
            return zip_path
    except OSError:
        pass
    if not artifact_url:
        raise RuntimeError("Recovered desktop manifest is missing its portable artifact URL.")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    download_file(artifact_url, zip_path)
    if expected_hash and compute_sha256(zip_path).lower() != expected_hash:
        raise RuntimeError("Downloaded desktop ZIP failed re-verification.")
    return zip_path


def _classify_install_failure(exc: Exception) -> str:
    message = str(exc).strip()
    lowered = message.lower()
    if "manifest" in lowered and (
        "cache" in lowered
        or "recover" in lowered
        or "release" in lowered
        or "signature" in lowered
        or "key" in lowered
    ):
        return f"desktop_update_manifest_recovery_failed: {message}"
    if "zip failed re-verification" in lowered:
        return f"desktop_update_zip_reverification_failed: {message}"
    if "startup readiness in time" in lowered:
        return f"desktop_update_relaunch_verification_failed: {message}"
    if ("zip" in lowered and "not found" in lowered) or "no such file or directory" in lowered:
        return f"desktop_update_zip_unavailable: {message}"
    if "access is denied" in lowered or "permission denied" in lowered:
        return f"desktop_update_zip_unavailable: {message}"
    return message or "desktop_install_failed"


def _show_message(title: str, message: str) -> None:
    if not _helper_failure_dialog_enabled():
        return
    if os.name == "nt":
        import ctypes

        ctypes.windll.user32.MessageBoxW(None, str(message or ""), str(title or "Baluffo"), 0)
        return
    print(f"{title}: {message}", file=sys.stderr)


def _wait_for_launcher_exit(plan: dict[str, Any], *, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + max(5.0, float(timeout_s))
    launcher_pid = int(plan.get("launcherPid") or 0)
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    while time.monotonic() < deadline:
        launcher_alive = pid_is_running(launcher_pid)
        if launcher_pid > 0 and launcher_alive:
            time.sleep(0.5)
            continue
        if session_state_path.exists():
            time.sleep(0.5)
            continue
        return
    raise RuntimeError("Timed out waiting for the desktop launcher to exit.")


def _copy_install_snapshot(install_root: Path, rollback_root: Path) -> None:
    snapshot_root = rollback_root / "runtime"
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    snapshot_root.mkdir(parents=True, exist_ok=True)
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root)
        rel_text = rel.as_posix()
        if rel_text.startswith("ship/data/"):
            continue
        target = snapshot_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _restore_install_snapshot(install_root: Path, rollback_root: Path) -> None:
    snapshot_root = rollback_root / "runtime"
    if not snapshot_root.is_dir():
        return
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        with contextlib.suppress(OSError):
            path.unlink()
    for path in snapshot_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(snapshot_root)
        target = install_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _sync_extract_to_install(install_root: Path, extracted_root: Path) -> None:
    target_paths = {
        path.relative_to(extracted_root).as_posix()
        for path in extracted_root.rglob("*")
        if path.is_file()
    }
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root).as_posix()
        if rel.startswith("ship/data/"):
            continue
        if rel not in target_paths:
            with contextlib.suppress(OSError):
                path.unlink()
    for path in extracted_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(extracted_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        target = install_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _verify_target_startup(plan: dict[str, Any], *, timeout_s: float = 90.0) -> None:
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    success_marker = DesktopUpdatePaths.from_data_dir(
        Path(str(plan.get("installRoot") or "")).expanduser().resolve() / "ship" / "data"
    ).success_marker_path
    target_version = str(plan.get("targetVersion") or "").strip()
    deadline = time.monotonic() + max(10.0, float(timeout_s))
    while time.monotonic() < deadline:
        if not session_state_path.exists():
            time.sleep(1.0)
            continue
        try:
            session_state = json.loads(session_state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            time.sleep(1.0)
            continue
        bridge_port = int(session_state.get("bridgePort") or 0)
        if bridge_port <= 0:
            time.sleep(1.0)
            continue
        try:
            health = fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
        except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
            # The relaunched bridge can briefly refuse connections before startup settles.
            time.sleep(1.0)
            continue
        if (
            isinstance(health, dict)
            and str(health.get("service") or "") == "baluffo-bridge"
            and bool(health.get("desktopMode"))
            and bool(health.get("startupReady"))
            and str(health.get("appVersion") or "").strip() == target_version
            and success_marker.exists()
        ):
            return
        time.sleep(1.0)
    raise RuntimeError("Updated desktop app did not report startup readiness in time.")


def _restore_data_backup_if_needed(ship_root: Path, status: dict[str, Any]) -> None:
    backup_ref_text = str(status.get("migrationBackupPath") or "").strip()
    if not backup_ref_text:
        return
    backup_ref = Path(backup_ref_text).expanduser().resolve()
    if not backup_ref.exists():
        return
    update_manager.restore_data_backup(
        update_manager.ShipPaths.from_root(ship_root),
        backup_ref,
    )


def _finalize_success(
    paths: DesktopUpdatePaths, plan: dict[str, Any], rollback_root: Path
) -> dict[str, Any]:
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
    clear_success_marker(paths)
    return _status_for_stage(
        paths,
        install_state="installed",
        install_stage="installed",
        downloadState="idle",
        downloadedBytes=0,
        totalBytes=0,
        downloadPercent=0,
        lastError="",
        lastCheckedAt=iso_now(),
        migrationBackupPath="",
        rollbackPath="",
        targetVersion=str(plan.get("targetVersion") or ""),
    )


def _recover_interrupted_install(
    plan: dict[str, Any],
    *,
    install_root: Path,
    ship_root: Path,
    paths: DesktopUpdatePaths,
    rollback_root: Path,
) -> bool:
    status = load_status(paths)
    stage = str(status.get("installStage") or "").strip().lower()
    if not stage or stage in {
        "idle",
        "preparing",
        "waiting_for_exit",
        "extracting",
        "snapshotting",
        "backup",
    }:
        return False
    if stage in SUCCESS_RECOVERY_STAGES:
        try:
            _verify_target_startup(plan, timeout_s=5.0)
        except Exception:  # noqa: BLE001
            pass
        else:
            _finalize_success(paths, plan, rollback_root)
            return True
    if stage not in MUTATING_INSTALL_STAGES:
        return False
    _status_for_stage(
        paths,
        install_state="installing",
        install_stage="recovering",
        lastError="",
    )
    _restore_data_backup_if_needed(ship_root, status)
    _restore_install_snapshot(install_root, rollback_root)
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
    _status_for_stage(
        paths,
        install_state="idle",
        install_stage="idle",
        lastError="",
        migrationBackupPath="",
        rollbackPath="",
    )
    return False


def run_install(
    plan_path: Path, progress: HelperProgressWindow | NullProgressWindow | None = None
) -> dict[str, Any]:
    plan = validate_install_plan(json.loads(plan_path.read_text(encoding="utf-8")))
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    ship_root = install_root / "ship"
    data_dir = ship_root / "data"
    paths = DesktopUpdatePaths.from_data_dir(data_dir)
    rollback_root = Path(str(plan.get("rollbackPath") or "")).expanduser().resolve()
    existing_status = load_status(paths)
    progress = progress if progress is not None else NullProgressWindow()
    progress.start(
        str(existing_status.get("installStageLabel") or "").strip()
        or install_stage_label("handoff_requested", "preparing")
    )
    staging_root = paths.updater_dir / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    temp_extract = (staging_root / f"baluffo-desktop-update-{uuid.uuid4().hex}").resolve()
    if temp_extract.exists():
        shutil.rmtree(temp_extract)
    temp_extract.mkdir(parents=True, exist_ok=True)
    backup_ref: Path | None = None
    try:
        manifest = _recover_manifest_for_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
        )
        zip_path = _ensure_verified_zip_for_install(
            plan,
            manifest=manifest,
            zip_path=Path(str(plan.get("downloadedZipPath") or "")).expanduser().resolve(),
        )
        recovered_as_complete = _recover_interrupted_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
            rollback_root=rollback_root,
        )
        if recovered_as_complete:
            return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
        _status_for_stage(
            paths,
            install_state="handoff_requested",
            install_stage="preparing",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        progress.update(install_stage_label("waiting_for_exit", "waiting_for_exit"))
        _status_for_stage(
            paths,
            install_state="waiting_for_exit",
            install_stage="waiting_for_exit",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        _wait_for_launcher_exit(plan)
        clear_handoff_request(paths)

        progress.update(install_stage_label("installing", "extracting"))
        _status_for_stage(
            paths,
            install_state="installing",
            install_stage="extracting",
            rollbackPath=str(rollback_root),
        )
        with zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(temp_extract)
        clear_success_marker(paths)
        rollback_root.mkdir(parents=True, exist_ok=True)
        _status_for_stage(
            paths,
            install_state="installing",
            install_stage="snapshotting",
            rollbackPath=str(rollback_root),
        )
        _copy_install_snapshot(install_root, rollback_root)
        if list(manifest.get("migration_plan") or []):
            _status_for_stage(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
            )
            backup_ref = update_manager.create_data_backup(
                update_manager.ShipPaths.from_root(ship_root)
            )
            _status_for_stage(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
        _status_for_stage(
            paths,
            install_state="installing",
            install_stage="replacing",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        _sync_extract_to_install(install_root, temp_extract)
        if list(manifest.get("migration_plan") or []):
            _status_for_stage(
                paths,
                install_state="installing",
                install_stage="migrating",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
            update_manager.run_migrations(
                update_manager.ShipPaths.from_root(ship_root),
                manifest.get("migration_plan") or [],
                backup_ref,
            )
        progress.update(install_stage_label("verifying", "relaunching"))
        _status_for_stage(
            paths,
            install_state="verifying",
            install_stage="relaunching",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        _launch_executable(install_root / "Baluffo.exe", clear_app_version_override=True)
        _status_for_stage(
            paths,
            install_state="verifying",
            install_stage="verifying",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        _verify_target_startup(plan, timeout_s=_helper_relaunch_verify_timeout_s())
        _finalize_success(paths, plan, rollback_root)
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except Exception as exc:
        clear_handoff_request(paths)
        progress.update(install_stage_label("installing", "rolling_back"))
        if backup_ref is not None:
            with contextlib.suppress(Exception):
                update_manager.restore_data_backup(
                    update_manager.ShipPaths.from_root(ship_root),
                    backup_ref,
                )
        current_status = _status_for_stage(
            paths,
            install_state="failed",
            install_stage="rolling_back",
            lastError=_classify_install_failure(exc),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        with contextlib.suppress(Exception):
            _restore_install_snapshot(install_root, rollback_root)
        with contextlib.suppress(Exception):
            _launch_executable(install_root / "Baluffo.exe")
        _status_for_stage(
            paths,
            install_state="failed",
            install_stage="failed",
            lastError=str(current_status.get("lastError") or "desktop_install_failed"),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        raise
    finally:
        clear_handoff_request(paths)
        progress.close()
        with contextlib.suppress(OSError):
            shutil.rmtree(temp_extract)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Baluffo desktop updater helper.")
    parser.add_argument("--install-plan", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    plan_path = Path(args.install_plan).expanduser().resolve()
    diagnostics_path = _helper_diagnostics_path_for_plan(plan_path)
    _append_helper_diagnostics(
        diagnostics_path,
        "helper_main_started",
        pid=os.getpid(),
        planPath=str(plan_path),
    )
    progress = HelperProgressWindow()
    result_holder: dict[str, Any] = {}
    error_holder: dict[str, Any] = {}

    def worker() -> None:
        _append_helper_diagnostics(diagnostics_path, "helper_worker_started", pid=os.getpid())
        try:
            result_holder["result"] = run_install(plan_path, progress=progress)
            result = (
                result_holder.get("result") if isinstance(result_holder.get("result"), dict) else {}
            )
            _append_helper_diagnostics(
                diagnostics_path,
                "helper_worker_succeeded",
                installedVersion=str(result.get("installedVersion") or ""),
            )
        except Exception as exc:  # noqa: BLE001
            error_holder["error"] = str(exc)
            error_holder["traceback"] = traceback.format_exc()
            _append_helper_diagnostics(
                diagnostics_path,
                "helper_worker_failed",
                error=str(exc),
                traceback=str(error_holder.get("traceback") or ""),
            )
            progress.close()

    thread = threading.Thread(target=worker, daemon=True, name="baluffo-updater-install")
    try:
        thread.start()
        _append_helper_diagnostics(diagnostics_path, "helper_progress_loop_started")
        progress.run("Preparing update")
        thread.join()
        if error_holder:
            raise RuntimeError(str(error_holder.get("error") or "Baluffo desktop update failed."))
        result = (
            result_holder.get("result") if isinstance(result_holder.get("result"), dict) else {}
        )
        _append_helper_diagnostics(
            diagnostics_path,
            "helper_main_succeeded",
            installedVersion=str(result.get("installedVersion") or ""),
        )
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001
        _append_helper_diagnostics(
            diagnostics_path,
            "helper_main_failed",
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        _show_message("Baluffo Update Failed", str(exc))
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 1
    finally:
        _append_helper_diagnostics(diagnostics_path, "helper_main_finished")


if __name__ == "__main__":
    raise SystemExit(main())
