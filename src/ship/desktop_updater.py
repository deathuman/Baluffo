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
import uuid
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ship import update_manager
from src.ship.desktop_update import (
    DesktopUpdatePaths,
    clear_handoff_request,
    clear_success_marker,
    compute_sha256,
    desktop_update_public_key_candidate_paths,
    fetch_json,
    install_stage_label,
    iso_now,
    load_desktop_update_public_keys,
    load_status,
    pid_is_running,
    read_cached_manifest,
    save_status,
    validate_desktop_manifest,
    validate_install_plan,
    verify_manifest_signature,
)

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


class HelperProgressWindow:
    """Best-effort native progress window for the one-shot updater helper."""

    def __init__(self) -> None:
        self._queue: queue.SimpleQueue[tuple[str, str]] = queue.SimpleQueue()
        self._closed = threading.Event()

    def start(self, message: str) -> None:
        self.update(str(message or "").strip() or "Preparing update")

    def update(self, message: str) -> None:
        self._queue.put(("message", str(message or "").strip()))

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

        root = tk.Tk()
        root.title("Baluffo Update")
        root.resizable(False, False)
        root.attributes("-topmost", True)
        root.protocol("WM_DELETE_WINDOW", lambda: None)
        frame = ttk.Frame(root, padding=18)
        frame.pack(fill="both", expand=True)
        title = ttk.Label(frame, text="Installing update", font=("", 11, "bold"))
        title.pack(anchor="w")
        message_var = tk.StringVar(value=str(initial_message or "").strip() or "Preparing update")
        detail = ttk.Label(frame, textvariable=message_var, padding=(0, 10, 0, 0))
        detail.pack(anchor="w")
        bar = ttk.Progressbar(frame, mode="indeterminate", length=260)
        bar.pack(fill="x", expand=True, pady=(14, 0))
        bar.start(12)
        root.update_idletasks()
        width = root.winfo_width() or 320
        height = root.winfo_height() or 110
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        offset_x = max(0, int((screen_width - width) / 2))
        offset_y = max(0, int((screen_height - height) / 3))
        root.geometry(f"{width}x{height}+{offset_x}+{offset_y}")

        def drain() -> None:
            while True:
                try:
                    kind, payload = self._queue.get_nowait()
                except queue.Empty:
                    break
                if kind == "close":
                    with contextlib.suppress(Exception):
                        bar.stop()
                    self._closed.set()
                    root.destroy()
                    return
                if kind == "message" and payload:
                    message_var.set(payload)
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


def _launch_executable(executable_path: Path, *, clear_app_version_override: bool = False) -> None:
    if not executable_path.is_file():
        raise RuntimeError(f"Desktop executable not found: {executable_path}")
    creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
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


def _show_message(title: str, message: str) -> None:
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
        health = fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
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


def _finalize_success(paths: DesktopUpdatePaths, plan: dict[str, Any], rollback_root: Path) -> dict[str, Any]:
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
    if not stage or stage in {"idle", "preparing", "waiting_for_exit", "extracting", "snapshotting", "backup"}:
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


def run_install(plan_path: Path, progress: HelperProgressWindow | NullProgressWindow | None = None) -> dict[str, Any]:
    plan = validate_install_plan(json.loads(plan_path.read_text(encoding="utf-8")))
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    ship_root = install_root / "ship"
    data_dir = ship_root / "data"
    paths = DesktopUpdatePaths.from_data_dir(data_dir)
    rollback_root = Path(str(plan.get("rollbackPath") or "")).expanduser().resolve()
    manifest_cache = read_cached_manifest(paths)
    manifest = manifest_cache.get("manifest") if isinstance(manifest_cache.get("manifest"), dict) else {}
    if not manifest:
        raise RuntimeError("Verified manifest cache is unavailable for desktop install.")
    validate_desktop_manifest(manifest)
    verify_manifest_signature(
        manifest,
        public_keys=load_desktop_update_public_keys(
            candidate_paths=desktop_update_public_key_candidate_paths(ship_root),
        ),
    )
    zip_path = Path(str(plan.get("downloadedZipPath") or "")).expanduser().resolve()
    expected_hash = str(plan.get("expectedZipSha256") or "").strip().lower()
    if expected_hash and compute_sha256(zip_path).lower() != expected_hash:
        raise RuntimeError("Downloaded desktop ZIP failed re-verification.")
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
            backup_ref = update_manager.create_data_backup(update_manager.ShipPaths.from_root(ship_root))
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
        _verify_target_startup(plan)
        _finalize_success(paths, plan, rollback_root)
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except Exception:
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
            lastError="desktop_install_failed",
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
        result = result_holder.get("result") if isinstance(result_holder.get("result"), dict) else {}
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
