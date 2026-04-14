#!/usr/bin/env python3
"""Helper executable for portable desktop in-app updates."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ship import update_manager
from src.ship.desktop_update import (
    DesktopUpdatePaths,
    clear_success_marker,
    compute_sha256,
    desktop_update_public_key_candidate_paths,
    fetch_json,
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


def run_install(plan_path: Path) -> dict[str, Any]:
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

    save_status(paths, {**load_status(paths), "installState": "waiting_for_exit", "lastError": ""})
    _wait_for_launcher_exit(plan)

    temp_extract = Path(tempfile.mkdtemp(prefix="baluffo-desktop-update-"))
    backup_ref: Path | None = None
    try:
        save_status(paths, {**load_status(paths), "installState": "installing"})
        with zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(temp_extract)
        clear_success_marker(paths)
        rollback_root.mkdir(parents=True, exist_ok=True)
        _copy_install_snapshot(install_root, rollback_root)
        if list(manifest.get("migration_plan") or []):
            backup_ref = update_manager.create_data_backup(update_manager.ShipPaths.from_root(ship_root))
        _sync_extract_to_install(install_root, temp_extract)
        if list(manifest.get("migration_plan") or []):
            update_manager.run_migrations(
                update_manager.ShipPaths.from_root(ship_root),
                manifest.get("migration_plan") or [],
                backup_ref,
            )
        save_status(paths, {**load_status(paths), "installState": "verifying"})
        creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
        subprocess.Popen(  # noqa: S603
            [str(install_root / "Baluffo.exe")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        _verify_target_startup(plan)
        with contextlib.suppress(OSError):
            shutil.rmtree(rollback_root)
        clear_success_marker(paths)
        save_status(
            paths,
            {
                **load_status(paths),
                "installState": "installed",
                "downloadState": "idle",
                "downloadedBytes": 0,
                "totalBytes": 0,
                "downloadPercent": 0,
                "lastError": "",
                "lastCheckedAt": iso_now(),
            },
        )
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except Exception:
        if backup_ref is not None:
            with contextlib.suppress(Exception):
                update_manager.restore_data_backup(
                    update_manager.ShipPaths.from_root(ship_root),
                    backup_ref,
                )
        with contextlib.suppress(Exception):
            _restore_install_snapshot(install_root, rollback_root)
        save_status(
            paths,
            {
                **load_status(paths),
                "installState": "failed",
                "lastError": "desktop_install_failed",
            },
        )
        raise
    finally:
        with contextlib.suppress(OSError):
            shutil.rmtree(temp_extract)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Baluffo desktop updater helper.")
    parser.add_argument("--install-plan", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_install(Path(args.install_plan).expanduser().resolve())
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001
        _show_message("Baluffo Update Failed", str(exc))
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
