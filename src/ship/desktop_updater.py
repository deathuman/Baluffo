#!/usr/bin/env python3
"""Thin helper executable root for portable desktop in-app updates."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ship import desktop_update as desktop_update_mod
from src.ship import desktop_update_shared as desktop_update_shared_mod
from src.ship import desktop_update_state as desktop_update_state_mod
from src.ship import desktop_updater_install as desktop_updater_install_mod
from src.ship import desktop_updater_release as desktop_updater_release_mod
from src.ship import desktop_updater_ui as desktop_updater_ui_mod

DESKTOP_UPDATER_NO_DIALOG_ENV = "BALUFFO_DESKTOP_UPDATER_NO_DIALOG"
DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV = "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S"

desktop_update_shared_mod.root = desktop_update_mod
desktop_update_state_mod.root = desktop_update_mod
desktop_updater_ui_mod.root = sys.modules[__name__]
desktop_updater_release_mod.root = sys.modules[__name__]
desktop_updater_install_mod.root = sys.modules[__name__]

HelperProgressWindow = desktop_updater_ui_mod.HelperProgressWindow
NullProgressWindow = desktop_updater_ui_mod.NullProgressWindow
subprocess = desktop_updater_ui_mod.subprocess
_append_helper_diagnostics = desktop_updater_ui_mod._append_helper_diagnostics
_drain_helper_queue = desktop_updater_ui_mod._drain_helper_queue
_helper_diagnostics_path_for_plan = desktop_updater_ui_mod._helper_diagnostics_path_for_plan
_helper_failure_dialog_enabled = desktop_updater_ui_mod._helper_failure_dialog_enabled
_helper_relaunch_verify_timeout_s = desktop_updater_ui_mod._helper_relaunch_verify_timeout_s
_helper_window_layout = desktop_updater_ui_mod._helper_window_layout
_launch_executable = desktop_updater_ui_mod._launch_executable
_normalize_helper_message = desktop_updater_ui_mod._normalize_helper_message
_show_message = desktop_updater_ui_mod._show_message

_classify_install_failure = desktop_updater_release_mod._classify_install_failure
_ensure_verified_zip_for_install = desktop_updater_release_mod._ensure_verified_zip_for_install
_find_release_for_target_version = desktop_updater_release_mod._find_release_for_target_version
_recover_manifest_for_install = desktop_updater_release_mod._recover_manifest_for_install
DESKTOP_UPDATE_MANIFEST_ASSET = desktop_updater_release_mod.DESKTOP_UPDATE_MANIFEST_ASSET
compute_sha256 = desktop_updater_release_mod.compute_sha256
desktop_update_public_key_candidate_paths = (
    desktop_updater_release_mod.desktop_update_public_key_candidate_paths
)
download_file = desktop_updater_release_mod.download_file
fetch_json = desktop_updater_release_mod.fetch_json
iso_now = desktop_updater_release_mod.iso_now
load_desktop_update_public_keys = desktop_updater_release_mod.load_desktop_update_public_keys
read_cached_manifest = desktop_updater_release_mod.read_cached_manifest
resolve_github_api_base = desktop_updater_release_mod.resolve_github_api_base
resolve_release_repo = desktop_updater_release_mod.resolve_release_repo
validate_desktop_manifest = desktop_updater_release_mod.validate_desktop_manifest
verify_manifest_signature = desktop_updater_release_mod.verify_manifest_signature
write_json_atomic = desktop_updater_release_mod.write_json_atomic

_copy_install_snapshot = desktop_updater_install_mod._copy_install_snapshot
_finalize_success = desktop_updater_install_mod._finalize_success
_recover_interrupted_install = desktop_updater_install_mod._recover_interrupted_install
_restore_data_backup_if_needed = desktop_updater_install_mod._restore_data_backup_if_needed
_restore_install_snapshot = desktop_updater_install_mod._restore_install_snapshot
_status_for_stage = desktop_updater_install_mod._status_for_stage
_sync_extract_to_install = desktop_updater_install_mod._sync_extract_to_install
_verify_target_startup = desktop_updater_install_mod._verify_target_startup
_wait_for_launcher_exit = desktop_updater_install_mod._wait_for_launcher_exit
DesktopUpdatePaths = desktop_updater_install_mod.DesktopUpdatePaths
clear_handoff_request = desktop_updater_install_mod.clear_handoff_request
clear_success_marker = desktop_updater_install_mod.clear_success_marker
install_stage_label = desktop_updater_install_mod.install_stage_label
load_status = desktop_updater_install_mod.load_status
pid_is_running = desktop_updater_install_mod.pid_is_running
save_status = desktop_updater_install_mod.save_status
validate_install_plan = desktop_updater_install_mod.validate_install_plan
time = desktop_updater_install_mod.time
update_manager = desktop_updater_install_mod.update_manager
zipfile = desktop_updater_install_mod.zipfile
run_install = desktop_updater_install_mod.run_install


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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
            result = _as_dict(result_holder.get("result"))
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
        result = _as_dict(result_holder.get("result"))
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
