#!/usr/bin/env python3
"""Release-gating smoke runner for the packaged Baluffo desktop executable.

AI boundary: this file owns packaged-smoke CLI/root patch compatibility only.
AI boundary implement in: `src.ship.packaged_smoke.*` and updater/runtime leaves.
AI boundary search before contracts: packaged frontend smoke scripts.
AI boundary verify: matching packaged lane plus `npm run test:refactor:changed`.
"""

from __future__ import annotations

import argparse
import ctypes as _ctypes
import errno as _errno
import os as _os
import shutil as _shutil
import subprocess as _subprocess
import sys
import time as _time
from datetime import UTC as _UTC
from datetime import datetime as _datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_portable_exe as _portable_build_mod
from src import source_sync as _source_sync_mod
from src.local_data_store import LocalDataPaths as _LocalDataPaths
from src.local_data_store import LocalDataStore as _LocalDataStore
from src.python_version_guard import ensure_required_python
from src.shared.process_memory import ProcessMemorySampler as _ProcessMemorySampler
from src.shared.utils import utc_now_iso as _utc_now_iso
from src.ship import desktop_app as _desktop_app_mod
from src.ship import desktop_update_shared as _desktop_update_shared_mod
from src.ship.packaged_smoke import build_env as packaged_smoke_build_env_mod
from src.ship.packaged_smoke import common as packaged_smoke_common_mod
from src.ship.packaged_smoke import orchestrator as packaged_smoke_orchestrator_mod
from src.ship.packaged_smoke import rehearsal_browser as packaged_smoke_rehearsal_browser_mod
from src.ship.packaged_smoke import rehearsal_sync as packaged_smoke_rehearsal_sync_mod
from src.ship.packaged_smoke import rehearsal_update as packaged_smoke_rehearsal_update_mod
from src.ship.packaged_smoke import rehearsals as packaged_smoke_rehearsals_mod
from src.ship.packaged_smoke import runtime as packaged_smoke_runtime_mod
from src.ship.packaged_smoke import startup_metrics as packaged_smoke_startup_metrics_mod
from src.ship.packaged_smoke import update_manifest_helpers as update_manifest_helpers_mod
from src.ship.startup_probe_policy import EMBEDDED_PAGE_PROBES as _EMBEDDED_PAGE_PROBES
from src.ship.startup_probe_policy import (
    REQUIRED_STARTUP_PROBE_LAUNCH_MODE as _REQUIRED_STARTUP_PROBE_LAUNCH_MODE,
)
from src.ship.startup_probe_policy import STARTUP_REQUIRED_EVENTS as _STARTUP_REQUIRED_EVENTS
from src.ship.startup_probe_policy import (
    classify_startup_probe_failure as _classify_startup_probe_failure,
)
from src.ship.startup_probe_policy import (
    refine_startup_probe_summary as _refine_startup_probe_summary,
)
from src.ship.startup_probe_policy import (
    required_startup_event_present as _required_startup_event_present_policy,
)
from src.ship.startup_probe_policy import (
    select_startup_probe_browser as _select_startup_probe_browser_policy,
)
from src.ship.startup_probe_policy import startup_metric_fields as _startup_metric_fields
from src.ship.startup_probe_policy import (
    startup_probe_browser_details as _startup_probe_browser_details,
)
from src.ship.startup_probe_policy import (
    startup_profile_required_events as _startup_profile_required_events,
)
from src.ship.startup_profile import render_startup_summary
from src.ship.startup_profile import (
    summarize_startup_metrics as _summarize_startup_metrics,
)
from src.ship.startup_profile import (
    write_startup_summary as _write_startup_summary,
)
from src.ship.startup_telemetry import read_startup_metrics as _read_startup_metrics_file

DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_ELF_PATH = ROOT / "dist" / "baluffo-portable" / "baluffo" / "baluffo"
DEFAULT_APPIMAGE_PATH = (
    ROOT / "dist" / f"Baluffo-{_portable_build_mod.DEFAULT_BUNDLE_VERSION}-x86_64.AppImage"
)
DEFAULT_BUNDLE_VERSION = _portable_build_mod.DEFAULT_BUNDLE_VERSION
PORTABLE_BUILD_VERSION_ENV = _portable_build_mod.PORTABLE_BUILD_VERSION_ENV
DEFAULT_REPORT_PATH = ROOT / "data" / "packaged-desktop-smoke-report.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke"
DEFAULT_ARTIFACT_RETENTION_RUNS = 2
DEFAULT_ARTIFACT_FILE_RETENTION_S = 24 * 60 * 60
DEFAULT_RUNTIME_TIMEOUT_S = 35.0
DEFAULT_SMOKE_RUNNER_TIMEOUT_S = 180.0
SMOKE_DIR = ROOT / "tests" / "frontend"
DEFAULT_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.mjs"
JOBS_PIPELINE_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.jobs-pipeline.mjs"
FIRST_RUN_JOBS_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.first-run-jobs.mjs"
FETCH_EVIDENCE_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.fetch-evidence.mjs"
ACTIVE_TASK_CLOSE_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.active-task-close.mjs"
TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT = SMOKE_DIR / "packaged-desktop-smoke.task-abort-schedule.mjs"
_PORTABLE_EXE_FRESHNESS_MARKERS = (
    ROOT / "scripts" / "build_portable_exe.py",
    ROOT / "scripts" / "build_ship_bundle.py",
    ROOT / "src" / "ship" / "runtime_launcher.py",
    ROOT / "src" / "ship" / "update_manager.py",
    ROOT / "src" / "ship" / "desktop_app" / "__init__.py",
    ROOT / "src" / "admin_bridge.py",
    ROOT / "src" / "bridge" / "task_launch_api.py",
    ROOT / "index.html",
    ROOT / "jobs.html",
    ROOT / "saved.html",
    ROOT / "admin.html",
    ROOT / "theme.js",
    ROOT / "frontend-runtime-config.js",
    ROOT / "baluffo.config.json",
)
_PORTABLE_EXE_FRESHNESS_DIRS = (ROOT / "frontend", ROOT / "probes", ROOT / "styles")
DESKTOP_SESSION_STATE_FILE = "desktop-session.json"
DESKTOP_INSTANCE_LOCK_FILE = "desktop-instance.lock"
DESKTOP_BROWSER_PROFILE_DIR = "desktop-browser-profile"
PORTABLE_BUILD_SCRATCH_NAMES = (
    ".pyinstaller-dist",
    ".pyinstaller-work",
    ".pyinstaller-spec",
    ".pyinstaller-helper-dist",
    ".pyinstaller-helper-work",
    ".pyinstaller-helper-spec",
)

packaged_smoke_build_env_mod.root = sys.modules[__name__]
packaged_smoke_runtime_mod.root = sys.modules[__name__]
packaged_smoke_startup_metrics_mod.root = sys.modules[__name__]
packaged_smoke_orchestrator_mod.root = sys.modules[__name__]
packaged_smoke_rehearsals_mod.root = sys.modules[__name__]
packaged_smoke_rehearsal_sync_mod.root = sys.modules[__name__]
packaged_smoke_rehearsal_update_mod.root = sys.modules[__name__]
packaged_smoke_rehearsal_browser_mod.root = sys.modules[__name__]
ctypes = _ctypes
errno = _errno
os = _os
shutil = _shutil
subprocess = _subprocess
time = _time
UTC = _UTC
datetime = _datetime
source_sync_mod = _source_sync_mod
LocalDataPaths = _LocalDataPaths
LocalDataStore = _LocalDataStore
ProcessMemorySampler = _ProcessMemorySampler
utc_now_iso = _utc_now_iso
desktop_app_mod = _desktop_app_mod
desktop_update_mod = _desktop_update_shared_mod
desktop_update_mod.PUBLIC_KEYS_FILE = update_manifest_helpers_mod.PUBLIC_KEYS_FILE
desktop_update_mod.get_app_version = update_manifest_helpers_mod.get_app_version
DESKTOP_UPDATE_MANIFEST_ASSET = update_manifest_helpers_mod.DESKTOP_UPDATE_MANIFEST_ASSET
DESKTOP_UPDATE_SCHEMA_VERSION = update_manifest_helpers_mod.DESKTOP_UPDATE_SCHEMA_VERSION
DESKTOP_UPDATER_VERSION = update_manifest_helpers_mod.DESKTOP_UPDATER_VERSION
Ed25519SigningClass = update_manifest_helpers_mod.Ed25519SigningClass
compute_sha256 = update_manifest_helpers_mod.compute_sha256
get_app_version = update_manifest_helpers_mod.get_app_version
sign_manifest = update_manifest_helpers_mod.sign_manifest
EMBEDDED_PAGE_PROBES = _EMBEDDED_PAGE_PROBES
STARTUP_REQUIRED_EVENTS = _STARTUP_REQUIRED_EVENTS
classify_startup_probe_failure = _classify_startup_probe_failure
refine_startup_probe_summary = _refine_startup_probe_summary
startup_metric_fields = _startup_metric_fields
startup_probe_browser_details = _startup_probe_browser_details
startup_profile_required_events = _startup_profile_required_events
REQUIRED_STARTUP_PROBE_LAUNCH_MODE = _REQUIRED_STARTUP_PROBE_LAUNCH_MODE
_required_startup_event_present = _required_startup_event_present_policy
select_startup_probe_browser_policy = _select_startup_probe_browser_policy
summarize_startup_metrics = _summarize_startup_metrics
write_startup_summary = _write_startup_summary
read_startup_metrics_file = _read_startup_metrics_file
slugify_token = packaged_smoke_common_mod.slugify_token
write_json = packaged_smoke_common_mod.write_json
remove_tree_or_file = packaged_smoke_common_mod.remove_tree_or_file
generate_packaged_smoke_run_token = packaged_smoke_common_mod.generate_packaged_smoke_run_token
fetch_json = packaged_smoke_common_mod.fetch_json
fetch_text = packaged_smoke_common_mod.fetch_text
request_json = packaged_smoke_common_mod.request_json
post_json = packaged_smoke_common_mod.post_json
fetch_startup_metrics = packaged_smoke_startup_metrics_mod.fetch_startup_metrics
startup_metric_launch_mode = packaged_smoke_startup_metrics_mod.startup_metric_launch_mode
startup_metric_event_present = packaged_smoke_startup_metrics_mod.startup_metric_event_present
find_startup_metric_fields = packaged_smoke_startup_metrics_mod.find_startup_metric_fields

choose_free_port = packaged_smoke_build_env_mod.choose_free_port
_default_portable_exe_stale = packaged_smoke_build_env_mod._default_portable_exe_stale
_exe_path_uses_default_dist = packaged_smoke_build_env_mod._exe_path_uses_default_dist
_portable_exe_marker_staleness = packaged_smoke_build_env_mod._portable_exe_marker_staleness
_iter_portable_exe_freshness_markers = (
    packaged_smoke_build_env_mod._iter_portable_exe_freshness_markers
)
run_portable_build = packaged_smoke_build_env_mod.run_portable_build
cleanup_portable_build_scratch = packaged_smoke_build_env_mod.cleanup_portable_build_scratch
select_startup_probe_browser = packaged_smoke_build_env_mod.select_startup_probe_browser
resolve_playwright_chromium_executable = (
    packaged_smoke_build_env_mod.resolve_playwright_chromium_executable
)
preferred_packaged_desktop_browser_env = (
    packaged_smoke_build_env_mod.preferred_packaged_desktop_browser_env
)
prune_packaged_smoke_artifacts = packaged_smoke_build_env_mod.prune_packaged_smoke_artifacts
resolve_node_command = packaged_smoke_build_env_mod.resolve_node_command
write_text = packaged_smoke_build_env_mod.write_text
packaged_desktop_local_appdata_root = (
    packaged_smoke_build_env_mod.packaged_desktop_local_appdata_root
)
packaged_desktop_roaming_appdata_root = (
    packaged_smoke_build_env_mod.packaged_desktop_roaming_appdata_root
)
packaged_desktop_session_paths = packaged_smoke_build_env_mod.packaged_desktop_session_paths
clear_packaged_desktop_session_state = (
    packaged_smoke_build_env_mod.clear_packaged_desktop_session_state
)
is_windows_process_elevated = packaged_smoke_build_env_mod.is_windows_process_elevated
path_is_writable = packaged_smoke_build_env_mod.path_is_writable
classify_subprocess_error = packaged_smoke_build_env_mod.classify_subprocess_error
collect_packaged_smoke_env_diagnostics = (
    packaged_smoke_build_env_mod.collect_packaged_smoke_env_diagnostics
)
build_packaged_smoke_env = packaged_smoke_build_env_mod.build_packaged_smoke_env
packaged_pipeline_smoke_mode = packaged_smoke_build_env_mod.packaged_pipeline_smoke_mode
packaged_fetch_evidence_smoke_mode = packaged_smoke_build_env_mod.packaged_fetch_evidence_smoke_mode
packaged_bootstrap_smoke_mode = packaged_smoke_build_env_mod.packaged_bootstrap_smoke_mode
packaged_runtime_env_overrides = packaged_smoke_build_env_mod.packaged_runtime_env_overrides
ensure_portable_exe = packaged_smoke_build_env_mod.ensure_portable_exe
expected_portable_build_version = packaged_smoke_build_env_mod.expected_portable_build_version
portable_build_status = _portable_build_mod.portable_build_status
read_portable_build_provenance = _portable_build_mod.read_portable_build_provenance

launch_packaged_exe = packaged_smoke_runtime_mod.launch_packaged_exe
launch_packaged_command = packaged_smoke_runtime_mod.launch_packaged_command
launch_packaged_desktop_child = packaged_smoke_runtime_mod.launch_packaged_desktop_child
_local_address_matches_listen_port = packaged_smoke_runtime_mod._local_address_matches_listen_port
pids_listening_on_tcp_port_windows = packaged_smoke_runtime_mod.pids_listening_on_tcp_port_windows
cleanup_orphaned_desktop_ports_nt = packaged_smoke_runtime_mod.cleanup_orphaned_desktop_ports_nt
terminate_process_tree = packaged_smoke_runtime_mod.terminate_process_tree
terminate_process_only = packaged_smoke_runtime_mod.terminate_process_only
_packaged_runtime_page_ready = packaged_smoke_runtime_mod._packaged_runtime_page_ready
wait_for_packaged_runtime = packaged_smoke_runtime_mod.wait_for_packaged_runtime
wait_for_packaged_runtime_with_port_pivot = (
    packaged_smoke_runtime_mod.wait_for_packaged_runtime_with_port_pivot
)
wait_for_packaged_child_runtime = packaged_smoke_runtime_mod.wait_for_packaged_child_runtime
capture_runtime_snapshot = packaged_smoke_runtime_mod.capture_runtime_snapshot
capture_performance_profile_snapshot = (
    packaged_smoke_runtime_mod.capture_performance_profile_snapshot
)
wait_for_runtime_events = packaged_smoke_runtime_mod.wait_for_runtime_events
run_embedded_runtime_probe = packaged_smoke_runtime_mod.run_embedded_runtime_probe
parse_packaged_node_smoke_report = packaged_smoke_runtime_mod.parse_packaged_node_smoke_report
read_packaged_node_smoke_payload = packaged_smoke_runtime_mod.read_packaged_node_smoke_payload
run_packaged_node_smoke = packaged_smoke_runtime_mod.run_packaged_node_smoke
build_failure_payload = packaged_smoke_runtime_mod.build_failure_payload
run_warmup_launch = packaged_smoke_runtime_mod.run_warmup_launch
_seed_jobs_pipeline_smoke_feed = packaged_smoke_orchestrator_mod._seed_jobs_pipeline_smoke_feed

_archive_portable_dir = packaged_smoke_rehearsals_mod._archive_portable_dir
_inject_desktop_update_public_keys = (
    packaged_smoke_rehearsals_mod._inject_desktop_update_public_keys
)
_portable_current_version = packaged_smoke_rehearsals_mod._portable_current_version
_portable_packaged_sync_config_path = (
    packaged_smoke_rehearsals_mod._portable_packaged_sync_config_path
)
_load_portable_packaged_sync_rehearsal_config = (
    packaged_smoke_rehearsals_mod._load_portable_packaged_sync_rehearsal_config
)
_seed_rehearsal_local_data = packaged_smoke_rehearsals_mod._seed_rehearsal_local_data
_PackagedSyncRehearsalHandler = packaged_smoke_rehearsals_mod._PackagedSyncRehearsalHandler
_start_packaged_sync_rehearsal_server = (
    packaged_smoke_rehearsals_mod._start_packaged_sync_rehearsal_server
)
_DesktopUpdateReleaseHandler = packaged_smoke_rehearsals_mod._DesktopUpdateReleaseHandler
_start_desktop_update_release_server = (
    packaged_smoke_rehearsals_mod._start_desktop_update_release_server
)
_wait_for_process_exit = packaged_smoke_rehearsals_mod._wait_for_process_exit
_wait_for_install_handoff_confirmation = (
    packaged_smoke_rehearsals_mod._wait_for_install_handoff_confirmation
)
_wait_for_pid_exit = packaged_smoke_rehearsals_mod._wait_for_pid_exit
_wait_for_launcher_exit = packaged_smoke_rehearsals_mod._wait_for_launcher_exit
_terminate_launcher_process_only = packaged_smoke_rehearsals_mod._terminate_launcher_process_only
_terminate_pid = packaged_smoke_rehearsals_mod._terminate_pid
_wait_for_desktop_ports_released = packaged_smoke_rehearsals_mod._wait_for_desktop_ports_released
_run_desktop_lifecycle_node_probe = packaged_smoke_rehearsals_mod._run_desktop_lifecycle_node_probe
_wait_for_relaunched_runtime = packaged_smoke_rehearsals_mod._wait_for_relaunched_runtime
_verify_rehearsal_local_data = packaged_smoke_rehearsals_mod._verify_rehearsal_local_data
_preferred_desktop_browser_env = packaged_smoke_rehearsals_mod._preferred_desktop_browser_env
_select_packaged_browser_job_browser = (
    packaged_smoke_rehearsals_mod._select_packaged_browser_job_browser
)
_select_browser_shutdown_proof = packaged_smoke_rehearsals_mod._select_browser_shutdown_proof
_run_active_task_close_node_probe = packaged_smoke_rehearsals_mod._run_active_task_close_node_probe
_run_desktop_lifecycle_close_node_probe = (
    packaged_smoke_rehearsals_mod._run_desktop_lifecycle_close_node_probe
)
_assert_desktop_update_helper_succeeded = (
    packaged_smoke_rehearsals_mod._assert_desktop_update_helper_succeeded
)
_wait_for_desktop_update_helper_completion = (
    packaged_smoke_rehearsals_mod._wait_for_desktop_update_helper_completion
)
run_packaged_sync_rehearsal = packaged_smoke_rehearsals_mod.run_packaged_sync_rehearsal
run_desktop_update_rehearsal = packaged_smoke_rehearsals_mod.run_desktop_update_rehearsal
run_packaged_browser_job_rehearsal = (
    packaged_smoke_rehearsals_mod.run_packaged_browser_job_rehearsal
)
run_packaged_desktop_lifecycle_rehearsal = (
    packaged_smoke_rehearsals_mod.run_packaged_desktop_lifecycle_rehearsal
)
run_packaged_active_task_close_rehearsal = (
    packaged_smoke_rehearsals_mod.run_packaged_active_task_close_rehearsal
)
run_packaged_orphan_reclaim_rehearsal = (
    packaged_smoke_rehearsals_mod.run_packaged_orphan_reclaim_rehearsal
)


def run_packaged_smoke(args: argparse.Namespace) -> dict[str, Any]:
    return packaged_smoke_orchestrator_mod.run_packaged_smoke(args)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run packaged desktop smoke validation against Baluffo.exe."
    )
    parser.add_argument("--exe-path", default=str(DEFAULT_EXE_PATH))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--artifacts-dir", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    parser.add_argument("--playwright-timeout", type=float, default=DEFAULT_SMOKE_RUNNER_TIMEOUT_S)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause-on-failure", action="store_true")
    parser.add_argument("--startup-probe", action="store_true")
    parser.add_argument("--embedded-probes", action="store_true")
    parser.add_argument("--sync-rehearsal", action="store_true")
    parser.add_argument("--desktop-update-rehearsal", action="store_true")
    parser.add_argument("--orphan-reclaim-rehearsal", action="store_true")
    parser.add_argument("--browser-job-rehearsal", action="store_true")
    parser.add_argument("--desktop-lifecycle-rehearsal", action="store_true")
    parser.add_argument("--active-task-close-rehearsal", action="store_true")
    parser.add_argument("--profile-only", action="store_true")
    parser.add_argument("--profile-mode", choices=("cold", "warm"), default="cold")
    parser.add_argument("--profile-record-only", action="store_true")
    parser.add_argument("--fail-on-threshold", action="store_true")
    parser.add_argument("--open-path", default="jobs.html")
    parser.add_argument("--node-smoke-script", default=str(DEFAULT_NODE_SMOKE_SCRIPT))
    parser.add_argument(
        "--fetch-evidence-mode",
        choices=("deterministic", "real"),
        default="deterministic",
    )
    return parser.parse_args(argv)


def _console_safe(value: Any) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return str(value).encode(encoding, errors="replace").decode(encoding, errors="replace")


def _print_console(value: Any) -> None:
    print(_console_safe(value))


def _print_failure_summary(report: dict[str, Any]) -> None:
    """Print a summary of the failure to stdout for CI visibility."""
    failure = report.get("failure")
    if not failure:
        return
    step = failure.get("step", "unknown")
    message = failure.get("message", "No error message available")
    category = failure.get("category", "")
    _print_console(f"\n[SMOKE FAILURE] Step: {step}")
    _print_console(f"[SMOKE FAILURE] Error: {message}")
    if category:
        _print_console(f"[SMOKE FAILURE] Category: {category}")
    artifacts = report.get("artifacts", {})
    exe_stdout = artifacts.get("exeStdout")
    exe_stderr = artifacts.get("exeStderr")
    report_path = artifacts.get("reportPath")
    if exe_stdout:
        _print_console(f"[SMOKE FAILURE] Exe stdout log: {exe_stdout}")
    if exe_stderr:
        _print_console(f"[SMOKE FAILURE] Exe stderr log: {exe_stderr}")
    if report_path:
        _print_console(f"[SMOKE FAILURE] Full report: {report_path}")
    scenarios = report.get("scenarios", [])
    if scenarios:
        _print_console("[SMOKE FAILURE] Scenarios summary:")
        for scenario in scenarios:
            name = scenario.get("name", "unknown")
            status = scenario.get("status", "unknown")
            status_char = "." if status == "passed" else "X"
            _print_console(f"  [{status_char}] {name}: {status}")
            if status != "passed" and scenario.get("error"):
                _print_console(f"      Error: {scenario['error']}")


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    args = parse_args(argv)
    report = run_packaged_smoke(args)
    if args.startup_probe or args.profile_only:
        summary = report.get("startupProfile")
        if isinstance(summary, dict) and summary:
            print(render_startup_summary(summary))
    if not report.get("ok"):
        _print_failure_summary(report)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
