#!/usr/bin/env python3
"""Release-gating smoke runner for the packaged Baluffo desktop executable."""

from __future__ import annotations

import argparse
import base64
import contextlib
import ctypes
import errno
import http.server
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import source_sync as source_sync_mod
from src.local_data_store import LocalDataPaths, LocalDataStore
from src.python_version_guard import ensure_required_python
from src.shared.utils import utc_now_iso
from src.ship import desktop_app as desktop_app_mod
from src.ship import desktop_update as desktop_update_mod
from src.ship.packaged_smoke import build_env as packaged_smoke_build_env_mod
from src.ship.packaged_smoke import rehearsals as packaged_smoke_rehearsals_mod
from src.ship.packaged_smoke import runtime as packaged_smoke_runtime_mod
from src.ship.startup_probe_policy import (
    EMBEDDED_PAGE_PROBES,
    REQUIRED_STARTUP_PROBE_LAUNCH_MODE,
    STARTUP_REQUIRED_EVENTS,
    classify_startup_probe_failure,
    refine_startup_probe_summary,
    startup_metric_fields,
    startup_probe_browser_details,
    startup_profile_required_events,
)
from src.ship.startup_probe_policy import (
    required_startup_event_present as _required_startup_event_present,
)
from src.ship.startup_probe_policy import (
    select_startup_probe_browser as select_startup_probe_browser_policy,
)
from src.ship.startup_profile import (
    render_startup_summary,
    summarize_startup_metrics,
    write_startup_summary,
)
from src.ship.startup_telemetry import read_startup_metrics as read_startup_metrics_file

DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_REPORT_PATH = ROOT / "data" / "packaged-desktop-smoke-report.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke"
DEFAULT_ARTIFACT_RETENTION_RUNS = 2
DEFAULT_ARTIFACT_FILE_RETENTION_S = 24 * 60 * 60
DEFAULT_RUNTIME_TIMEOUT_S = 35.0
DEFAULT_SMOKE_RUNNER_TIMEOUT_S = 180.0
DEFAULT_NODE_SMOKE_SCRIPT = ROOT / "tests" / "frontend" / "packaged-desktop-smoke.mjs"
JOBS_PIPELINE_NODE_SMOKE_SCRIPT = (
    ROOT / "tests" / "frontend" / "packaged-desktop-smoke.jobs-pipeline.mjs"
)
# If any of these are newer than ``dist/.../Baluffo.exe``, the smoke gate must rebuild
# so CI/local never runs an obsolete PyInstaller payload against current sources.
_PORTABLE_EXE_FRESHNESS_MARKERS = (
    ROOT / "scripts" / "build_portable_exe.py",
    ROOT / "scripts" / "build_ship_bundle.py",
    ROOT / "src" / "ship" / "runtime_launcher.py",
    ROOT / "src" / "ship" / "update_manager.py",
    ROOT / "src" / "ship" / "desktop_app" / "__init__.py",
    ROOT / "src" / "admin_bridge.py",
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
    ".pyinstaller-assets",
    ".pyinstaller-dist",
    ".pyinstaller-work",
    ".pyinstaller-spec",
    ".pyinstaller-helper-dist",
    ".pyinstaller-helper-work",
    ".pyinstaller-helper-spec",
)


def slugify_token(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    compact = "-".join(part for part in lowered.split("-") if part)
    return compact or "scenario"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def remove_tree_or_file(path: Path) -> bool:
    candidate = Path(path).expanduser().resolve()
    if not candidate.exists():
        return False
    try:
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)
        else:
            candidate.unlink()
    except OSError:
        return False
    return not candidate.exists()


def generate_packaged_smoke_run_token(
    *, now: datetime | None = None, entropy_ns: int | None = None
) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    resolved_entropy = int(entropy_ns if entropy_ns is not None else time.time_ns())
    return f"{resolved_now.strftime('%Y%m%d-%H%M%S-%f')}-{resolved_entropy % 1_000_000_000:09d}"


def fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


def fetch_text(url: str, timeout_s: float = 2.5) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return response.read().decode("utf-8", errors="replace")


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        str(url),
        data=body,
        headers=headers,
        method=str(method or "GET").upper(),
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw or "{}")
            return int(getattr(response, "status", 200) or 200), parsed if isinstance(
                parsed, dict
            ) else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw or "{}")
        except json.JSONDecodeError:
            parsed = {"error": raw or str(exc)}
        return int(getattr(exc, "code", 500) or 500), parsed if isinstance(parsed, dict) else {}


def post_json(
    url: str, payload: dict[str, Any] | None = None, *, timeout_s: float = 10.0
) -> tuple[int, dict[str, Any]]:
    return request_json(url, method="POST", payload=payload or {}, timeout_s=timeout_s)


def fetch_startup_metrics(bridge_base_url: str, limit: int = 1000) -> list[dict[str, Any]]:
    metrics_payload = fetch_json(
        f"{bridge_base_url}/desktop-local-data/startup-metrics?limit={int(limit)}"
    )
    rows = metrics_payload.get("rows") if isinstance(metrics_payload.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def startup_metric_launch_mode(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        if str(row.get("event") or "").strip() != "desktop_browser_launch_selected":
            continue
        fields = startup_metric_fields(row)
        return str(fields.get("mode") or "").strip().lower()
    return ""


def startup_metric_event_present(
    rows: list[dict[str, Any]], event: str, **expected_fields: object
) -> bool:
    expected_event = str(event or "").strip()
    for row in rows:
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return True
    return False


def find_startup_metric_fields(
    rows: list[dict[str, Any]], event: str, **expected_fields: object
) -> dict[str, Any] | None:
    expected_event = str(event or "").strip()
    for row in reversed(rows):
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return fields
    return None


packaged_smoke_build_env_mod.root = sys.modules[__name__]
packaged_smoke_runtime_mod.root = sys.modules[__name__]
packaged_smoke_rehearsals_mod.root = sys.modules[__name__]

choose_free_port = packaged_smoke_build_env_mod.choose_free_port
_default_portable_exe_stale = packaged_smoke_build_env_mod._default_portable_exe_stale
_exe_path_uses_default_dist = packaged_smoke_build_env_mod._exe_path_uses_default_dist
_portable_exe_marker_staleness = packaged_smoke_build_env_mod._portable_exe_marker_staleness
_iter_portable_exe_freshness_markers = packaged_smoke_build_env_mod._iter_portable_exe_freshness_markers
run_portable_build = packaged_smoke_build_env_mod.run_portable_build
cleanup_portable_build_scratch = packaged_smoke_build_env_mod.cleanup_portable_build_scratch
select_startup_probe_browser = packaged_smoke_build_env_mod.select_startup_probe_browser
prune_packaged_smoke_artifacts = packaged_smoke_build_env_mod.prune_packaged_smoke_artifacts
resolve_node_command = packaged_smoke_build_env_mod.resolve_node_command
write_text = packaged_smoke_build_env_mod.write_text
packaged_desktop_local_appdata_root = packaged_smoke_build_env_mod.packaged_desktop_local_appdata_root
packaged_desktop_session_paths = packaged_smoke_build_env_mod.packaged_desktop_session_paths
clear_packaged_desktop_session_state = packaged_smoke_build_env_mod.clear_packaged_desktop_session_state
is_windows_process_elevated = packaged_smoke_build_env_mod.is_windows_process_elevated
path_is_writable = packaged_smoke_build_env_mod.path_is_writable
classify_subprocess_error = packaged_smoke_build_env_mod.classify_subprocess_error
collect_packaged_smoke_env_diagnostics = packaged_smoke_build_env_mod.collect_packaged_smoke_env_diagnostics
build_packaged_smoke_env = packaged_smoke_build_env_mod.build_packaged_smoke_env
packaged_pipeline_smoke_mode = packaged_smoke_build_env_mod.packaged_pipeline_smoke_mode
packaged_runtime_env_overrides = packaged_smoke_build_env_mod.packaged_runtime_env_overrides
ensure_portable_exe = packaged_smoke_build_env_mod.ensure_portable_exe

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
wait_for_packaged_runtime_with_port_pivot = packaged_smoke_runtime_mod.wait_for_packaged_runtime_with_port_pivot
wait_for_packaged_child_runtime = packaged_smoke_runtime_mod.wait_for_packaged_child_runtime
capture_runtime_snapshot = packaged_smoke_runtime_mod.capture_runtime_snapshot
wait_for_runtime_events = packaged_smoke_runtime_mod.wait_for_runtime_events
run_embedded_runtime_probe = packaged_smoke_runtime_mod.run_embedded_runtime_probe
parse_packaged_node_smoke_report = packaged_smoke_runtime_mod.parse_packaged_node_smoke_report
read_packaged_node_smoke_payload = packaged_smoke_runtime_mod.read_packaged_node_smoke_payload
run_packaged_node_smoke = packaged_smoke_runtime_mod.run_packaged_node_smoke
build_failure_payload = packaged_smoke_runtime_mod.build_failure_payload
run_warmup_launch = packaged_smoke_runtime_mod.run_warmup_launch

_archive_portable_dir = packaged_smoke_rehearsals_mod._archive_portable_dir
_inject_desktop_update_public_keys = packaged_smoke_rehearsals_mod._inject_desktop_update_public_keys
_portable_current_version = packaged_smoke_rehearsals_mod._portable_current_version
_portable_packaged_sync_config_path = packaged_smoke_rehearsals_mod._portable_packaged_sync_config_path
_load_portable_packaged_sync_rehearsal_config = (
    packaged_smoke_rehearsals_mod._load_portable_packaged_sync_rehearsal_config
)
_seed_rehearsal_local_data = packaged_smoke_rehearsals_mod._seed_rehearsal_local_data
_PackagedSyncRehearsalHandler = packaged_smoke_rehearsals_mod._PackagedSyncRehearsalHandler
_start_packaged_sync_rehearsal_server = packaged_smoke_rehearsals_mod._start_packaged_sync_rehearsal_server
_DesktopUpdateReleaseHandler = packaged_smoke_rehearsals_mod._DesktopUpdateReleaseHandler
_start_desktop_update_release_server = (
    packaged_smoke_rehearsals_mod._start_desktop_update_release_server
)
_wait_for_process_exit = packaged_smoke_rehearsals_mod._wait_for_process_exit
_wait_for_install_handoff_confirmation = (
    packaged_smoke_rehearsals_mod._wait_for_install_handoff_confirmation
)
_wait_for_pid_exit = packaged_smoke_rehearsals_mod._wait_for_pid_exit
_wait_for_relaunched_runtime = packaged_smoke_rehearsals_mod._wait_for_relaunched_runtime
_verify_rehearsal_local_data = packaged_smoke_rehearsals_mod._verify_rehearsal_local_data
_preferred_desktop_browser_env = packaged_smoke_rehearsals_mod._preferred_desktop_browser_env
_select_packaged_browser_job_browser = (
    packaged_smoke_rehearsals_mod._select_packaged_browser_job_browser
)
_select_browser_shutdown_proof = packaged_smoke_rehearsals_mod._select_browser_shutdown_proof
_assert_desktop_update_helper_succeeded = (
    packaged_smoke_rehearsals_mod._assert_desktop_update_helper_succeeded
)
run_packaged_sync_rehearsal = packaged_smoke_rehearsals_mod.run_packaged_sync_rehearsal
run_desktop_update_rehearsal = packaged_smoke_rehearsals_mod.run_desktop_update_rehearsal
run_packaged_browser_job_rehearsal = packaged_smoke_rehearsals_mod.run_packaged_browser_job_rehearsal
run_packaged_orphan_reclaim_rehearsal = (
    packaged_smoke_rehearsals_mod.run_packaged_orphan_reclaim_rehearsal
)


def run_packaged_smoke(args: argparse.Namespace) -> dict[str, Any]:
    started_at = utc_now_iso()
    run_token = generate_packaged_smoke_run_token()
    artifacts_dir = (
        Path(args.artifacts_dir or (DEFAULT_ARTIFACT_ROOT / run_token)).expanduser().resolve()
    )
    runtime_data_dir = artifacts_dir / "runtime-data"
    embedded_artifacts_dir = artifacts_dir / "embedded-runtime-probes"
    stdout_path = artifacts_dir / "desktop-exe.stdout.log"
    stderr_path = artifacts_dir / "desktop-exe.stderr.log"
    latest_report_path = Path(args.report_path or DEFAULT_REPORT_PATH).expanduser().resolve()
    report_path = artifacts_dir / "report.json"
    site_port = int(args.site_port or choose_free_port())
    bridge_port = int(args.bridge_port or choose_free_port())
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    startup_probe = bool(args.startup_probe or args.profile_only)
    embedded_probes = bool(args.embedded_probes)
    profile_mode = "warm" if str(args.profile_mode or "").strip().lower() == "warm" else "cold"
    open_path = str(args.open_path or "jobs.html").strip() or "jobs.html"
    node_smoke_script = (
        Path(args.node_smoke_script or DEFAULT_NODE_SMOKE_SCRIPT).expanduser().resolve()
    )
    if artifacts_dir.parent == DEFAULT_ARTIFACT_ROOT.resolve():
        prune_packaged_smoke_artifacts(
            DEFAULT_ARTIFACT_ROOT,
            current_artifacts_dir=artifacts_dir,
        )
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    embedded_artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_env = os.environ.copy()
    runtime_env.update(
        packaged_runtime_env_overrides(
            node_smoke_script,
            artifacts_dir=artifacts_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode=profile_mode,
        )
    )
    clear_packaged_desktop_session_state(runtime_env)
    preferred_probe_browser_name = ""
    preferred_probe_browser_path = ""
    startup_page = Path(open_path).stem or "jobs"
    requested_exe_path = Path(args.exe_path or DEFAULT_EXE_PATH).expanduser().resolve()
    rebuild_output_dir = (
        artifacts_dir / "portable-build"
        if bool(args.rebuild) and requested_exe_path == DEFAULT_EXE_PATH.resolve()
        else None
    )
    exe_path = requested_exe_path

    report: dict[str, Any] = {
        "ok": False,
        "startedAt": started_at,
        "finishedAt": "",
        "exePath": str(exe_path),
        "dataDir": str(runtime_data_dir),
        "siteBaseUrl": site_base_url,
        "bridgeBaseUrl": bridge_base_url,
        "startupMetrics": [],
        "bridgeReady": False,
        "scenarios": [],
        "startupProfile": {},
        "artifacts": {
            "artifactsDir": str(artifacts_dir),
            "reportPath": str(report_path),
            "exeStdout": str(stdout_path),
            "exeStderr": str(stderr_path),
        },
        "environment": {},
        "probeBrowser": {
            "requiredManagedWindow": bool(startup_probe),
            "preferredBrowserName": preferred_probe_browser_name,
            "preferredBrowserPath": preferred_probe_browser_path,
            "selectedBrowserName": "",
            "selectedBrowserPath": "",
            "launchMode": "",
            "launchError": "",
            "launchErrorType": "",
            "windowClosedReason": "",
        },
        "failure": None,
    }
    if rebuild_output_dir is not None:
        report["artifacts"]["rebuiltPortableDir"] = str(rebuild_output_dir)

    process: subprocess.Popen[Any] | None = None
    stdout_handle = None
    stderr_handle = None
    try:
        if startup_probe:
            preferred_probe_browser = select_startup_probe_browser(runtime_env)
            preferred_probe_browser_name = str(
                preferred_probe_browser.get("browserName") or ""
            ).strip()
            preferred_probe_browser_path = str(
                preferred_probe_browser.get("browserPath") or ""
            ).strip()
            runtime_env[desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] = preferred_probe_browser_path
            report["probeBrowser"]["preferredBrowserName"] = preferred_probe_browser_name
            report["probeBrowser"]["preferredBrowserPath"] = preferred_probe_browser_path
        exe_path = ensure_portable_exe(
            requested_exe_path, rebuild=bool(args.rebuild), rebuild_output_dir=rebuild_output_dir
        )
        report["exePath"] = str(exe_path)
        report["environment"] = collect_packaged_smoke_env_diagnostics(
            artifacts_dir=artifacts_dir,
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            node_smoke_script=node_smoke_script,
            rebuilt_portable_dir=rebuild_output_dir,
            env=runtime_env,
        )
        if bool(args.sync_rehearsal):
            rehearsal = run_packaged_sync_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            )
            report["scenarios"].append(rehearsal)
            if isinstance(rehearsal.get("details"), dict):
                details = rehearsal.get("details") or {}
                for src_key, artifact_key in (
                    ("runtimeStdout", "syncRehearsalStdout"),
                    ("runtimeStderr", "syncRehearsalStderr"),
                ):
                    value = str(details.get(src_key) or "").strip()
                    if value:
                        report["artifacts"][artifact_key] = value
            report["ok"] = str(rehearsal.get("status")) == "passed"
            if not report["ok"]:
                report["failure"] = build_failure_payload(
                    "packaged-sync-rehearsal",
                    str(rehearsal.get("error") or "Packaged sync rehearsal failed."),
                )
            return report
        if bool(args.desktop_update_rehearsal):
            rehearsal = run_desktop_update_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            )
            report["scenarios"].append(rehearsal)
            if isinstance(rehearsal.get("details"), dict):
                details = rehearsal.get("details") or {}
                for src_key, artifact_key in (
                    ("helperStdoutLog", "helperStdout"),
                    ("helperStderrLog", "helperStderr"),
                    ("helperDiagnosticsLog", "helperDiagnostics"),
                ):
                    value = str(details.get(src_key) or "").strip()
                    if value:
                        report["artifacts"][artifact_key] = value
            report["ok"] = str(rehearsal.get("status")) == "passed"
            if not report["ok"]:
                report["failure"] = build_failure_payload(
                    "desktop-update-rehearsal",
                    str(rehearsal.get("error") or "Packaged desktop update rehearsal failed."),
                )
            return report
        if bool(args.orphan_reclaim_rehearsal):
            rehearsal = run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            )
            report["scenarios"].append(rehearsal)
            if isinstance(rehearsal.get("details"), dict):
                details = rehearsal.get("details") or {}
                for src_key, artifact_key in (
                    ("runtimeStdout", "orphanRehearsalRuntimeStdout"),
                    ("runtimeStderr", "orphanRehearsalRuntimeStderr"),
                    ("staleSiteStdout", "orphanRehearsalSiteStdout"),
                    ("staleSiteStderr", "orphanRehearsalSiteStderr"),
                    ("staleBridgeStdout", "orphanRehearsalBridgeStdout"),
                    ("staleBridgeStderr", "orphanRehearsalBridgeStderr"),
                ):
                    value = str(details.get(src_key) or "").strip()
                    if value:
                        report["artifacts"][artifact_key] = value
            report["ok"] = str(rehearsal.get("status")) == "passed"
            if not report["ok"]:
                report["failure"] = build_failure_payload(
                    "packaged-orphan-reclaim-rehearsal",
                    str(rehearsal.get("error") or "Packaged orphan reclaim rehearsal failed."),
                )
            return report
        if bool(args.browser_job_rehearsal):
            rehearsal = run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            )
            report["scenarios"].append(rehearsal)
            if isinstance(rehearsal.get("details"), dict):
                details = rehearsal.get("details") or {}
                for src_key, artifact_key in (
                    ("runtimeStdout", "browserJobRehearsalRuntimeStdout"),
                    ("runtimeStderr", "browserJobRehearsalRuntimeStderr"),
                    ("startupMetrics", "browserJobRehearsalStartupMetrics"),
                ):
                    value = str(details.get(src_key) or "").strip()
                    if value:
                        report["artifacts"][artifact_key] = value
            report["ok"] = str(rehearsal.get("status")) == "passed"
            if not report["ok"]:
                report["failure"] = build_failure_payload(
                    "packaged-browser-job-rehearsal",
                    str(rehearsal.get("error") or "Packaged browser job rehearsal failed."),
                )
            return report
        if profile_mode == "warm":
            run_warmup_launch(
                exe_path,
                artifacts_root=artifacts_dir,
                open_path=open_path,
                runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
                startup_probe=startup_probe,
                env=runtime_env,
            )
        if embedded_probes and not bool(args.profile_only):
            embedded_scenarios = [
                run_embedded_runtime_probe(
                    exe_path=exe_path,
                    probe=probe,
                    artifacts_root=embedded_artifacts_dir,
                    runtime_timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
                    startup_probe=startup_probe,
                    profile_mode=profile_mode,
                    env=runtime_env,
                )
                for probe in EMBEDDED_PAGE_PROBES
            ]
            for row in embedded_scenarios:
                if str(row.get("status")) == "passed":
                    print(".", end="", flush=True)
                else:
                    print("✘", end="", flush=True)
            report["scenarios"].extend(embedded_scenarios)
            first_failed_embedded = next(
                (row for row in embedded_scenarios if str(row.get("status")) != "passed"), None
            )
            if first_failed_embedded:
                raise RuntimeError(
                    f"{first_failed_embedded.get('name', 'Embedded probe')} failed: {first_failed_embedded.get('error', '')}".strip()
                )
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=open_path,
            startup_probe=startup_probe,
            env=runtime_env,
        )
        runtime_state = wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
            open_path=open_path,
            required_events=STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        report["bridgeReady"] = True
        report["startupMetrics"] = runtime_state.get("startupMetrics") or []
        if startup_probe:
            report["startupMetrics"] = wait_for_runtime_events(
                bridge_base_url,
                startup_profile_required_events(startup_page),
                timeout_s=max(5.0, float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S)),
            )
            startup_profile = summarize_startup_metrics(
                report["startupMetrics"], page=startup_page, profile_mode=profile_mode
            )
            startup_profile = refine_startup_probe_summary(
                startup_profile,
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
            report["startupProfile"] = startup_profile
            report["probeBrowser"] = startup_probe_browser_details(
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
            report["artifacts"]["startupProfileSummary"] = str(
                artifacts_dir / "startup-profile-summary.json"
            )
            write_startup_summary(artifacts_dir / "startup-profile-summary.json", startup_profile)
            report["scenarios"].append(
                {
                    "name": "Startup Profile",
                    "slug": "startup-profile",
                    "status": "passed"
                    if str(startup_profile.get("status")) == "passed"
                    else "failed",
                    "durationMs": int(startup_profile.get("firstUsableMs") or 0),
                    "error": ""
                    if str(startup_profile.get("status")) == "passed"
                    else str(
                        startup_profile.get("classification")
                        or "startup profile threshold exceeded"
                    ),
                    "startupProfile": startup_profile,
                }
            )
        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))

        if bool(args.profile_only):
            report["ok"] = all(str(row.get("status")) == "passed" for row in report["scenarios"])
            if not report["ok"] and not report["failure"]:
                report["failure"] = build_failure_payload(
                    "startup-profile",
                    str(
                        report["startupProfile"].get("classification")
                        or "startup profile threshold exceeded"
                    ),
                    category=classify_startup_probe_failure(
                        report.get("startupMetrics") or [],
                        summary=report.get("startupProfile")
                        if isinstance(report.get("startupProfile"), dict)
                        else None,
                    )[1],
                )
            return report

        smoke_runner_result = run_packaged_node_smoke(
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            artifacts_dir=artifacts_dir,
            node_smoke_script=node_smoke_script,
            headed=bool(args.headed),
            pause_on_failure=bool(args.pause_on_failure),
            timeout_s=float(args.playwright_timeout or DEFAULT_SMOKE_RUNNER_TIMEOUT_S),
        )
        report["artifacts"]["smokeReport"] = str(smoke_runner_result["reportPath"])
        report["artifacts"]["smokeOutputDir"] = str(smoke_runner_result["outputDir"])
        report["artifacts"]["smokeRunnerStdout"] = str(artifacts_dir / "smoke-runner-stdout.log")
        report["artifacts"]["smokeRunnerStderr"] = str(artifacts_dir / "smoke-runner-stderr.log")
        report["artifacts"]["playwrightReport"] = report["artifacts"]["smokeReport"]
        report["artifacts"]["playwrightOutputDir"] = report["artifacts"]["smokeOutputDir"]
        report["artifacts"]["playwrightStdout"] = report["artifacts"]["smokeRunnerStdout"]
        report["artifacts"]["playwrightStderr"] = report["artifacts"]["smokeRunnerStderr"]
        report["scenarios"].extend(list(smoke_runner_result.get("scenarios") or []))
        if isinstance(smoke_runner_result.get("environment"), dict):
            report["environment"] = dict(smoke_runner_result["environment"])
        if int(smoke_runner_result.get("exitCode", 1)) != 0:
            failed = next(
                (row for row in report["scenarios"] if str(row.get("status")) != "passed"), None
            )
            report["failure"] = build_failure_payload(
                "playwright",
                failed.get("error")
                if isinstance(failed, dict) and failed.get("error")
                else str(
                    smoke_runner_result.get("runnerError") or "Packaged desktop smoke failed."
                ),
                category=str(smoke_runner_result.get("failureCategory") or ""),
            )
        else:
            report["ok"] = all(str(row.get("status")) == "passed" for row in report["scenarios"])

        # Output dots for Node smoke scenarios
        for row in smoke_runner_result.get("scenarios", []):
            if str(row.get("status")) == "passed":
                print(".", end="", flush=True)
            else:
                print("✘", end="", flush=True)

        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))
    except Exception as exc:  # noqa: BLE001
        if startup_probe:
            partial_metrics = list(report.get("startupMetrics") or [])
            if not partial_metrics:
                try:
                    partial_metrics = fetch_startup_metrics(bridge_base_url, limit=1000)
                except Exception:  # noqa: BLE001
                    partial_metrics = []
            if not partial_metrics:
                partial_metrics = read_startup_metrics_file(runtime_data_dir, limit=1000)
            if partial_metrics:
                report["startupMetrics"] = partial_metrics
                startup_profile = summarize_startup_metrics(
                    partial_metrics, page=startup_page, profile_mode=profile_mode
                )
                startup_profile = refine_startup_probe_summary(
                    startup_profile,
                    partial_metrics,
                    error_message=str(exc),
                    preferred_browser_name=preferred_probe_browser_name,
                    preferred_browser_path=preferred_probe_browser_path,
                )
                report["startupProfile"] = startup_profile
                report["probeBrowser"] = startup_probe_browser_details(
                    partial_metrics,
                    preferred_browser_name=preferred_probe_browser_name,
                    preferred_browser_path=preferred_probe_browser_path,
                )
                report["artifacts"]["startupProfileSummary"] = str(
                    artifacts_dir / "startup-profile-summary.json"
                )
                write_startup_summary(
                    artifacts_dir / "startup-profile-summary.json", startup_profile
                )
                if not any(
                    str(row.get("slug")) == "startup-profile"
                    for row in report["scenarios"]
                    if isinstance(row, dict)
                ):
                    report["scenarios"].append(
                        {
                            "name": "Startup Profile",
                            "slug": "startup-profile",
                            "status": "passed"
                            if str(startup_profile.get("status")) == "passed"
                            else "failed",
                            "durationMs": int(startup_profile.get("firstUsableMs") or 0),
                            "error": ""
                            if str(startup_profile.get("status")) == "passed"
                            else str(
                                startup_profile.get("classification")
                                or "startup profile threshold exceeded"
                            ),
                            "startupProfile": startup_profile,
                        }
                    )
        if not report["failure"]:
            report["failure"] = build_failure_payload(
                "runner",
                exc,
                category=classify_startup_probe_failure(
                    report.get("startupMetrics") or [],
                    error_message=str(exc),
                    summary=report.get("startupProfile")
                    if isinstance(report.get("startupProfile"), dict)
                    else None,
                )[1]
                or classify_subprocess_error(exc),
            )
    finally:
        terminate_process_tree(process)
        if os.name == "nt":
            time.sleep(0.25)
        cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        report["finishedAt"] = utc_now_iso()
        write_json(report_path, report)
        write_json(latest_report_path, report)
    return report


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
    parser.add_argument("--profile-only", action="store_true")
    parser.add_argument("--profile-mode", choices=("cold", "warm"), default="cold")
    parser.add_argument("--open-path", default="jobs.html")
    parser.add_argument("--node-smoke-script", default=str(DEFAULT_NODE_SMOKE_SCRIPT))
    return parser.parse_args(argv)


def _print_failure_summary(report: dict[str, Any]) -> None:
    """Print a summary of the failure to stdout for CI visibility."""
    failure = report.get("failure")
    if not failure:
        return
    step = failure.get("step", "unknown")
    message = failure.get("message", "No error message available")
    category = failure.get("category", "")
    print(f"\n[SMOKE FAILURE] Step: {step}")
    print(f"[SMOKE FAILURE] Error: {message}")
    if category:
        print(f"[SMOKE FAILURE] Category: {category}")
    artifacts = report.get("artifacts", {})
    exe_stdout = artifacts.get("exeStdout")
    exe_stderr = artifacts.get("exeStderr")
    report_path = artifacts.get("reportPath")
    if exe_stdout:
        print(f"[SMOKE FAILURE] Exe stdout log: {exe_stdout}")
    if exe_stderr:
        print(f"[SMOKE FAILURE] Exe stderr log: {exe_stderr}")
    if report_path:
        print(f"[SMOKE FAILURE] Full report: {report_path}")
    scenarios = report.get("scenarios", [])
    if scenarios:
        print("[SMOKE FAILURE] Scenarios summary:")
        for scenario in scenarios:
            name = scenario.get("name", "unknown")
            status = scenario.get("status", "unknown")
            status_char = "." if status == "passed" else "X"
            print(f"  [{status_char}] {name}: {status}")
            if status != "passed" and scenario.get("error"):
                print(f"      Error: {scenario['error']}")


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
