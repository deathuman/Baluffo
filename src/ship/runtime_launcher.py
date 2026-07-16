#!/usr/bin/env python3
"""Platform-neutral runtime launcher for Baluffo ship bundles.

AI boundary owns: desktop runtime process launch, port/session setup, and packaged/runtime handoff.
AI boundary implement in: this file for launcher script coordination; desktop app internals stay in src.ship.desktop_app leaves.
AI boundary search before contracts: desktop app launcher flow, packaged smoke, and runtime launcher tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused desktop runtime tests.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import runpy
import shutil
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_version import get_app_version
from src.baluffo_config import get_bridge_defaults, get_desktop_defaults, get_security_defaults
from src.shared.json_io import PIPELINE_GZIP_JSON_NAMES, existing_json_candidate
from src.ship.jobs_first_run_state import (
    ROW_BEARING_JOBS_ARTIFACTS,
    RUNTIME_FEED_ARTIFACTS,
    has_plausible_runtime_feed_artifacts_for_static_serving,
    has_successful_runtime_jobs_report,
    jobs_cold_start_required,
    jobs_cold_start_required_for_static_serving,
)
from src.ship.startup_telemetry import (
    append_runtime_startup_trace as _append_runtime_startup_trace,
)
from src.ship.startup_telemetry import (
    append_startup_trace as _append_startup_trace,
)
from src.ship.startup_telemetry import (
    startup_probe_enabled,
)
from src.ship.startup_telemetry import (
    wait_for_url as wait_for_url,
)
from src.ship.update_manager_bootstrap import (
    repair_version_from_runtime_bootstrap as _repair_version_from_runtime_bootstrap,
)
from src.ship.update_manager_paths import (
    REQUIRED_VERSION_FILES as _REQUIRED_VERSION_FILES,
)
from src.ship.update_manager_paths import (
    ShipPaths as _ShipPaths,
)
from src.ship.update_manager_recovery import startup_check as _startup_check
from src.ship.update_manager_state import ensure_state as _ensure_state
from src.ship.update_manager_validation import health_check_version as _health_check_version

BRIDGE_DEFAULTS = get_bridge_defaults()
DESKTOP_DEFAULTS = get_desktop_defaults()
SECURITY_DEFAULTS = get_security_defaults()
DESKTOP_BRIDGE_HOST_ENV = "BALUFFO_DESKTOP_BRIDGE_HOST"
DESKTOP_BRIDGE_PORT_ENV = "BALUFFO_DESKTOP_BRIDGE_PORT"
ROOT_DATA_FILE_ALIASES = frozenset(
    {
        "jobs-fetch-report.json",
        "jobs-lifecycle-state.json",
        "jobs-source-state.json",
    }
)
PRIVATE_DATA_ARTIFACTS = frozenset(
    {
        "jobs-unified.json",
        "jobs-unified.csv",
        "jobs-availability-tombstones.json",
        "jobs-availability-direct-checkpoints.json",
        "jobs-availability-priority.json",
        "jobs-availability-sweep-plan.json",
        "jobs-availability-shadow-results.json",
    }
)
ROW_BEARING_JOBS_ARTIFACT_NAMES = frozenset(ROW_BEARING_JOBS_ARTIFACTS)
_EXPECTED_RUNTIME_LAUNCHER_CLI_EXCEPTIONS = (OSError, RuntimeError, ValueError)
update_manager = SimpleNamespace(
    ShipPaths=_ShipPaths,
    REQUIRED_VERSION_FILES=_REQUIRED_VERSION_FILES,
    ensure_state=_ensure_state,
    startup_check=_startup_check,
    repair_version_from_runtime_bootstrap=_repair_version_from_runtime_bootstrap,
    health_check_version=_health_check_version,
)


def _is_expected_client_disconnect(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return True
        winerror = getattr(current, "winerror", None)
        if isinstance(winerror, int) and winerror in {10053, 10054}:
            return True
        errno = getattr(current, "errno", None)
        if isinstance(errno, int) and errno in {32, 104}:
            return True
        current = current.__cause__ or current.__context__
    return False


@dataclass(frozen=True)
class RuntimeLayout:
    root: Path
    current_version: str
    active_root: Path
    data_dir: Path


class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def handle_one_request(self) -> None:
        try:
            return super().handle_one_request()
        except OSError as exc:
            if _is_expected_client_disconnect(exc):
                self.close_connection = True
                return
            raise


def _row_artifact_candidates(data_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for name in ROW_BEARING_JOBS_ARTIFACTS:
        path = Path(data_dir) / name
        if path.exists():
            candidates.append(path)
        if name.endswith(".json"):
            gzip_path = Path(data_dir) / f"{name}.gz"
            if gzip_path.exists():
                candidates.append(gzip_path)
    return sorted({path.resolve() for path in candidates})


def _runtime_feed_artifact_candidates(data_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for name in RUNTIME_FEED_ARTIFACTS:
        path = Path(data_dir) / name
        if name.endswith(".json"):
            candidate = existing_json_candidate(path)
            if candidate is not None:
                candidates.append(candidate)
            continue
        if path.exists():
            candidates.append(path)
    return sorted({path.resolve() for path in candidates})


def _parse_iso_timestamp(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _load_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _completed_user_data_migration_timestamp(data_dir: Path) -> float | None:
    report_path = Path(data_dir) / "migration-reports" / "windows-user-data-migration.json"
    report = _load_json_object(report_path)
    if not report:
        return None
    if report.get("completed") is False:
        return None
    status = str(report.get("status") or "").strip().lower()
    if status in {"error", "failed"}:
        return None
    return _parse_iso_timestamp(report.get("createdAt"))


def _artifact_modified_after(path: Path, cutoff_timestamp: float) -> bool:
    try:
        return Path(path).stat().st_mtime > cutoff_timestamp + 1
    except OSError:
        return False


def _runtime_feed_artifacts_include_updates_after(
    data_dir: Path,
    cutoff_timestamp: float | None,
) -> bool:
    if cutoff_timestamp is None:
        return False
    if not has_plausible_runtime_feed_artifacts_for_static_serving(data_dir):
        return False
    candidates = _runtime_feed_artifact_candidates(data_dir)
    return bool(candidates) and all(
        _artifact_modified_after(path, cutoff_timestamp) for path in candidates
    )


def _runtime_feed_artifacts_include_updates_after_migration(data_dir: Path) -> bool:
    return _runtime_feed_artifacts_include_updates_after(
        data_dir,
        _completed_user_data_migration_timestamp(data_dir),
    )


def _write_jobs_row_artifact_restore_report(
    data_dir: Path,
    timestamp: str,
    report: dict[str, object],
) -> dict[str, object]:
    report_dir = data_dir / "migration-reports"
    try:
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"stripped-packaged-jobs-restore-{timestamp}.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        report["reportPath"] = str(report_path)
    except OSError as exc:
        report["reportError"] = str(exc)
    return report


def _false_quarantine_backup_dir_from_report(
    report_path: Path,
    migration_timestamp: float,
) -> Path | None:
    cleanup_report = _load_json_object(report_path)
    if cleanup_report.get("reason") != "no_successful_runtime_jobs_report":
        return None
    backup_dir = Path(str(cleanup_report.get("backupDir") or ""))
    if not backup_dir.is_dir():
        return None
    if not _row_artifact_candidates(backup_dir):
        return None
    if not _runtime_feed_artifacts_include_updates_after(backup_dir, migration_timestamp):
        return None
    return backup_dir


def _restore_jobs_row_artifact_backup(
    data_dir: Path,
    report_path: Path,
    backup_dir: Path,
) -> dict[str, object]:
    restored: list[str] = []
    failed: list[dict[str, str]] = []
    for source in _row_artifact_candidates(backup_dir):
        target = data_dir / source.name
        if target.exists():
            continue
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        except (OSError, shutil.Error) as exc:
            failed.append({"path": str(source), "target": str(target), "error": str(exc)})
            continue
        restored.append(str(target))

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    restore_report = {
        "schemaVersion": 1,
        "createdAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "reason": "false_quarantined_runtime_jobs_artifacts",
        "backupDir": str(backup_dir),
        "sourceReportPath": str(report_path),
        "restored": restored,
        "failed": failed,
    }
    return _write_jobs_row_artifact_restore_report(data_dir, timestamp, restore_report)


def _restore_false_quarantined_jobs_row_artifacts(data_dir: Path) -> dict[str, object]:
    migration_timestamp = _completed_user_data_migration_timestamp(data_dir)
    if migration_timestamp is None:
        return {"restored": [], "failed": [], "skipped": "no_completed_migration_report"}

    report_dir = data_dir / "migration-reports"
    try:
        reports = sorted(
            report_dir.glob("stripped-packaged-jobs-cleanup-*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        reports = []

    for report_path in reports:
        backup_dir = _false_quarantine_backup_dir_from_report(report_path, migration_timestamp)
        if backup_dir is None:
            continue
        return _restore_jobs_row_artifact_backup(data_dir, report_path, backup_dir)

    return {"restored": [], "failed": [], "skipped": "no_false_quarantine_backup"}


def _is_row_bearing_jobs_artifact_request(trace_path: str) -> bool:
    normalized = str(trace_path or "").split("?", 1)[0].split("#", 1)[0].lstrip("/")
    artifact_name = normalized.removesuffix(".gz")
    if artifact_name in ROW_BEARING_JOBS_ARTIFACT_NAMES:
        return True
    if not normalized.startswith("data/"):
        return False
    rel_path = normalized[5:]
    safe_parts = [token for token in PurePosixPath(rel_path).parts if token not in {"", ".", ".."}]
    if len(safe_parts) != 1:
        return False
    return safe_parts[0].removesuffix(".gz") in ROW_BEARING_JOBS_ARTIFACT_NAMES


def _jobs_row_artifact_cleanup_report(
    *,
    backup_dir: Path,
    quarantined: list[str],
    failed: list[dict[str, str]],
) -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "createdAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "reason": "no_successful_runtime_jobs_report",
        "backupDir": str(backup_dir),
        "quarantined": quarantined,
        "failed": failed,
    }


def _write_jobs_row_artifact_cleanup_report(
    *,
    report_dir: Path,
    timestamp: str,
    report: dict[str, object],
    report_error: str,
) -> dict[str, object]:
    if report_error:
        report["reportError"] = report_error
        return report
    if not report_dir.exists():
        return report
    report_path = report_dir / f"stripped-packaged-jobs-cleanup-{timestamp}.json"
    try:
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        report["reportPath"] = str(report_path)
    except OSError as exc:
        report["reportError"] = str(exc)
    return report


def _jobs_row_artifact_backup_failures(
    candidates: list[Path],
    backup_dir: Path,
    error: BaseException,
) -> list[dict[str, str]]:
    return [
        {"path": str(source), "target": str(backup_dir / source.name), "error": str(error)}
        for source in candidates
    ]


def _move_jobs_row_artifacts_to_backup(
    candidates: list[Path],
    backup_dir: Path,
) -> tuple[list[str], list[dict[str, str]]]:
    quarantined: list[str] = []
    failed: list[dict[str, str]] = []
    for source in candidates:
        target = backup_dir / source.name
        if target.exists():
            target = backup_dir / f"{source.stem}-{len(quarantined) + 1}{source.suffix}"
        try:
            shutil.move(str(source), str(target))
        except (OSError, shutil.Error) as exc:
            failed.append({"path": str(source), "target": str(target), "error": str(exc)})
            continue
        quarantined.append(str(source))
    return quarantined, failed


def _restore_or_skip_missing_jobs_row_artifacts(data_path: Path) -> dict[str, object]:
    restore_result = _restore_false_quarantined_jobs_row_artifacts(data_path)
    if restore_result.get("restored") or restore_result.get("failed"):
        return {
            "quarantined": [],
            "failed": restore_result.get("failed", []),
            "skipped": "restored_false_quarantined_runtime_artifacts",
            "restored": restore_result.get("restored", []),
            "restoreReportPath": restore_result.get("reportPath", ""),
        }
    return {"quarantined": [], "failed": [], "skipped": "no_artifacts"}


def quarantine_stale_jobs_row_artifacts(data_dir: str | Path) -> dict[str, object]:
    data_path = Path(data_dir)
    candidates = _row_artifact_candidates(data_path)
    if not candidates:
        return _restore_or_skip_missing_jobs_row_artifacts(data_path)
    if has_successful_runtime_jobs_report(data_path):
        return {"quarantined": [], "failed": [], "skipped": "successful_runtime_report"}
    if _runtime_feed_artifacts_include_updates_after_migration(data_path):
        return {
            "quarantined": [],
            "failed": [],
            "skipped": "runtime_artifacts_newer_than_migration",
        }

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = data_path / "backups" / f"stripped-packaged-jobs-{timestamp}"
    report_dir = data_path / "migration-reports"
    quarantined: list[str] = []
    failed: list[dict[str, str]] = []
    report_error = ""

    try:
        report_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        report_error = str(exc)

    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        failed = _jobs_row_artifact_backup_failures(candidates, backup_dir, exc)
        report = _jobs_row_artifact_cleanup_report(
            backup_dir=backup_dir,
            quarantined=quarantined,
            failed=failed,
        )
        return _write_jobs_row_artifact_cleanup_report(
            report_dir=report_dir,
            timestamp=timestamp,
            report=report,
            report_error=report_error,
        )

    quarantined, failed = _move_jobs_row_artifacts_to_backup(candidates, backup_dir)
    report = _jobs_row_artifact_cleanup_report(
        backup_dir=backup_dir,
        quarantined=quarantined,
        failed=failed,
    )
    return _write_jobs_row_artifact_cleanup_report(
        report_dir=report_dir,
        timestamp=timestamp,
        report=report,
        report_error=report_error,
    )


def safe_quarantine_stale_jobs_row_artifacts(data_dir: str | Path) -> dict[str, object]:
    try:
        return quarantine_stale_jobs_row_artifacts(data_dir)
    except (RuntimeError, OSError, shutil.Error, TypeError, ValueError) as exc:
        _append_runtime_startup_trace(
            "jobs_row_artifact_quarantine_failed",
            dataDir=str(data_dir),
            error=str(exc),
        )
        return {
            "quarantined": [],
            "failed": [{"path": str(data_dir), "error": str(exc)}],
        }


def _normalize_bridge_runtime_config(
    bridge_host: str | None,
    bridge_port: str | int | None,
) -> tuple[str, int] | None:
    host = str(bridge_host or "").strip() or str(BRIDGE_DEFAULTS["host"])
    try:
        port = int(str(bridge_port or "").strip())
    except ValueError:
        return None
    if port <= 0 or port > 65535:
        return None
    return host, port


def _render_frontend_runtime_config_js(
    bridge_host: str, bridge_port: int, *, jobs_cold_start: bool = False
) -> str:
    payload = {
        "bridge": {
            "host": str(bridge_host),
            "port": int(bridge_port),
        },
        "security": {
            "github_app_enabled_default": bool(SECURITY_DEFAULTS["github_app_enabled_default"]),
        },
        "runtime": {
            "desktop": True,
            "jobsColdStart": bool(jobs_cold_start),
        },
    }
    rendered = json.dumps(payload, indent=2, ensure_ascii=True)
    return (
        "// Generated by src.ship.runtime_launcher for this desktop session.\n"
        "// Contains only frontend-safe runtime defaults.\n"
        f"globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({rendered});\n"
    )


class ProbeAwareSimpleHTTPRequestHandler(QuietSimpleHTTPRequestHandler):
    _directory: Path | None = None
    _runtime_data_dir: Path | None = None
    _static_data_dir: Path | None = None
    _bridge_runtime_config: tuple[str, int] | None = None
    _jobs_cold_start = False
    _startup_probe = False
    _serve_gzip_json = False

    def __init__(self, *args, **kwargs):
        directory = self.__class__._directory
        if directory is None:
            raise ValueError("Request handler directory was not configured.")
        super().__init__(*args, directory=str(directory), **kwargs)

    def _existing_data_candidate(self, base_dir: Path | None, rel_parts: list[str]) -> str | None:
        if base_dir is None:
            return None
        base = Path(base_dir).resolve()
        candidate = base.joinpath(*rel_parts).resolve()
        try:
            candidate.relative_to(base)
        except ValueError:
            return None
        if candidate.name.removesuffix(".gz") in PIPELINE_GZIP_JSON_NAMES:
            gzip_candidate = (
                candidate
                if candidate.suffix == ".gz"
                else candidate.with_name(candidate.name + ".gz")
            )
            if gzip_candidate.exists():
                self._serve_gzip_json = True
                return str(gzip_candidate.resolve())
        if candidate.exists():
            return str(candidate)
        return None

    def _missing_data_candidate(self, base_dir: Path | None, rel_parts: list[str]) -> str | None:
        if base_dir is None:
            return None
        base = Path(base_dir).resolve()
        candidate = base.joinpath(*rel_parts).resolve()
        try:
            candidate.relative_to(base)
        except ValueError:
            return None
        return str(candidate)

    def _resolve_data_path(self, rel_parts: list[str]) -> str | None:
        runtime_data_dir = self.__class__._runtime_data_dir
        static_data_dir = self.__class__._static_data_dir
        for base_dir in (runtime_data_dir, static_data_dir):
            candidate = self._existing_data_candidate(base_dir, rel_parts)
            if candidate:
                return candidate
        return self._missing_data_candidate(
            static_data_dir, rel_parts
        ) or self._missing_data_candidate(runtime_data_dir, rel_parts)

    def _resolve_static_data_path(self, normalized: str) -> str:
        self._serve_gzip_json = False
        relative_data_path = normalized.removeprefix("data/")
        data_parts = PurePosixPath(relative_data_path).parts
        artifact_name = PurePosixPath(relative_data_path).name.removesuffix(".gz")
        if artifact_name in PRIVATE_DATA_ARTIFACTS or data_parts[:1] == ("local-user-data",):
            return str(Path(self.directory or ".") / ".baluffo-private-artifact")
        if normalized in ROOT_DATA_FILE_ALIASES:
            candidate = self._resolve_data_path([normalized])
            return candidate or super().translate_path(normalized)
        if normalized.startswith("data/"):
            rel = normalized[5:]
            safe_parts = [
                token for token in PurePosixPath(rel).parts if token not in {"", ".", ".."}
            ]
            candidate = self._resolve_data_path(list(safe_parts))
            return candidate or super().translate_path(normalized)
        return super().translate_path(normalized)

    def translate_path(self, path: str) -> str:
        raw_path = str(path or "").split("?", 1)[0].split("#", 1)[0]
        normalized = raw_path.lstrip("/")
        return self._resolve_static_data_path(normalized)

    def guess_type(self, path: str) -> str:
        if self._serve_gzip_json:
            return "application/json; charset=utf-8"
        return super().guess_type(path)

    @classmethod
    def _jobs_cold_start_required_for_request(cls) -> bool:
        if not cls._jobs_cold_start:
            return False
        data_dir = cls._runtime_data_dir or cls._static_data_dir
        if data_dir is None:
            return True
        try:
            return jobs_cold_start_required_for_static_serving(data_dir)
        except (OSError, RuntimeError, TypeError, ValueError):
            return True

    def end_headers(self):
        # Desktop runtime should always load the latest local bundle assets.
        if self._serve_gzip_json:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        return super().end_headers()

    def do_GET(self):
        trace_enabled = bool(self.__class__._startup_probe and self.__class__._runtime_data_dir)
        path_only = str(getattr(self, "path", "") or "").split("?", 1)[0]
        trace_path = path_only.lstrip("/")
        request_started = time.perf_counter()
        bridge_runtime_config = self.__class__._bridge_runtime_config
        is_row_artifact_request = _is_row_bearing_jobs_artifact_request(trace_path)
        is_runtime_config_request = trace_path == "frontend-runtime-config.js"
        jobs_cold_start_required_now = False
        if is_row_artifact_request or is_runtime_config_request:
            jobs_cold_start_required_now = self.__class__._jobs_cold_start_required_for_request()
        if jobs_cold_start_required_now and is_row_artifact_request:
            self.send_error(404, "Jobs feed artifacts are unavailable during first-run bootstrap.")
            return
        if is_runtime_config_request and bridge_runtime_config:
            bridge_host, bridge_port = bridge_runtime_config
            body = _render_frontend_runtime_config_js(
                bridge_host,
                bridge_port,
                jobs_cold_start=jobs_cold_start_required_now,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if trace_enabled and trace_path in {"jobs.html", "saved.html", "admin.html"}:
            _append_startup_trace(
                Path(self.__class__._runtime_data_dir),
                "desktop_site_request_start",
                path=trace_path,
            )
        try:
            return super().do_GET()
        finally:
            if trace_enabled and trace_path in {"jobs.html", "saved.html", "admin.html"}:
                _append_startup_trace(
                    Path(self.__class__._runtime_data_dir),
                    "desktop_site_request_complete",
                    path=trace_path,
                    durationMs=int((time.perf_counter() - request_started) * 1000),
                )


def _make_probe_aware_simple_http_request_handler(
    directory: Path,
    *,
    runtime_data_dir: Path | None = None,
    static_data_dir: Path | None = None,
    startup_probe: bool = False,
    desktop_bridge_host: str | None = None,
    desktop_bridge_port: str | int | None = None,
    jobs_cold_start: bool = False,
):
    bridge_runtime_config = _normalize_bridge_runtime_config(
        desktop_bridge_host,
        desktop_bridge_port,
    )
    return type(
        "ConfiguredProbeAwareSimpleHTTPRequestHandler",
        (ProbeAwareSimpleHTTPRequestHandler,),
        {
            "_directory": Path(directory),
            "_runtime_data_dir": Path(runtime_data_dir).expanduser().resolve()
            if runtime_data_dir
            else None,
            "_static_data_dir": Path(static_data_dir).expanduser().resolve()
            if static_data_dir
            else None,
            "_bridge_runtime_config": bridge_runtime_config,
            "_jobs_cold_start": bool(jobs_cold_start),
            "_startup_probe": bool(startup_probe),
        },
    )


def build_site_request_handler(
    directory: Path,
    *,
    runtime_data_dir: Path | None = None,
    static_data_dir: Path | None = None,
    startup_probe: bool = False,
    desktop_bridge_host: str | None = None,
    desktop_bridge_port: str | int | None = None,
    jobs_cold_start: bool = False,
):
    return _make_probe_aware_simple_http_request_handler(
        directory,
        runtime_data_dir=runtime_data_dir,
        static_data_dir=static_data_dir,
        startup_probe=startup_probe,
        desktop_bridge_host=desktop_bridge_host,
        desktop_bridge_port=desktop_bridge_port,
        jobs_cold_start=jobs_cold_start,
    )


def resolve_root(root: str | Path | None = None) -> Path:
    return Path(root).expanduser().resolve() if root else ROOT


def resolve_runtime_layout(
    root: str | Path | None = None, *, data_dir: str | Path | None = None
) -> RuntimeLayout:
    bundle_root = resolve_root(root)
    default_paths = update_manager.ShipPaths.from_root(bundle_root)
    resolved_data_dir = Path(data_dir).expanduser().resolve() if data_dir else default_paths.data
    paths = update_manager.ShipPaths.from_root(bundle_root, data_dir=resolved_data_dir)
    update_manager.ensure_state(paths)
    current_version = paths.current.read_text(encoding="utf-8").strip()
    if not current_version:
        raise RuntimeError("Current version pointer is empty.")
    active_root = paths.versions / current_version
    if not active_root.exists():
        startup_check_result = update_manager.startup_check(bundle_root, resolved_data_dir)
        current_version = str(startup_check_result.get("current_version") or "").strip()
        if not current_version:
            raise RuntimeError("Current version pointer is empty.")
        active_root = paths.versions / current_version
        if not active_root.exists():
            raise RuntimeError(f"Active version directory not found: {active_root}")
    return RuntimeLayout(
        root=bundle_root,
        current_version=current_version,
        active_root=active_root,
        data_dir=resolved_data_dir,
    )


def _try_heal_required_files_from_repo(layout: RuntimeLayout) -> int:
    """Dev checkout: fill missing ``REQUIRED_VERSION_FILES`` from the repo beside ``src/ship``."""
    if getattr(sys, "frozen", False):
        return 0
    repo = Path(__file__).resolve().parents[2]
    copied = 0
    for rel in update_manager.REQUIRED_VERSION_FILES:
        dest = layout.active_root / rel
        if dest.exists():
            continue
        if rel.startswith("src/"):
            src = repo / rel
        else:
            src = repo / Path(rel).name
        if not src.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied += 1
    return copied


def _try_heal_required_files_from_meipass(layout: RuntimeLayout) -> int:
    """Frozen exe: copy missing required files from PyInstaller ``baluffo_embed`` payload."""
    if not getattr(sys, "frozen", False):
        return 0
    meipass = getattr(sys, "_MEIPASS", None)
    if not meipass:
        return 0
    base = Path(meipass) / "baluffo_embed"
    if not base.is_dir():
        return 0
    copied = 0
    for rel in update_manager.REQUIRED_VERSION_FILES:
        dest = layout.active_root / rel
        if dest.exists():
            continue
        name = Path(rel).name
        src = (base / "src" / name) if rel.startswith("src/") else (base / name)
        if not src.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied += 1
    return copied


def heal_active_ship_version(layout: RuntimeLayout) -> None:
    """Best-effort restore of missing critical files before health checks or static serving."""
    paths = update_manager.ShipPaths.from_root(layout.root)
    update_manager.repair_version_from_runtime_bootstrap(
        paths, layout.active_root, layout.current_version
    )
    _try_heal_required_files_from_repo(layout)
    _try_heal_required_files_from_meipass(layout)


@contextlib.contextmanager
def _pushd(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextlib.contextmanager
def _patched_argv(argv: list[str]) -> Iterator[None]:
    previous = list(sys.argv)
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = previous


@contextlib.contextmanager
def _patched_syspath(path: Path) -> Iterator[None]:
    token = str(path)
    inserted = token not in sys.path
    if inserted:
        sys.path.insert(0, token)
    try:
        yield
    finally:
        if inserted:
            with contextlib.suppress(ValueError):
                sys.path.remove(token)


@contextlib.contextmanager
def _isolated_src_package() -> Iterator[None]:
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "src" or name.startswith("src.")
    }
    for name in list(saved):
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def run_site_server(
    root: str | Path | None = None,
    *,
    port: int = int(DESKTOP_DEFAULTS["site_port"]),
    desktop_bridge_host: str | None = None,
    desktop_bridge_port: str | int | None = None,
) -> None:
    _append_runtime_startup_trace("desktop_site_layout_resolve_started")
    layout = resolve_runtime_layout(root)
    _append_runtime_startup_trace(
        "desktop_site_layout_resolved",
        activeRoot=str(layout.active_root),
        currentVersion=str(layout.current_version),
    )
    _append_runtime_startup_trace(
        "desktop_site_health_check_started",
        activeRoot=str(layout.active_root),
    )
    healthy_version, health_error = update_manager.health_check_version(layout.active_root)
    _append_runtime_startup_trace(
        "desktop_site_health_check_completed",
        activeRoot=str(layout.active_root),
        ok=bool(healthy_version),
        error=str(health_error or ""),
    )
    if not healthy_version:
        _append_runtime_startup_trace(
            "desktop_site_repair_started",
            activeRoot=str(layout.active_root),
        )
        heal_active_ship_version(layout)
        _append_runtime_startup_trace(
            "desktop_site_repair_completed",
            activeRoot=str(layout.active_root),
        )
    static_data_dir = update_manager.ShipPaths.from_root(layout.root).data
    runtime_data_env = str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
    runtime_data_dir = Path(runtime_data_env).expanduser().resolve() if runtime_data_env else None
    safe_quarantine_stale_jobs_row_artifacts(static_data_dir)
    if runtime_data_dir is not None and runtime_data_dir != static_data_dir.resolve():
        safe_quarantine_stale_jobs_row_artifacts(runtime_data_dir)
    jobs_cold_start = jobs_cold_start_required(runtime_data_dir or static_data_dir)
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "site",
                "appVersion": get_app_version(),
                "root": str(layout.root),
                "activeRoot": str(layout.active_root),
                "currentVersion": layout.current_version,
                "port": int(port),
            }
        )
    )
    handler = build_site_request_handler(
        layout.active_root,
        runtime_data_dir=runtime_data_dir,
        static_data_dir=static_data_dir,
        startup_probe=startup_probe_enabled(),
        desktop_bridge_host=desktop_bridge_host or os.environ.get(DESKTOP_BRIDGE_HOST_ENV),
        desktop_bridge_port=desktop_bridge_port or os.environ.get(DESKTOP_BRIDGE_PORT_ENV),
        jobs_cold_start=jobs_cold_start,
    )
    server = ThreadingHTTPServer(("127.0.0.1", int(port)), handler)
    _append_runtime_startup_trace(
        "desktop_site_server_listening",
        bindHost="127.0.0.1",
        port=int(port),
    )
    with server:
        server.serve_forever()


def run_bridge_server(
    root: str | Path | None = None,
    *,
    bind_host: str = "127.0.0.1",
    port: int = int(BRIDGE_DEFAULTS["port"]),
    data_dir: str | Path | None = None,
    desktop_mode: bool = False,
    owner_mode: str = "",
    owner_token: str = "",
    desktop_session_id: str = "",
    started_by: str = "",
    owner_idle_timeout_s: float = 0.0,
) -> None:
    _append_runtime_startup_trace("desktop_bridge_layout_resolve_started")
    layout = resolve_runtime_layout(root, data_dir=data_dir)
    _append_runtime_startup_trace(
        "desktop_bridge_layout_resolved",
        activeRoot=str(layout.active_root),
        currentVersion=str(layout.current_version),
        dataDir=str(layout.data_dir),
    )
    _append_runtime_startup_trace(
        "desktop_bridge_repair_started",
        activeRoot=str(layout.active_root),
    )
    heal_active_ship_version(layout)
    _append_runtime_startup_trace(
        "desktop_bridge_repair_completed",
        activeRoot=str(layout.active_root),
    )
    _append_runtime_startup_trace(
        "desktop_bridge_startup_check_started",
        activeRoot=str(layout.active_root),
        dataDir=str(layout.data_dir),
    )
    startup_check_result = update_manager.startup_check(layout.root, layout.data_dir)
    _append_runtime_startup_trace(
        "desktop_bridge_startup_check_completed",
        activeRoot=str(layout.active_root),
        dataDir=str(layout.data_dir),
        currentVersion=str(startup_check_result.get("current_version") or ""),
        rolledBack=bool(startup_check_result.get("rolled_back")),
        repairedPointer=bool(startup_check_result.get("repaired_pointer")),
        bootstrapRepair=int(startup_check_result.get("bootstrap_repair") or 0),
    )
    checked_version = str(startup_check_result.get("current_version") or "").strip()
    if checked_version and checked_version != layout.current_version:
        updated_active_root = (
            update_manager.ShipPaths.from_root(layout.root, data_dir=layout.data_dir).versions
            / checked_version
        )
        layout = RuntimeLayout(
            root=layout.root,
            current_version=checked_version,
            active_root=updated_active_root,
            data_dir=layout.data_dir,
        )
    safe_quarantine_stale_jobs_row_artifacts(layout.data_dir)
    bridge_script = layout.active_root / "src" / "admin_bridge.py"
    if not bridge_script.exists():
        raise RuntimeError(f"Admin bridge entrypoint not found: {bridge_script}")
    install_root = layout.root.parent if layout.root.name.lower() == "ship" else layout.root
    os.environ["BALUFFO_DATA_DIR"] = str(layout.data_dir)
    os.environ["BALUFFO_SHIP_ROOT"] = str(layout.root)
    os.environ["BALUFFO_INSTALL_ROOT"] = str(install_root)
    if desktop_mode:
        os.environ["BALUFFO_DESKTOP_MODE"] = "1"
    else:
        os.environ.pop("BALUFFO_DESKTOP_MODE", None)
    if str(desktop_session_id or "").strip():
        os.environ["BALUFFO_BRIDGE_SESSION_ID"] = str(desktop_session_id)
    else:
        os.environ.pop("BALUFFO_BRIDGE_SESSION_ID", None)
    argv = [
        str(bridge_script),
        "--host",
        str(bind_host),
        "--port",
        str(port),
        "--data-dir",
        str(layout.data_dir),
        "--desktop-mode" if desktop_mode else "",
        "--log-format",
        "human",
        "--log-level",
        "info",
    ]
    if str(owner_mode or "").strip():
        argv.extend(["--owner-mode", str(owner_mode)])
    if str(owner_token or "").strip():
        argv.extend(["--owner-token", str(owner_token)])
    if str(desktop_session_id or "").strip():
        argv.extend(["--desktop-session-id", str(desktop_session_id)])
    if str(started_by or "").strip():
        argv.extend(["--started-by", str(started_by)])
    if float(owner_idle_timeout_s or 0.0) > 0.0:
        argv.extend(["--owner-idle-timeout-s", str(float(owner_idle_timeout_s))])
    argv = [item for item in argv if str(item).strip()]
    with (
        _pushd(layout.active_root),
        _patched_syspath(layout.active_root),
        _isolated_src_package(),
        _patched_argv(argv),
    ):
        runpy.run_path(str(bridge_script), run_name="__main__")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo ship runtime launcher.")
    sub = parser.add_subparsers(dest="command", required=True)

    site_parser = sub.add_parser("site", help="Run the static site from the active app version.")
    site_parser.add_argument("--root", default="")
    site_parser.add_argument("--port", type=int, default=int(DESKTOP_DEFAULTS["site_port"]))

    bridge_parser = sub.add_parser(
        "bridge", help="Run the admin bridge from the active app version."
    )
    bridge_parser.add_argument("--root", default="")
    bridge_parser.add_argument("--bind-host", default=str(BRIDGE_DEFAULTS["host"]))
    bridge_parser.add_argument("--port", type=int, default=int(BRIDGE_DEFAULTS["port"]))
    bridge_parser.add_argument("--data-dir", default="")
    bridge_parser.add_argument("--owner-mode", default="")
    bridge_parser.add_argument("--owner-token", default="")
    bridge_parser.add_argument("--desktop-session-id", default="")
    bridge_parser.add_argument("--started-by", default="")
    bridge_parser.add_argument("--owner-idle-timeout-s", type=float, default=0.0)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "site":
            run_site_server(args.root or None, port=int(args.port))
            return 0
        if args.command == "bridge":
            run_bridge_server(
                args.root or None,
                bind_host=str(args.bind_host),
                port=int(args.port),
                data_dir=args.data_dir or None,
                desktop_mode=str(os.environ.get("BALUFFO_DESKTOP_MODE") or "").strip().lower()
                in {"1", "true", "yes", "on"},
                owner_mode=str(args.owner_mode or ""),
                owner_token=str(args.owner_token or ""),
                desktop_session_id=str(args.desktop_session_id or ""),
                started_by=str(args.started_by or ""),
                owner_idle_timeout_s=float(args.owner_idle_timeout_s or 0.0),
            )
        return 0
    except KeyboardInterrupt:
        return 0
    except _EXPECTED_RUNTIME_LAUNCHER_CLI_EXCEPTIONS as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
