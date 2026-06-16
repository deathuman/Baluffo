from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from src.bridge.server import runtime_state as bridge_runtime_state
from src.local_data_store import LocalDataPaths, LocalDataStore
from src.shared.utils import parse_iso as parse_iso_from_utils

root: Any | None = None
_PROC_ROOT = Path("/proc")


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("admin bridge root is not bound")
    return root


def log_enabled(level: str) -> bool:
    root_mod = _require_root()
    current = root_mod.LOG_LEVEL_ORDER.get(
        root_mod._normalize_log_level(root_mod.RUNTIME_CONFIG.log_level), 20
    )
    target = root_mod.LOG_LEVEL_ORDER.get(root_mod._normalize_log_level(level), 20)
    return bool(target >= current)


def bridge_log(level: str, message: str, **fields: Any) -> None:
    root_mod = _require_root()
    normalized_level = root_mod._normalize_log_level(level, "info")
    if not root_mod._log_enabled(normalized_level):
        return
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "level": normalized_level,
        "message": str(message or ""),
        **{key: value for key, value in fields.items() if value is not None and value != ""},
    }
    try:
        event = root_mod.diagnostic_events.build_bridge_event(
            normalized_level,
            str(message or ""),
            fields,
            payload["ts"],
        )
        root_mod.diagnostic_events.append_bridge_event(root_mod.ADMIN_BRIDGE_EVENTS_PATH, event)
    except Exception:
        pass
    if root_mod._normalize_log_format(root_mod.RUNTIME_CONFIG.log_format) == "jsonl":
        try:
            print(json.dumps(payload, ensure_ascii=False), flush=True)
        except OSError:
            pass
        return
    field_text = " ".join(
        f"{key}={value}" for key, value in payload.items() if key not in {"ts", "level", "message"}
    )
    line = f"[admin_bridge][{normalized_level.upper()}] {payload['message']}"
    if field_text:
        line = f"{line} {field_text}"
    try:
        print(line, flush=True)
    except OSError:
        pass


def configure_runtime_paths(config: Any) -> None:
    root_mod = _require_root()
    root_mod.RUNTIME_CONFIG = config
    data_dir = Path(config.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    root_mod.OPS_HISTORY_PATH = data_dir / "admin-run-history.json"
    root_mod.TASK_LIFECYCLE_PATH = data_dir / "admin-task-lifecycle.json"
    root_mod.OPS_ALERT_STATE_PATH = data_dir / "admin-alert-state.json"
    root_mod.JOBS_FETCH_REPORT_PATH = data_dir / "jobs-fetch-report.json"
    root_mod.ADMIN_ACTIVE_TASK_SNAPSHOT_PATH = data_dir / "admin-active-task-snapshot.json"
    root_mod.JOBS_PIPELINE_SCHEDULE_CONFIG_PATH = data_dir / "jobs-pipeline-schedule-config.json"
    root_mod.SOURCE_POLICY_RECOMMENDATIONS_PATH = data_dir / "source-policy-recommendations.json"
    root_mod.SOURCE_POLICY_REVIEW_STATE_PATH = data_dir / "source-policy-review-state.json"
    root_mod.DEDUP_REVIEW_STATE_PATH = data_dir / "dedup-review-state.json"
    root_mod.JOBS_FETCH_TASKS_PATH = data_dir / "jobs-fetch-tasks.json"
    root_mod.TASK_STATE_PATH = data_dir / "admin-task-state.json"
    root_mod.SYNC_LIVE_TASK_PATH = data_dir / "sync-live-task.json"
    root_mod.DISCOVERY_LOG_PATH = data_dir / "source-discovery.log"
    root_mod.FETCHER_LOG_PATH = data_dir / "jobs-fetcher.log"
    root_mod.ADMIN_BRIDGE_EVENTS_PATH = data_dir / "admin-bridge-events.jsonl"
    root_mod.SYNC_CONFIG_PATH = data_dir / root_mod.SYNC_CONFIG_PATH_DEFAULT.name
    root_mod.DISCOVERY_CONFIG_PATH = data_dir / "source-discovery-config.json"
    root_mod.SYNC_RUNTIME_PATH = data_dir / root_mod.SYNC_RUNTIME_PATH_DEFAULT.name
    root_mod.STARTUP_METRICS_PATH = data_dir / "desktop-startup-metrics.jsonl"
    root_mod.DESKTOP_UPDATE_STATE_PATH = data_dir / "updater" / "install-state.json"
    root_mod.ACTIVE_PATH = data_dir / "source-registry-active.json"
    root_mod.PENDING_PATH = data_dir / "source-registry-pending.json"
    root_mod.DEFAULTS_DIR = data_dir / "defaults"
    root_mod.ACTIVE_SEED_PATH = root_mod.DEFAULTS_DIR / "source-registry-active.seed.json"
    root_mod.PENDING_SEED_PATH = root_mod.DEFAULTS_DIR / "source-registry-pending.seed.json"
    root_mod.REJECTED_PATH = data_dir / "source-registry-rejected.json"
    root_mod.DISCOVERY_CANDIDATES_PATH = data_dir / "source-discovery-candidates.json"
    root_mod.TOMBSTONES_PATH = data_dir / "source-registry-tombstones.json"
    root_mod.DISCOVERY_REPORT_PATH = data_dir / "source-discovery-report.json"
    root_mod.APPROVAL_STATE_PATH = data_dir / "source-approval-state.json"
    root_mod.TASKS_CONFIG_PATH = Path(config.root) / ".vscode" / "tasks.json"

    root_mod.source_registry_module.DATA_DIR = data_dir
    root_mod.source_registry_module.DEFAULTS_DIR = root_mod.DEFAULTS_DIR
    root_mod.source_registry_module.ACTIVE_PATH = root_mod.ACTIVE_PATH
    root_mod.source_registry_module.PENDING_PATH = root_mod.PENDING_PATH
    root_mod.source_registry_module.ACTIVE_SEED_PATH = root_mod.ACTIVE_SEED_PATH
    root_mod.source_registry_module.PENDING_SEED_PATH = root_mod.PENDING_SEED_PATH
    root_mod.source_registry_module.REJECTED_PATH = root_mod.REJECTED_PATH
    root_mod.source_registry_module.TOMBSTONES_PATH = root_mod.TOMBSTONES_PATH
    root_mod.source_registry_module.DISCOVERY_REPORT_PATH = root_mod.DISCOVERY_REPORT_PATH
    root_mod.source_registry_module.DISCOVERY_CANDIDATES_PATH = root_mod.DISCOVERY_CANDIDATES_PATH
    root_mod.source_registry_module.APPROVAL_STATE_PATH = root_mod.APPROVAL_STATE_PATH
    bridge_runtime_state.configure_runtime_paths(
        startup_metrics_path=root_mod.STARTUP_METRICS_PATH,
        desktop_local_data_store=LocalDataStore(LocalDataPaths.from_data_dir(data_dir)),
        now_iso=root_mod.now_iso,
        owner_mode=config.owner_mode,
        owner_token=config.owner_token,
        desktop_session_id=config.desktop_session_id,
        started_by=config.started_by,
        owner_idle_timeout_s=config.owner_idle_timeout_s,
    )
    with root_mod._REGISTRY_SERVICE_LOCK:
        root_mod._REGISTRY_SERVICE = None
        root_mod._REGISTRY_SERVICE_PATHS = None
    with root_mod._DISCOVERY_SERVICE_LOCK:
        root_mod._DISCOVERY_SERVICE = None
        root_mod._DISCOVERY_SERVICE_PATHS = None
    with root_mod._PIPELINE_SERVICE_LOCK:
        root_mod._PIPELINE_SERVICE = None
    with root_mod._DESKTOP_UPDATE_SERVICE_LOCK:
        root_mod._DESKTOP_UPDATE_SERVICE = None
        root_mod._DESKTOP_UPDATE_SERVICE_DATA_DIR = None


def startup_banner(config: Any) -> None:
    root_mod = _require_root()
    root_mod.bridge_config.startup_banner(config=config, bridge_log=root_mod.bridge_log)


def append_startup_metric(event: str, payload: dict[str, Any] | None = None) -> None:
    root_mod = _require_root()
    bridge_runtime_state.append_startup_metric(event, payload, now_iso=root_mod.now_iso)


def read_startup_metrics(limit: int = 200) -> list[dict[str, Any]]:
    return bridge_runtime_state.read_startup_metrics(limit)


def get_desktop_session_payload() -> dict[str, Any]:
    return bridge_runtime_state.get_desktop_session_payload()


def update_desktop_session_lifecycle(
    *, owner_token: str, session_id: str, page_id: str, state: str, reason: str = ""
) -> tuple[int, dict[str, Any]]:
    root_mod = _require_root()
    previous_session = bridge_runtime_state.get_desktop_session_payload()
    previous_shutdown_reason = str(previous_session.get("shutdownReason") or "").strip().lower()
    previous_shutdown_page_id = str(previous_session.get("shutdownPageId") or "").strip()
    status_code, result = bridge_runtime_state.update_desktop_session_lifecycle(
        owner_token=owner_token,
        session_id=session_id,
        page_id=page_id,
        state=state,
        reason=reason,
        now_iso=root_mod.now_iso,
    )
    normalized_reason = str(reason or "").strip().lower()
    if (
        status_code == 200
        and str(state or "").strip().lower() == "alive"
        and previous_shutdown_reason == bridge_runtime_state.ACTIVE_WORK_CLOSE_ATTEMPT_REASON
        and (
            not previous_shutdown_page_id or previous_shutdown_page_id == str(page_id or "").strip()
        )
    ):
        owner_state = bridge_runtime_state.get_owner_state()
        append_startup_metric(
            "desktop_active_work_close_attempt_cleared",
            {
                "sessionId": str(owner_state.get("sessionId") or session_id or ""),
                "pageId": str(page_id or ""),
                "reason": bridge_runtime_state.ACTIVE_WORK_CLOSE_ATTEMPT_REASON,
            },
        )
    if (
        status_code == 200
        and str(state or "").strip().lower() == "closing"
        and normalized_reason
        in {
            bridge_runtime_state.CONFIRMED_ACTIVE_WORK_CLOSE_REASON,
            bridge_runtime_state.ACTIVE_WORK_CLOSE_ATTEMPT_REASON,
            *bridge_runtime_state.REGULAR_DESKTOP_CLOSE_REASONS,
        }
    ):
        owner_state = bridge_runtime_state.get_owner_state()
        payload = {
            "sessionId": str(owner_state.get("sessionId") or session_id or ""),
            "pageId": str(page_id or ""),
            "reason": normalized_reason,
        }
        if normalized_reason == bridge_runtime_state.CONFIRMED_ACTIVE_WORK_CLOSE_REASON:
            event_name = "desktop_confirmed_active_work_shutdown_requested"
            log_event_name = "admin_bridge_confirmed_active_work_shutdown_requested"
        elif normalized_reason == bridge_runtime_state.ACTIVE_WORK_CLOSE_ATTEMPT_REASON:
            event_name = "desktop_active_work_close_attempt_requested"
            log_event_name = "admin_bridge_desktop_active_work_close_attempt_requested"
        else:
            event_name = "desktop_regular_close_shutdown_requested"
            log_event_name = "admin_bridge_desktop_regular_close_shutdown_requested"
        root_mod.bridge_log(
            "info",
            log_event_name,
            session_id=payload["sessionId"],
            page_id=payload["pageId"],
            reason=payload["reason"],
        )
        append_startup_metric(event_name, payload)
    return status_code, result


def _desktop_update_handoff_active(root_mod: Any) -> bool:
    try:
        status = root_mod._get_desktop_update_service().get_status_payload()
    except Exception:
        return False
    if not isinstance(status, dict):
        return False
    download_state = str(status.get("downloadState") or "").strip().lower()
    install_state = str(status.get("installState") or "").strip().lower()
    if download_state == "downloading":
        return True
    return install_state in {"handoff_requested", "waiting_for_exit", "installing", "verifying"}


def owner_session_should_exit() -> bool:
    root_mod = _require_root()
    expired = bridge_runtime_state.owner_session_should_exit(
        parse_iso=root_mod.parse_iso,
        now_utc=root_mod.now_utc,
    )
    if expired:
        try:
            active_tasks_payload = root_mod._get_ops_api().get_current_task_state_payload()
            active_tasks = [
                {
                    "taskType": str(task.get("taskType") or task.get("type") or "").strip().lower(),
                    "runId": str(task.get("runId") or "").strip(),
                }
                for task in (
                    active_tasks_payload.get("tasks")
                    if isinstance(active_tasks_payload.get("tasks"), list)
                    else []
                )
                if isinstance(task, dict)
                and bool(task.get("active"))
                and str(task.get("taskType") or task.get("type") or "").strip().lower()
                in {"fetch", "discovery", "pipeline", "sync"}
            ]
        except Exception:
            active_tasks = []
        shutdown_reason = (
            str(bridge_runtime_state.get_desktop_session_payload().get("shutdownReason") or "")
            .strip()
            .lower()
        )
        if _desktop_update_handoff_active(root_mod):
            owner_state = bridge_runtime_state.get_owner_state()
            root_mod.bridge_log(
                "info",
                "admin_bridge_owner_session_exit_suppressed_for_update_handoff",
                owner_mode=str(owner_state.get("ownerMode") or ""),
                owner_token=str(owner_state.get("ownerToken") or ""),
                session_id=str(owner_state.get("sessionId") or ""),
                shutdown_reason=shutdown_reason,
            )
            return False
        if (
            active_tasks
            and shutdown_reason != bridge_runtime_state.CONFIRMED_ACTIVE_WORK_CLOSE_REASON
        ):
            owner_state = bridge_runtime_state.get_owner_state()
            root_mod.bridge_log(
                "info",
                "admin_bridge_owner_session_exit_suppressed_for_active_tasks",
                owner_mode=str(owner_state.get("ownerMode") or ""),
                owner_token=str(owner_state.get("ownerToken") or ""),
                session_id=str(owner_state.get("sessionId") or ""),
                active_tasks=active_tasks,
            )
            return False
        owner_state = bridge_runtime_state.get_owner_state()
        root_mod.bridge_log(
            "info",
            "admin_bridge_owner_session_exit_requested",
            owner_mode=str(owner_state.get("ownerMode") or ""),
            owner_token=str(owner_state.get("ownerToken") or ""),
            session_id=str(owner_state.get("sessionId") or ""),
            started_by=str(owner_state.get("startedBy") or ""),
            last_activity_at=str(owner_state.get("lastActivityAt") or ""),
            idle_timeout_seconds=float(owner_state.get("idleTimeoutSeconds") or 0.0),
        )
    return expired


def parse_iso(value: Any) -> datetime | None:
    return parse_iso_from_utils(value)


def _posix_pid_is_zombie(pid: int) -> bool:
    stat_path = _PROC_ROOT / str(int(pid)) / "stat"
    try:
        stat_text = stat_path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        stat_text = ""
    if stat_text:
        try:
            state = stat_text.rsplit(") ", 1)[1].split(maxsplit=1)[0]
        except (IndexError, ValueError):
            state = ""
        if state.upper() == "Z":
            return True

    status_path = _PROC_ROOT / str(int(pid)) / "status"
    try:
        for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("State:"):
                return line.split(":", 1)[1].strip().upper().startswith("Z")
    except OSError:
        return False
    return False


def pid_is_running(pid: int) -> bool:
    root_mod = _require_root()
    if int(pid or 0) <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            process_query_limited_information = 0x1000
            still_active = 259
            handle = ctypes.windll.kernel32.OpenProcess(
                process_query_limited_information,
                False,
                int(pid),
            )
            if not handle:
                return False
            exit_code = wintypes.DWORD()
            try:
                if not ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return False
                return int(exit_code.value) == still_active
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except (OSError, TypeError, ValueError, AttributeError):
            return False
    try:
        root_mod.os.kill(int(pid), 0)
    except OSError:
        return False
    return not _posix_pid_is_zombie(int(pid))


def desktop_local_data_store() -> LocalDataStore:
    return cast(LocalDataStore, bridge_runtime_state.get_desktop_local_data_store())
