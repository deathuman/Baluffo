from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

from src.source_registry_io import (
    cleanup_runtime_evidence_journals,
    compact_registry_journals,
    load_runtime_evidence,
)

JsonObject = dict[str, Any]


class _SyncStateLike(Protocol):
    def set_sync_status(
        self,
        *,
        action: str = "",
        result: str = "",
        error: str = "",
        pulled: bool = False,
        pushed: bool = False,
    ) -> None: ...

    def save_sync_runtime_state(self, payload: JsonObject) -> None: ...


class _SyncServiceLike(Protocol):
    def get_sync_status_payload(self) -> JsonObject: ...
    def _sync_guard(self) -> JsonObject | None: ...
    def sync_pull_sources(self) -> JsonObject: ...
    def sync_push_sources(self) -> JsonObject: ...
    def startup_sync_pull(self) -> None: ...
    def schedule_startup_sync_pull(self) -> JsonObject: ...
    def sync_task_running(self) -> bool: ...
    def wait_for_sync_tasks(self, timeout_s: float = 5.0) -> None: ...


class _RegistrySyncFlowLike(Protocol):
    def maybe_trigger_auto_sync_push(
        self,
        *,
        reason: str,
        sync_guard: Callable[[], JsonObject | None],
        sync_task_running: Callable[[], bool],
        start_sync_task: Callable[..., JsonObject],
    ) -> bool: ...


class _SyncTaskFlowLike(Protocol):
    def run_sync_task_worker(self, **kwargs: Any) -> None: ...


class _RunHistoryApiLike(Protocol):
    SyncHistoryDeps: Callable[..., Any]

    def reconcile_sync_history_locked(self, deps: Any) -> None: ...


class _PipelineServiceLike(Protocol):
    def get_status_payload(self) -> JsonObject: ...

    def wait_for_report_completion(
        self,
        *,
        report_path: Any,
        started_at: str,
        timeout_s: float,
        report_name: str,
        load_json_object: Callable[..., JsonObject],
        report_is_stale_in_progress: Callable[..., bool],
        fail_on_stale: bool = False,
    ) -> JsonObject: ...

    def start_task(self, payload: JsonObject | None = None) -> JsonObject: ...


class _TaskLaunchApiLike(Protocol):
    def start_fetcher_task(
        self, payload: JsonObject | None = None, **kwargs: Any
    ) -> JsonObject: ...

    def start_jobs_bootstrap_task(
        self, payload: JsonObject | None = None, **kwargs: Any
    ) -> JsonObject: ...


class _AdminTaskRuntimeRoot(Protocol):
    TASKS_CONFIG_PATH: Path
    OPS_STATE_LOCK: Any
    SYNC_STATE_LOCK: Any
    _run_history_api: _RunHistoryApiLike
    _registry_sync_flow: _RegistrySyncFlowLike
    _sync_task_flow: _SyncTaskFlowLike
    SyncState: Any
    parse_iso: Callable[[Any], Any]
    now_iso: Callable[[], str]
    now_utc: Callable[[], datetime]
    load_run_history: Callable[..., Any]
    save_run_history: Callable[..., Any]
    save_json_atomic: Callable[..., Any]
    prune_started_rows_for_type: Callable[..., Any]
    clear_task_state: Callable[..., Any]
    _clear_task_state_locked: Callable[..., Any]
    upsert_run_history: Callable[..., Any]
    task_running_from_state: Callable[..., bool]
    report_is_stale_in_progress: Callable[..., bool]
    load_json_object: Callable[..., JsonObject]
    normalize_fetch_report_contract: Callable[[JsonObject], JsonObject]
    normalize_discovery_report_contract: Callable[[JsonObject], JsonObject]
    summarize_fetch_report: Callable[[JsonObject], JsonObject]
    summarize_discovery_report: Callable[[JsonObject], JsonObject]
    JOBS_FETCH_REPORT_PATH: Any
    JOBS_FETCH_TASKS_PATH: Any
    DISCOVERY_REPORT_PATH: Any
    TASK_STATE_PATH: Any
    _sync_guard: Callable[[], JsonObject | None]
    sync_task_running: Callable[[], bool]
    start_sync_task: Callable[..., JsonObject]
    _set_sync_status: Callable[..., None]
    bridge_log: Callable[..., Any]
    SYNC_LIVE_TASK_PATH: Any
    SCHEMA_VERSION: int
    append_run_history: Callable[..., Any]
    run_background_script: Callable[..., Any]
    sync_history_from_reports: Callable[[], list[JsonObject]]
    sync_pull_sources: Callable[[], JsonObject]
    sync_push_sources: Callable[[], JsonObject]
    threading: Any

    def _get_sync_state(self) -> _SyncStateLike: ...
    def _get_sync_service(self) -> _SyncServiceLike: ...
    def _get_pipeline_service(self) -> _PipelineServiceLike: ...
    def _get_job_availability_service(self) -> Any: ...
    def _get_task_launch_api(self) -> _TaskLaunchApiLike: ...


def read_tasks_config(*, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    try:
        parsed = json.loads(root_mod.TASKS_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def background_process_flags(subprocess_mod: Any) -> JsonObject:
    return {
        "create_no_window": int(getattr(subprocess_mod, "CREATE_NO_WINDOW", 0)),
        "create_new_process_group": int(getattr(subprocess_mod, "CREATE_NEW_PROCESS_GROUP", 0)),
    }


def set_sync_status(
    *,
    action: str = "",
    result: str = "",
    error: str = "",
    pulled: bool = False,
    pushed: bool = False,
    root_mod: Any,
) -> None:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    root_mod._get_sync_state().set_sync_status(
        action=action,
        result=result,
        error=error,
        pulled=bool(pulled),
        pushed=bool(pushed),
    )


def get_sync_status_payload(*, root_mod: Any) -> JsonObject:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().get_sync_status_payload()


def sync_guard(*, root_mod: Any) -> JsonObject | None:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service()._sync_guard()  # noqa: SLF001


def sync_pull_sources(*, root_mod: Any) -> JsonObject:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().sync_pull_sources()


def sync_push_sources(*, root_mod: Any) -> JsonObject:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().sync_push_sources()


def startup_sync_pull(*, root_mod: Any) -> None:
    cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().startup_sync_pull()


def schedule_startup_sync_pull(*, root_mod: Any) -> JsonObject:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().schedule_startup_sync_pull()


def on_bridge_started(*, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    runtime_journals = cleanup_runtime_evidence_journals(
        Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    )
    if runtime_journals.get("quarantined") or runtime_journals.get("errors"):
        root_mod.bridge_log(
            "info" if not runtime_journals.get("errors") else "warn",
            "runtime_evidence_journal_startup_cleanup",
            checked=runtime_journals.get("checked", 0),
            quarantined=len(runtime_journals.get("quarantined") or []),
            errors=len(runtime_journals.get("errors") or []),
        )
    registry_journals = compact_registry_journals(Path(root_mod.RUNTIME_CONFIG.data_dir).resolve())
    if registry_journals.get("compacted") or registry_journals.get("errors"):
        root_mod.bridge_log(
            "info" if not registry_journals.get("errors") else "warn",
            "registry_journal_startup_maintenance",
            checked=registry_journals.get("checked", 0),
            compacted=len(registry_journals.get("compacted") or []),
            errors=len(registry_journals.get("errors") or []),
        )
    cleanup = root_mod.cleanup_stale_startup_tasks()
    startup_sync = root_mod.schedule_startup_sync_pull()
    pipeline_schedule = root_mod.admin_entrypoint_services_mod.get_pipeline_schedule_service(
        root_mod=root_mod
    ).start_background_polling()
    return {
        "runtimeEvidenceJournals": runtime_journals,
        "registryJournals": registry_journals,
        "cleanup": cleanup,
        "startupSync": startup_sync,
        "pipelineSchedule": pipeline_schedule,
    }


def sync_task_running(*, root_mod: Any) -> bool:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    with root_mod.OPS_STATE_LOCK:
        root_mod._run_history_api.reconcile_sync_history_locked(
            root_mod._run_history_api.SyncHistoryDeps(
                ops_state_lock=root_mod.OPS_STATE_LOCK,
                load_run_history=root_mod.load_run_history,
                save_run_history=root_mod.save_run_history,
                save_json_atomic=root_mod.save_json_atomic,
                prune_started_rows_for_type=root_mod.prune_started_rows_for_type,
                clear_task_state=root_mod.clear_task_state,
                clear_task_state_locked=root_mod._clear_task_state_locked,
                upsert_run_history=root_mod.upsert_run_history,
                task_running_from_state=root_mod.task_running_from_state,
                report_is_stale_in_progress=root_mod.report_is_stale_in_progress,
                load_json_object=root_mod.load_json_object,
                load_runtime_evidence=root_mod.load_runtime_evidence,
                normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
                normalize_discovery_report_contract=root_mod.normalize_discovery_report_contract,
                summarize_fetch_report=root_mod.summarize_fetch_report,
                summarize_discovery_report=root_mod.summarize_discovery_report,
                jobs_fetch_report_path=root_mod.JOBS_FETCH_REPORT_PATH,
                jobs_fetch_tasks_path=root_mod.JOBS_FETCH_TASKS_PATH,
                discovery_report_path=root_mod.DISCOVERY_REPORT_PATH,
                task_state_path=root_mod.TASK_STATE_PATH,
                get_active_sync_runs=root_mod.SyncState.get_active_sync_runs,
                parse_iso=root_mod.parse_iso,
                now_iso=root_mod.now_iso,
                now_utc=root_mod.now_utc,
                pid_is_running=root_mod.pid_is_running,
                get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
            )
        )
    return root_mod._get_sync_service().sync_task_running()


def wait_for_sync_tasks(timeout_s: float = 5.0, *, root_mod: Any) -> None:
    cast(_AdminTaskRuntimeRoot, root_mod)._get_sync_service().wait_for_sync_tasks(
        timeout_s=float(timeout_s)
    )


def mark_discovery_sync_finished(finished_at: str, *, root_mod: Any) -> None:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    with root_mod.SYNC_STATE_LOCK:
        root_mod._get_sync_state().save_sync_runtime_state(
            {"lastDiscoverySyncFinishedAt": str(finished_at or "")}
        )


def maybe_trigger_auto_sync_push(reason: str, *, root_mod: Any) -> bool:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    return root_mod._registry_sync_flow.maybe_trigger_auto_sync_push(
        reason=reason,
        sync_guard=root_mod._sync_guard,
        sync_task_running=root_mod.sync_task_running,
        start_sync_task=root_mod.start_sync_task,
    )


def migrate_legacy_task_state_to_lifecycle(*, root_mod: Any) -> JsonObject:
    """Migration function: import legacy task state/run-history into the lifecycle ledger.

    This is an explicit migration function, not a normal lifecycle path.  Call
    it once after upgrading from a version that used admin-task-state.json /
    admin-run-history.json as lifecycle authority.
    """
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    history_rows = root_mod.load_run_history()
    task_state = root_mod.load_json_object(root_mod.TASK_STATE_PATH, {})
    rows = root_mod.reconcile_lifecycle_from_legacy(
        history_rows=history_rows,
        task_state=task_state if isinstance(task_state, dict) else {},
        pid_is_running=root_mod.pid_is_running,
    )
    return {
        "ok": True,
        "rowCount": len(rows),
        "historyRows": len(history_rows),
        "taskStateRows": len(task_state) if isinstance(task_state, dict) else 0,
    }


def run_sync_task_worker(
    run_id: str,
    action: str,
    started_at: str,
    *,
    reason: str = "",
    automatic: bool = False,
    root_mod: Any,
) -> None:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    root_mod._sync_task_flow.run_sync_task_worker(
        run_id=run_id,
        action=action,
        started_at=started_at,
        reason=reason,
        automatic=automatic,
        parse_iso=root_mod.parse_iso,
        now_utc=root_mod.now_utc,
        run_sync_pull=root_mod.sync_pull_sources,
        run_sync_push=root_mod.sync_push_sources,
        set_sync_status=root_mod._set_sync_status,
        remove_active_sync_run=root_mod.SyncState.remove_active_sync_run,
        remove_active_sync_thread=root_mod.SyncState.remove_active_sync_thread,
        prune_started_rows_for_type=lambda _entry_type, *, finished_at: None,
        upsert_run_history=lambda entry: (
            root_mod.fail_lifecycle_run(
                str(entry.get("runId") or run_id or ""),
                "sync",
                finished_at=str(entry.get("finishedAt") or ""),
                terminal_reason="failed",
                summary=dict(entry.get("summary") or {}),
            )
            if str(entry.get("status") or "").strip().lower() == "error"
            else root_mod.finish_lifecycle_run(
                str(entry.get("runId") or run_id or ""),
                "sync",
                finished_at=str(entry.get("finishedAt") or ""),
                terminal_reason="completed",
                summary=dict(entry.get("summary") or {}),
            )
        ),
        bridge_log=root_mod.bridge_log,
        save_json_atomic=root_mod.save_json_atomic,
        live_task_path=root_mod.SYNC_LIVE_TASK_PATH,
        active_snapshot_path=Path(
            getattr(
                root_mod,
                "ADMIN_ACTIVE_TASK_SNAPSHOT_PATH",
                Path(root_mod.RUNTIME_CONFIG.data_dir) / "admin-active-task-snapshot.json",
            )
        ),
    )


def current_fetch_output_count(*, root_mod: Any) -> int:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    report = root_mod.normalize_fetch_report_contract(
        load_runtime_evidence(root_mod.JOBS_FETCH_REPORT_PATH, {})
    )
    summary = root_mod.summarize_fetch_report(report)
    return int(summary.get("outputCount") or 0)


def get_jobs_pipeline_status_payload(*, root_mod: Any) -> JsonObject:
    return cast(_AdminTaskRuntimeRoot, root_mod)._get_pipeline_service().get_status_payload()


def wait_for_report_completion(
    *,
    report_path: Any,
    started_at: str,
    timeout_s: float,
    report_name: str,
    fail_on_stale: bool = False,
    root_mod: Any,
) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    return root_mod._get_pipeline_service().wait_for_report_completion(
        report_path=report_path,
        started_at=started_at,
        timeout_s=timeout_s,
        report_name=report_name,
        load_json_object=root_mod.load_json_object,
        report_is_stale_in_progress=root_mod.report_is_stale_in_progress,
        fail_on_stale=fail_on_stale,
    )


def wait_for_sync_completion(run_id: str, timeout_s: float = 900.0, *, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    deadline = datetime.now(UTC) + timedelta(seconds=max(10.0, float(timeout_s)))
    while datetime.now(UTC) < deadline:
        projection = root_mod.get_projected_run_history()
        history = list(projection.rows if hasattr(projection, "rows") else [])
        for row in reversed(history):
            if str(row.get("runId") or row.get("id") or "") != str(run_id or ""):
                continue
            if str(row.get("type") or "").strip().lower() != "sync":
                continue
            lifecycle_status = str(row.get("lifecycleStatus") or "").strip().lower()
            status = str(row.get("status") or lifecycle_status or "").strip().lower()
            if (
                lifecycle_status in {"succeeded", "failed", "canceled", "orphaned"}
                or status in {"ok", "warning", "error", "succeeded", "failed", "canceled"}
            ) and str(row.get("finishedAt") or "").strip():
                return row
        root_mod.threading.Event().wait(1.0)
    raise TimeoutError("sync task did not finish within timeout")


def start_fetcher_task(payload: JsonObject | None = None, *, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    return root_mod._get_task_launch_api().start_fetcher_task(
        payload,
        append_run_history=root_mod.append_run_history,
        normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
        prune_started_rows_for_type=root_mod.prune_started_rows_for_type,
        run_background_script=root_mod.run_background_script,
        save_json_atomic=root_mod.save_json_atomic,
        schema_version=root_mod.SCHEMA_VERSION,
        load_json_object=root_mod.load_json_object,
        start_lifecycle_run=root_mod.start_lifecycle_run,
        finish_lifecycle_run=root_mod.finish_lifecycle_run,
        fail_lifecycle_run=root_mod.fail_lifecycle_run,
        cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
        heartbeat_lifecycle_run=root_mod.heartbeat_lifecycle_run,
        get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
    )


def start_jobs_bootstrap_task(payload: JsonObject | None = None, *, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    return root_mod._get_task_launch_api().start_jobs_bootstrap_task(
        payload,
        normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
        run_background_script=root_mod.run_background_script,
        save_json_atomic=root_mod.save_json_atomic,
        schema_version=root_mod.SCHEMA_VERSION,
        start_lifecycle_run=root_mod.start_lifecycle_run,
        finish_lifecycle_run=root_mod.finish_lifecycle_run,
        fail_lifecycle_run=root_mod.fail_lifecycle_run,
        cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
        heartbeat_lifecycle_run=root_mod.heartbeat_lifecycle_run,
        get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
        get_lifecycle_run_history_rows=root_mod.get_lifecycle_run_history_rows,
    )


def start_jobs_pipeline_task(payload: JsonObject | None = None, *, root_mod: Any) -> JsonObject:
    root_mod = cast(_AdminTaskRuntimeRoot, root_mod)
    try:
        root_mod._get_job_availability_service().prepare_priority_manifest()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
        root_mod.bridge_log(
            "warn", "availability_priority_manifest_failed", error=type(exc).__name__
        )
    return root_mod._get_pipeline_service().start_task(payload)
