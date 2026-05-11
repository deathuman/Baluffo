from __future__ import annotations

from typing import Any

from src.bridge import run_history_api as _run_history_api


def build_sync_history_deps(
    *,
    deps: Any,
    paths: Any,
    summarize_fetch_report: Any,
    summarize_discovery_report: Any,
) -> _run_history_api.SyncHistoryDeps:
    return _run_history_api.SyncHistoryDeps(
        ops_state_lock=deps.ops_state_lock,
        load_run_history=deps.load_run_history,
        save_run_history=deps.save_run_history,
        save_json_atomic=deps.save_json_atomic,
        prune_started_rows_for_type=deps.prune_started_rows_for_type,
        clear_task_state=deps.clear_task_state,
        clear_task_state_locked=deps.clear_task_state_locked,
        upsert_run_history=deps.upsert_run_history,
        task_running_from_state=deps.task_running_from_state,
        report_is_stale_in_progress=deps.report_is_stale_in_progress,
        load_json_object=deps.load_json_object,
        load_runtime_evidence=getattr(deps, "load_runtime_evidence", None)
        or (lambda path, default=None: dict(default or {})),
        normalize_fetch_report_contract=deps.normalize_fetch_report_contract,
        normalize_discovery_report_contract=deps.normalize_discovery_report_contract,
        summarize_fetch_report=summarize_fetch_report,
        summarize_discovery_report=summarize_discovery_report,
        jobs_fetch_report_path=paths.jobs_fetch_report,
        jobs_fetch_tasks_path=paths.jobs_fetch_tasks,
        discovery_report_path=paths.discovery_report,
        task_state_path=paths.task_state,
        get_active_sync_runs=deps.get_active_sync_runs,
        parse_iso=deps.parse_iso,
        now_iso=deps.now_iso,
        now_utc=deps.now_utc,
        get_jobs_pipeline_status_payload=getattr(
            deps,
            "get_jobs_pipeline_status_payload",
            lambda: {},
        ),
        pid_is_running=getattr(deps, "pid_is_running", None),
        get_lifecycle_current_runs=getattr(deps, "get_lifecycle_current_runs", None),
    )


def sync_history_from_reports(
    *,
    deps: Any,
    paths: Any,
    summarize_fetch_report: Any,
    summarize_discovery_report: Any,
) -> list[dict[str, Any]]:
    return _run_history_api.sync_history_from_reports(
        build_sync_history_deps(
            deps=deps,
            paths=paths,
            summarize_fetch_report=summarize_fetch_report,
            summarize_discovery_report=summarize_discovery_report,
        )
    )


def get_projected_run_history(
    *,
    deps: Any,
    paths: Any,
    summarize_fetch_report: Any,
    summarize_discovery_report: Any,
) -> _run_history_api.LifecycleProjection:
    return _run_history_api.project_run_history(
        build_sync_history_deps(
            deps=deps,
            paths=paths,
            summarize_fetch_report=summarize_fetch_report,
            summarize_discovery_report=summarize_discovery_report,
        )
    )
