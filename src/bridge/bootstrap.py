from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from src.bridge.api import BridgeApi, RuntimeConfigLike
from src.bridge.server import make_handler, run_http_server


def _empty_task_live_payload(_task_type: str = "") -> dict[str, Any]:
    return {}


def build_bridge_api(
    *,
    config: RuntimeConfigLike,
    registry: Any,
    sync: Any,
    pipeline: Any,
    discovery: Any,
    availability: Any,
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    discovery_report_path: Path,
    discovery_candidates_path: Path | None,
    jobs_fetch_report_path: Path,
    source_policy_recommendations_path: Path,
    source_policy_review_state_path: Path,
    dedup_review_state_path: Path,
    approval_state_path: Path,
    discovery_log_path: Path,
    fetcher_log_path: Path,
    startup_metrics_path: Path,
    desktop_update_state_path: Path | None,
    desktop_session_activity_at: str,
    bridge_log: Callable[..., None],
    now_iso: Callable[[], str],
    mark_desktop_session_activity: Callable[[str], None],
    get_desktop_session_payload: Callable[[], dict[str, Any]],
    update_desktop_session_lifecycle: Callable[..., tuple[int, dict[str, Any]]],
    desktop_local_data_store: Callable[[], Any],
    append_startup_metric: Callable[[str, dict[str, Any] | None], None],
    read_startup_metrics: Callable[[int], list[dict[str, Any]]],
    get_update_status_payload: Callable[[], dict[str, Any]],
    check_for_update: Callable[..., dict[str, Any]],
    download_update: Callable[[], dict[str, Any]],
    install_update: Callable[[], dict[str, Any]],
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]],
    add_manual_source: Callable[[str], dict[str, Any]],
    trigger_source_check: Callable[..., dict[str, Any]],
    load_json_object: Callable[[Path, Any], Any],
    save_json_atomic: Callable[[Path, Any], None],
    start_jobs_bootstrap_task: Callable[[dict[str, Any] | None], dict[str, Any]],
    start_fetcher_task: Callable[[dict[str, Any] | None], dict[str, Any]],
    get_jobs_pipeline_schedule_payload: Callable[[], dict[str, Any]],
    update_jobs_pipeline_schedule: Callable[[dict[str, Any] | None], dict[str, Any]],
    start_sync_task: Callable[..., dict[str, Any]],
    load_sync_runtime_state: Callable[[], dict[str, Any]],
    get_discovery_config_payload: Callable[[], dict[str, Any]],
    update_saved_discovery_settings: Callable[[dict[str, Any]], dict[str, Any]],
    compute_ops_health: Callable[[], dict[str, Any]],
    compute_ops_health_ready: Callable[[], dict[str, Any]] | None,
    compute_ops_dashboard_health: Callable[[], dict[str, Any]],
    get_storage_health_payload: Callable[[], dict[str, Any]],
    compute_fetcher_metrics: Callable[..., dict[str, Any]],
    get_projected_run_history: Callable[[], Any],
    get_lifecycle_run_history_rows: Callable[[], list[dict[str, Any]]],
    get_current_task_state_payload: Callable[[], dict[str, Any]],
    should_exit_for_owner_timeout: Callable[[], bool],
    load_alert_state: Callable[[], dict[str, Any]],
    save_alert_state: Callable[[dict[str, Any]], None],
    ops_state_lock: Any | None = None,
    app_version: str = "",
    get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] | None = None,
    get_lifecycle_recent_runs: Callable[[], list[dict[str, Any]]] | None = None,
    get_task_live_payload: Callable[[str], dict[str, Any]] = _empty_task_live_payload,
    check_registry_conflicts: Callable[[dict[str, Any] | None], dict[str, Any]] | None = None,
    load_registry_conflict_adjudication: Callable[[], dict[str, Any]] | None = None,
    compute_ops_dashboard_health_summary: Callable[[], dict[str, Any]] | None = None,
    compute_ops_fetch_kpis_summary: Callable[[], dict[str, Any]] | None = None,
    get_current_task_state_summary_payload: Callable[[], dict[str, Any]] | None = None,
    abort_task: Callable[[dict[str, Any] | None], tuple[int, dict[str, Any]]] | None = None,
    abort_task_async: Callable[[dict[str, Any] | None], tuple[int, dict[str, Any]]] | None = None,
) -> BridgeApi:
    def _empty_fetch_kpis_summary() -> dict[str, Any]:
        return {
            "ok": True,
            "summaryView": True,
            "detailLevel": "summary",
            "kpis": {},
        }

    return BridgeApi(
        runtime_config=config,
        app_version=app_version,
        registry=registry,
        sync=sync,
        pipeline=pipeline,
        discovery=discovery,
        availability=availability,
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        normalize_discovery_report_contract=normalize_discovery_report_contract,
        DISCOVERY_REPORT_PATH=discovery_report_path,
        DISCOVERY_CANDIDATES_PATH=discovery_candidates_path,
        JOBS_FETCH_REPORT_PATH=jobs_fetch_report_path,
        SOURCE_POLICY_RECOMMENDATIONS_PATH=source_policy_recommendations_path,
        SOURCE_POLICY_REVIEW_STATE_PATH=source_policy_review_state_path,
        DEDUP_REVIEW_STATE_PATH=dedup_review_state_path,
        APPROVAL_STATE_PATH=approval_state_path,
        DISCOVERY_LOG_PATH=discovery_log_path,
        FETCHER_LOG_PATH=fetcher_log_path,
        STARTUP_METRICS_PATH=startup_metrics_path,
        DESKTOP_UPDATE_STATE_PATH=desktop_update_state_path,
        DESKTOP_SESSION_ACTIVITY_AT=desktop_session_activity_at,
        bridge_log=bridge_log,
        now_iso=now_iso,
        _mark_desktop_session_activity=mark_desktop_session_activity,
        get_desktop_session_payload=get_desktop_session_payload,
        update_desktop_session_lifecycle=update_desktop_session_lifecycle,
        desktop_local_data_store=desktop_local_data_store,
        append_startup_metric=append_startup_metric,
        read_startup_metrics=read_startup_metrics,
        get_update_status_payload=get_update_status_payload,
        check_for_update=check_for_update,
        download_update=download_update,
        install_update=install_update,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
        add_manual_source=add_manual_source,
        trigger_source_check=trigger_source_check,
        check_registry_conflicts=cast(
            Callable[[dict[str, Any] | None], dict[str, Any]],
            check_registry_conflicts or _empty_task_live_payload,
        ),
        load_registry_conflict_adjudication=(
            load_registry_conflict_adjudication or _empty_task_live_payload
        ),
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        start_jobs_bootstrap_task=start_jobs_bootstrap_task,
        start_fetcher_task=start_fetcher_task,
        get_jobs_pipeline_schedule_payload=get_jobs_pipeline_schedule_payload,
        update_jobs_pipeline_schedule=update_jobs_pipeline_schedule,
        abort_task=abort_task
        or (lambda _payload: (400, {"ok": False, "error": "task_abort_not_available"})),
        abort_task_async=abort_task_async
        or (
            lambda _payload: (
                400,
                {"ok": False, "error": "task_abort_not_available"},
            )
        ),
        start_sync_task=start_sync_task,
        load_sync_runtime_state=load_sync_runtime_state,
        get_discovery_config_payload=get_discovery_config_payload,
        update_saved_discovery_settings=update_saved_discovery_settings,
        compute_ops_health=compute_ops_health,
        compute_ops_health_ready=compute_ops_health_ready or compute_ops_health,
        compute_ops_dashboard_health=compute_ops_dashboard_health,
        compute_ops_dashboard_health_summary=(
            compute_ops_dashboard_health_summary or compute_ops_dashboard_health
        ),
        compute_ops_fetch_kpis_summary=(
            compute_ops_fetch_kpis_summary or _empty_fetch_kpis_summary
        ),
        get_storage_health_payload=get_storage_health_payload,
        compute_fetcher_metrics=compute_fetcher_metrics,
        get_projected_run_history=get_projected_run_history,
        get_lifecycle_run_history_rows=get_lifecycle_run_history_rows,
        get_lifecycle_current_runs=get_lifecycle_current_runs or (lambda: []),
        get_lifecycle_recent_runs=get_lifecycle_recent_runs or (lambda: []),
        get_task_live_payload=get_task_live_payload,
        get_current_task_state_payload=get_current_task_state_payload,
        get_current_task_state_summary_payload=(
            get_current_task_state_summary_payload or get_current_task_state_payload
        ),
        should_exit_for_owner_timeout=should_exit_for_owner_timeout,
        load_alert_state=load_alert_state,
        save_alert_state=save_alert_state,
        ops_state_lock=ops_state_lock,
    )


def run_bridge_server(
    *,
    api: BridgeApi,
    host: str,
    port: int,
    on_started: Callable[[], Any] | None = None,
) -> int:
    handler_cls = make_handler(api=api)
    return run_http_server(
        api=api,
        host=host,
        port=port,
        handler_cls=handler_cls,
        on_started=on_started,
    )
