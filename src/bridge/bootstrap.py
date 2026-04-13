from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.api import BridgeApi, RuntimeConfigLike
from src.bridge.server import make_handler, run_http_server


def build_bridge_api(
    *,
    config: RuntimeConfigLike,
    registry: Any,
    sync: Any,
    pipeline: Any,
    discovery: Any,
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    discovery_report_path: Path,
    jobs_fetch_report_path: Path,
    approval_state_path: Path,
    discovery_log_path: Path,
    fetcher_log_path: Path,
    startup_metrics_path: Path,
    desktop_session_activity_at: str,
    bridge_log: Callable[..., None],
    now_iso: Callable[[], str],
    mark_desktop_session_activity: Callable[[str], None],
    desktop_local_data_store: Callable[[], Any],
    append_startup_metric: Callable[[str, dict[str, Any] | None], None],
    read_startup_metrics: Callable[[int], list[dict[str, Any]]],
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]],
    add_manual_source: Callable[[str], dict[str, Any]],
    trigger_source_check: Callable[..., dict[str, Any]],
    load_json_object: Callable[[Path, Any], Any],
    save_json_atomic: Callable[[Path, Any], None],
    start_fetcher_task: Callable[[dict[str, Any] | None], dict[str, Any]],
    start_sync_task: Callable[..., dict[str, Any]],
    get_discovery_config_payload: Callable[[], dict[str, Any]],
    update_saved_discovery_settings: Callable[[dict[str, Any]], dict[str, Any]],
    compute_ops_health: Callable[[], dict[str, Any]],
    compute_fetcher_metrics: Callable[..., dict[str, Any]],
    sync_history_from_reports: Callable[[], list[dict[str, Any]]],
    get_projected_run_history: Callable[[], Any],
    get_current_task_state_payload: Callable[[], dict[str, Any]],
    should_exit_for_owner_timeout: Callable[[], bool],
    load_alert_state: Callable[[], dict[str, Any]],
    save_alert_state: Callable[[dict[str, Any]], None],
) -> BridgeApi:
    return BridgeApi(
        runtime_config=config,
        registry=registry,
        sync=sync,
        pipeline=pipeline,
        discovery=discovery,
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        normalize_discovery_report_contract=normalize_discovery_report_contract,
        DISCOVERY_REPORT_PATH=discovery_report_path,
        JOBS_FETCH_REPORT_PATH=jobs_fetch_report_path,
        APPROVAL_STATE_PATH=approval_state_path,
        DISCOVERY_LOG_PATH=discovery_log_path,
        FETCHER_LOG_PATH=fetcher_log_path,
        STARTUP_METRICS_PATH=startup_metrics_path,
        DESKTOP_SESSION_ACTIVITY_AT=desktop_session_activity_at,
        bridge_log=bridge_log,
        now_iso=now_iso,
        _mark_desktop_session_activity=mark_desktop_session_activity,
        desktop_local_data_store=desktop_local_data_store,
        append_startup_metric=append_startup_metric,
        read_startup_metrics=read_startup_metrics,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
        add_manual_source=add_manual_source,
        trigger_source_check=trigger_source_check,
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        start_fetcher_task=start_fetcher_task,
        start_sync_task=start_sync_task,
        get_discovery_config_payload=get_discovery_config_payload,
        update_saved_discovery_settings=update_saved_discovery_settings,
        compute_ops_health=compute_ops_health,
        compute_fetcher_metrics=compute_fetcher_metrics,
        sync_history_from_reports=sync_history_from_reports,
        get_projected_run_history=get_projected_run_history,
        get_current_task_state_payload=get_current_task_state_payload,
        should_exit_for_owner_timeout=should_exit_for_owner_timeout,
        load_alert_state=load_alert_state,
        save_alert_state=save_alert_state,
    )


def run_bridge_server(*, api: BridgeApi, host: str, port: int) -> int:
    handler_cls = make_handler(api=api)
    return run_http_server(api=api, host=host, port=port, handler_cls=handler_cls)
