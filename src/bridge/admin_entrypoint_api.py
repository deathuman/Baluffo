from __future__ import annotations

from typing import Any

from src.bridge import bootstrap as bridge_bootstrap
from src.bridge.api import BridgeApi

root: Any | None = None


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("admin bridge root is not bound")
    return root


def build_bridge_api(config: Any) -> BridgeApi:
    root_mod = _require_root()
    ops_api = root_mod._get_ops_api()
    desktop_update_service = root_mod._get_desktop_update_service()
    return bridge_bootstrap.build_bridge_api(
        config=config,
        registry=root_mod._get_registry_service(),
        sync=root_mod._get_sync_service(),
        pipeline=root_mod._get_pipeline_service(),
        discovery=root_mod._get_discovery_service(),
        normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
        normalize_discovery_report_contract=root_mod.normalize_discovery_report_contract,
        discovery_report_path=root_mod.DISCOVERY_REPORT_PATH,
        discovery_candidates_path=root_mod.DISCOVERY_CANDIDATES_PATH,
        jobs_fetch_report_path=root_mod.JOBS_FETCH_REPORT_PATH,
        approval_state_path=root_mod.APPROVAL_STATE_PATH,
        discovery_log_path=root_mod.DISCOVERY_LOG_PATH,
        fetcher_log_path=root_mod.FETCHER_LOG_PATH,
        startup_metrics_path=root_mod.STARTUP_METRICS_PATH,
        desktop_update_state_path=root_mod.DESKTOP_UPDATE_STATE_PATH,
        desktop_session_activity_at=root_mod.bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
        bridge_log=root_mod.bridge_log,
        now_iso=root_mod.now_iso,
        mark_desktop_session_activity=root_mod.mark_desktop_session_activity,
        get_desktop_session_payload=root_mod.get_desktop_session_payload,
        update_desktop_session_lifecycle=root_mod.update_desktop_session_lifecycle,
        desktop_local_data_store=root_mod.desktop_local_data_store,
        append_startup_metric=root_mod.append_startup_metric,
        read_startup_metrics=root_mod.read_startup_metrics,
        get_update_status_payload=desktop_update_service.get_status_payload,
        check_for_update=desktop_update_service.check_for_update,
        download_update=desktop_update_service.download_update,
        install_update=desktop_update_service.request_install,
        persist_state_and_auto_sync=root_mod.persist_state_and_auto_sync,
        add_manual_source=root_mod.add_manual_source,
        trigger_source_check=root_mod.trigger_source_check,
        load_json_object=root_mod.load_json_object,
        save_json_atomic=root_mod.save_json_atomic,
        start_fetcher_task=root_mod.start_fetcher_task,
        start_sync_task=root_mod.start_sync_task,
        get_discovery_config_payload=root_mod.get_discovery_config_payload,
        update_saved_discovery_settings=root_mod.update_saved_discovery_settings,
        compute_ops_health=root_mod.compute_ops_health,
        compute_fetcher_metrics=root_mod.compute_fetcher_metrics,
        sync_history_from_reports=root_mod.sync_history_from_reports,
        get_projected_run_history=ops_api.get_projected_run_history,
        get_task_live_payload=ops_api.get_task_live_payload,
        get_current_task_state_payload=ops_api.get_current_task_state_payload,
        should_exit_for_owner_timeout=root_mod.owner_session_should_exit,
        load_alert_state=root_mod.load_alert_state,
        save_alert_state=root_mod.save_alert_state,
    )
