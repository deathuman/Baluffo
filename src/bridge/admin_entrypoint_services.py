from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.bridge.server import runtime_state as bridge_runtime_state

root: Any | None = None


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("admin bridge root is not bound")
    return root


def get_sync_service() -> Any:
    root_mod = _require_root()
    data_dir = Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    with root_mod._SYNC_SERVICE_LOCK:
        if root_mod._SYNC_SERVICE is not None and root_mod._SYNC_SERVICE_DATA_DIR == data_dir:
            return root_mod._SYNC_SERVICE
        root_mod._SYNC_SERVICE_DATA_DIR = data_dir
        root_mod._SYNC_SERVICE = root_mod.SyncService(
            data_dir=data_dir,
            source_sync=root_mod.source_sync_module,
            bridge_log=root_mod.bridge_log,
            load_state=root_mod.load_state,
            persist_state=root_mod.persist_state,
            summarize_state=root_mod.summarize_state,
            run_history=root_mod._TASK_HISTORY,
            ops_state_lock=root_mod.OPS_STATE_LOCK,
            get_security_defaults=root_mod.get_security_defaults,
            sync_state=root_mod.SyncState(data_dir=data_dir),
        )
        return root_mod._SYNC_SERVICE


def get_sync_state() -> Any:
    return get_sync_service()._sync_state  # noqa: SLF001


def get_registry_service() -> Any:
    root_mod = _require_root()
    current_paths = (
        Path(root_mod.ACTIVE_PATH),
        Path(root_mod.PENDING_PATH),
        Path(root_mod.REJECTED_PATH),
    )
    with root_mod._REGISTRY_SERVICE_LOCK:
        if root_mod._REGISTRY_SERVICE is None or root_mod._REGISTRY_SERVICE_PATHS != current_paths:
            root_mod._REGISTRY_SERVICE_PATHS = current_paths
            root_mod._REGISTRY_SERVICE = root_mod.RegistryService(
                paths=root_mod.RegistryPaths(
                    active=root_mod.ACTIVE_PATH,
                    pending=root_mod.PENDING_PATH,
                    rejected=root_mod.REJECTED_PATH,
                ),
                default_active=[dict(row) for row in root_mod.DEFAULT_STUDIO_SOURCE_REGISTRY],
                normalize_manual_static=root_mod.normalize_manual_static_studio_fields,
            )
        return root_mod._REGISTRY_SERVICE


def get_discovery_service() -> Any:
    root_mod = _require_root()
    current_paths = (
        Path(root_mod.DISCOVERY_REPORT_PATH),
        Path(root_mod.DISCOVERY_CANDIDATES_PATH),
        Path(root_mod.PENDING_PATH),
        Path(root_mod.DISCOVERY_LOG_PATH),
    )
    with root_mod._DISCOVERY_SERVICE_LOCK:
        if (
            root_mod._DISCOVERY_SERVICE is None
            or root_mod._DISCOVERY_SERVICE_PATHS != current_paths
        ):
            root_mod._DISCOVERY_SERVICE_PATHS = current_paths
            root_mod._DISCOVERY_SERVICE = root_mod.DiscoveryService(
                paths=root_mod.DiscoveryPaths(
                    report=root_mod.DISCOVERY_REPORT_PATH,
                    candidates=root_mod.DISCOVERY_CANDIDATES_PATH,
                    pending=root_mod.PENDING_PATH,
                    log=root_mod.DISCOVERY_LOG_PATH,
                    settings=root_mod.DISCOVERY_CONFIG_PATH,
                    approval_state=root_mod.APPROVAL_STATE_PATH,
                    task_state=root_mod.TASK_STATE_PATH,
                ),
                deps=root_mod.DiscoveryDeps(
                    schema_version=root_mod.SCHEMA_VERSION,
                    now_iso=root_mod.now_iso,
                    now_utc=root_mod.now_utc,
                    parse_iso=root_mod.parse_iso,
                    pid_is_running=root_mod.pid_is_running,
                    bridge_log=root_mod.bridge_log,
                    load_json_object=root_mod.load_json_object,
                    save_json_atomic=root_mod.save_json_atomic,
                    run_background_script=root_mod.run_background_script,
                    append_run_history=root_mod.append_run_history,
                    upsert_run_history=root_mod.upsert_run_history,
                    prune_started_rows_for_type=root_mod.prune_started_rows_for_type,
                    clear_task_state=root_mod.clear_task_state,
                    normalize_discovery_report_contract=root_mod.normalize_discovery_report_contract,
                    load_state=root_mod.load_state,
                    persist_state_and_auto_sync=root_mod.persist_state_and_auto_sync,
                    load_sync_runtime_state=root_mod.load_sync_runtime_state,
                    maybe_trigger_auto_sync_push=root_mod._maybe_trigger_auto_sync_push,
                    mark_discovery_sync_finished=root_mod._mark_discovery_sync_finished,
                    task_state_lock=root_mod.OPS_STATE_LOCK,
                ),
            )
        return root_mod._DISCOVERY_SERVICE


def get_task_launch_api() -> Any:
    root_mod = _require_root()
    return root_mod._task_launch_api.TaskLaunchApi(
        runtime=root_mod._task_launch_api.TaskLaunchRuntime(
            root=Path(root_mod.RUNTIME_CONFIG.root),
            data_dir=Path(root_mod.RUNTIME_CONFIG.data_dir),
        ),
        paths=root_mod._task_launch_api.TaskLaunchPaths(
            discovery_log=root_mod.DISCOVERY_LOG_PATH,
            discovery_report=root_mod.DISCOVERY_REPORT_PATH,
            fetcher_log=root_mod.FETCHER_LOG_PATH,
            task_state=root_mod.TASK_STATE_PATH,
            jobs_fetch_report=root_mod.JOBS_FETCH_REPORT_PATH,
            approval_state=root_mod.APPROVAL_STATE_PATH,
        ),
        deps=root_mod._task_launch_api.TaskLaunchDeps(
            now_iso=root_mod.now_iso,
            bridge_log=root_mod.bridge_log,
            load_json_object=root_mod.load_json_object,
            save_json_atomic=root_mod.save_json_atomic,
            task_state_lock=root_mod.OPS_STATE_LOCK,
            default_source_loaders=root_mod.default_source_loaders,
            failed_source_names_from_latest_report=lambda allowed: (
                root_mod._failed_source_names_from_latest_report(allowed_names=allowed)
            ),
            safe_int=root_mod._safe_int,
            pid_is_running=root_mod.pid_is_running,
        ),
    )


def get_ops_api() -> Any:
    root_mod = _require_root()
    return root_mod._ops_api.OpsApi(
        paths=root_mod._ops_api.OpsPaths(
            ops_alert_state=root_mod.OPS_ALERT_STATE_PATH,
            jobs_fetch_report=root_mod.JOBS_FETCH_REPORT_PATH,
            jobs_fetch_tasks=root_mod.JOBS_FETCH_TASKS_PATH,
            discovery_report=root_mod.DISCOVERY_REPORT_PATH,
            sync_live_task=root_mod.SYNC_LIVE_TASK_PATH,
            task_state=root_mod.TASK_STATE_PATH,
        ),
        deps=root_mod._ops_api.OpsDeps(
            load_json_object=root_mod.load_json_object,
            save_json_atomic=root_mod.save_json_atomic,
            load_state=root_mod.load_state,
            now_iso=root_mod.now_iso,
            now_utc=root_mod.now_utc,
            parse_iso=root_mod.parse_iso,
            read_tasks_config=root_mod._read_tasks_config,
            ops_state_lock=root_mod.OPS_STATE_LOCK,
            load_run_history=root_mod.load_run_history,
            save_run_history=root_mod.save_run_history,
            prune_started_rows_for_type=root_mod.prune_started_rows_for_type,
            clear_task_state=root_mod.clear_task_state,
            clear_task_state_locked=root_mod._clear_task_state_locked,
            upsert_run_history=root_mod.upsert_run_history,
            task_running_from_state=root_mod.task_running_from_state,
            report_is_stale_in_progress=root_mod.report_is_stale_in_progress,
            get_active_sync_runs=root_mod.SyncState.get_active_sync_runs,
            get_sync_status_payload=root_mod.get_sync_status_payload,
            get_jobs_pipeline_status_payload=root_mod.get_jobs_pipeline_status_payload,
            normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
            normalize_discovery_report_contract=root_mod.normalize_discovery_report_contract,
            desktop_mode=root_mod.RUNTIME_CONFIG.desktop_mode,
            get_desktop_last_activity_at=lambda: bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
            get_owner_state=bridge_runtime_state.get_owner_state,
            ops_schema_version=root_mod.OPS_SCHEMA_VERSION,
            get_updater_status_payload=lambda: get_desktop_update_service().get_status_payload(),
            app_version=root_mod.get_app_version(),
        ),
    )


def get_pipeline_service() -> Any:
    root_mod = _require_root()
    with root_mod._PIPELINE_SERVICE_LOCK:
        if root_mod._PIPELINE_SERVICE is None:
            smoke_mode = (
                str(root_mod.os.getenv("BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE") or "")
                .strip()
                .lower()
            )
            stub_success_mode = smoke_mode == "stub-success"
            pipeline_load_json_object = root_mod.load_json_object
            pipeline_trigger_discovery_task = root_mod.trigger_discovery_task
            pipeline_start_fetcher_task = root_mod.start_fetcher_task
            pipeline_start_sync_task = root_mod.start_sync_task
            pipeline_wait_for_sync_completion = root_mod._wait_for_sync_completion
            pipeline_current_fetch_output_count = root_mod._current_fetch_output_count

            if stub_success_mode:
                smoke_runtime: dict[str, Any] = {
                    "discoveryStartedAt": "",
                    "discoveryReadyAt": 0.0,
                    "fetchStartedAt": "",
                    "fetchReadyAt": 0.0,
                }

                def pipeline_load_json_object(path: Any, default: Any) -> Any:
                    resolved = Path(path).resolve()
                    if resolved == Path(root_mod.DISCOVERY_REPORT_PATH).resolve():
                        started_at = str(smoke_runtime.get("discoveryStartedAt") or "")
                        if started_at:
                            finished_at = (
                                started_at
                                if root_mod.time.monotonic()
                                >= float(smoke_runtime.get("discoveryReadyAt") or 0.0)
                                else ""
                            )
                            return {
                                "runId": "discovery_smoke",
                                "startedAt": started_at,
                                "finishedAt": finished_at,
                                "status": "ok" if finished_at else "running",
                                "summary": {},
                            }
                    if resolved == Path(root_mod.JOBS_FETCH_REPORT_PATH).resolve():
                        started_at = str(smoke_runtime.get("fetchStartedAt") or "")
                        if started_at:
                            finished_at = (
                                started_at
                                if root_mod.time.monotonic()
                                >= float(smoke_runtime.get("fetchReadyAt") or 0.0)
                                else ""
                            )
                            return {
                                "runId": "fetch_smoke",
                                "startedAt": started_at,
                                "finishedAt": finished_at,
                                "status": "ok" if finished_at else "running",
                                "summary": {"outputCount": 0},
                            }
                    return root_mod.load_json_object(path, default)

                def pipeline_trigger_discovery_task(**kwargs: Any) -> tuple[int, dict[str, Any]]:
                    started_at = root_mod.now_iso()
                    smoke_runtime["discoveryStartedAt"] = started_at
                    smoke_runtime["discoveryReadyAt"] = root_mod.time.monotonic() + 1.2
                    return 200, {
                        "started": True,
                        "startedAt": started_at,
                        "runId": "discovery_smoke",
                    }

                def pipeline_start_fetcher_task(
                    payload: dict[str, Any] | None = None,
                ) -> dict[str, Any]:
                    started_at = root_mod.now_iso()
                    smoke_runtime["fetchStartedAt"] = started_at
                    smoke_runtime["fetchReadyAt"] = root_mod.time.monotonic() + 1.2
                    return {"started": True, "startedAt": started_at, "runId": "fetch_smoke"}

                def pipeline_start_sync_task(
                    action: str, *, reason: str, automatic: bool
                ) -> dict[str, Any]:
                    return {"started": True, "runId": "sync_smoke"}

                def pipeline_wait_for_sync_completion(
                    run_id: str, timeout_s: float = 900.0
                ) -> dict[str, Any]:
                    finished_at = root_mod.now_iso()
                    return {
                        "id": str(run_id or "sync_smoke"),
                        "type": "sync",
                        "status": "ok",
                        "finishedAt": finished_at,
                        "summary": {},
                    }

                def pipeline_current_fetch_output_count() -> int:
                    report = pipeline_load_json_object(root_mod.JOBS_FETCH_REPORT_PATH, {})
                    summary = root_mod.summarize_fetch_report(
                        root_mod.normalize_fetch_report_contract(report)
                    )
                    return int(summary.get("outputCount") or 0)

            def pipeline_child_run_is_live(task_type: str, run_id: str) -> bool:
                normalized_type = str(task_type or "").strip().lower()
                normalized_run_id = str(run_id or "").strip()
                if not normalized_type or not normalized_run_id:
                    return False

                task_state = pipeline_load_json_object(root_mod.TASK_STATE_PATH, {})
                task_state_entry = (
                    task_state.get(normalized_type)
                    if isinstance(task_state, dict)
                    and isinstance(task_state.get(normalized_type), dict)
                    else {}
                )
                if str(
                    task_state_entry.get("runId") or ""
                ).strip() == normalized_run_id and root_mod.task_running_from_state(
                    normalized_type
                ):
                    return True

                if normalized_type != "fetch":
                    return False

                fetch_tasks = pipeline_load_json_object(root_mod.JOBS_FETCH_TASKS_PATH, {})
                if not isinstance(fetch_tasks, dict):
                    return False
                if str(fetch_tasks.get("runId") or "").strip() != normalized_run_id:
                    return False
                if str(fetch_tasks.get("finishedAt") or "").strip():
                    return False

                lifecycle = (
                    fetch_tasks.get("runtime", {}).get("lifecycle")
                    if isinstance(fetch_tasks.get("runtime"), dict)
                    and isinstance((fetch_tasks.get("runtime") or {}).get("lifecycle"), dict)
                    else {}
                )
                heartbeat_at = str(
                    lifecycle.get("heartbeatAt") or fetch_tasks.get("heartbeatAt") or ""
                ).strip()
                heartbeat_dt = root_mod.parse_iso(heartbeat_at) if heartbeat_at else None
                recent_heartbeat = bool(
                    heartbeat_dt and (datetime.now(UTC) - heartbeat_dt) <= timedelta(minutes=2)
                )
                recent_artifact = False
                try:
                    artifact_mtime = datetime.fromtimestamp(
                        Path(root_mod.JOBS_FETCH_TASKS_PATH).stat().st_mtime,
                        tz=UTC,
                    )
                    recent_artifact = (datetime.now(UTC) - artifact_mtime) <= timedelta(minutes=2)
                except OSError:
                    recent_artifact = False
                task_progress = (
                    fetch_tasks.get("taskProgress")
                    if isinstance(fetch_tasks.get("taskProgress"), dict)
                    else {}
                )
                has_live_evidence = bool(
                    fetch_tasks.get("active")
                    or task_progress.get("active")
                    or str(fetch_tasks.get("startedAt") or "").strip()
                    or bool(fetch_tasks.get("workItems"))
                    or bool(fetch_tasks.get("recentEvents"))
                )
                return bool(has_live_evidence and (recent_heartbeat or recent_artifact))

            root_mod._PIPELINE_SERVICE = root_mod.PipelineService(
                pipeline_state_lock=bridge_runtime_state.PIPELINE_STATE_LOCK,
                pipeline_status=bridge_runtime_state.PIPELINE_STATUS,
                runtime=bridge_runtime_state.PIPELINE_RUNTIME,
                bridge_log=root_mod.bridge_log,
                now_iso=root_mod.now_iso,
                parse_iso=root_mod.parse_iso,
                append_run_history=root_mod.append_run_history,
                upsert_run_history=root_mod.upsert_run_history,
                task_running_from_state=root_mod.task_running_from_state,
                sync_task_running=root_mod.sync_task_running,
                current_fetch_output_count=pipeline_current_fetch_output_count,
                load_json_object=pipeline_load_json_object,
                wait_for_sync_completion=pipeline_wait_for_sync_completion,
                discovery_report_path=root_mod.DISCOVERY_REPORT_PATH,
                fetch_report_path=root_mod.JOBS_FETCH_REPORT_PATH,
                trigger_discovery_task=pipeline_trigger_discovery_task,
                start_fetcher_task=pipeline_start_fetcher_task,
                start_sync_task=pipeline_start_sync_task,
                get_app_version=root_mod.get_app_version,
                child_run_is_live=pipeline_child_run_is_live,
                get_projected_run_history=root_mod._get_ops_api().get_projected_run_history,
            )
        return root_mod._PIPELINE_SERVICE


def get_desktop_update_service() -> Any:
    root_mod = _require_root()
    data_dir = Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    with root_mod._DESKTOP_UPDATE_SERVICE_LOCK:
        if (
            root_mod._DESKTOP_UPDATE_SERVICE is not None
            and root_mod._DESKTOP_UPDATE_SERVICE_DATA_DIR == data_dir
        ):
            return root_mod._DESKTOP_UPDATE_SERVICE
        root_mod._DESKTOP_UPDATE_SERVICE_DATA_DIR = data_dir
        root_mod._DESKTOP_UPDATE_SERVICE = root_mod.DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=root_mod.get_app_version,
        )
        return root_mod._DESKTOP_UPDATE_SERVICE
