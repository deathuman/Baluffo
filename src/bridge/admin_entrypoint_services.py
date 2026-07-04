"""Admin bridge service wiring helpers.

AI boundary owns: BridgeServices construction and injected service-holder compatibility wiring.
AI boundary implement in: this file for service composition only; domain behavior belongs in the injected service modules.
AI boundary search before contracts: admin_bridge compatibility root, BridgeApi field inventory, and admin runtime tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused admin service wiring tests.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, cast

from src.bridge import run_history_api as _run_history_api
from src.bridge.admin_service_holder import BridgeServices
from src.bridge.desktop_attention import notify_pipeline_completion_attention
from src.bridge.pipeline_schedule_service import PipelineScheduleService
from src.bridge.server import runtime_state as bridge_runtime_state
from src.bridge.task_abort_service import TaskAbortDeps, TaskAbortPaths, TaskAbortService
from src.source_registry_io import load_runtime_evidence

BRIDGE_SERVICES = BridgeServices()

JsonObject = dict[str, Any]


class _SyncStateLike(Protocol):
    def set_sync_status(self, **kwargs: Any) -> None: ...
    def load_sync_runtime_state(self) -> JsonObject: ...
    def save_sync_runtime_state(self, payload: JsonObject) -> JsonObject: ...


class _SyncServiceLike(Protocol):
    def load_saved_sync_settings(self) -> JsonObject: ...
    def refresh_sync_config(self) -> Any: ...
    def get_saved_sync_config_payload(self) -> JsonObject: ...
    def update_saved_sync_settings(self, payload: JsonObject) -> JsonObject: ...
    def test_sync_config(self) -> JsonObject: ...
    def get_sync_status_payload(self) -> JsonObject: ...
    def sync_config_status(self) -> JsonObject: ...
    def sync_task_running(self) -> bool: ...
    def sync_pull_sources(self) -> JsonObject: ...
    def sync_push_sources(self) -> JsonObject: ...
    def startup_sync_pull(self) -> None: ...
    def schedule_startup_sync_pull(self) -> JsonObject: ...
    def wait_for_sync_tasks(self, timeout_s: float = 5.0) -> None: ...
    def start_sync_task(
        self, action: str, *, reason: str = "", automatic: bool = False
    ) -> JsonObject: ...

    _sync_state: _SyncStateLike


class _RegistryServiceLike(Protocol):
    def ensure_active_registry(self) -> list[JsonObject]: ...


class _DiscoveryServiceLike(Protocol):
    def load_saved_discovery_settings(self) -> JsonObject: ...
    def get_discovery_config_payload(self) -> JsonObject: ...
    def update_saved_discovery_settings(self, payload: JsonObject) -> JsonObject: ...
    def trigger_discovery_task(
        self,
        *,
        route_name: str,
        payload: JsonObject,
        enable_auto_sync_watch: bool = True,
    ) -> tuple[int, JsonObject]: ...


class _TaskLaunchApiLike(Protocol):
    def run_background_script(
        self,
        script_name: str,
        args: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
        *,
        is_frozen: bool,
        executable: str,
        spawn_process: Callable[..., Any],
        devnull: Any,
        stdout_target: Any,
        create_no_window: int = 0,
        create_new_process_group: int = 0,
        run_id: str = "",
        task_type: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int: ...

    def build_fetcher_args_from_payload(self, payload: JsonObject) -> tuple[list[str], str]: ...

    def start_fetcher_task(
        self, payload: JsonObject | None = None, **kwargs: Any
    ) -> JsonObject: ...


class _PipelineServiceLike(Protocol):
    def get_status_payload(self) -> JsonObject: ...

    def wait_for_report_completion(self, **kwargs: Any) -> JsonObject: ...

    def start_task(self, payload: JsonObject | None = None) -> JsonObject: ...

    def request_abort(self, run_id: str, **kwargs: Any) -> JsonObject: ...


class _PipelineScheduleServiceLike(Protocol):
    def get_payload(self) -> JsonObject: ...

    def update_config(self, payload: JsonObject | None) -> JsonObject: ...

    def get_ops_schedule_entry(self) -> JsonObject: ...

    def start_background_polling(self) -> JsonObject: ...


_PIPELINE_SCHEDULE_SERVICE: PipelineScheduleService | None = None
_PIPELINE_SCHEDULE_SERVICE_PATH: Path | None = None
_PIPELINE_SCHEDULE_SERVICE_LOCK = threading.RLock()


class _DesktopUpdateServiceLike(Protocol):
    def get_status_payload(self) -> JsonObject: ...


class _OpsApiLike(Protocol):
    def failed_source_names_from_latest_report(
        self, *, allowed_names: set[str] | None = None
    ) -> list[str]: ...
    def load_alert_state(self) -> JsonObject: ...
    def save_alert_state(self, state: JsonObject) -> None: ...
    def detect_task_interval_hours(self, task: JsonObject) -> float | None: ...
    def parse_schedule_metadata(self) -> JsonObject: ...
    def summarize_fetch_report(self, report: JsonObject) -> JsonObject: ...
    def summarize_discovery_report(self, report: JsonObject) -> tuple[JsonObject, str]: ...
    def sync_history_from_reports(self) -> list[JsonObject]: ...
    def get_projected_run_history(self) -> _run_history_api.LifecycleProjection: ...
    def get_lifecycle_run_history_rows(self) -> list[JsonObject]: ...
    def compute_ops_health(self) -> JsonObject: ...
    def compute_ops_dashboard_health(self) -> JsonObject: ...
    def compute_ops_dashboard_health_summary(self) -> JsonObject: ...
    def get_current_task_state_payload(self) -> JsonObject: ...
    def get_current_task_state_summary_payload(self) -> JsonObject: ...
    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> JsonObject: ...


def _as_json_object(payload: Any) -> JsonObject:
    return payload if isinstance(payload, dict) else {}


def _active_task_snapshot_path(root_mod: Any) -> Path:
    return Path(
        getattr(
            root_mod,
            "ADMIN_ACTIVE_TASK_SNAPSHOT_PATH",
            Path(root_mod.RUNTIME_CONFIG.data_dir) / "admin-active-task-snapshot.json",
        )
    )


def _matching_live_report_progress(
    root_mod: Any,
    *,
    report_path: Any,
    run_id: str,
    started_at: str,
) -> tuple[JsonObject, JsonObject]:
    report = load_runtime_evidence(report_path, {})
    if not isinstance(report, dict):
        return {}, {}
    report_run_id = str(report.get("runId") or "").strip()
    if report_run_id != str(run_id or "").strip():
        return {}, {}
    report_started_at = str(report.get("startedAt") or "").strip()
    report_started_dt = root_mod.parse_iso(report_started_at)
    started_dt = root_mod.parse_iso(started_at)
    if report_started_at and started_dt and report_started_dt and report_started_dt < started_dt:
        return {}, {}
    if str(report.get("finishedAt") or "").strip():
        return {}, {}
    return _as_json_object(report.get("taskProgress")), _as_json_object(report.get("summary"))


def _pipeline_smoke_report(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
    started_key: str,
    ready_key: str,
    run_id: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    started_at = str(smoke_runtime.get(started_key) or "")
    if not started_at:
        return {}
    finished_at = (
        started_at
        if root_mod.time.monotonic() >= float(smoke_runtime.get(ready_key) or 0.0)
        else ""
    )
    return {
        "runId": run_id,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "status": "ok" if finished_at else "running",
        "summary": summary,
    }


def _pipeline_smoke_runtime() -> dict[str, Any]:
    return {
        "discoveryStartedAt": "",
        "discoveryReadyAt": 0.0,
        "fetchStartedAt": "",
        "fetchReadyAt": 0.0,
    }


def _pipeline_smoke_load_json_object(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
    path: Any,
    default: Any,
) -> Any:
    resolved = Path(path).resolve()
    specs = (
        (
            root_mod.DISCOVERY_REPORT_PATH,
            "discoveryStartedAt",
            "discoveryReadyAt",
            "discovery_smoke",
            {},
        ),
        (
            root_mod.JOBS_FETCH_REPORT_PATH,
            "fetchStartedAt",
            "fetchReadyAt",
            "fetch_smoke",
            {"outputCount": 0},
        ),
    )
    for report_path, started_key, ready_key, run_id, summary in specs:
        if resolved == Path(report_path).resolve():
            return _pipeline_smoke_report(
                root_mod,
                smoke_runtime,
                started_key,
                ready_key,
                run_id,
                summary,
            ) or root_mod.load_json_object(path, default)
    return root_mod.load_json_object(path, default)


def _finish_pipeline_smoke_report_after_delay(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
    path: Any,
    started_key: str,
    ready_key: str,
    run_id: str,
    summary: dict[str, Any],
) -> None:
    ready_at = float(smoke_runtime.get(ready_key) or 0.0)
    delay_s = max(0.0, ready_at - float(root_mod.time.monotonic()))
    if delay_s:
        root_mod.time.sleep(delay_s)
    final_report = _pipeline_smoke_report(
        root_mod, smoke_runtime, started_key, ready_key, run_id, summary
    )
    if final_report:
        root_mod.save_json_atomic(path, final_report)


def _publish_pipeline_smoke_report(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
    path: Any,
    started_key: str,
    ready_key: str,
    run_id: str,
    summary: dict[str, Any],
) -> None:
    report = _pipeline_smoke_report(
        root_mod, smoke_runtime, started_key, ready_key, run_id, summary
    )
    if report:
        root_mod.save_json_atomic(path, report)
    threading.Thread(
        target=_finish_pipeline_smoke_report_after_delay,
        args=(root_mod, smoke_runtime, path, started_key, ready_key, run_id, summary),
        daemon=True,
    ).start()


def _start_pipeline_smoke_child(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
    path: Any,
    started_key: str,
    ready_key: str,
    run_id: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    started_at = root_mod.now_iso()
    smoke_runtime[started_key] = started_at
    smoke_runtime[ready_key] = root_mod.time.monotonic() + 1.2
    _publish_pipeline_smoke_report(
        root_mod,
        smoke_runtime,
        path,
        started_key,
        ready_key,
        run_id,
        summary,
    )
    return {"started": True, "startedAt": started_at, "runId": run_id}


def _pipeline_smoke_fetch_output_count(
    root_mod: Any,
    smoke_runtime: dict[str, Any],
) -> int:
    report = _pipeline_smoke_load_json_object(
        root_mod, smoke_runtime, root_mod.JOBS_FETCH_REPORT_PATH, {}
    )
    summary = root_mod.summarize_fetch_report(root_mod.normalize_fetch_report_contract(report))
    return int(summary.get("outputCount") or 0)


def _build_pipeline_smoke_overrides(root_mod: Any) -> dict[str, Callable[..., Any]]:
    smoke_runtime = _pipeline_smoke_runtime()

    def pipeline_load_json_object(path: Any, default: Any) -> Any:
        return _pipeline_smoke_load_json_object(root_mod, smoke_runtime, path, default)

    def pipeline_trigger_discovery_task(**kwargs: Any) -> tuple[int, dict[str, Any]]:
        result = _start_pipeline_smoke_child(
            root_mod,
            smoke_runtime,
            root_mod.DISCOVERY_REPORT_PATH,
            "discoveryStartedAt",
            "discoveryReadyAt",
            "discovery_smoke",
            {},
        )
        return 200, result

    def pipeline_start_fetcher_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return _start_pipeline_smoke_child(
            root_mod,
            smoke_runtime,
            root_mod.JOBS_FETCH_REPORT_PATH,
            "fetchStartedAt",
            "fetchReadyAt",
            "fetch_smoke",
            {"outputCount": 0},
        )

    def pipeline_start_sync_task(action: str, *, reason: str, automatic: bool) -> dict[str, Any]:
        return {"started": True, "runId": "sync_smoke"}

    def pipeline_wait_for_sync_completion(run_id: str, timeout_s: float = 900.0) -> dict[str, Any]:
        return {
            "id": str(run_id or "sync_smoke"),
            "type": "sync",
            "status": "ok",
            "finishedAt": root_mod.now_iso(),
            "summary": {},
        }

    def pipeline_current_fetch_output_count() -> int:
        return _pipeline_smoke_fetch_output_count(root_mod, smoke_runtime)

    return {
        "load_json_object": pipeline_load_json_object,
        "load_runtime_evidence": pipeline_load_json_object,
        "trigger_discovery_task": pipeline_trigger_discovery_task,
        "start_fetcher_task": pipeline_start_fetcher_task,
        "start_sync_task": pipeline_start_sync_task,
        "wait_for_sync_completion": pipeline_wait_for_sync_completion,
        "current_fetch_output_count": pipeline_current_fetch_output_count,
    }


def get_sync_service(*, root_mod: Any) -> _SyncServiceLike:
    data_dir = Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    services = root_mod.BRIDGE_SERVICES
    with services.sync_service_lock:
        if services.sync_service is not None and services.sync_service_data_dir == data_dir:
            return cast(_SyncServiceLike, services.sync_service)
        services.sync_service_data_dir = data_dir
        services.sync_service = root_mod.SyncService(
            data_dir=data_dir,
            source_sync=root_mod.source_sync_module,
            bridge_log=root_mod.bridge_log,
            load_state=root_mod.load_state,
            persist_state=root_mod.persist_state,
            summarize_state=root_mod.summarize_state,
            ops_state_lock=root_mod.OPS_STATE_LOCK,
            get_security_defaults=root_mod.get_security_defaults,
            sync_state=root_mod.SyncState(data_dir=data_dir),
            get_registry_auto_heal_report=root_mod.get_registry_auto_heal_report,
            task_lifecycle=root_mod._TASK_LIFECYCLE,
        )
        return cast(_SyncServiceLike, services.sync_service)


def get_sync_state(*, root_mod: Any) -> _SyncStateLike:
    return cast(_SyncStateLike, get_sync_service(root_mod=root_mod)._sync_state)  # noqa: SLF001


def get_registry_service(*, root_mod: Any) -> _RegistryServiceLike:
    current_paths = (
        Path(root_mod.ACTIVE_PATH),
        Path(root_mod.PENDING_PATH),
        Path(root_mod.REJECTED_PATH),
    )
    services = root_mod.BRIDGE_SERVICES
    with services.registry_service_lock:
        if services.registry_service is None or services.registry_service_paths != current_paths:
            services.registry_service_paths = current_paths
            services.registry_service = root_mod.RegistryService(
                paths=root_mod.RegistryPaths(
                    active=root_mod.ACTIVE_PATH,
                    pending=root_mod.PENDING_PATH,
                    rejected=root_mod.REJECTED_PATH,
                ),
                default_active=[dict(row) for row in root_mod.DEFAULT_STUDIO_SOURCE_REGISTRY],
                normalize_manual_static=root_mod.normalize_manual_static_studio_fields,
            )
        return cast(_RegistryServiceLike, services.registry_service)


def get_discovery_service(*, root_mod: Any) -> _DiscoveryServiceLike:
    current_paths = (
        Path(root_mod.DISCOVERY_REPORT_PATH),
        Path(root_mod.DISCOVERY_CANDIDATES_PATH),
        Path(root_mod.PENDING_PATH),
        Path(root_mod.DISCOVERY_LOG_PATH),
    )
    services = root_mod.BRIDGE_SERVICES
    with services.discovery_service_lock:
        if services.discovery_service is None or services.discovery_service_paths != current_paths:
            services.discovery_service_paths = current_paths
            services.discovery_service = root_mod.DiscoveryService(
                paths=root_mod.DiscoveryPaths(
                    report=root_mod.DISCOVERY_REPORT_PATH,
                    candidates=root_mod.DISCOVERY_CANDIDATES_PATH,
                    pending=root_mod.PENDING_PATH,
                    log=root_mod.DISCOVERY_LOG_PATH,
                    settings=root_mod.DISCOVERY_CONFIG_PATH,
                    approval_state=root_mod.APPROVAL_STATE_PATH,
                    task_state=root_mod.TASK_STATE_PATH,
                    active_task_snapshot=_active_task_snapshot_path(root_mod),
                ),
                deps=root_mod.DiscoveryDeps(
                    schema_version=root_mod.SCHEMA_VERSION,
                    now_iso=root_mod.now_iso,
                    now_utc=root_mod.now_utc,
                    parse_iso=root_mod.parse_iso,
                    pid_is_running=root_mod.pid_is_running,
                    bridge_log=root_mod.bridge_log,
                    load_json_object=root_mod.load_json_object,
                    load_runtime_evidence=root_mod.load_runtime_evidence,
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
                    start_lifecycle_run=root_mod.start_lifecycle_run,
                    heartbeat_lifecycle_run=root_mod.heartbeat_lifecycle_run,
                    finish_lifecycle_run=root_mod.finish_lifecycle_run,
                    fail_lifecycle_run=root_mod.fail_lifecycle_run,
                    cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
                    get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
                    get_lifecycle_row=lambda run_id, task_type: next(
                        (
                            dict(row)
                            for row in root_mod.get_lifecycle_rows()
                            if str(row.get("runId") or row.get("id") or "").strip()
                            == str(run_id or "").strip()
                            and str(row.get("taskType") or row.get("type") or "").strip().lower()
                            == str(task_type or "").strip().lower()
                        ),
                        None,
                    ),
                ),
            )
        return cast(_DiscoveryServiceLike, services.discovery_service)


def get_task_launch_api(*, root_mod: Any) -> _TaskLaunchApiLike:
    return cast(
        _TaskLaunchApiLike,
        root_mod._task_launch_api.TaskLaunchApi(
            runtime=root_mod._task_launch_api.TaskLaunchRuntime(
                root=Path(root_mod.RUNTIME_CONFIG.root),
                data_dir=Path(root_mod.RUNTIME_CONFIG.data_dir),
                container_mode=bool(getattr(root_mod.RUNTIME_CONFIG, "container_mode", False)),
            ),
            paths=root_mod._task_launch_api.TaskLaunchPaths(
                discovery_log=root_mod.DISCOVERY_LOG_PATH,
                discovery_report=root_mod.DISCOVERY_REPORT_PATH,
                fetcher_log=root_mod.FETCHER_LOG_PATH,
                task_state=root_mod.TASK_STATE_PATH,
                jobs_fetch_report=root_mod.JOBS_FETCH_REPORT_PATH,
                jobs_fetch_tasks=root_mod.JOBS_FETCH_TASKS_PATH,
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
                load_runtime_evidence=root_mod.load_runtime_evidence,
                process_registry=bridge_runtime_state.TASK_PROCESS_REGISTRY,
                cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
                get_lifecycle_row=lambda run_id, task_type: next(
                    (
                        dict(row)
                        for row in root_mod.get_lifecycle_rows()
                        if str(row.get("runId") or row.get("id") or "").strip()
                        == str(run_id or "").strip()
                        and str(row.get("taskType") or row.get("type") or "").strip().lower()
                        == str(task_type or "").strip().lower()
                    ),
                    None,
                ),
            ),
        ),
    )


def get_ops_api(*, root_mod: Any) -> _OpsApiLike:
    return cast(
        _OpsApiLike,
        root_mod._ops_api.OpsApi(
            paths=root_mod._ops_api.OpsPaths(
                ops_alert_state=root_mod.OPS_ALERT_STATE_PATH,
                jobs_fetch_report=root_mod.JOBS_FETCH_REPORT_PATH,
                active_task_snapshot=_active_task_snapshot_path(root_mod),
                dedup_review_state=root_mod.DEDUP_REVIEW_STATE_PATH,
                jobs_fetch_tasks=root_mod.JOBS_FETCH_TASKS_PATH,
                discovery_report=root_mod.DISCOVERY_REPORT_PATH,
                sync_live_task=root_mod.SYNC_LIVE_TASK_PATH,
                task_state=root_mod.TASK_STATE_PATH,
            ),
            deps=root_mod._ops_api.OpsDeps(
                load_json_object=root_mod.load_json_object,
                save_json_atomic=root_mod.save_json_atomic,
                load_state=root_mod.load_state,
                get_registry_summary_payload=root_mod.get_registry_summary_payload,
                load_tombstones=root_mod.load_tombstones,
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
                sync_config_status=get_sync_service(root_mod=root_mod).sync_config_status,
                load_sync_runtime_state=root_mod.load_sync_runtime_state,
                get_jobs_pipeline_status_payload=root_mod.get_jobs_pipeline_status_payload,
                get_jobs_pipeline_schedule_ops_entry=lambda: get_pipeline_schedule_service(
                    root_mod=root_mod
                ).get_ops_schedule_entry(),
                normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
                normalize_discovery_report_contract=root_mod.normalize_discovery_report_contract,
                desktop_mode=root_mod.RUNTIME_CONFIG.desktop_mode,
                get_desktop_last_activity_at=lambda: (
                    bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT
                ),
                get_owner_state=bridge_runtime_state.get_owner_state,
                ops_schema_version=root_mod.OPS_SCHEMA_VERSION,
                get_updater_status_payload=lambda: get_desktop_update_service(
                    root_mod=root_mod
                ).get_status_payload(),
                app_version=root_mod.get_app_version(),
                get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
                get_lifecycle_recent_runs=root_mod.get_lifecycle_recent_runs,
                get_lifecycle_task_events=root_mod._TASK_LIFECYCLE.task_events,
                orphan_lifecycle_run=root_mod.orphan_lifecycle_run,
                load_runtime_evidence=root_mod.load_runtime_evidence,
            ),
        ),
    )


def get_pipeline_service(*, root_mod: Any) -> _PipelineServiceLike:
    services = root_mod.BRIDGE_SERVICES
    with services.pipeline_service_lock:
        if services.pipeline_service is None:
            smoke_mode = (
                str(root_mod.os.getenv("BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE") or "")
                .strip()
                .lower()
            )
            stub_success_mode = smoke_mode == "stub-success"
            pipeline_load_json_object = root_mod.load_json_object
            pipeline_load_runtime_evidence = root_mod.load_runtime_evidence
            pipeline_trigger_discovery_task = root_mod.trigger_discovery_task
            pipeline_start_fetcher_task = root_mod.start_fetcher_task
            pipeline_start_sync_task = root_mod.start_sync_task
            pipeline_wait_for_sync_completion = root_mod._wait_for_sync_completion
            pipeline_current_fetch_output_count = root_mod._current_fetch_output_count
            pipeline_run_registry_conflict_adjudication = root_mod.check_registry_conflicts

            if stub_success_mode:
                smoke_overrides = _build_pipeline_smoke_overrides(root_mod)
                pipeline_load_json_object = smoke_overrides["load_json_object"]
                pipeline_load_runtime_evidence = smoke_overrides["load_runtime_evidence"]
                pipeline_trigger_discovery_task = smoke_overrides["trigger_discovery_task"]
                pipeline_start_fetcher_task = smoke_overrides["start_fetcher_task"]
                pipeline_start_sync_task = smoke_overrides["start_sync_task"]
                pipeline_wait_for_sync_completion = smoke_overrides["wait_for_sync_completion"]
                pipeline_current_fetch_output_count = smoke_overrides["current_fetch_output_count"]
                pipeline_run_registry_conflict_adjudication = lambda _payload: {
                    "demoted": 0,
                    "checkedFamilyCount": 0,
                }

            def pipeline_child_run_is_live(task_type: str, run_id: str) -> bool:
                normalized_type = str(task_type or "").strip().lower()
                normalized_run_id = str(run_id or "").strip()
                if not normalized_type or not normalized_run_id:
                    return False
                for row in root_mod.get_lifecycle_current_runs():
                    if not isinstance(row, dict):
                        continue
                    row_type = str(row.get("type") or row.get("taskType") or "").strip().lower()
                    row_run_id = str(row.get("runId") or row.get("id") or "").strip()
                    if row_type != normalized_type or row_run_id != normalized_run_id:
                        continue
                    if str(row.get("finishedAt") or "").strip():
                        return False
                    try:
                        owner_pid = int(row.get("ownerPid") or 0)
                    except (TypeError, ValueError):
                        owner_pid = 0
                    if owner_pid > 0 and not root_mod.pid_is_running(owner_pid):
                        return False
                    lifecycle_status = (
                        str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()
                    )
                    return lifecycle_status in {"", "queued", "running", "started"}
                return False

            def pipeline_refresh_child_task_heartbeat(
                task_type: str, run_id: str, started_at: str
            ) -> bool:
                normalized_type = str(task_type or "").strip().lower()
                normalized_run_id = str(run_id or "").strip()
                if normalized_type not in {"discovery", "fetch"} or not normalized_run_id:
                    return False
                if not pipeline_child_run_is_live(normalized_type, normalized_run_id):
                    return False
                progress: JsonObject = {}
                summary: JsonObject = {}
                if normalized_type == "discovery":
                    progress, summary = _matching_live_report_progress(
                        root_mod,
                        report_path=root_mod.DISCOVERY_REPORT_PATH,
                        run_id=normalized_run_id,
                        started_at=started_at,
                    )
                elif normalized_type == "fetch":
                    progress, summary = _matching_live_report_progress(
                        root_mod,
                        report_path=root_mod.JOBS_FETCH_TASKS_PATH,
                        run_id=normalized_run_id,
                        started_at=started_at,
                    )
                root_mod.heartbeat_lifecycle_run(
                    normalized_run_id,
                    normalized_type,
                    heartbeat_at=root_mod.now_iso(),
                    stage="pipeline_owned",
                    progress=progress or None,
                    summary=summary or None,
                )
                return True

            def pipeline_completion_notifier(payload: dict[str, Any]) -> dict[str, Any]:
                return notify_pipeline_completion_attention(
                    runtime_config=root_mod.RUNTIME_CONFIG,
                    completion=payload,
                )

            def pipeline_abort_child_run(
                task_type: str, run_id: str, reason: str
            ) -> dict[str, Any]:
                _status, result = get_task_abort_service(root_mod=root_mod).abort_task(
                    {"taskType": task_type, "runId": run_id, "reason": reason}
                )
                return result

            services.pipeline_service = root_mod.PipelineService(
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
                load_runtime_evidence=pipeline_load_runtime_evidence,
                wait_for_sync_completion=pipeline_wait_for_sync_completion,
                discovery_report_path=root_mod.DISCOVERY_REPORT_PATH,
                fetch_report_path=root_mod.JOBS_FETCH_REPORT_PATH,
                trigger_discovery_task=pipeline_trigger_discovery_task,
                start_fetcher_task=pipeline_start_fetcher_task,
                start_sync_task=pipeline_start_sync_task,
                get_app_version=root_mod.get_app_version,
                child_run_is_live=pipeline_child_run_is_live,
                get_projected_run_history=root_mod._get_ops_api().get_projected_run_history,
                run_registry_conflict_adjudication=pipeline_run_registry_conflict_adjudication,
                refresh_child_task_heartbeat=pipeline_refresh_child_task_heartbeat,
                abort_child_run=pipeline_abort_child_run,
                start_lifecycle_run=root_mod.start_lifecycle_run,
                heartbeat_lifecycle_run=root_mod.heartbeat_lifecycle_run,
                finish_lifecycle_run=root_mod.finish_lifecycle_run,
                fail_lifecycle_run=root_mod.fail_lifecycle_run,
                cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
                attach_lifecycle_child=root_mod.attach_lifecycle_child,
                clear_task_state=root_mod.clear_task_state,
                pipeline_completion_notifier=pipeline_completion_notifier,
                control_data_dir=getattr(root_mod.RUNTIME_CONFIG, "data_dir", None),
                container_mode=bool(getattr(root_mod.RUNTIME_CONFIG, "container_mode", False)),
            )
        return cast(_PipelineServiceLike, services.pipeline_service)


def get_pipeline_schedule_service(*, root_mod: Any) -> _PipelineScheduleServiceLike:
    current_path = Path(
        getattr(
            root_mod,
            "JOBS_PIPELINE_SCHEDULE_CONFIG_PATH",
            root_mod.ROOT / "data" / "jobs-pipeline-schedule-config.json",
        )
    )
    global _PIPELINE_SCHEDULE_SERVICE, _PIPELINE_SCHEDULE_SERVICE_PATH
    with _PIPELINE_SCHEDULE_SERVICE_LOCK:
        if _PIPELINE_SCHEDULE_SERVICE is None or _PIPELINE_SCHEDULE_SERVICE_PATH != current_path:
            _PIPELINE_SCHEDULE_SERVICE_PATH = current_path
            _PIPELINE_SCHEDULE_SERVICE = PipelineScheduleService(
                config_path=current_path,
                load_json_object=root_mod.load_json_object,
                save_json_atomic=root_mod.save_json_atomic,
                now_iso=root_mod.now_iso,
                parse_iso=root_mod.parse_iso,
                bridge_log=root_mod.bridge_log,
                get_lifecycle_current_runs=root_mod.get_lifecycle_current_runs,
                get_lifecycle_recent_runs=root_mod.get_lifecycle_recent_runs,
                get_jobs_pipeline_status_payload=root_mod.get_jobs_pipeline_status_payload,
                start_jobs_pipeline_task=root_mod.start_jobs_pipeline_task,
            )
        return cast(_PipelineScheduleServiceLike, _PIPELINE_SCHEDULE_SERVICE)


def get_task_abort_service(*, root_mod: Any) -> Any:
    data_dir = Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    with bridge_runtime_state.TASK_ABORT_SERVICE_LOCK:
        if (
            bridge_runtime_state.TASK_ABORT_SERVICE is not None
            and bridge_runtime_state.TASK_ABORT_SERVICE_DATA_DIR == data_dir
        ):
            return bridge_runtime_state.TASK_ABORT_SERVICE
        bridge_runtime_state.TASK_ABORT_SERVICE_DATA_DIR = data_dir
        bridge_runtime_state.TASK_ABORT_SERVICE = TaskAbortService(
            paths=TaskAbortPaths(
                jobs_fetch_report=root_mod.JOBS_FETCH_REPORT_PATH,
                jobs_fetch_tasks=root_mod.JOBS_FETCH_TASKS_PATH,
                discovery_report=root_mod.DISCOVERY_REPORT_PATH,
            ),
            deps=TaskAbortDeps(
                now_iso=root_mod.now_iso,
                bridge_log=root_mod.bridge_log,
                load_json_object=root_mod.load_json_object,
                save_json_atomic=root_mod.save_json_atomic,
                normalize_fetch_report_contract=root_mod.normalize_fetch_report_contract,
                normalize_discovery_report_contract=(root_mod.normalize_discovery_report_contract),
                get_lifecycle_rows=root_mod.get_lifecycle_rows,
                request_abort_run=root_mod._TASK_LIFECYCLE.request_abort_run,
                cancel_lifecycle_run=root_mod.cancel_lifecycle_run,
                pid_is_running=root_mod.pid_is_running,
                process_registry=bridge_runtime_state.TASK_PROCESS_REGISTRY,
                pipeline_service=root_mod._get_pipeline_service,
            ),
        )
        return bridge_runtime_state.TASK_ABORT_SERVICE


def get_desktop_update_service(*, root_mod: Any) -> _DesktopUpdateServiceLike:
    data_dir = Path(root_mod.RUNTIME_CONFIG.data_dir).resolve()
    services = root_mod.BRIDGE_SERVICES
    with services.desktop_update_service_lock:
        if (
            services.desktop_update_service is not None
            and services.desktop_update_service_data_dir == data_dir
        ):
            return cast(_DesktopUpdateServiceLike, services.desktop_update_service)
        services.desktop_update_service_data_dir = data_dir
        services.desktop_update_service = root_mod.DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=root_mod.get_app_version,
        )
        return cast(_DesktopUpdateServiceLike, services.desktop_update_service)
