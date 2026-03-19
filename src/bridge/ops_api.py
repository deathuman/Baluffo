from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from src import fetcher_metrics as fetcher_metrics_module
from src.bridge import ops_health as _ops_health
from src.bridge import report_normalizer
from src.bridge import run_history_api as _run_history_api


@dataclass(frozen=True)
class OpsPaths:
    ops_alert_state: Path
    jobs_fetch_report: Path
    discovery_report: Path


@dataclass(frozen=True)
class OpsDeps:
    load_json_object: Callable[[Path, Any], Any]
    save_json_atomic: Callable[[Path, Any], None]
    load_state: Callable[[], Dict[str, Any]]
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    read_tasks_config: Callable[[], Dict[str, Any]]
    ops_state_lock: Any
    load_run_history: Callable[[], List[Dict[str, Any]]]
    save_run_history: Callable[[List[Dict[str, Any]]], None]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    clear_task_state_locked: Callable[[str], None]
    upsert_run_history: Callable[..., Dict[str, Any]]
    task_running_from_state: Callable[[str], bool]
    report_is_stale_in_progress: Callable[..., bool]
    get_active_sync_runs: Callable[[], set[str]]
    normalize_fetch_report_contract: Callable[[Dict[str, Any]], Dict[str, Any]]
    normalize_discovery_report_contract: Callable[[Dict[str, Any]], Dict[str, Any]]
    desktop_mode: bool
    get_desktop_last_activity_at: Callable[[], str]
    ops_schema_version: int


@dataclass(frozen=True)
class OpsHealthDeps:
    get_history: Callable[[], List[Dict[str, Any]]]
    get_fetch_report: Callable[[], Dict[str, Any]]
    get_state: Callable[[], Dict[str, Any]]
    now_iso: Callable[[], str]
    desktop_mode: bool
    desktop_last_activity_at: str
    load_alert_state_fn: Callable[[], Dict[str, Any]]
    save_alert_state_fn: Callable[[Dict[str, Any]], None]
    parse_schedule_metadata_fn: Callable[[], Dict[str, Any]]
    parse_iso: Callable[[Any], Any]
    now_utc: Callable[[], Any]


class OpsApi:
    def __init__(self, *, paths: OpsPaths, deps: OpsDeps) -> None:
        self._paths = paths
        self._deps = deps

    def failed_source_names_from_latest_report(
        self,
        *,
        allowed_names: set[str] | None = None,
    ) -> List[str]:
        report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        return report_normalizer.failed_source_names_from_report(
            report,
            allowed_names=allowed_names,
        )

    def load_alert_state(self) -> Dict[str, Any]:
        return _ops_health.load_alert_state(
            self._deps.load_json_object,
            self._paths.ops_alert_state,
            self._deps.ops_schema_version,
        )

    def save_alert_state(self, state: Dict[str, Any]) -> None:
        _ops_health.save_alert_state(
            self._deps.save_json_atomic,
            self._paths.ops_alert_state,
            state,
            self._deps.ops_schema_version,
            self._deps.now_iso,
        )

    def parse_schedule_metadata(self) -> Dict[str, Any]:
        return _ops_health.parse_schedule_metadata(self._deps.read_tasks_config)

    def detect_task_interval_hours(self, task: Dict[str, Any]) -> float | None:
        return _ops_health.detect_task_interval_hours(task)

    def summarize_fetch_report(self, report: Dict[str, Any]) -> Dict[str, Any]:
        return _ops_health.summarize_fetch_report(report)

    def summarize_discovery_report(self, report: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        return _ops_health.summarize_discovery_report(
            report,
            self._deps.normalize_discovery_report_contract,
            self._deps.parse_iso,
        )

    def sync_history_from_reports(self) -> List[Dict[str, Any]]:
        return _run_history_api.sync_history_from_reports(
            _run_history_api.SyncHistoryDeps(
                ops_state_lock=self._deps.ops_state_lock,
                load_run_history=self._deps.load_run_history,
                save_run_history=self._deps.save_run_history,
                save_json_atomic=self._deps.save_json_atomic,
                prune_started_rows_for_type=self._deps.prune_started_rows_for_type,
                clear_task_state=self._deps.clear_task_state,
                clear_task_state_locked=self._deps.clear_task_state_locked,
                upsert_run_history=self._deps.upsert_run_history,
                task_running_from_state=self._deps.task_running_from_state,
                report_is_stale_in_progress=self._deps.report_is_stale_in_progress,
                load_json_object=self._deps.load_json_object,
                normalize_fetch_report_contract=self._deps.normalize_fetch_report_contract,
                normalize_discovery_report_contract=self._deps.normalize_discovery_report_contract,
                summarize_fetch_report=self.summarize_fetch_report,
                summarize_discovery_report=self.summarize_discovery_report,
                jobs_fetch_report_path=self._paths.jobs_fetch_report,
                discovery_report_path=self._paths.discovery_report,
                get_active_sync_runs=self._deps.get_active_sync_runs,
                parse_iso=self._deps.parse_iso,
                now_iso=self._deps.now_iso,
                now_utc=self._deps.now_utc,
            )
        )

    def build_ops_health_deps(self) -> OpsHealthDeps:
        return OpsHealthDeps(
            get_history=self.sync_history_from_reports,
            get_fetch_report=lambda: self._deps.normalize_fetch_report_contract(
                self._deps.load_json_object(self._paths.jobs_fetch_report, {})
            ),
            get_state=self._deps.load_state,
            now_iso=self._deps.now_iso,
            desktop_mode=bool(self._deps.desktop_mode),
            desktop_last_activity_at=str(self._deps.get_desktop_last_activity_at() or ""),
            load_alert_state_fn=self.load_alert_state,
            save_alert_state_fn=self.save_alert_state,
            parse_schedule_metadata_fn=self.parse_schedule_metadata,
            parse_iso=self._deps.parse_iso,
            now_utc=self._deps.now_utc,
        )

    def compute_ops_health(self) -> Dict[str, Any]:
        return _ops_health.compute_ops_health(self.build_ops_health_deps())

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> Dict[str, Any]:
        latest_fetch_report = self._deps.normalize_fetch_report_contract(
            self._deps.load_json_object(self._paths.jobs_fetch_report, {})
        )
        history = self.sync_history_from_reports()
        return fetcher_metrics_module.build_metrics(
            latest_fetch_report,
            history,
            window=max(1, int(window_runs or 1)),
        )


__all__ = ["OpsApi", "OpsDeps", "OpsHealthDeps", "OpsPaths"]
