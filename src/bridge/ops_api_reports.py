"""Ops API reports — alert state, schedule metadata, and report summarization/history sync.

AI boundary owns: alert state, schedule metadata, and report summarization/history sync.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge import ops_health as _ops_health
from src.bridge import ops_history_projection as _ops_history_projection
from src.bridge import report_normalizer
from src.bridge.ops_api_core import OpsApiState
from src.source_registry_io import load_runtime_evidence


class OpsApiReportsMixin(OpsApiState):
    def failed_source_names_from_latest_report(
        self,
        *,
        allowed_names: set[str] | None = None,
    ) -> list[str]:
        report = self._deps.normalize_fetch_report_contract(
            load_runtime_evidence(self._paths.jobs_fetch_report, {})
        )
        return report_normalizer.failed_source_names_from_report(
            report,
            allowed_names=allowed_names,
        )

    def load_alert_state(self) -> dict[str, Any]:
        return _ops_health.load_alert_state(
            self._deps.load_json_object,
            self._paths.ops_alert_state,
            self._deps.ops_schema_version,
        )

    def save_alert_state(self, state: dict[str, Any]) -> None:
        _ops_health.save_alert_state(
            self._deps.save_json_atomic,
            self._paths.ops_alert_state,
            state,
            self._deps.ops_schema_version,
            self._deps.now_iso,
        )

    def parse_schedule_metadata(self) -> dict[str, Any]:
        return _ops_health.parse_schedule_metadata(self._deps.read_tasks_config)

    def detect_task_interval_hours(self, task: dict[str, Any]) -> float | None:
        return _ops_health.detect_task_interval_hours(task)

    def summarize_fetch_report(self, report: dict[str, Any]) -> dict[str, Any]:
        return _ops_health.summarize_fetch_report(report)

    def summarize_discovery_report(self, report: dict[str, Any]) -> tuple[dict[str, Any], str]:
        return _ops_health.summarize_discovery_report(
            report,
            self._deps.normalize_discovery_report_contract,
            self._deps.parse_iso,
        )

    def sync_history_from_reports(self) -> list[dict[str, Any]]:
        return _ops_history_projection.sync_history_from_reports(
            deps=self._deps,
            paths=self._paths,
            summarize_fetch_report=self.summarize_fetch_report,
            summarize_discovery_report=self.summarize_discovery_report,
        )
