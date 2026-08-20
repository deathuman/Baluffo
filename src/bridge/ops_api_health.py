"""Ops API health — dashboard health, KPIs, and readiness payloads plus OpsHealthDeps.

AI boundary owns: dashboard health, KPIs, and readiness payloads plus OpsHealthDeps.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.bridge import ops_health as _ops_health
from src.bridge.ops_api_core import OpsApiState, _latest_time_text, _row_active, _task_type
from src.bridge.performance_profile import time_operation
from src.shared.json_shapes import as_json_object


@dataclass(frozen=True)
class OpsHealthDeps:
    get_history: Callable[[], list[dict[str, Any]]]
    get_fetch_report: Callable[[], dict[str, Any]]
    get_state: Callable[[], dict[str, Any]]
    get_registry_summary_payload: Callable[[], dict[str, Any]] | None
    get_tombstones: Callable[[], dict[str, Any]]
    get_sync_status_payload: Callable[[], dict[str, Any]]
    now_iso: Callable[[], str]
    desktop_mode: bool
    desktop_last_activity_at: str
    owner_state: dict[str, Any]
    load_alert_state_fn: Callable[[], dict[str, Any]]
    save_alert_state_fn: Callable[[dict[str, Any]], None]
    parse_schedule_metadata_fn: Callable[[], dict[str, Any]]
    parse_iso: Callable[[Any], Any]
    now_utc: Callable[[], Any]
    get_jobs_pipeline_schedule_ops_entry: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_source_policy_soak_report: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_updater_status_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    app_version: str = ""
    startup_ready: bool = False


class OpsApiHealthMixin(OpsApiState):
    def build_ops_health_deps(self) -> OpsHealthDeps:
        return OpsHealthDeps(
            get_history=lambda: self.get_projected_run_history().rows,
            get_fetch_report=self._load_fetch_report_with_dedup_review_state,
            get_source_policy_soak_report=lambda: self._deps.load_json_object(
                self._paths.jobs_fetch_report.parent.parent
                / "_out"
                / "source-policy-soak-report.json",
                {},
            ),
            get_state=self._deps.load_state,
            get_registry_summary_payload=self._deps.get_registry_summary_payload,
            get_tombstones=self._deps.load_tombstones,
            get_sync_status_payload=self._deps.get_sync_status_payload,
            now_iso=self._deps.now_iso,
            desktop_mode=bool(self._deps.desktop_mode),
            desktop_last_activity_at=str(self._deps.get_desktop_last_activity_at() or ""),
            owner_state=dict(self._deps.get_owner_state() or {}),
            load_alert_state_fn=self.load_alert_state,
            save_alert_state_fn=self.save_alert_state,
            parse_schedule_metadata_fn=self.parse_schedule_metadata,
            parse_iso=self._deps.parse_iso,
            now_utc=self._deps.now_utc,
            get_jobs_pipeline_schedule_ops_entry=self._pipeline_schedule_ops_entry_cached,
            get_updater_status_payload=self._deps.get_updater_status_payload,
            app_version=str(self._deps.app_version or ""),
            startup_ready=True
            if not bool(self._deps.desktop_mode)
            else bool(self._deps.get_owner_state().get("startedAt")),
        )

    def compute_ops_dashboard_health(self) -> dict[str, Any]:
        with time_operation("ops.dashboard_health.total"):
            return _ops_health.compute_ops_health(self.build_ops_health_deps())

    def _active_pipeline_status_payload(self) -> dict[str, Any]:
        try:
            payload = self._deps.get_jobs_pipeline_status_payload()
        except (OSError, TypeError, ValueError):
            return {}
        return as_json_object(payload)

    def _active_pipeline_or_fetch_summary(self) -> dict[str, Any]:
        payload = self._active_pipeline_status_payload()
        if not bool(payload.get("active")):
            return {}
        return {
            "active": True,
            "runId": str(payload.get("runId") or "").strip(),
            "stage": str(payload.get("stage") or "").strip(),
            "activeChildTaskType": str(payload.get("activeChildTaskType") or "").strip(),
            "activeChildRunId": str(payload.get("activeChildRunId") or "").strip(),
        }

    def _active_run_deferred_dashboard_summary(
        self, active_summary: dict[str, Any]
    ) -> dict[str, Any]:
        owner_state = dict(self._deps.get_owner_state() or {})
        startup_ready = (
            True if not bool(self._deps.desktop_mode) else bool(owner_state.get("startedAt"))
        )
        return {
            "service": "baluffo-bridge",
            "desktopMode": bool(self._deps.desktop_mode),
            "appVersion": str(self._deps.app_version or ""),
            "startupReady": startup_ready,
            "generatedAt": self._deps.now_iso(),
            "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
            "owner": {
                "mode": str(owner_state.get("ownerMode") or ""),
                "token": str(owner_state.get("ownerToken") or ""),
                "sessionId": str(owner_state.get("sessionId") or ""),
                "startedBy": str(owner_state.get("startedBy") or ""),
                "startedAt": str(owner_state.get("startedAt") or ""),
                "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
                "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
            },
            "status": "healthy",
            "summaryView": True,
            "detailLevel": "summary",
            "deferredDuringActiveRun": True,
            "activePipelineOrFetchRunning": True,
            "activePipeline": active_summary,
            "fetchKpisDelayedDuringActiveRun": True,
            "alertsEvaluated": False,
            "alertBasis": "active-run-deferred",
            "kpis": {},
            "schedule": {},
            "alerts": [],
            "suppressedAlertsCount": 0,
            "historyCount": 0,
        }

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]:
        with time_operation("ops.dashboard_health.summary.total"):
            active_summary = self._active_pipeline_or_fetch_summary()
            if active_summary:
                return self._active_run_deferred_dashboard_summary(active_summary)
            with time_operation("ops.dashboard_health.summary.history"):
                history = list(self.get_projected_run_history().rows or [])
            with time_operation("ops.dashboard_health.summary.registry"):
                try:
                    registry_summary = as_json_object(self._deps.get_registry_summary_payload())
                except (OSError, TypeError, ValueError):
                    registry_summary = {}
            if not _ops_health._has_registry_summary_counts(registry_summary):
                registry_summary = {}
            with time_operation("ops.dashboard_health.summary.sync"):
                try:
                    sync_status = {
                        "config": as_json_object(self._deps.sync_config_status()),
                        "runtime": as_json_object(self._deps.load_sync_runtime_state()),
                    }
                except (OSError, TypeError, ValueError):
                    sync_status = {}
            with time_operation("ops.dashboard_health.summary.schedule"):
                schedule = _ops_health.populate_schedule_next_run(
                    self.parse_schedule_metadata(),
                    [],
                    self._deps.parse_iso,
                )
                try:
                    pipeline_schedule = self._pipeline_schedule_ops_entry_cached()
                except (RuntimeError, OSError, TypeError, ValueError):
                    pipeline_schedule = {}
                if isinstance(pipeline_schedule, dict):
                    schedule["pipeline"] = dict(pipeline_schedule)
            owner_state = dict(self._deps.get_owner_state() or {})
            startup_ready = (
                True if not bool(self._deps.desktop_mode) else bool(owner_state.get("startedAt"))
            )
            registry_sync = _ops_health.derive_registry_sync_summary(
                state={},
                summary=registry_summary,
                tombstones={},
                sync_status=sync_status,
                history=history,
            )
            pending_count = int(registry_summary.get("pendingCount") or 0)
            sync_ready = bool(as_json_object(sync_status.get("config")).get("ready", True))
            alert_result = _ops_health.evaluate_alerts_summary(
                history=history,
                pending_count=pending_count,
                load_alert_state_fn=self.load_alert_state,
                save_alert_state_fn=self.save_alert_state,
                parse_iso=self._deps.parse_iso,
                now_iso=self._deps.now_iso,
                now_utc=self._deps.now_utc,
            )
            alerts = list(alert_result.get("alerts") or [])
            severity = _ops_health.derive_ops_severity(alerts)
            if not sync_ready and severity == "healthy":
                severity = "warning"
            return {
                "service": "baluffo-bridge",
                "desktopMode": bool(self._deps.desktop_mode),
                "appVersion": str(self._deps.app_version or ""),
                "startupReady": startup_ready,
                "generatedAt": self._deps.now_iso(),
                "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
                "owner": {
                    "mode": str(owner_state.get("ownerMode") or ""),
                    "token": str(owner_state.get("ownerToken") or ""),
                    "sessionId": str(owner_state.get("sessionId") or ""),
                    "startedBy": str(owner_state.get("startedBy") or ""),
                    "startedAt": str(owner_state.get("startedAt") or ""),
                    "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
                    "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
                },
                "status": severity,
                "summaryView": True,
                "detailLevel": "summary",
                "alertsEvaluated": True,
                "alertBasis": "history",
                "kpis": {
                    "pendingApprovalsCount": pending_count,
                    "sourcePolicyRecommendationExport": {},
                    "registrySync": registry_sync,
                },
                "schedule": schedule,
                "alerts": alerts,
                "suppressedAlertsCount": int(alert_result.get("suppressedCount") or 0),
                "historyCount": len(history),
                "updater": {
                    "currentVersion": str(self._deps.app_version or ""),
                    "latestVersion": "",
                    "availability": "unknown",
                    "downloadState": "idle",
                    "installState": "idle",
                    "lastCheckedAt": "",
                    "lastError": "",
                },
            }

    def compute_ops_fetch_kpis_summary(self) -> dict[str, Any]:
        with time_operation("ops.fetch_kpis.summary.total"):
            active_summary = self._active_pipeline_or_fetch_summary()
            with time_operation("ops.fetch_kpis.summary.history"):
                history = list(self.get_projected_run_history().rows or [])
                metrics = _ops_health.collect_fetch_history_metrics(
                    history,
                    self._deps.parse_iso,
                    self._deps.now_utc,
                )
            with time_operation("ops.fetch_kpis.summary.registry"):
                try:
                    registry_summary = as_json_object(self._deps.get_registry_summary_payload())
                except (OSError, TypeError, ValueError):
                    registry_summary = {}
            last_success = as_json_object(metrics.get("lastSuccessFetch"))
            latest_fetch = as_json_object(metrics.get("latestFetch"))
            latest_summary = as_json_object(latest_fetch.get("summary"))
            source_count = int(
                latest_summary.get("sourceCount") or latest_summary.get("totalSources") or 0
            )
            failed_sources = int(latest_summary.get("failedSources") or 0)
            kpis: dict[str, Any] = {
                "sevenDayFetchSuccessRate": round(float(metrics["successRate7d"]), 4),
                "avgFetchDurationMs7d": int(metrics["avgDurationMs7d"]),
            }
            if last_success:
                finished_at = str(last_success.get("finishedAt") or "")
                kpis["lastSuccessfulFetchAt"] = finished_at
                kpis["lastSuccessfulFetchAge"] = _ops_health.format_age(
                    finished_at,
                    self._deps.parse_iso,
                    self._deps.now_utc,
                )
            if source_count > 0:
                kpis["failedSourceRatioLatest"] = round(failed_sources / source_count, 4)
            if "pendingCount" in registry_summary:
                pending_sources_count = int(registry_summary.get("pendingCount") or 0)
                kpis["pendingSourcesCount"] = pending_sources_count
                kpis["pendingApprovalsCount"] = pending_sources_count
            alert_result = _ops_health.evaluate_alerts_summary(
                history=history,
                pending_count=int(registry_summary.get("pendingCount") or 0),
                load_alert_state_fn=self.load_alert_state,
                save_alert_state_fn=self.save_alert_state,
                parse_iso=self._deps.parse_iso,
                now_iso=self._deps.now_iso,
                now_utc=self._deps.now_utc,
            )
            alerts = list(alert_result.get("alerts") or [])
            return {
                "ok": True,
                "summaryView": True,
                "detailLevel": "summary",
                "generatedAt": self._deps.now_iso(),
                "status": _ops_health.derive_ops_severity(alerts),
                "alerts": alerts,
                "suppressedAlertsCount": int(alert_result.get("suppressedCount") or 0),
                "alertsEvaluated": True,
                "alertBasis": "history-active-run" if active_summary else "history",
                **(
                    {
                        "activePipelineOrFetchRunning": True,
                        "activePipeline": active_summary,
                        "fetchKpisStaleDuringActiveRun": True,
                    }
                    if active_summary
                    else {}
                ),
                "kpis": kpis,
            }

    def compute_ops_health_ready(self) -> dict[str, Any]:
        with time_operation("ops.health.ready.total"):
            owner_state = dict(self._deps.get_owner_state() or {})
            startup_ready = (
                True if not bool(self._deps.desktop_mode) else bool(owner_state.get("startedAt"))
            )
            return {
                "service": "baluffo-bridge",
                "status": "healthy",
                "ok": True,
                "summaryView": True,
                "detailLevel": "ready",
                "timestamp": self._deps.now_iso(),
                "desktopMode": bool(self._deps.desktop_mode),
                "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
                "startupReady": startup_ready,
                "appVersion": str(self._deps.app_version or ""),
                "owner": {
                    "mode": str(owner_state.get("ownerMode") or ""),
                    "token": str(owner_state.get("ownerToken") or ""),
                    "sessionId": str(owner_state.get("sessionId") or ""),
                    "startedBy": str(owner_state.get("startedBy") or ""),
                    "startedAt": str(owner_state.get("startedAt") or ""),
                    "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
                    "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
                },
                "lifecycle": {
                    "currentCount": 0,
                    "recentCount": 0,
                    "latestHeartbeatAt": "",
                },
                "schedule": {},
            }

    def compute_ops_health(self) -> dict[str, Any]:
        with time_operation("ops.health.pipeline_status"):
            pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        pipeline_active = bool(
            pipeline_status.get("active") if isinstance(pipeline_status, dict) else False
        )
        with time_operation("ops.health.current_runs"):
            current_rows = self._current_lifecycle_rows()
        active_run_present = pipeline_active or any(
            _row_active(row) and _task_type(row) in {"pipeline", "fetch", "discovery"}
            for row in current_rows
        )
        if active_run_present:
            recent_rows = []
        else:
            with time_operation("ops.health.recent_runs"):
                recent_rows = self._recent_lifecycle_rows()
        with time_operation("ops.health.owner_state"):
            owner_state = dict(self._deps.get_owner_state() or {})
        startup_ready = (
            True if not bool(self._deps.desktop_mode) else bool(owner_state.get("startedAt"))
        )
        heartbeats = [
            str(row.get("heartbeatAt") or "").strip()
            for row in current_rows
            if str(row.get("heartbeatAt") or "").strip()
        ]
        if isinstance(pipeline_status, dict) and pipeline_active:
            heartbeat_at = str(
                pipeline_status.get("heartbeatAt")
                or as_json_object(pipeline_status.get("runtime")).get("heartbeatAt")
                or ""
            ).strip()
            if heartbeat_at:
                heartbeats.append(heartbeat_at)
        with time_operation("ops.health.schedule"):
            schedule = _ops_health.populate_schedule_next_run(
                self.parse_schedule_metadata(),
                recent_rows,
                self._deps.parse_iso,
            )
        with time_operation("ops.health.pipeline_schedule"):
            try:
                pipeline_schedule = self._pipeline_schedule_ops_entry_cached()
            except (RuntimeError, OSError, TypeError, ValueError):
                pipeline_schedule = {}
        if isinstance(pipeline_schedule, dict):
            schedule["pipeline"] = dict(pipeline_schedule)
        return {
            "service": "baluffo-bridge",
            "status": "healthy",
            "ok": True,
            "timestamp": self._deps.now_iso(),
            "desktopMode": bool(self._deps.desktop_mode),
            "desktopLastActivityAt": str(self._deps.get_desktop_last_activity_at() or ""),
            "startupReady": startup_ready,
            "appVersion": str(self._deps.app_version or ""),
            "owner": {
                "mode": str(owner_state.get("ownerMode") or ""),
                "token": str(owner_state.get("ownerToken") or ""),
                "sessionId": str(owner_state.get("sessionId") or ""),
                "startedBy": str(owner_state.get("startedBy") or ""),
                "startedAt": str(owner_state.get("startedAt") or ""),
                "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
                "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
            },
            "lifecycle": {
                "currentCount": len(current_rows),
                "recentCount": len(recent_rows),
                "latestHeartbeatAt": _latest_time_text(*heartbeats),
            },
            "schedule": schedule,
            "pipeline": {
                "active": pipeline_active,
                "runId": str(
                    pipeline_status.get("runId") if isinstance(pipeline_status, dict) else ""
                ).strip(),
                "stage": str(
                    pipeline_status.get("stage") if isinstance(pipeline_status, dict) else ""
                ).strip(),
            },
        }
