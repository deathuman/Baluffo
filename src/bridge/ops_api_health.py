"""Ops API health — dashboard health, KPIs, and readiness payloads plus OpsHealthDeps.

AI boundary owns: dashboard health, KPIs, and readiness payloads plus OpsHealthDeps.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.bridge import active_task_snapshot as _active_snapshot
from src.bridge import ops_health as _ops_health
from src.bridge.discovery_audit_artifacts import (
    load_page_fetch_lifecycle_receipt as load_page_fetch_lifecycle_receipt,
)
from src.bridge.discovery_audit_artifacts import (
    load_probe_lifecycle_receipt as load_probe_lifecycle_receipt,
)
from src.bridge.fetch_report_summary import (
    compact_browser_fallback_health,
    compact_source_health_summary,
    load_fetch_report_summary_artifact,
)
from src.bridge.ops_api_core import OpsApiState, _latest_time_text, _row_active, _task_type
from src.bridge.performance_profile import time_operation
from src.build_info import read_build_info
from src.shared.json_shapes import as_json_object

# Active runs recompute the KPI payload on every call so stage transitions
# surface at once, but its expensive inputs only move when a fetch finishes or
# the registry is written: history projection measured p95 1555ms and registry
# summary p95 745ms on a Pi with a run active. The payload already declares
# itself stale during a run (fetchKpisStaleDuringActiveRun), so this TTL bounds
# that declared staleness; the active-run probe, alerts, and generatedAt stay
# per-call.
_OPS_FETCH_KPIS_ACTIVE_STABLE_TTL_S = 15.0


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
    alert_state_lock: Any = None
    get_jobs_pipeline_schedule_ops_entry: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_source_policy_soak_report: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_probe_lifecycle_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_page_fetch_lifecycle_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    get_updater_status_payload: Callable[[], dict[str, Any]] = field(
        default_factory=lambda: lambda: {}
    )
    app_version: str = ""
    startup_ready: bool = False


class OpsApiHealthMixin(OpsApiState):
    # (expiresAt, (history, metrics, registry summary)); filled only while a run
    # is active, where the route recomputes on every call.
    _ops_fetch_kpis_stable_cache: (
        tuple[float, tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]] | None
    ) = None

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
            # The receipt is published beside the discovery report (both live in the active data
            # dir), so the read is a small file's own stat-and-parse rather than the tens-of-MB
            # audit artifact it summarizes.
            get_probe_lifecycle_payload=lambda: load_probe_lifecycle_receipt(
                self._paths.discovery_report.parent
            ),
            get_page_fetch_lifecycle_payload=lambda: load_page_fetch_lifecycle_receipt(
                self._paths.discovery_report.parent
            ),
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
            alert_state_lock=self._deps.ops_state_lock,
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
            # ponytail: standalone fetch/bootstrap runs never appear in pipeline
            # status; the fresh hot snapshot covers them with one small read.
            snapshot = self._fresh_active_task_snapshot()
            if snapshot is None or not _active_snapshot.snapshot_has_active_task(snapshot):
                return {}
            return {
                "active": True,
                "source": str(snapshot.get("source") or "hot-active-snapshot"),
                "runId": "",
                "stage": "",
                "activeChildTaskType": "",
                "activeChildRunId": "",
            }
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
            # The receipt is a few hundred bytes beside the report, so the summary -- the poll the
            # Admin page runs at its steady cadence -- carries it too, read from the same place
            # `build_ops_health_deps` reads it for the full payload. Omitting it here made this view
            # read as "no probe-wave receipt published" until a full payload happened to be fetched,
            # which is a different statement from a wave that left nothing behind.
            with time_operation("ops.dashboard_health.summary.probe_lifecycle"):
                try:
                    probe_lifecycle = as_json_object(
                        load_probe_lifecycle_receipt(self._paths.discovery_report.parent)
                    )
                except (OSError, TypeError, ValueError):
                    probe_lifecycle = {}
            # Same reasoning as the probe receipt above: the page-fetch envelope is read from the
            # published sidecar (a bounded stat-and-parse, never the artifact), so the summary view
            # does not need the full payload to report it.
            with time_operation("ops.dashboard_health.summary.page_fetch_lifecycle"):
                try:
                    page_fetch_lifecycle = as_json_object(
                        load_page_fetch_lifecycle_receipt(self._paths.discovery_report.parent)
                    )
                except (OSError, TypeError, ValueError):
                    page_fetch_lifecycle = {}
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
                state_lock=self._deps.ops_state_lock,
            )
            alerts = list(alert_result.get("alerts") or [])
            severity = _ops_health.derive_ops_severity(alerts)
            if not sync_ready and severity == "healthy":
                severity = "warning"
            return {
                "service": "baluffo-bridge",
                "desktopMode": bool(self._deps.desktop_mode),
                "appVersion": str(self._deps.app_version or ""),
                "build": read_build_info(),
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
                    "probeLifecycle": probe_lifecycle,
                    "pageFetchLifecycle": page_fetch_lifecycle,
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

    def _fetch_kpis_stable_parts(
        self, *, active: bool
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        """History rows, fetch metrics, and registry summary for the KPI payload.

        Idle calls compute them fresh; the route's own 15s cache already covers
        that case. While a run is active the route recomputes per call, so the
        parts are reused for one declared-staleness window instead of paying
        ~1.2s of history projection and registry scanning per navigation.
        """
        if not active:
            return self._compute_fetch_kpis_stable_parts()
        now = time.monotonic()
        cached = self._ops_fetch_kpis_stable_cache
        if cached is not None and cached[0] > now:
            return cached[1]
        parts = self._compute_fetch_kpis_stable_parts()
        self._ops_fetch_kpis_stable_cache = (now + _OPS_FETCH_KPIS_ACTIVE_STABLE_TTL_S, parts)
        return parts

    def _compute_fetch_kpis_stable_parts(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
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
        return history, metrics, registry_summary

    def compute_ops_fetch_kpis_summary(self) -> dict[str, Any]:
        with time_operation("ops.fetch_kpis.summary.total"):
            active_summary = self._active_pipeline_or_fetch_summary()
            history, metrics, registry_summary = self._fetch_kpis_stable_parts(
                active=bool(active_summary)
            )
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
            # Read the bounded sidecar, never the full report: this runs on the
            # cached fetch-kpis poll. Omit the key when the sidecar is absent so a
            # bootstrap/first-run payload keeps its previous shape.
            source_artifact = load_fetch_report_summary_artifact(self._paths.jobs_fetch_report)
            source_outcome = compact_source_health_summary(source_artifact.get("sourceHealth"))
            if source_outcome:
                kpis["sourceOutcomeSummary"] = source_outcome
            source_runtime = as_json_object(source_artifact.get("runtime"))
            browser_health = compact_browser_fallback_health(
                source_runtime.get("browserFallbackHealth")
            )
            if browser_health:
                kpis["browserFallbackHealth"] = browser_health
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
                state_lock=self._deps.ops_state_lock,
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
                "build": read_build_info(),
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
                # The recent rows above already cover the ledger; hand them over so
                # the schedule entry does not project the same ledger again.
                pipeline_schedule = self._pipeline_schedule_ops_entry_cached(
                    recent_rows=None if active_run_present else recent_rows
                )
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
