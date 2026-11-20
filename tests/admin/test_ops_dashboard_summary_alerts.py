"""Dashboard summary alerts: the history-only poll must not drop storage/browser.

The Ops Status chip and Overview badge render `health.status` and
`health.alerts`. The dashboard summary poll evaluated history only, so it could
answer "healthy" between fetch-KPI writes and flip the chip. These tests pin the
shared storage/browser append and the matching alert ids.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from src import admin_bridge
from src.bridge import storage_health

# The container gateway replaces a summary slower than this with its degraded stub.
_GATEWAY_CONTROL_CUTOFF_S = 0.75


def _seed_recent_runs() -> None:
    now = datetime.now(UTC)
    for run_id, task_type in (
        ("fetch_dashboard_summary_1", "fetch"),
        ("pipeline_dashboard_summary_1", "pipeline"),
    ):
        admin_bridge.append_run_history(
            {
                "id": run_id,
                "runId": run_id,
                "type": task_type,
                "status": "ok",
                "startedAt": (now - timedelta(minutes=10)).isoformat(),
                "finishedAt": (now - timedelta(minutes=5)).isoformat(),
                "durationMs": 300000,
            }
        )


def _patch_storage(monkeypatch, storage: dict[str, Any]) -> None:
    monkeypatch.setattr(
        "src.bridge.ops_api_health._storage_health.get_storage_health_payload",
        lambda _data_dir, **_kwargs: {
            "ok": storage.get("healthy") is not False,
            "storage": storage,
        },
    )


def _patch_browser_health(monkeypatch, runtime: dict[str, Any]) -> None:
    monkeypatch.setattr(
        "src.bridge.ops_api_health.load_fetch_report_summary_artifact",
        lambda _path: {"runtime": runtime},
    )


def test_dashboard_summary_keeps_the_browser_alert_the_history_poll_cannot_see(
    admin_bridge_entrypoint_root, monkeypatch
) -> None:
    _seed_recent_runs()
    _patch_storage(
        monkeypatch, {"healthy": True, "quickCheck": "ok", "walMaintenance": {"status": "ok"}}
    )
    _patch_browser_health(
        monkeypatch,
        {
            "browserFallbackHealth": {
                "ok": False,
                "stage": "context",
                "error": "BrowserContext.new_page: Target page, context or browser has been closed",
                "errorType": "TargetClosedError",
            },
            "browserFallbackEnvironmentBlocked": True,
            "browserFallbackEnvironmentBlockedSources": 186,
        },
    )

    api: Any = admin_bridge._get_ops_api()
    dashboard = api.compute_ops_dashboard_health_summary()
    kpis = api.compute_ops_fetch_kpis_summary()

    dashboard_ids = {alert["id"] for alert in dashboard["alerts"]}
    assert "browser_fallback_health" in dashboard_ids
    assert dashboard_ids == {alert["id"] for alert in kpis["alerts"]}
    assert dashboard["alertBasis"] == "history+storage+browser"
    assert dashboard["status"] == kpis["status"] == "warning"


def test_dashboard_summary_keeps_the_storage_alert_and_matches_fetch_kpis(
    admin_bridge_entrypoint_root, monkeypatch
) -> None:
    _seed_recent_runs()
    _patch_storage(
        monkeypatch,
        {
            "healthy": False,
            "quickCheck": "*** in database main ***",
            "quickCheckDeferred": False,
            "unhealthyReason": {
                "code": "integrity_check",
                "message": "malformed database",
                "at": "2026-11-15T10:51:49+00:00",
            },
            "walMaintenance": {"status": "ok"},
        },
    )
    _patch_browser_health(monkeypatch, {})

    api: Any = admin_bridge._get_ops_api()
    dashboard = api.compute_ops_dashboard_health_summary()
    kpis = api.compute_ops_fetch_kpis_summary()

    storage_alert = next(a for a in dashboard["alerts"] if a["id"] == "storage_unhealthy")
    assert storage_alert["severity"] == "critical"
    assert {a["id"] for a in dashboard["alerts"]} == {a["id"] for a in kpis["alerts"]}
    assert dashboard["status"] == kpis["status"] == "critical"


def test_dashboard_summary_bounds_a_slow_storage_read(
    admin_bridge_entrypoint_root, monkeypatch
) -> None:
    """A cold storage read must not consume the gateway's 0.75s budget."""
    started = threading.Event()
    finished = threading.Event()
    release = threading.Event()
    budgets: list[float | None] = []
    real_get_storage_health_payload = storage_health.get_storage_health_payload

    def slow_compute(_data_dir: Any) -> dict[str, Any]:
        started.set()
        release.wait(5.0)
        finished.set()
        return {
            "ok": False,
            "storage": {
                "healthy": False,
                "quickCheck": "*** in database main ***",
                "walMaintenance": {"status": "ok"},
            },
        }

    def record_storage_budget(data_dir: Any, *, timeout_s: float | None = None) -> dict[str, Any]:
        budgets.append(timeout_s)
        return real_get_storage_health_payload(data_dir, timeout_s=timeout_s)

    monkeypatch.setattr(storage_health, "_compute_storage_health", slow_compute)
    monkeypatch.setattr(storage_health, "get_storage_health_payload", record_storage_budget)
    storage_health._HEALTH_CACHE.clear()
    api: Any = admin_bridge._get_ops_api()
    try:
        began = time.perf_counter()
        payload = api.compute_ops_dashboard_health_summary()
        elapsed = time.perf_counter() - began

        assert started.wait(1.0)
        # The bound is requested, not merely observed: a dashboard that dropped
        # the budget would ask storage for its 3s default here.
        budget = budgets[0] if budgets else None
        assert budget is not None
        assert 0 < budget < _GATEWAY_CONTROL_CUTOFF_S
        # timing-ok: dashboard must stay below the gateway stub cutoff.
        assert elapsed < _GATEWAY_CONTROL_CUTOFF_S
        assert "storage_unhealthy" not in {alert["id"] for alert in payload["alerts"]}
    finally:
        release.set()
        finished.wait(5.0)
