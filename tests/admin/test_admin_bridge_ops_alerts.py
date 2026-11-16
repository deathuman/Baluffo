"""Pure alert-evaluation rules split out of the ops health payload tests.

These pin the bootstrap/coverage-scope rules that decide whether fetch guidance
alerts survive a run; they do not read the bridge payload or its routes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src import admin_bridge
from src.bridge import ops_health


def test_evaluate_alerts_keeps_pipeline_never_run_after_bootstrap_fetch() -> None:
    saved_states: list[dict[str, Any]] = []

    result = ops_health.evaluate_alerts(
        history=[
            {
                "type": "fetch",
                "status": "ok",
                "finishedAt": "2026-05-17T10:00:00+00:00",
                "summary": {"outputCount": 12, "coverageScope": "bootstrap_sheets"},
            }
        ],
        latest_fetch_report={
            "finishedAt": "2026-05-17T10:00:00+00:00",
            "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 3},
        },
        pending_count=0,
        load_alert_state_fn=lambda: {"acked": {"pipeline_never_run": "2026-05-17T10:01:00Z"}},
        save_alert_state_fn=lambda state: saved_states.append(state),
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T10:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 10, 2, tzinfo=UTC),
    )

    guidance = next(
        (alert for alert in result["alerts"] if alert.get("id") == "pipeline_never_run"),
        None,
    )
    assert guidance is not None
    assert guidance["dismissible"] is False
    assert "pipeline_never_run" not in saved_states[-1]["acked"]


def test_evaluate_alerts_clears_pipeline_never_run_after_full_pipeline_success() -> None:
    result = ops_health.evaluate_alerts(
        history=[
            {
                "type": "pipeline",
                "status": "ok",
                "finishedAt": "2026-05-17T10:00:00+00:00",
            }
        ],
        latest_fetch_report={
            "finishedAt": "2026-05-17T10:00:00+00:00",
            "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 3},
        },
        pending_count=0,
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _state: None,
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T10:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 10, 2, tzinfo=UTC),
    )

    alert_ids = {alert["id"] for alert in result["alerts"]}
    assert "pipeline_never_run" not in alert_ids


def test_evaluate_alerts_excludes_bootstrap_runs_from_output_drop_baseline() -> None:
    history = [
        {
            "type": "fetch",
            "status": "ok",
            "finishedAt": f"2026-05-17T0{idx}:00:00+00:00",
            "summary": {"outputCount": output},
        }
        for idx, output in enumerate([100, 105, 98], start=1)
    ]
    history.append(
        {
            "type": "fetch",
            "status": "ok",
            "finishedAt": "2026-05-17T04:00:00+00:00",
            "summary": {"outputCount": 8, "coverageScope": "bootstrap_sheets"},
        }
    )

    result = ops_health.evaluate_alerts(
        history=history,
        latest_fetch_report={
            "finishedAt": "2026-05-17T04:00:00+00:00",
            "summary": {
                "outputCount": 8,
                "failedSources": 0,
                "sourceCount": 3,
                "coverageScope": "bootstrap_sheets",
            },
        },
        pending_count=0,
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _state: None,
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T04:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 4, 2, tzinfo=UTC),
    )

    alert_ids = {alert["id"] for alert in result["alerts"]}
    assert "output_drop" not in alert_ids


def test_browser_health_alert_turns_a_blocked_count_into_a_warning() -> None:
    alert = ops_health.browser_fallback_health_alert(
        None,
        environment_blocked_sources=12,
        now_iso="2026-11-16T12:00:00+00:00",
    )

    assert alert is not None
    assert alert["id"] == "browser_fallback_health"
    assert alert["severity"] == "warning"
    assert alert["dismissible"] is False
    assert "blocked at preflight for 12 sources" in alert["message"]


def test_browser_health_alert_ignores_a_healthy_probe() -> None:
    assert ops_health.browser_fallback_health_alert({"ok": True, "stage": "ready"}) is None
