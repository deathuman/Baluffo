from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge import ops_health


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def test_summary_alerts_warn_when_successful_fetch_is_stale() -> None:
    now = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    history = [
        {
            "taskType": "fetch",
            "status": "completed",
            "finishedAt": (now - timedelta(hours=26)).isoformat(),
        },
        {
            "taskType": "pipeline",
            "status": "completed",
            "finishedAt": (now - timedelta(hours=25)).isoformat(),
        },
    ]

    result = ops_health.evaluate_alerts_summary(
        history=history,
        pending_count=7,
        load_alert_state_fn=lambda: {"acked": {}},
        save_alert_state_fn=lambda _state: None,
        parse_iso=_parse_iso,
        now_iso=lambda: now.isoformat(),
        now_utc=lambda: now,
    )

    alert_ids = {row["id"] for row in result["alerts"]}
    assert "stale_fetch" in alert_ids
    assert "pipeline_never_run" not in alert_ids
    assert result["pendingApprovals"] == 7
    assert ops_health.derive_ops_severity(result["alerts"]) == "warning"


def test_summary_alerts_warn_when_pipeline_never_completed() -> None:
    now = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    history = [
        {
            "taskType": "fetch",
            "status": "completed",
            "finishedAt": (now - timedelta(minutes=20)).isoformat(),
        }
    ]

    result = ops_health.evaluate_alerts_summary(
        history=history,
        pending_count=0,
        load_alert_state_fn=lambda: {"acked": {}},
        save_alert_state_fn=lambda _state: None,
        parse_iso=_parse_iso,
        now_iso=lambda: now.isoformat(),
        now_utc=lambda: now,
    )

    assert [row["id"] for row in result["alerts"]] == ["pipeline_never_run"]
