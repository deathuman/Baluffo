from datetime import UTC, datetime
from types import SimpleNamespace

from src.bridge import ops_health


def test_compute_ops_health_includes_pipeline_schedule_entry() -> None:
    deps = SimpleNamespace(
        get_history=lambda: [],
        get_fetch_report=lambda: {},
        get_state=lambda: {"active": [], "pending": []},
        get_registry_summary_payload=lambda: {
            "activeCount": 0,
            "pendingCount": 0,
            "rejectedCount": 0,
            "tombstoneCount": 0,
        },
        get_tombstones=lambda: {},
        get_sync_status_payload=lambda: {},
        now_iso=lambda: "2026-05-14T10:00:00+00:00",
        desktop_mode=True,
        desktop_last_activity_at="2026-05-14T10:00:00+00:00",
        owner_state={"startedAt": "2026-05-14T09:59:00+00:00"},
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _payload: None,
        parse_schedule_metadata_fn=lambda: {"fetcher": {}, "discovery": {}},
        get_jobs_pipeline_schedule_ops_entry=lambda: {
            "enabled": True,
            "intervalHours": 24,
            "nextRunAt": "2026-05-15T10:00:00+00:00",
            "pending": False,
            "due": False,
        },
        parse_iso=lambda _value: None,
        now_utc=lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        get_source_policy_soak_report=lambda: {},
        get_updater_status_payload=lambda: {},
        app_version="0.0.0-test",
        startup_ready=True,
    )

    health = ops_health.compute_ops_health(deps)

    assert health["schedule"]["fetcher"]["nextRunAt"] == ""
    assert health["schedule"]["discovery"]["nextRunAt"] == ""
    assert health["schedule"]["pipeline"]["enabled"] is True
    assert health["schedule"]["pipeline"]["intervalHours"] == 24
