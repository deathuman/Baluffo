from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from src.bridge import ops_health


def _dashboard_health_deps(**overrides: Any) -> SimpleNamespace:
    deps = {
        "get_history": lambda: [],
        "get_fetch_report": lambda: {},
        "get_state": lambda: {"active": [], "pending": [{"sourceId": "pending-1"}], "rejected": []},
        "get_registry_summary_payload": lambda: {
            "activeCount": 3,
            "pendingCount": 2,
            "rejectedCount": 1,
            "tombstoneCount": 0,
        },
        "get_tombstones": lambda: {},
        "get_sync_status_payload": lambda: {},
        "now_iso": lambda: "2026-05-14T10:00:00+00:00",
        "desktop_mode": True,
        "desktop_last_activity_at": "2026-05-14T10:00:00+00:00",
        "owner_state": {"startedAt": "2026-05-14T09:59:00+00:00"},
        "load_alert_state_fn": lambda: {},
        "save_alert_state_fn": lambda _payload: None,
        "parse_schedule_metadata_fn": lambda: {"fetcher": {}, "discovery": {}},
        "parse_iso": lambda _value: None,
        "now_utc": lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        "get_source_policy_soak_report": lambda: {},
        "get_updater_status_payload": lambda: {},
        "app_version": "0.0.0-test",
        "startup_ready": True,
    }
    deps.update(overrides)
    return SimpleNamespace(**deps)


def test_parse_schedule_metadata_fallback_is_expected_failures_only() -> None:
    fallback = ops_health.parse_schedule_metadata(
        lambda: (_ for _ in ()).throw(OSError("tasks config unavailable"))
    )

    assert fallback["fetcher"]["note"] == "unknown"

    with pytest.raises(RuntimeError, match="programmer bug"):
        ops_health.parse_schedule_metadata(
            lambda: (_ for _ in ()).throw(RuntimeError("programmer bug"))
        )


def test_dashboard_health_registry_summary_fallback_is_expected_failures_only() -> None:
    payload = ops_health.compute_ops_health(
        _dashboard_health_deps(
            get_registry_summary_payload=mock.Mock(side_effect=OSError("registry unavailable"))
        )
    )

    assert payload["kpis"]["pendingApprovalsCount"] == 1

    with pytest.raises(RuntimeError, match="programmer bug"):
        ops_health.compute_ops_health(
            _dashboard_health_deps(
                get_registry_summary_payload=mock.Mock(side_effect=RuntimeError("programmer bug"))
            )
        )


@pytest.mark.parametrize(
    ("dep_name", "expected_failure"),
    [
        ("get_tombstones", OSError("tombstones unavailable")),
        ("get_sync_status_payload", ValueError("bad sync status")),
    ],
)
def test_dashboard_health_registry_sync_fallbacks_are_expected_failures_only(
    dep_name: str,
    expected_failure: Exception,
) -> None:
    payload = ops_health.compute_ops_health(
        _dashboard_health_deps(**{dep_name: mock.Mock(side_effect=expected_failure)})
    )

    assert payload["kpis"]["registrySync"]["activeCount"] == 3

    with pytest.raises(RuntimeError, match="programmer bug"):
        ops_health.compute_ops_health(
            _dashboard_health_deps(
                **{dep_name: mock.Mock(side_effect=RuntimeError("programmer bug"))}
            )
        )
