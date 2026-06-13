from dataclasses import replace
from unittest import mock

import pytest

from src import admin_bridge

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_compute_ops_health_skips_recent_rows_when_pipeline_active() -> None:
    api = admin_bridge._get_ops_api()
    pipeline_status = {
        "active": True,
        "runId": "pipeline_active_health_1",
        "stage": "fetch",
        "startedAt": "2026-05-07T10:00:00+00:00",
        "heartbeatAt": "2026-05-07T10:02:00+00:00",
    }

    original_deps = api._deps
    api._deps = replace(
        original_deps,
        get_jobs_pipeline_status_payload=lambda: pipeline_status,
    )
    try:
        with mock.patch.object(
            api,
            "_recent_lifecycle_rows",
            side_effect=AssertionError("recent rows should be skipped while active"),
        ):
            health = api.compute_ops_health()
    finally:
        api._deps = original_deps

    assert health["pipeline"]["active"] is True
    assert health["pipeline"]["stage"] == "fetch"
    assert health["lifecycle"]["recentCount"] == 0
    assert health["lifecycle"]["latestHeartbeatAt"] == "2026-05-07T10:02:00+00:00"


def test_compute_ops_health_skips_recent_rows_when_fetch_active() -> None:
    api = admin_bridge._get_ops_api()
    admin_bridge.start_lifecycle_run(
        run_id="fetch_active_health_1",
        task_type="fetch",
        started_at="2026-05-07T10:00:00+00:00",
        owner_kind="process",
        owner_pid=111,
    )
    admin_bridge.heartbeat_lifecycle_run(
        "fetch_active_health_1",
        "fetch",
        heartbeat_at="2026-05-07T10:02:00+00:00",
    )

    with mock.patch.object(
        api,
        "_recent_lifecycle_rows",
        side_effect=AssertionError("recent rows should be skipped while fetch is active"),
    ):
        health = api.compute_ops_health()

    assert health["pipeline"]["active"] is False
    assert health["lifecycle"]["currentCount"] == 1
    assert health["lifecycle"]["recentCount"] == 0
    assert health["lifecycle"]["latestHeartbeatAt"] == "2026-05-07T10:02:00+00:00"
