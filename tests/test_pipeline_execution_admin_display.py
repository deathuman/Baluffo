"""Tests for pipeline execution admin display payloads."""

from tests._pipeline_execution_shared import (
    Path,
    make_parse_iso,
)


class TestAdminPanelTaskDisplay:
    """Tests for Admin panel task information display accuracy."""

    def test_ops_health_returns_service_info(self, tmp_path: Path) -> None:
        """Test ops health returns service info for Admin display."""
        from datetime import datetime

        from src.bridge import ops_health
        from src.bridge.ops_api import OpsHealthDeps

        history = [
            {
                "id": "pipeline-1",
                "type": "pipeline",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:05:00Z",
                "durationMs": 300000,
                "summary": {
                    "baselineOutputCount": 10,
                    "jobsPageLoadedCount": 8,
                    "finalOutputCount": 15,
                    "updatesFound": True,
                },
            },
        ]
        deps = OpsHealthDeps(
            get_history=lambda: history,
            get_fetch_report=lambda: {"summary": {}},
            get_state=lambda: {"active": False, "pending": []},
            get_registry_summary_payload=None,
            get_tombstones=lambda: {},
            get_sync_status_payload=lambda: {},
            now_iso=lambda: "2026-03-22T12:10:00Z",
            desktop_mode=True,
            desktop_last_activity_at="2026-03-22T12:10:00Z",
            owner_state={},
            load_alert_state_fn=lambda: {},
            save_alert_state_fn=lambda x: None,
            parse_schedule_metadata_fn=lambda: {
                "fetcher": {"intervalHours": 6},
                "discovery": {"intervalHours": 24},
            },
            parse_iso=make_parse_iso(),
            now_utc=lambda: datetime(2026, 3, 22, 12, 10, 0),
        )

        health = ops_health.compute_ops_health(deps)

        # Verify health includes service info for Admin display
        assert "service" in health
        assert health["service"] == "baluffo-bridge"
        assert "status" in health

    def test_ops_history_includes_pipeline_runs(self, tmp_path: Path) -> None:
        """Test /ops/history endpoint returns pipeline runs for Admin display."""
        from src.bridge.routes import get_routes

        history = [
            {
                "id": "pipeline-123",
                "type": "pipeline",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:05:00Z",
                "durationMs": 300000,
                "summary": {
                    "baselineOutputCount": 10,
                    "jobsPageLoadedCount": 8,
                    "finalOutputCount": 15,
                    "updatesFound": True,
                },
            },
            {
                "id": "discovery-123",
                "type": "discovery",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:01:00Z",
                "durationMs": 60000,
                "summary": {"candidatesFound": 25},
            },
        ]

        class FakeHandler:
            def __init__(self):
                self.sent = []

            def send_json(self, payload, status=200):
                self.sent.append({"status": status, "payload": payload})

        class FakeApi:
            def get_lifecycle_run_history_rows(self):
                return history

        handler = FakeHandler()
        api = FakeApi()

        result = get_routes.handle_get(handler, api=api, path="/ops/history", query={})

        assert result is True
        payload = handler.sent[-1]["payload"]

        assert "runs" in payload
        runs = payload["runs"]

        # Verify pipeline run is included
        pipeline_runs = [r for r in runs if r.get("type") == "pipeline"]
        assert len(pipeline_runs) >= 1

        pipeline = pipeline_runs[0]
        assert pipeline["id"] == "pipeline-123"
        assert pipeline["status"] == "ok"
        assert "summary" in pipeline
        assert pipeline["summary"]["updatesFound"] is True
