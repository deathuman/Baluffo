from pathlib import Path
from typing import Any, cast

from src.bridge.routes.get_pipeline_tasks import _PipelineTaskRouteApi, handle_pipeline_task_routes
from src.bridge.routes.get_routes import handle_get
from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


class MinimalPipelineTaskApi:
    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]:
        return {"ok": True, "kind": "schedule"}

    def get_jobs_pipeline_status_payload(self) -> dict[str, Any]:
        return {"ok": True, "kind": "status"}


def test_pipeline_schedule_status(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_jobs_pipeline_schedule_payload = lambda: {
        "ok": True,
        "savedConfig": {"schemaVersion": 1, "enabled": True, "intervalHours": 24},
        "status": {"enabled": True, "pending": False, "due": False},
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/tasks/jobs-pipeline-schedule", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["savedConfig"]["enabled"] is True


def test_pipeline_task_get_routes_accept_minimal_capability_object() -> None:
    api = MinimalPipelineTaskApi()

    schedule_handler = FakeHandler()
    assert (
        handle_pipeline_task_routes(
            schedule_handler,
            api=cast(_PipelineTaskRouteApi, api),
            path="/tasks/jobs-pipeline-schedule",
            query={},
        )
        is True
    )
    assert schedule_handler.sent[-1]["payload"] == {"ok": True, "kind": "schedule"}

    status_handler = FakeHandler()
    assert (
        handle_pipeline_task_routes(
            status_handler,
            api=cast(_PipelineTaskRouteApi, api),
            path="/tasks/run-jobs-pipeline-status",
            query={},
        )
        is True
    )
    assert status_handler.sent[-1]["payload"] == {"ok": True, "kind": "status"}


def test_ops_health_includes_pipeline_schedule_entry(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_health = lambda: {
        "ok": True,
        "detail": "unit-test",
        "schedule": {
            "fetcher": {"nextRunAt": ""},
            "discovery": {"nextRunAt": ""},
            "pipeline": {"enabled": False, "pending": False},
        },
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert set(handler.sent[-1]["payload"]["schedule"]) >= {
        "fetcher",
        "discovery",
        "pipeline",
    }


def test_update_jobs_pipeline_schedule(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    saved: dict[str, object] = {}

    def update_schedule(payload: dict[str, object] | None) -> dict[str, object]:
        saved.update(payload or {})
        return {
            "ok": True,
            "savedConfig": {"schemaVersion": 1, "enabled": True, "intervalHours": 24},
            "status": {"enabled": True, "pending": False, "due": False},
        }

    api.update_jobs_pipeline_schedule = update_schedule

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/jobs-pipeline-schedule",
        payload={"enabled": True, "intervalHours": 24},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert saved == {"enabled": True, "intervalHours": 24}
    assert handler.sent[-1]["payload"]["savedConfig"]["intervalHours"] == 24


def test_update_jobs_pipeline_schedule_invalid_payload_returns_400(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.update_jobs_pipeline_schedule = lambda _payload: (_ for _ in ()).throw(
        ValueError("intervalHours must be between 1 and 168")
    )

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/jobs-pipeline-schedule",
        payload={"enabled": True, "intervalHours": 0},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert "intervalHours" in handler.sent[-1]["payload"]["error"]
