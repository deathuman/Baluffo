from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_startup_metrics_batch_accepts_bounded_metric_rows(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    captured: list[dict[str, Any]] = []

    def _append_startup_metric(event: str, payload: dict[str, Any] | None) -> None:
        captured.append({"event": event, "payload": dict(payload or {})})

    api.append_startup_metric = _append_startup_metric

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/startup-metrics/batch",
        payload={
            "metrics": [
                {"event": "admin_shell_ready", "payload": {"page": "admin"}},
                {"event": "", "payload": {"ignored": True}},
                "not-a-row",
                {"event": "admin_first_useful", "payload": {"ms": 1234}},
            ]
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "accepted": 2}
    assert [row["event"] for row in captured] == ["admin_shell_ready", "admin_first_useful"]
    assert captured[0]["payload"] == {"page": "admin"}


def test_startup_metrics_batch_rejects_non_array_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/startup-metrics/batch",
        payload={"metrics": {"event": "not-an-array"}},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert "metrics must be an array" in handler.sent[-1]["payload"]["error"]
