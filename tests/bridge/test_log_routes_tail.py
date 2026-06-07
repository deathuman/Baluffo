"""Focused tests for bounded bridge log tail routes."""

from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_log_routes_support_bounded_tail_view(tmp_path: Path) -> None:
    content = "a" * 512 + "b" * 4096
    cases = [
        ("discovery-tail", "DISCOVERY_LOG_PATH", "/discovery/log"),
        ("fetcher-tail", "FETCHER_LOG_PATH", "/fetcher/log"),
    ]

    for case_id, path_attr, route_path in cases:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        log_path = getattr(api, path_attr)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(content, encoding="utf-8", newline="\n")

        handler = FakeHandler()
        result = handle_get(
            handler,
            api=api,
            path=route_path,
            query={"view": ["tail"], "limitChars": ["4096"]},
        )

        payload = handler.sent[-1]["payload"]
        assert result is True, case_id
        assert handler.sent[-1]["status"] == 200
        assert payload["text"] == "b" * 4096
        assert payload["offset"] == 512
        assert payload["nextOffset"] == len(content)
        assert payload["hasMore"] is True


def test_log_routes_reject_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/fetcher/log",
        query={"view": ["everything"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert handler.sent[-1]["payload"]["error"] == "unsupported log view: everything"
