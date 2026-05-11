import json
import os
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_discovery_candidates_route_ignores_stale_journal(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"
    api.DISCOVERY_CANDIDATES_PATH.write_text(
        json.dumps([{"id": "canonical", "name": "Canonical"}]),
        encoding="utf-8",
    )
    journal_path = tmp_path / "source-discovery-candidates.jsonl"
    journal_path.write_text(
        '{"schemaVersion":1,"contentHash":"stale","payload":[{"id":"stale"}]}\n',
        encoding="utf-8",
    )
    os.utime(api.DISCOVERY_CANDIDATES_PATH, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    handler = FakeHandler()
    assert handle_get(handler, api=api, path="/discovery/candidates", query={}) is True

    payload = handler.sent[-1]["payload"]
    assert payload["count"] == 1
    assert payload["candidates"][0]["id"] == "canonical"
