from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api
from tests.helpers.mutation import append_and_return


def test_get_routes_discovery_report_reconciles_before_serving(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = FakeHandler()
    calls: list[str] = []
    api.DISCOVERY_REPORT_PATH.write_text(
        json.dumps({"summary": {}, "candidates": [], "failures": []}),
        encoding="utf-8",
    )
    api.reconcile_terminal_discovery_report_from_state = lambda: append_and_return(
        calls, "reconcile", None
    )

    assert handle_get(handler, api=api, path="/discovery/report", query={}) is True

    assert calls == ["reconcile"]
    assert handler.bytes_sent[-1]["status"] == 200
