from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.storage import SourceRuntimeStore
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_ops_fetch_report_hydrates_sources_from_sqlite_when_authoritative(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "fetch_sqlite_1",
        "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 1},
        "sourceRuns": {
            "format": "sqlite",
            "sourceDetailsArchive": {
                "path": "artifacts/fetch/fetch_sqlite_1/source-details.json.gz"
            },
        },
        "sources": [
            {
                "name": "studio_a",
                "status": "ok",
                "adapter": "static",
                "fetchedCount": 1,
                "keptCount": 1,
            }
        ],
    }
    (tmp_path / "jobs-fetch-report.json").write_text(json.dumps(report), encoding="utf-8")
    try:
        runtime_store = get_storage_store(tmp_path)
        runtime_store.set_authority_mode("sourceRuns", "sqlite", reason="test")
        SourceRuntimeStore(runtime_store).upsert_source_runs(
            run_id="fetch_sqlite_1",
            rows=[
                {
                    "name": "studio_a",
                    "status": "ok",
                    "adapter": "static",
                    "fetchedCount": 1,
                    "keptCount": 1,
                    "details": [{"url": "https://example.com/job/1"}],
                }
            ],
        )

        handler = FakeHandler()
        result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

        assert result is True
        payload = handler.sent[-1]["payload"]
        assert payload["sources"][0]["details"] == [{"url": "https://example.com/job/1"}]
        assert payload["sourceRuns"]["sourceDetailsArchive"]["path"].endswith(
            "source-details.json.gz"
        )

        live_handler = FakeHandler()
        handle_get(live_handler, api=api, path="/ops/fetch-report", query={"view": ["live"]})
        live_payload = live_handler.sent[-1]["payload"]
        assert live_payload["sources"] == []
        assert live_payload["sourceCount"] == 1
        assert live_payload["sourcesTruncated"] is True
    finally:
        close_storage_stores()


def test_ops_fetch_report_sqlite_mismatch_rolls_source_runs_back_to_json(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "fetch_sqlite_mismatch_1",
        "summary": {"sourceCount": 2},
        "sources": [{"name": "studio_a", "status": "ok"}, {"name": "studio_b", "status": "ok"}],
    }
    (tmp_path / "jobs-fetch-report.json").write_text(json.dumps(report), encoding="utf-8")
    try:
        runtime_store = get_storage_store(tmp_path)
        runtime_store.set_authority_mode("sourceRuns", "sqlite", reason="test")
        SourceRuntimeStore(runtime_store).upsert_source_runs(
            run_id="fetch_sqlite_mismatch_1",
            rows=[{"name": "studio_a", "status": "ok"}],
        )

        handler = FakeHandler()
        handle_get(handler, api=api, path="/ops/fetch-report", query={})

        payload = handler.sent[-1]["payload"]
        assert [row["name"] for row in payload["sources"]] == ["studio_a", "studio_b"]
        assert runtime_store.get_authority_modes()["sourceRuns"] == "json"
    finally:
        close_storage_stores()
