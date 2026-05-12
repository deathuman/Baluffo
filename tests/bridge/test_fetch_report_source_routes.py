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

        live_handler = FakeHandler()
        handle_get(live_handler, api=api, path="/ops/fetch-report", query={"view": ["live"]})
        assert "details" not in live_handler.sent[-1]["payload"]["sources"][0]
    finally:
        close_storage_stores()


def test_ops_fetch_report_sources_route_uses_sqlite_and_json_fallback(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "fetch_sources_1",
        "summary": {"sourceCount": 2},
        "sources": [
            {"name": "studio_a", "status": "ok"},
            {"name": "studio_b", "status": "error"},
        ],
    }
    (tmp_path / "jobs-fetch-report.json").write_text(json.dumps(report), encoding="utf-8")
    try:
        runtime_store = get_storage_store(tmp_path)
        runtime_store.set_authority_mode("sourceRuns", "sqlite", reason="test")
        SourceRuntimeStore(runtime_store).upsert_source_runs(
            run_id="fetch_sources_1",
            rows=[
                {"name": "studio_a", "status": "ok"},
                {"name": "studio_b", "status": "error"},
            ],
        )

        handler = FakeHandler()
        handle_get(
            handler,
            api=api,
            path="/ops/fetch-report/sources",
            query={"status": ["error"], "limit": ["1"]},
        )

        payload = handler.sent[-1]["payload"]
        assert payload["source"] == "sqlite"
        assert payload["count"] == 1
        assert payload["sources"][0]["name"] == "studio_b"

        runtime_store.set_authority_mode("sourceRuns", "json", reason="test")
        fallback_handler = FakeHandler()
        handle_get(
            fallback_handler,
            api=api,
            path="/ops/fetch-report/sources",
            query={"status": ["error"], "limit": ["1"]},
        )
        fallback_payload = fallback_handler.sent[-1]["payload"]
        assert fallback_payload["source"] == "json"
        assert fallback_payload["sources"][0]["name"] == "studio_b"
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
