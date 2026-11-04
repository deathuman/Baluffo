from __future__ import annotations

import gzip
import json
from pathlib import Path

from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    load_registry_conflicts_payload,
)
from src.bridge.registry_conflicts_summary import write_registry_conflicts_full_cache
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_registry_conflicts_route_prefers_gzip_source_state_over_stale_plain(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    source_state_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "sources": {
                    "Winner Source": {
                        "health": "unknown",
                        "healthReason": "",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    with gzip.open(
        source_state_path.with_name("jobs-source-state.json.gz"), "wt", encoding="utf-8"
    ) as handle:
        json.dump(
            {
                "schemaVersion": 1,
                "sources": {
                    "Winner Source": {
                        "health": "healthy",
                        "healthReason": "fresh gzip source-state",
                        "lastSuccessfulFetchAt": "2026-05-09T10:00:00Z",
                        "lastSeenInFetchAt": "2026-05-09T10:00:00Z",
                        "lastJobsKept": 10,
                    },
                    "Loser Source": {
                        "health": "warning",
                        "healthReason": "fresh gzip loser",
                    },
                },
            },
            handle,
        )
    api.load_state = lambda: {
        "active": [
            {
                "id": "winner-1",
                "name": "Winner Source",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 10,
            },
            {
                "id": "loser-1",
                "name": "Loser Source",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    adjudication = api.load_registry_conflict_adjudication()
    cache_key = build_registry_conflicts_summary_cache_key(
        registry_summary=api.get_registry_summary_payload(),
        source_state_path=source_state_path,
        adjudication_payload=adjudication,
    )
    cached_payload = load_registry_conflicts_payload(
        load_state=api.load_state,
        load_json_object=api.load_json_object,
        source_state_path=source_state_path,
        adjudication_payload=adjudication,
    )
    write_registry_conflicts_full_cache(source_state_path, cache_key, cached_payload)

    handler = FakeHandler()
    assert handle_get(handler, api=api, path="/registry/conflicts", query={}) is True

    payload = handler.sent[-1]["payload"]
    assert payload["fullCacheStatus"] == "fresh"
    assert payload["conflicts"][0]["winner"]["healthReason"] == "fresh gzip source-state"
    assert payload["conflicts"][0]["winner"]["lastKeptCount"] == 10
