"""Admin tab badges must answer from caches, never from the full conflict scan.

Measured on the Pi: `admin.ops_tab_counts.summary.route_payload` averaged 118s
with a 177s max while a fetch run was active, because the badge path rebuilt the
full conflict queue whenever the exact summary key moved. These tests pin the
cache-only contract and the single-flight guard.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from src.bridge.routes.get_admin_ops_tab_counts import (
    _OPS_TAB_COUNTS_COMPUTE_LOCK,
    handle_admin_ops_tab_counts_routes,
)
from tests.bridge.test_admin_ops_tab_counts_route_capability import (
    MinimalAdminOpsTabCountsRouteApi,
)
from tests.helpers.bridge_api import FakeHandler
from tests.helpers.json_files import write_json as _write_json


def _api_with_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> MinimalAdminOpsTabCountsRouteApi:
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {"summary": {"candidateCount": 2}, "candidateReview": {"totalCandidates": 2}},
    )
    _write_json(
        api.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": [{"staticSourceId": "static-1"}]},
    )
    _write_json(api.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    # A non-empty registry keeps the zero-row shortcut out of the way; these tests
    # cover the cache-only conflict count path.
    monkeypatch.setattr(
        api,
        "get_registry_summary_payload",
        lambda: {
            "activeCount": 1,
            "pendingCount": 0,
            "rejectedCount": 0,
        },
    )
    return api


def _seed_stale_conflicts_summary(api: MinimalAdminOpsTabCountsRouteApi, count: int) -> None:
    path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("registry-conflicts-summary.json")
    path.write_text(
        json.dumps(
            {
                "cacheKey": "key-from-an-earlier-source-state",
                "payload": {"summary": {"conflictCount": count}, "summaryStatus": "ready"},
            }
        ),
        encoding="utf-8",
    )


def _call_counts(api: MinimalAdminOpsTabCountsRouteApi) -> dict:
    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    return cast(dict, payload)


def test_conflict_badge_never_builds_the_full_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api_with_inputs(tmp_path, monkeypatch)

    def _forbidden() -> dict:
        raise AssertionError("the badge must not derive the full conflict queue")

    monkeypatch.setattr(api, "load_state", _forbidden)

    badge = _call_counts(api)["badges"]["registry-conflicts"]
    assert badge["loaded"] is False
    assert badge["error"] == "conflict_summary_not_ready"


def test_conflict_badge_serves_last_known_summary_when_key_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api_with_inputs(tmp_path, monkeypatch)
    _seed_stale_conflicts_summary(api, 7)

    def _forbidden() -> dict:
        raise AssertionError("the badge must not derive the full conflict queue")

    monkeypatch.setattr(api, "load_state", _forbidden)

    badge = _call_counts(api)["badges"]["registry-conflicts"]
    assert badge["loaded"] is True
    assert badge["count"] == 7
    assert badge["tone"] == "warning"


def test_concurrent_counts_poll_serves_last_known_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api_with_inputs(tmp_path, monkeypatch)
    _seed_stale_conflicts_summary(api, 4)
    first = _call_counts(api)
    assert first["badges"]["registry-conflicts"]["count"] == 4

    # Expire the keyed envelope without changing its key, so the next call
    # reaches the lock instead of the fast path.
    cache_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("ops-tab-counts.json")
    cached_document = json.loads(cache_path.read_text(encoding="utf-8"))
    cached_document["cachedAtUnix"] = 1.0
    cache_path.write_text(json.dumps(cached_document), encoding="utf-8")

    assert _OPS_TAB_COUNTS_COMPUTE_LOCK.acquire(blocking=False)
    try:
        payload = _call_counts(api)
    finally:
        _OPS_TAB_COUNTS_COMPUTE_LOCK.release()

    assert payload["computing"] is True
    assert payload["cachedStale"] is True
    assert payload["badges"]["registry-conflicts"]["count"] == 4
