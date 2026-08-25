from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.bridge.routes.get_admin_ops_tab_counts import handle_admin_ops_tab_counts_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalAdminOpsTabCountsRouteApi:
    def __init__(self, root: Path) -> None:
        self.DISCOVERY_REPORT_PATH = root / "discovery-report.json"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.SOURCE_POLICY_RECOMMENDATIONS_PATH = root / "source-policy-recommendations.json"
        self.SOURCE_POLICY_REVIEW_STATE_PATH = root / "source-policy-review-state.json"

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]:
        return {"alerts": [{"severity": "critical"}]}

    def get_registry_auto_heal_report(self) -> dict[str, Any]:
        return {}

    def get_registry_summary_payload(self) -> dict[str, Any]:
        return {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0}

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = default
        return dict(payload) if isinstance(payload, dict) else {}

    def load_registry_conflict_adjudication(self) -> dict[str, Any]:
        return {}

    def load_state(self) -> dict[str, Any]:
        return {"active": [], "pending": [], "rejected": []}

    def normalize_discovery_report_contract(self, payload: Any) -> dict[str, Any]:
        return dict(payload) if isinstance(payload, dict) else {}

    def now_iso(self) -> str:
        return "2026-06-19T10:00:00+00:00"

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "activeCount": len(state.get("active", [])),
            "pendingCount": len(state.get("pending", [])),
            "rejectedCount": len(state.get("rejected", [])),
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_admin_ops_tab_counts_accepts_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {
            "summary": {"candidateCount": 2},
            "candidateReview": {"totalCandidates": 2},
        },
    )
    _write_json(
        api.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": [{"staticSourceId": "static-1"}]},
    )
    _write_json(api.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})

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
    assert payload["ok"] is True
    assert payload["generatedAt"] == "2026-06-19T10:00:00+00:00"
    assert payload["badges"]["overview"]["count"] == 1
    assert payload["badges"]["discovery"]["count"] == 2
    assert "source-policy" in payload["badges"]
    assert "registry-conflicts" in payload["badges"]
    assert payload["badges"]["dedup"]["loaded"] is False


def test_admin_ops_tab_counts_minimal_capability_rejects_unknown_view() -> None:
    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler,
            api=MinimalAdminOpsTabCountsRouteApi(Path(".")),
            path="/admin/ops-tab-counts",
            query={"view": ["full"]},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 400
    assert "unsupported ops-tab-counts view" in handler.sent[-1]["payload"]["error"]


def _open_counts_for(tmp_path: Path) -> tuple[MinimalAdminOpsTabCountsRouteApi, FakeHandler]:
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
    return api, handler


def test_admin_ops_tab_counts_serves_fresh_cache_when_inputs_unchanged(
    tmp_path: Path,
) -> None:
    api, first_handler = _open_counts_for(tmp_path)
    assert first_handler.sent[-1]["payload"]["badges"]["discovery"]["count"] == 2

    # No input mutation. Second call within TTL + matching mtime key must serve
    # cached payload without recomputing.
    second_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            second_handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    second_payload = second_handler.sent[-1]["payload"]
    assert second_payload.get("cachedResponse") is True
    assert isinstance(second_payload.get("cachedAgeS"), (int, float))
    # Cached payload still carries the original computed numbers.
    assert second_payload["badges"]["discovery"]["count"] == 2


def test_admin_ops_tab_counts_recomputes_when_inputs_move(tmp_path: Path) -> None:
    api, first_handler = _open_counts_for(tmp_path)
    assert first_handler.sent[-1]["payload"]["badges"]["discovery"]["count"] == 2

    # Mutate one input file -> mtime key mismatch -> recompute, even within TTL.
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {"summary": {"candidateCount": 7}, "candidateReview": {"totalCandidates": 7}},
    )

    second_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            second_handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    second_payload = second_handler.sent[-1]["payload"]
    # No cachedResponse marker means the cache was bypassed and a fresh payload served.
    assert "cachedResponse" not in second_payload
    assert second_payload["badges"]["discovery"]["count"] == 7


def test_admin_ops_tab_counts_drops_stale_envelope_after_ttl(tmp_path: Path) -> None:
    api, _ = _open_counts_for(tmp_path)

    # Force the cachedAt stamp into the past; mtime key still matches, but the
    # safety-net TTL has expired -> must recompute.
    cache_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("ops-tab-counts.json")
    cache_doc = json.loads(cache_path.read_text(encoding="utf-8"))
    cache_doc["cachedAtUnix"] = 1.0
    cache_path.write_text(json.dumps(cache_doc), encoding="utf-8")

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
    assert "cachedResponse" not in payload
    assert payload["badges"]["discovery"]["count"] == 2


def test_admin_ops_tab_counts_cache_survives_source_state_heartbeat(
    tmp_path: Path,
) -> None:
    """Heartbeat rewrites keep a stable size; the cache must survive them."""
    api, first_handler = _open_counts_for(tmp_path)
    assert first_handler.sent[-1]["payload"]["badges"]["discovery"]["count"] == 2

    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    heartbeat_payload = {
        "schemaVersion": 1,
        "updatedAt": "2026-06-19T10:05:00+00:00",
        "sources": {},
    }
    _write_json(source_state_path, heartbeat_payload)
    source_state_path.touch()
    size_after_first = source_state_path.stat().st_size

    # Second heartbeat: new mtime, identical size -> still a cache hit.
    heartbeat_payload["updatedAt"] = "2026-06-19T10:06:00+00:00"
    _write_json(source_state_path, heartbeat_payload)
    assert source_state_path.stat().st_size == size_after_first

    # First drive after the size appeared: recomputes and refreshes the cache
    # with the new (size-based) key. Second drive: hit.
    refresh_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            refresh_handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    assert "cachedResponse" not in refresh_handler.sent[-1]["payload"]

    second_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            second_handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    second_payload = second_handler.sent[-1]["payload"]
    assert second_payload.get("cachedResponse") is True
    assert second_payload["badges"]["discovery"]["count"] == 2


def test_admin_ops_tab_counts_recomputes_when_source_state_grows(
    tmp_path: Path,
) -> None:
    """A real merge changes the row set -> size key mismatch -> recompute."""
    api, first_handler = _open_counts_for(tmp_path)
    assert first_handler.sent[-1]["payload"]["badges"]["discovery"]["count"] == 2

    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    _write_json(
        source_state_path,
        {
            "schemaVersion": 1,
            "updatedAt": "2026-06-19T10:07:00+00:00",
            "sources": {"static_source::new": {"status": "ok"}},
        },
    )

    second_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            second_handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )
    second_payload = second_handler.sent[-1]["payload"]
    assert "cachedResponse" not in second_payload
    assert second_payload["badges"]["discovery"]["count"] == 2


@pytest.mark.parametrize("bad_stamp", ["not-a-number", None, float("inf")])
def test_admin_ops_tab_counts_corrupt_envelope_recomputes_without_error(
    tmp_path: Path, bad_stamp: Any
) -> None:
    api, _ = _open_counts_for(tmp_path)

    cache_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("ops-tab-counts.json")
    cache_doc = json.loads(cache_path.read_text(encoding="utf-8"))
    cache_doc["cachedAtUnix"] = bad_stamp
    cache_path.write_text(json.dumps(cache_doc), encoding="utf-8")

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
    assert "cachedResponse" not in payload
    assert payload["badges"]["discovery"]["count"] == 2


def _write_fetch_report_with_dedup(api: MinimalAdminOpsTabCountsRouteApi, gate_status: str) -> None:
    _write_json(
        api.JOBS_FETCH_REPORT_PATH,
        {
            "latestRun": {
                "dedupEvidence": {
                    "reviewQueue": [
                        {"title": "Role A"},
                        {"title": "Role B"},
                    ],
                    "providerStaticDisagreementExamples": [{"title": "X"}],
                    "dedupAuditGate": {
                        "status": gate_status,
                        "currentRunBlockingReviewQueueCount": 1,
                        "carriedBlockingReviewQueueCount": 0,
                        "providerStaticDisagreementBlockedCount": 0,
                        "blockers": (
                            ["provider_static_disagreement_needs_review"]
                            if gate_status == "blocked"
                            else []
                        ),
                        "warnings": [],
                    },
                }
            }
        },
    )


def test_admin_ops_tab_counts_dedup_badge_loads_from_fetch_report(tmp_path: Path) -> None:
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {"summary": {"candidateCount": 1}, "candidateReview": {"totalCandidates": 1}},
    )
    _write_fetch_report_with_dedup(api, gate_status="blocked")

    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler, api=api, path="/admin/ops-tab-counts", query={"view": ["summary"]}
        )
        is True
    )
    badge = handler.sent[-1]["payload"]["badges"]["dedup"]
    # max(review rows=3, blocking=1, gate flags=1) and blocked gate -> critical
    assert badge["loaded"] is True
    assert badge["count"] == 3
    assert badge["tone"] == "critical"
    assert badge["title"] == "3 dedup review items"


def test_admin_ops_tab_counts_dedup_badge_loads_from_flat_fetch_report_shape(
    tmp_path: Path,
) -> None:
    """Live fetch reports carry top-level dedupEvidence (no latestRun wrapper)."""
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {"summary": {"candidateCount": 1}, "candidateReview": {"totalCandidates": 1}},
    )
    _write_json(
        api.JOBS_FETCH_REPORT_PATH,
        {
            "schemaVersion": 1,
            "status": "completed",
            "dedupEvidence": {
                "reviewQueue": [{"dedupKey": "url:abc"}],
                "dedupAuditGate": {
                    "status": "blocked",
                    "currentRunBlockingReviewQueueCount": 0,
                    "blockers": ["provider_static_disagreement_needs_review"],
                    "warnings": [],
                },
            },
        },
    )

    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler, api=api, path="/admin/ops-tab-counts", query={"view": ["summary"]}
        )
        is True
    )
    badge = handler.sent[-1]["payload"]["badges"]["dedup"]
    assert badge["loaded"] is True
    assert badge["count"] == 1
    assert badge["tone"] == "critical"


def test_admin_ops_tab_counts_dedup_badge_recomputes_when_fetch_report_moves(
    tmp_path: Path,
) -> None:
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {"summary": {"candidateCount": 1}, "candidateReview": {"totalCandidates": 1}},
    )
    _write_fetch_report_with_dedup(api, gate_status="ok")

    first_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            first_handler, api=api, path="/admin/ops-tab-counts", query={"view": ["summary"]}
        )
        is True
    )
    first_badge = first_handler.sent[-1]["payload"]["badges"]["dedup"]
    assert first_badge["count"] == 3
    assert first_badge["tone"] == "warning"

    _write_fetch_report_with_dedup(api, gate_status="ok")
    second_handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            second_handler, api=api, path="/admin/ops-tab-counts", query={"view": ["summary"]}
        )
        is True
    )
    second_payload = second_handler.sent[-1]["payload"]
    # Fetch report mtime moved -> cache invalidated even though other inputs match.
    assert "cachedResponse" not in second_payload
    assert second_payload["badges"]["dedup"]["loaded"] is True
