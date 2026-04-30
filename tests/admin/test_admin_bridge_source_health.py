import pytest

from src import admin_bridge

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def _source_health_report() -> dict[str, object]:
    return {
        "startedAt": "2026-03-23T16:16:54.905369+00:00",
        "finishedAt": "2026-03-23T16:18:10.053424+00:00",
        "summary": {"outputCount": 5, "sourceCount": 3, "failedSources": 1},
        "sources": [
            {
                "name": "provider_good",
                "status": "ok",
                "adapter": "greenhouse",
                "keptCount": 5,
                "durationMs": 1000,
            },
            {
                "name": "static_needs_review",
                "status": "ok",
                "adapter": "static",
                "keptCount": 0,
                "durationMs": 45000,
                "failureBucket": "needs_review",
                "browserFallbackRecommended": True,
            },
            {
                "name": "provider_failed",
                "status": "error",
                "adapter": "personio",
                "keptCount": 0,
                "durationMs": 12000,
                "failureBucket": "provider_rate_limited",
            },
        ],
    }


def test_bridge_fetch_report_normalizer_derives_source_health() -> None:
    payload = admin_bridge.normalize_fetch_report_contract(_source_health_report())

    health = payload.get("sourceHealth") or {}
    assert int(health.get("totalSources") or 0) == 3
    assert int(health.get("failedSources") or 0) == 1
    assert int(health.get("zeroKeptNeedsReviewSources") or 0) == 2
    assert (health.get("slowestSources") or [])[0]["name"] == "static_needs_review"
    assert (health.get("topProductiveSources") or [])[0]["name"] == "provider_good"


def test_ops_health_exposes_source_health_triage() -> None:
    admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, _source_health_report())

    health = admin_bridge.compute_ops_health()
    source_health = (health.get("kpis") or {}).get("sourceHealth") or {}
    assert int(source_health.get("totalSources") or 0) == 3
    assert int(source_health.get("browserFallbackRecommendedSources") or 0) == 1
    assert (source_health.get("sourcesNeedingAttention") or [])[0]["name"] == "provider_failed"
