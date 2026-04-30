from __future__ import annotations

import src.admin_bridge as admin_bridge


def test_discovery_report_normalization_derives_candidate_review_payload() -> None:
    payload = admin_bridge.normalize_discovery_report_contract(
        {
            "finishedAt": "2026-03-01T00:01:00+00:00",
            "summary": {"queuedCandidateCount": 1},
            "candidates": [
                {"name": "Live", "adapter": "greenhouse", "jobsFound": 3, "score": 80},
                {"name": "Blocked", "adapter": "static", "lastProbeError": "HTTP 403"},
            ],
        }
    )

    review = payload.get("candidateReview") or {}
    assert review.get("totalCandidates") == 2
    assert (review.get("recommendationCounts") or {}).get("promote_candidate") == 1
    assert (review.get("recommendationCounts") or {}).get("needs_browser_probe") == 1
    assert (payload.get("candidates") or [])[0]["promotionRecommendation"] == "promote_candidate"
