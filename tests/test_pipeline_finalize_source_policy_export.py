from __future__ import annotations

from pathlib import Path

import pytest

from src.jobs import pipeline_finalize


def _report_payload() -> dict[str, object]:
    return {
        "redundantStaticProposals": {
            "proposals": [
                {
                    "staticSourceId": "static:listing_url:https://studio.example/jobs",
                    "staticSourceName": "static_source::static:listing_url:https://studio.example/jobs",
                    "providerSourceId": "greenhouse:studio",
                    "providerSourceName": "Studio Greenhouse",
                    "proposal": "safe_redundant_static",
                    "recommendedAction": "keep_runtime_suppression",
                    "confidence": 0.92,
                }
            ]
        }
    }


def test_source_policy_export_reports_expected_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(_path: Path, _text: str) -> bool:
        raise OSError("readonly artifact")

    monkeypatch.setattr(pipeline_finalize, "write_atomic_if_changed", fail_write)
    report_payload = _report_payload()
    recommendations_path = tmp_path / "source-policy-recommendations.json"
    review_state_path = tmp_path / "source-policy-review-state.json"

    pipeline_finalize._export_source_policy_recommendations(
        report_payload=report_payload,
        source_policy_recommendations_path=recommendations_path,
        source_policy_review_state_path=review_state_path,
        finished_at="2026-06-19T12:00:00+00:00",
    )

    assert report_payload["sourcePolicyRecommendationExport"] == {
        "status": "warning",
        "artifactPath": str(recommendations_path),
        "reviewStatePath": str(review_state_path),
        "updatedPairCount": 0,
        "reviewStatePairCount": 0,
        "manualForcePausedCount": 0,
        "warning": "source_policy_recommendation_export_failed:OSError",
    }


def test_source_policy_export_does_not_swallow_unexpected_build_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_build(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("unexpected recommendation bug")

    monkeypatch.setattr(
        pipeline_finalize,
        "build_source_policy_recommendations_artifact",
        fail_build,
    )
    report_payload = _report_payload()

    with pytest.raises(AssertionError, match="unexpected recommendation bug"):
        pipeline_finalize._export_source_policy_recommendations(
            report_payload=report_payload,
            source_policy_recommendations_path=tmp_path / "source-policy-recommendations.json",
            source_policy_review_state_path=tmp_path / "source-policy-review-state.json",
            finished_at="2026-06-19T12:00:00+00:00",
        )

    assert "sourcePolicyRecommendationExport" not in report_payload
