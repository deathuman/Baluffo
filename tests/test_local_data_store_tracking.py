import json
from pathlib import Path

from src.local_data_store_tracking import (
    can_set_outcome_status,
    can_transition_pipeline_phase,
    normalize_tracking_fields,
    split_application_status,
    to_application_status_mirror,
)

CASES = json.loads(
    (Path(__file__).parent / "fixtures" / "saved_job_tracking_cases.json").read_text(
        encoding="utf-8"
    )
)


def test_local_data_tracking_normalizes_shared_parity_fixtures() -> None:
    for item in CASES:
        normalized = normalize_tracking_fields(
            item["input"],
            {},
            saved_at=str(item["input"].get("savedAt") or ""),
            now_iso=lambda: "2026-04-01T00:00:00.000Z",
            normalize_iso=lambda value, fallback: str(value or fallback),
        )
        assert normalized["pipelinePhase"] == item["expected"]["pipelinePhase"], item["name"]
        assert normalized["outcomeStatus"] == item["expected"]["outcomeStatus"], item["name"]
        assert normalized["applicationStatus"] == item["expected"]["applicationStatus"], item[
            "name"
        ]
        assert normalized["phaseTimestamps"]["bookmark"] == item["input"]["savedAt"], item["name"]
        outcome_key = item["expected"].get("outcomeTimestampKey")
        if outcome_key:
            assert normalized["outcomeTimestamps"][outcome_key]


def test_local_data_tracking_applies_legacy_source_status_before_base_split() -> None:
    normalized = normalize_tracking_fields(
        {
            "applicationStatus": "rejected",
            "savedAt": "2026-03-08T09:00:00.000Z",
        },
        {
            "pipelinePhase": "interview_2",
            "outcomeStatus": "active",
            "applicationStatus": "interview_2",
            "savedAt": "2026-03-08T09:00:00.000Z",
            "phaseTimestamps": {
                "bookmark": "2026-03-08T09:00:00.000Z",
                "interview_2": "2026-03-10T09:00:00.000Z",
            },
        },
        saved_at="2026-03-08T09:00:00.000Z",
        now_iso=lambda: "2026-04-01T00:00:00.000Z",
        normalize_iso=lambda value, fallback: str(value or fallback),
    )

    assert normalized["pipelinePhase"] == "interview_2"
    assert normalized["outcomeStatus"] == "rejected"
    assert normalized["applicationStatus"] == "rejected"
    assert normalized["outcomeTimestamps"]["rejected"]


def test_local_data_tracking_mirror_and_transition_guards() -> None:
    assert split_application_status(
        "rejected",
        phase_timestamps={"applied": "2026-03-08T10:00:00.000Z"},
    ) == {
        "pipelinePhase": "applied",
        "outcomeStatus": "rejected",
    }
    assert to_application_status_mirror("offer", "active") == "offer"
    assert to_application_status_mirror("offer", "accepted") == "accepted"
    assert can_transition_pipeline_phase("bookmark", "applied", "active") is True
    assert can_transition_pipeline_phase("bookmark", "interview_1", "active") is False
    assert can_transition_pipeline_phase("offer", "offer", "accepted") is False
    assert can_set_outcome_status("active", "rejected") is True
    assert can_set_outcome_status("rejected", "accepted") is False
    assert can_set_outcome_status("rejected", "accepted", override=True) is True
