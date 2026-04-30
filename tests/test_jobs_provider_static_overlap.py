from src.jobs.common.contracts_provider_static_overlap import (
    build_provider_static_overlap_summary,
    normalize_provider_static_overlap_payload,
)

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
STATIC_SOURCE_ID = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _suppressed_static_row(**overrides):
    row = {
        "name": STATIC_SOURCE_NAME,
        "adapter": "static",
        "status": "excluded",
        "exclusionReason": "dynamic_redundant_provider",
        "coveredByProviderSourceId": PROVIDER_SOURCE_NAME,
        "coveredByProviderAdapter": "greenhouse",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 4,
        "migrationSourceIdentity": STATIC_SOURCE_ID,
    }
    row.update(overrides)
    return row


def _source_state(
    *,
    static_last_kept=2,
    provider_status="validated_provider",
    provider_successes=2,
    provider_kept=4,
):
    rows = {
        PROVIDER_SOURCE_NAME: {
            "lastAdapter": "greenhouse",
            "providerCoverageStatus": provider_status,
            "providerCoverageConsecutiveSuccesses": provider_successes,
            "providerCoverageLatestKeptCount": provider_kept,
            "migrationSourceIdentity": STATIC_SOURCE_ID,
        }
    }
    if static_last_kept is not None:
        rows[STATIC_SOURCE_NAME] = {"lastKeptCount": static_last_kept}
    return rows


def test_provider_static_overlap_empty_without_suppressed_sources():
    audit = build_provider_static_overlap_summary(
        source_rows=[{"name": "greenhouse_boards", "status": "ok"}],
        source_state_rows=_source_state(),
        canonical_rows=[],
    )

    assert audit["suppressedStaticCount"] == 0
    assert audit["auditedPairCount"] == 0
    assert audit["pairs"] == []


def test_provider_static_overlap_marks_repeated_valid_provider_safe():
    audit = build_provider_static_overlap_summary(
        source_rows=[_suppressed_static_row()],
        source_state_rows=_source_state(),
        canonical_rows=[
            {
                "title": "Engineer",
                "sourceBundle": [{"source": PROVIDER_SOURCE_NAME}],
            }
        ],
    )

    pair = audit["pairs"][0]
    assert audit["safePairCount"] == 1
    assert pair["auditStatus"] == "safe"
    assert pair["staticSourceId"] == STATIC_SOURCE_ID
    assert pair["latestProviderKeptCount"] == 4
    assert "prior_static_history_present" in pair["auditReasons"]


def test_provider_static_overlap_marks_static_only_evidence_needs_review():
    audit = build_provider_static_overlap_summary(
        source_rows=[_suppressed_static_row()],
        source_state_rows=_source_state(),
        canonical_rows=[
            {
                "title": "Static-only job",
                "sourceBundle": [{"source": STATIC_SOURCE_NAME}],
            }
        ],
    )

    pair = audit["pairs"][0]
    assert audit["needsReviewPairCount"] == 1
    assert audit["staticOnlyJobCount"] == 1
    assert pair["auditStatus"] == "needs_review"
    assert pair["staticOnlyCount"] == 1
    assert "static_only_jobs_detected" in pair["auditReasons"]


def test_provider_static_overlap_marks_missing_static_history_insufficient():
    audit = build_provider_static_overlap_summary(
        source_rows=[_suppressed_static_row()],
        source_state_rows=_source_state(static_last_kept=None),
        canonical_rows=[],
    )

    pair = audit["pairs"][0]
    assert audit["insufficientHistoryPairCount"] == 1
    assert pair["auditStatus"] == "insufficient_history"


def test_provider_static_overlap_marks_unstable_provider():
    audit = build_provider_static_overlap_summary(
        source_rows=[_suppressed_static_row()],
        source_state_rows=_source_state(provider_status="unstable_provider"),
        canonical_rows=[],
    )

    pair = audit["pairs"][0]
    assert audit["needsReviewPairCount"] == 1
    assert pair["auditStatus"] == "provider_unstable"
    assert "provider_status:unstable_provider" in pair["auditReasons"]


def test_normalize_provider_static_overlap_payload_derives_when_missing():
    normalized = normalize_provider_static_overlap_payload(
        {},
        source_rows=[_suppressed_static_row()],
        source_state_rows=_source_state(),
        canonical_rows=[],
    )

    assert normalized["suppressedStaticCount"] == 1
    assert normalized["pairs"][0]["auditStatus"] == "safe"
