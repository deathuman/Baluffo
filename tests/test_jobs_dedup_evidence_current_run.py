from __future__ import annotations

from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def test_dedup_evidence_marks_known_mirror_pair_current_run_examples_non_blocking() -> None:
    evidence = build_dedup_evidence(
        {
            "mergedCount": 1,
            "mergedByKnownMirrorPair": 1,
            "collisionSamplesCount": 1,
            "collisionSamples": [
                {
                    "reason": "known_mirror_pair",
                    "existingDedupKey": "guerrilla-key-1",
                    "incomingSource": (
                        "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-8608_guerrilla-games?page=1"
                    ),
                    "incomingTitle": "Senior Foundational Tools Programmer",
                    "incomingCompany": "Guerrilla Games",
                    "incomingJobLink": (
                        "https://www.gamesjobsdirect.com/job/senior-foundational-tools-programmer/12345"
                    ),
                }
            ],
        },
        [],
    )

    assert evidence["mergeReasonCounts"]["knownMirrorPair"] == 1
    assert evidence["currentRunMergeExamples"] == [
        {
            "mergeReason": "known_mirror_pair",
            "existingDedupKey": "guerrilla-key-1",
            "incomingSource": (
                "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-8608_guerrilla-games?page=1"
            ),
            "title": "Senior Foundational Tools Programmer",
            "company": "Guerrilla Games",
            "incomingJobLink": (
                "https://www.gamesjobsdirect.com/job/senior-foundational-tools-programmer/12345"
            ),
            "bundleEvidenceOrigin": "current_run",
            "blocksLifecycle": False,
            "nonBlockingReason": "known_gracklehq_gamesjobsdirect_mirror_pair",
            "recommendedReviewAction": "monitor",
            "suspectedCause": "known_mirror_pair",
        }
    ]


def test_fetch_report_normalization_preserves_dedup_evidence() -> None:
    normalized = normalize_fetch_report_payload(
        {
            "summary": {"inputCount": 2, "outputCount": 1},
            "sources": [],
            "dedupEvidence": {
                "schemaVersion": 1,
                "mergedCount": 1,
                "topMergedJobs": [{"title": "Senior Engineer"}],
            },
        }
    )

    assert normalized["dedupEvidence"]["mergedCount"] == 1
    assert normalized["dedupEvidence"]["topMergedJobs"][0]["title"] == "Senior Engineer"
