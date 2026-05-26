from __future__ import annotations

from src.jobs.common.dedup_evidence_provider_static import _review_pressure_origin_counts
from src.jobs.dedup import deduplicate_jobs
from src.jobs.reporting_dedup_evidence import (
    _recommended_review_action,
    build_dedup_evidence,
)


def _job(
    *,
    title: str = "Senior Artist",
    company: str = "Example Studio",
    city: str = "Remote",
    country: str = "",
    job_link: str,
    source: str,
    adapter: str = "",
    source_job_id: str = "",
) -> dict[str, str]:
    return {
        "title": title,
        "company": company,
        "city": city,
        "country": country,
        "jobLink": job_link,
        "source": source,
        "adapter": adapter,
        "sourceJobId": source_job_id,
    }


def test_confidence_gate_monitors_static_secondary_key_merge() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                job_link="https://example.com/jobs/a",
                source="static_source::static:listing_url:https://example.com/jobs",
                adapter="static",
                source_job_id="static-a",
            ),
            _job(
                job_link="https://example.org/jobs/a",
                source="static_source::static:listing_url:https://example.org/jobs",
                adapter="static",
                source_job_id="static-b",
            ),
        ]
    )
    evidence = build_dedup_evidence(stats, merged)
    example = evidence["currentRunMergeExamplesByReason"]["secondaryKey"][0]

    assert stats["mergedBySecondaryKey"] == 1
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["blocking"] == 0
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["monitor"] == 1
    assert example["blocksLifecycle"] is False
    assert example["nonBlockingReason"] == "weak_non_provider_identity"
    assert (
        "current_run_non_primary_merges_need_review" not in (evidence["dedupAuditGate"]["blockers"])
    )


def test_confidence_gate_monitors_static_sparse_identity_merge() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                city="Berlin",
                country="Germany",
                job_link="https://example.com/jobs/senior-artist-berlin",
                source="static_source::static:listing_url:https://example.com/jobs",
                adapter="static",
                source_job_id="static-berlin",
            ),
            _job(
                city="",
                country="",
                job_link="https://example.org/jobs/senior-artist",
                source="static_source::static:listing_url:https://example.org/jobs",
                adapter="static",
                source_job_id="static-remote",
            ),
        ]
    )
    evidence = build_dedup_evidence(stats, merged)
    example = evidence["currentRunMergeExamplesByReason"]["sparseIdentity"][0]

    assert stats["mergedBySparseIdentity"] == 1
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["blocking"] == 0
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["monitor"] == 1
    assert example["blocksLifecycle"] is False
    assert example["nonBlockingReason"] == "weak_non_provider_identity"


def test_confidence_gate_keeps_provider_backed_non_primary_merge_blocking() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                job_link="https://boards.greenhouse.io/example/jobs/1",
                source="greenhouse:listing_url:https://boards.greenhouse.io/example",
                adapter="greenhouse",
                source_job_id="greenhouse-1",
            ),
            _job(
                job_link="",
                source="static_source::static:listing_url:https://example.com/jobs",
                adapter="static",
                source_job_id="static-1",
            ),
        ]
    )
    evidence = build_dedup_evidence(stats, merged)
    example = evidence["currentRunMergeExamplesByReason"]["secondaryKey"][0]

    assert stats["mergedBySecondaryKey"] == 1
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["blocking"] == 1
    assert example["blocksLifecycle"] is True
    assert example["recommendedReviewAction"] == "review_current_run_merge"
    assert "current_run_non_primary_merges_need_review" in evidence["dedupAuditGate"]["blockers"]


def test_distinct_provider_jobs_do_not_merge_on_secondary_key() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                title="Software Development Director",
                company="Xsolla",
                city="Montreal",
                country="",
                job_link="https://jobs.lever.co/xsolla/58126ec2-842a-492d-a662-02c33f8c7442",
                source="lever_sources",
                source_job_id="lever:xsolla:58126ec2-842a-492d-a662-02c33f8c7442",
            ),
            _job(
                title="Software Development Director",
                company="Xsolla",
                city="Montreal",
                country="",
                job_link="https://jobs.lever.co/xsolla/654897bf-ce7d-4c4b-bce4-47636ab66c3a",
                source="lever_sources",
                source_job_id="lever:xsolla:654897bf-ce7d-4c4b-bce4-47636ab66c3a",
            ),
        ]
    )

    assert len(merged) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_distinct_provider_jobs_do_not_merge_on_sparse_identity() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                title="Middle Software Engineer (PHP)",
                company="Xsolla",
                city="CIS",
                country="",
                job_link="https://jobs.lever.co/xsolla/164a8740-d61a-41ee-9d10-8892b60ac0cb",
                source="lever_sources",
                source_job_id="lever:xsolla:164a8740-d61a-41ee-9d10-8892b60ac0cb",
            ),
            _job(
                title="Middle Software Engineer (PHP)",
                company="Xsolla",
                city="",
                country="",
                job_link="https://jobs.lever.co/xsolla/663410e6-7c27-401b-92a5-8dbee21c5adf",
                source="lever_sources",
                source_job_id="lever:xsolla:663410e6-7c27-401b-92a5-8dbee21c5adf",
            ),
        ]
    )

    assert len(merged) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySparseIdentity"] == 0


def test_distinct_provider_static_urls_do_not_merge_on_secondary_key() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                title="Lead Writer",
                company="Guerrilla Games",
                city="Amsterdam",
                country="NL",
                job_link="https://job-boards.greenhouse.io/guerrilla-games/jobs/5832333004",
                source="greenhouse_boards",
                source_job_id="greenhouse:guerrilla-games:5832333004",
            ),
            _job(
                title="Lead Writer",
                company="Guerrilla Games",
                city="Amsterdam",
                country="NL",
                job_link=(
                    "https://www.gamesjobsdirect.com/job/sony-interactive-entertainment/"
                    "lead-writer/334517"
                ),
                source=(
                    "static_source::static:listing_url:"
                    "https://www.gamesjobsdirect.com/jobs-with-8608_guerrilla-games?page=1"
                ),
                adapter="static",
                source_job_id="static-gjd-lead-writer",
            ),
        ]
    )

    assert len(merged) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_distinct_smartrecruiters_jobs_do_not_merge_on_title_location_alias() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                title=(
                    "[Nouvelle IP/New IP - PC/Console] Programmeur(-euse) jouabilité "
                    "sénior - Senior Gameplay Programmer"
                ),
                company="Gameloft",
                city="Montreal",
                country="CA",
                job_link="https://jobs.smartrecruiters.com/Gameloft/744000117668455",
                source="smartrecruiters_sources",
                source_job_id="smartrecruiters:Gameloft:744000117668455",
            ),
            _job(
                title=(
                    "[Nouvelle IP/New IP - PC/Console] Programmeur(-euse) jouabilité - "
                    "Gameplay Programmer"
                ),
                company="Gameloft",
                city="Montreal",
                country="CA",
                job_link="https://jobs.smartrecruiters.com/Gameloft/744000117670256",
                source="smartrecruiters_sources",
                source_job_id="smartrecruiters:Gameloft:744000117670256",
            ),
        ]
    )

    assert len(merged) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_confidence_gate_monitors_provider_gracklehq_redirect_secondary_alias() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                job_link="https://gracklehq.com/rd/375224",
                source="gracklehq",
                source_job_id="gracklehq:https://gracklehq.com/rd/375224",
            ),
            _job(
                job_link="https://job-boards.greenhouse.io/example/jobs/8490814002",
                source="greenhouse:listing_url:https://job-boards.greenhouse.io/example",
                adapter="greenhouse",
                source_job_id="greenhouse:example:8490814002",
            ),
        ]
    )
    evidence = build_dedup_evidence(stats, merged)
    example = evidence["currentRunMergeExamplesByReason"]["secondaryKey"][0]

    assert stats["mergedBySecondaryKey"] == 1
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["blocking"] == 0
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["monitor"] == 1
    assert (
        evidence["dedupAuditGate"]["currentRunMergeGateTierCounts"][
            "monitorProviderGracklehqRedirectAlias"
        ]
        == 1
    )
    assert example["blocksLifecycle"] is False
    assert example["nonBlockingReason"] == "provider_gracklehq_redirect_alias"


def test_confidence_gate_monitors_provider_gracklehq_redirect_sparse_alias() -> None:
    merged, stats = deduplicate_jobs(
        [
            _job(
                city="Remote",
                country="US",
                job_link="https://job-boards.greenhouse.io/example/jobs/8490814002",
                source="greenhouse:listing_url:https://job-boards.greenhouse.io/example",
                adapter="greenhouse",
                source_job_id="greenhouse:example:8490814002",
            ),
            _job(
                city="",
                country="",
                job_link="https://gracklehq.com/rd/375224",
                source="gracklehq",
                source_job_id="gracklehq:https://gracklehq.com/rd/375224",
            ),
        ]
    )
    evidence = build_dedup_evidence(stats, merged)
    example = evidence["currentRunMergeExamplesByReason"]["sparseIdentity"][0]

    assert stats["mergedBySparseIdentity"] == 1
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["blocking"] == 0
    assert evidence["dedupAuditGate"]["currentRunNonPrimaryMergeCounts"]["monitor"] == 1
    assert example["blocksLifecycle"] is False
    assert example["nonBlockingReason"] == "provider_gracklehq_redirect_alias"


def test_confidence_gate_exposes_blocking_examples_when_mixed_samples_are_monitor_only() -> None:
    rows = [
        _job(
            job_link=f"https://static-{index}.example/jobs/senior-artist",
            source=f"static_source::static:listing_url:https://static-{index}.example/jobs",
            adapter="static",
            source_job_id=f"static-{index}",
        )
        for index in range(6)
    ]
    rows.append(
        _job(
            job_link="",
            source="greenhouse:listing_url:https://boards.greenhouse.io/example",
            adapter="greenhouse",
            source_job_id="greenhouse-1",
        )
    )
    merged, stats = deduplicate_jobs(rows)
    evidence = build_dedup_evidence(stats, merged)

    mixed_examples = evidence["currentRunMergeExamplesByReason"]["secondaryKey"]
    blocking_examples = evidence["currentRunBlockingMergeExamplesByReason"]["secondaryKey"]

    assert stats["currentRunBlockingNonPrimaryMergeReasonCounts"]["secondaryKey"] == 1
    assert len(mixed_examples) == 5
    assert all(row["blocksLifecycle"] is False for row in mixed_examples)
    assert blocking_examples == [
        {
            "mergeReason": "secondary_key",
            "existingDedupKey": blocking_examples[0]["existingDedupKey"],
            "incomingSource": "greenhouse:listing_url:https://boards.greenhouse.io/example",
            "title": "Senior Artist",
            "company": "Example Studio",
            "incomingJobLink": "",
            "bundleEvidenceOrigin": "current_run",
            "blocksLifecycle": True,
            "nonBlockingReason": "",
            "recommendedReviewAction": "review_current_run_merge",
            "suspectedCause": "current_run_non_primary_merge",
        }
    ]


def test_confidence_gate_monitors_weak_google_sheets_review_queue_summary() -> None:
    summary = {
        "outlierReason": "",
        "dominantSourceClass": "other",
        "providerSourceJobIdCount": 0,
        "identityQuality": "other_source_id_untrusted",
        "suspectedCause": "google_sheets_role_bucket_needs_review",
    }
    review_action = _recommended_review_action(summary)

    (
        current_high_risk,
        _carried_high_risk,
        current_blocking,
        _carried_blocking,
        current_monitor,
        _carried_monitor,
    ) = _review_pressure_origin_counts(
        summary=summary,
        origin="current_run",
        current_run_known_mirror_pair_dedup_keys=set(),
        review_action=review_action,
    )

    assert review_action == "monitor"
    assert current_high_risk == 1
    assert current_blocking == 0
    assert current_monitor == 1
