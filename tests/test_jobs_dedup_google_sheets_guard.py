from __future__ import annotations

from src import jobs_fetcher as jf
from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def _google_sheets_job(*, title: str, company: str, link: str, source_job_id: str):
    return jf.canonicalize_job(
        {
            "title": title,
            "company": company,
            "city": "",
            "country": "Unknown",
            "locations": [{"city": "", "country": "Unknown"}],
            "jobLink": link,
            "sector": "Tech",
            "sourceJobId": source_job_id,
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )


def test_deduplicate_jobs_keeps_google_sheets_generic_role_bucket_detail_urls_separate() -> None:
    first = _google_sheets_job(
        title="Animator",
        company="eBay",
        link="https://jobs.ebayinc.com/us/en/job/R0065718",
        source_job_id="sheet-5632",
    )
    second = _google_sheets_job(
        title="Animator",
        company="eBay",
        link="https://jobs.ebayinc.com/us/en/job/R0068764",
        source_job_id="sheet-30257",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
    assert int(stats["sheetRoleBucketGuardBlockedCount"]) == 2
    assert stats["sheetRoleBucketGuardBlockedReasonCounts"]["secondaryKey"] == 1
    assert stats["sheetRoleBucketGuardBlockedReasonCounts"]["sparseIdentity"] == 1
    assert stats["sheetRoleBucketGuardBlockedSamples"][0]["guardReason"] == (
        "sheet_role_bucket_different_primary_url"
    )
    assert int(stats["googleSheetsGenericRoleGuardBlockedCount"]) == 2
    assert stats["googleSheetsGenericRoleGuardBlockedReasonCounts"]["secondaryKey"] == 1
    assert stats["googleSheetsGenericRoleGuardBlockedReasonCounts"]["sparseIdentity"] == 1
    assert stats["googleSheetsGenericRoleGuardBlockedSamples"][0]["classification"] == (
        "fixed_by_generic_role_guard"
    )
    assert sorted(row.jobLink for row in rows) == sorted([first.jobLink, second.jobLink])

    evidence = build_dedup_evidence(stats, rows)
    audit = evidence["googleSheetsRoleBucketAudit"]
    assert audit["blockedByDifferentPrimaryUrlCount"] == 2
    assert audit["classificationCounts"]["fixed_by_generic_role_guard"] == 2
    assert audit["examples"][0]["classification"] == "fixed_by_generic_role_guard"
    assert evidence["sheetRoleBucketGuardBlockedCount"] == 2
    assert evidence["sheetRoleBucketGuardBlockedSamples"][0]["guardReason"] == (
        "sheet_role_bucket_different_primary_url"
    )


def test_deduplicate_jobs_blocks_google_sheets_category_label_into_provider_merge() -> None:
    gs = jf.canonicalize_job(
        {
            "title": "Animator",
            "company": "Unknown company",
            "city": "Remote",
            "country": "Unknown",
            "locations": [{"city": "Remote", "country": "Unknown"}],
            "jobLink": "https://job-boards.greenhouse.io/scopely/jobs/8490814002",
            "sector": "Game",
            "sourceJobId": "sheet-5000",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    provider = jf.canonicalize_job(
        {
            "title": "Senior Product Manager Economy Monopoly GO",
            "company": "Scopely",
            "city": "Culver City",
            "country": "United States",
            "locations": [{"city": "Culver City", "country": "United States"}],
            "jobLink": "https://job-boards.eu.greenhouse.io/scopely/jobs/8490814002",
            "sector": "Game",
            "sourceJobId": "greenhouse:scopely:8490814002",
        },
        source="greenhouse_boards",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert gs is not None
    assert provider is not None

    rows, stats = jf.deduplicate_jobs([provider, gs])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
    assert sorted(row.title for row in rows) == sorted(
        ["Animator", "Senior Product Manager Economy Monopoly GO"]
    )


def test_deduplicate_jobs_keeps_google_sheets_animator_buckets_on_different_urls_separate() -> None:
    first = _google_sheets_job(
        title="Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/12345",
        source_job_id="sheet-animator-1",
    )
    second = _google_sheets_job(
        title="Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/67890",
        source_job_id="sheet-animator-2",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
    assert int(stats["sheetRoleBucketGuardBlockedCount"]) == 2
    assert int(stats["googleSheetsGenericRoleGuardBlockedCount"]) == 2
    assert sorted(row.jobLink for row in rows) == sorted([first.jobLink, second.jobLink])


def test_deduplicate_jobs_prefers_specific_animation_title_for_same_url() -> None:
    broad = _google_sheets_job(
        title="Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/12345",
        source_job_id="sheet-animator",
    )
    specific = _google_sheets_job(
        title="Technical Cinematic Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/12345",
        source_job_id="sheet-specific-animator",
    )
    assert broad is not None
    assert specific is not None

    rows, stats = jf.deduplicate_jobs([broad, specific])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert rows[0].title == "Technical Cinematic Animator"


def test_deduplicate_jobs_does_not_replace_qualified_animation_bucket_with_lateral_title() -> None:
    broad = _google_sheets_job(
        title="Technical Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/12345",
        source_job_id="sheet-technical-animator",
    )
    lateral = _google_sheets_job(
        title="Cinematic Animator",
        company="Example Games",
        link="https://jobs.example.com/postings/12345",
        source_job_id="sheet-cinematic-animator",
    )
    assert broad is not None
    assert lateral is not None

    rows, stats = jf.deduplicate_jobs([broad, lateral])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert rows[0].title == "Technical Animator"


def test_deduplicate_jobs_prefers_more_specific_same_opening_title_for_resolved_sheet_row() -> None:
    resolved_sheet_row = jf.canonicalize_job(
        {
            "title": "Senior Design Director",
            "company": "Unknown company",
            "city": "Los Angeles",
            "country": "United States",
            "locations": [{"city": "Los Angeles", "country": "United States"}],
            "jobLink": "https://believer.gg/jobs/7f038142-b3aa-4562-9578-2237d4b2d88a",
            "sector": "Tech",
            "sourceJobId": "sheet-32",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    sibling = jf.canonicalize_job(
        {
            "title": "Design Director",
            "company": "Believer Entertainment",
            "city": "Los Angeles",
            "country": "United States",
            "locations": [{"city": "Los Angeles", "country": "United States"}],
            "jobLink": "https://believer.gg/jobs/7f038142-b3aa-4562-9578-2237d4b2d88a",
            "sector": "Tech",
            "sourceJobId": "sheet-2534",
        },
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert resolved_sheet_row is not None
    assert sibling is not None

    rows, stats = jf.deduplicate_jobs([sibling, resolved_sheet_row])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert rows[0].title == "Senior Design Director"
    assert rows[0].company == "Believer Entertainment"


def test_deduplicate_jobs_still_merges_google_sheets_generic_role_bucket_same_url() -> None:
    first = _google_sheets_job(
        title="Animator",
        company="Mercor",
        link="https://work.mercor.com/explore/animation",
        source_job_id="sheet-1164",
    )
    second = _google_sheets_job(
        title="Animator",
        company="Mercor",
        link="https://work.mercor.com/explore/animation",
        source_job_id="sheet-17079",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert int(stats["mergedByPrimaryUrl"]) == 1
    assert int(stats["googleSheetsGenericRoleGuardBlockedCount"]) == 0
    assert rows[0].sourceBundleCount == 2

    evidence = build_dedup_evidence(stats, rows)
    audit = evidence["googleSheetsRoleBucketAudit"]
    assert audit["allowedSamePrimaryUrlCount"] == 1
    assert audit["unresolvedRoleBucketCount"] == 0
    assert audit["examples"][0]["classification"] == "allowed_same_primary_url"
    assert evidence["dedupAuditGate"]["lifecycleUxReady"] is True


def test_google_sheets_taxonomy_category_titles_drop_before_dedup() -> None:
    for title in ("Mobile-development", "System-design", "Software-development-&-engineering"):
        first = _google_sheets_job(
            title=title,
            company="Bucket Studio",
            link=f"https://example.com/jobs/{title}/one",
            source_job_id=f"{title}-1",
        )
        second = _google_sheets_job(
            title=title,
            company="Bucket Studio",
            link=f"https://example.com/jobs/{title}/two",
            source_job_id=f"{title}-2",
        )
        assert first is None
        assert second is None


def test_google_sheets_guard_audit_counts_uncapped_blocked_attempts() -> None:
    evidence = build_dedup_evidence(
        {
            "mergedCount": 0,
            "googleSheetsGenericRoleGuardBlockedCount": 12,
            "googleSheetsGenericRoleGuardBlockedSamples": [
                {
                    "classification": "fixed_by_generic_role_guard",
                    "blockedMergeReason": "secondary_key",
                    "incomingTitle": "Programming",
                    "incomingCompany": "Studio",
                }
            ],
        },
        [],
    )

    audit = evidence["googleSheetsRoleBucketAudit"]
    assert audit["blockedByDifferentPrimaryUrlCount"] == 12
    assert audit["classificationCounts"]["fixed_by_generic_role_guard"] == 12
    assert len(audit["examples"]) == 1
