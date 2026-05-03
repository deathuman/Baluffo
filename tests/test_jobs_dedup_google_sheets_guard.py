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
        title="Product-management",
        company="eBay",
        link="https://jobs.ebayinc.com/us/en/job/R0065718/product-manager-buyer-experience",
        source_job_id="sheet-5632",
    )
    second = _google_sheets_job(
        title="Product-management",
        company="eBay",
        link="https://jobs.ebayinc.com/us/en/job/R0068764/product-manager-seller-experience",
        source_job_id="sheet-30257",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
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


def test_deduplicate_jobs_still_merges_google_sheets_generic_role_bucket_same_url() -> None:
    first = _google_sheets_job(
        title="Localization",
        company="Mercor",
        link="https://work.mercor.com/explore/localization",
        source_job_id="sheet-1164",
    )
    second = _google_sheets_job(
        title="Localization",
        company="Mercor",
        link="https://work.mercor.com/explore/localization",
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
