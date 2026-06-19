from src.jobs.common.contracts_source_reports import normalize_source_report_row
from src.jobs.text_utils import clean_text
from src.shared.fetch_report_normalization import (
    enrich_fetch_report_source_row_metadata,
    enrich_jobs_fetch_report_source_row_fields,
)


def test_jobs_source_report_row_uses_shared_field_enrichment() -> None:
    row = {
        "name": "Source A",
        "status": "OK",
        "keptCount": 1,
        "browserEscalationEligible": True,
        "browserEscalationEnabled": False,
        "browserEscalationEligibilityReason": "js_required",
        "cacheDecision": "skip_fresh",
        "cacheDecisionReason": "within_freshness_window",
        "httpStatus": "304",
        "httpEtag": "etag-a",
        "httpLastModified": "Fri, 19 Jun 2026 08:00:00 GMT",
        "listingFingerprint": "listing-a",
        "listingCheckedAt": "2026-06-19T08:00:00+00:00",
        "listingChanged": "",
        "detailSkippedByListingFingerprint": 1,
        "structuredMigrationTargetAdapter": "greenhouse",
        "structuredMigrationPromotedAt": "2026-06-19T09:00:00+00:00",
        "structuredMigrationDemotedAt": "",
        "structuredMigrationShadowRunCount": "4",
        "structuredMigrationHealthyRunCount": "3",
        "structuredMigrationLastKeptCount": "12",
        "structuredMigrationLastDuplicateRate": "0.25",
        "browserFallbackQuarantinedUntilAt": "2026-06-19T10:00:00+00:00",
        "browserFallbackLastAttemptAt": "2026-06-19T08:05:00+00:00",
        "browserFallbackLastFailureAt": "",
        "browserFallbackLastSuccessAt": "2026-06-19T08:06:00+00:00",
        "browserFallbackLastError": "",
        "browserFallbackFailureCount": "2",
        "boardCount": "2",
        "boardCacheDecisionCounts": {"skip_fresh": "1", "": "9"},
        "boardSkippedCount": "1",
        "boardRevalidatedCount": "2",
        "subsourceCount": "3",
        "subsourceCacheDecisionCounts": {"run_now": "2"},
        "subsourceNotModifiedCount": "1",
        "subsourceRefreshedCount": "2",
        "details": [
            {
                "name": "detail-a",
                "status": "OK",
                "browserEscalationEligible": True,
                "browserEscalationEligibilityReason": "js_required",
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
                "httpStatus": "200",
                "httpEtag": "detail-etag",
                "httpLastModified": "Fri, 19 Jun 2026 07:00:00 GMT",
                "listingFingerprint": "detail-listing",
                "listingCheckedAt": "2026-06-19T07:00:00+00:00",
                "listingChanged": True,
                "detailSkippedByListingFingerprint": False,
            }
        ],
    }

    normalized = normalize_source_report_row(row)
    shared_enrichment: dict[str, object] = {}
    enrich_jobs_fetch_report_source_row_fields(
        shared_enrichment,
        row,
        clean_text_func=clean_text,
    )

    for key, value in shared_enrichment.items():
        assert normalized[key] == value

    detail_enrichment: dict[str, object] = {}
    enrich_fetch_report_source_row_metadata(
        detail_enrichment,
        row["details"][0],
        clean_text_func=clean_text,
    )
    for key, value in detail_enrichment.items():
        assert normalized["details"][0][key] == value
