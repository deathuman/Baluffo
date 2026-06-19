from src.jobs.common.contracts_source_reports import normalize_source_report_row
from src.jobs.text_utils import clean_text, norm_text
from src.shared.fetch_report_normalization import (
    apply_jobs_fetch_report_details,
    enrich_fetch_report_dead_listing_fields,
    enrich_fetch_report_source_row_metadata,
    enrich_jobs_fetch_report_site_changed_url_surface,
    enrich_jobs_fetch_report_source_row_fields,
    normalize_fetch_report_detail_stats,
    normalize_fetch_report_loss,
    normalize_fetch_report_stage_timings,
    normalize_jobs_fetch_report_detail_item,
)


def test_jobs_source_report_row_uses_shared_field_enrichment() -> None:
    row = {
        "name": "Source A",
        "adapter": "static",
        "status": "OK",
        "keptCount": 1,
        "failureBucket": "site_changed",
        "listingUrl": "https://example.com/jobs",
        "pages": [" https://example.com/jobs/1 ", ""],
        "sourceId": "source-a",
        "providerUrl": "https://provider.example/source-a",
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
        "deadListingPageCount": "2",
        "deadListingPageExamples": [" https://example.com/dead ", ""],
        "stageTimingsMs": {
            "fetchAndParse": "10",
            "listingFetch": "11",
            "parseCsv": "12",
            "candidateExtraction": "13",
            "detailFetch": "14",
            "redirectResolve": "15",
            "canonicalization": "16",
        },
        "loss": {
            "rawFetched": "5",
            "canonicalDropped": "3",
            "canonicalKept": "2",
            "dedupMerged": "1",
            "finalOutput": "1",
            "canonicalDropReasons": {"missing_title": "2", "custom_reason": "1"},
            "staticNonJobUrlRejected": "4",
        },
        "details": [
            {
                "name": "detail-a",
                "adapter": "greenhouse",
                "status": "OK",
                "migrationSourceIdentity": "greenhouse:123",
                "detectedProviderFamily": "greenhouse",
                "detectedProviderUrl": "https://boards.greenhouse.io/source-a",
                "detectedProviderId": "123",
                "createdFromAdvisory": True,
                "migrationConfidence": "91",
                "migrationReasons": [" provider_match ", ""],
                "top_reject_reasons": ["missing_title:2", ""],
                "sourceId": "detail-source-a",
                "slug": "detail-a",
                "providerUrl": "https://provider.example/detail-a",
                "pages": [" https://example.com/detail-a ", ""],
                "deadListingPageCount": "1",
                "deadListingPageExamples": [" https://example.com/dead-detail "],
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
                "stats": {
                    "redirect_candidates": "7",
                    "title_hydration_repaired": "3",
                    "listing_terminal_reason": "browser_fallback_empty",
                    "detail_batch_count": "2",
                    "finish_reason": "done",
                },
                "loss": {
                    "rawFetched": "2",
                    "canonicalDropped": "1",
                    "canonicalKept": "1",
                    "canonicalDropReasons": {"missing_job_link": "1"},
                },
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
    dead_listing_enrichment: dict[str, object] = {}
    enrich_fetch_report_dead_listing_fields(
        dead_listing_enrichment,
        row,
        clean_text_func=clean_text,
    )
    for key, value in dead_listing_enrichment.items():
        assert normalized[key] == value

    detail_enrichment: dict[str, object] = {}
    enrich_fetch_report_source_row_metadata(
        detail_enrichment,
        row["details"][0],
        clean_text_func=clean_text,
    )
    for key, value in detail_enrichment.items():
        assert normalized["details"][0][key] == value
    shared_details: dict[str, object] = {}
    apply_jobs_fetch_report_details(
        shared_details,
        row,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )
    assert normalized["details"] == shared_details["details"]
    assert normalized["details"][0] == normalize_jobs_fetch_report_detail_item(
        row["details"][0],
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )
    site_changed_enrichment: dict[str, object] = {}
    enrich_jobs_fetch_report_site_changed_url_surface(
        site_changed_enrichment,
        row,
        failure_bucket="site_changed",
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )
    for key, value in site_changed_enrichment.items():
        assert normalized[key] == value
    assert normalized["stageTimingsMs"] == normalize_fetch_report_stage_timings(row)
    assert normalized["loss"] == normalize_fetch_report_loss(
        row["loss"], clean_text_func=clean_text
    )
    assert normalized["details"][0]["stats"] == normalize_fetch_report_detail_stats(
        row["details"][0]["stats"],
        clean_text_func=clean_text,
    )
    assert normalized["details"][0]["loss"] == normalize_fetch_report_loss(
        row["details"][0]["loss"],
        clean_text_func=clean_text,
    )
