from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def _row(**overrides):
    payload = {
        "id": "job-identity",
        "dedupKey": "identity-key",
        "title": "Senior Engineer",
        "company": "Studio One",
        "jobLink": "https://example.com/jobs/1",
        "locationSummary": "Amsterdam, NL",
        "sourceBundleCount": 2,
        "sourceBundle": [],
        "locations": [{"city": "Amsterdam", "country": "NL"}],
    }
    payload.update(overrides)
    return payload


def _sheet_item(row_id: str, url: str):
    return {
        "source": "google_sheets",
        "sourceJobId": row_id,
        "jobLink": url,
        "adapter": "custom",
    }


def test_dedup_evidence_reports_google_sheets_single_location_many_urls() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/jobs/1"),
                    _sheet_item("sheet-row-2", "https://studio.example/jobs/2"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["nonProviderIdentityProvenance"] == "google_sheets_row_identity"
    assert row["googleSheetsBundleShape"] == "single_location_many_urls"
    assert "shape:single_location_many_urls" in row["googleSheetsBundleEvidence"]
    assert evidence["googleSheetsBundleShapeCounts"]["single_location_many_urls"] == 1


def test_dedup_evidence_audits_role_bucket_listing_or_search_paths() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Product-management",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/jobs"),
                    _sheet_item("sheet-row-2", "https://studio.example/careers"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "listing_or_search_url_bucket"
    assert row["googleSheetsBucketIntent"] == "listing_or_search_bucket"
    assert "paths_listing_or_search" in row["googleSheetsRoleBucketAuditEvidence"]
    assert "intent:listing_or_search_bucket" in row["googleSheetsBucketIntentEvidence"]
    assert evidence["googleSheetsRoleBucketAuditCounts"]["listing_or_search_url_bucket"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["listing_or_search_bucket"] == 1


def test_dedup_evidence_audits_role_bucket_job_detail_paths() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Program-management",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/details/123"),
                    _sheet_item("sheet-row-2", "https://studio.example/jobs/456"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "job_detail_urls_same_role"
    assert row["googleSheetsBucketIntent"] == "likely_spreadsheet_taxonomy_bucket"
    assert "paths_job_detail_like" in row["googleSheetsRoleBucketAuditEvidence"]
    assert evidence["googleSheetsRoleBucketAuditCounts"]["job_detail_urls_same_role"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["likely_spreadsheet_taxonomy_bucket"] == 1


def test_dedup_evidence_audits_generic_role_bucket_titles() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Localization",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/openings/language"),
                    _sheet_item("sheet-row-2", "https://studio.example/roles/localization"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "likely_spreadsheet_category_bucket"
    assert row["googleSheetsBucketIntent"] == "likely_spreadsheet_taxonomy_bucket"
    assert "title_token:localization" in row["googleSheetsRoleBucketAuditEvidence"]
    assert evidence["googleSheetsRoleBucketAuditCounts"]["likely_spreadsheet_category_bucket"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["likely_spreadsheet_taxonomy_bucket"] == 1


def test_dedup_evidence_audits_concrete_role_family_for_manual_review() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Senior Gameplay Engineer",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/gameplay/a"),
                    _sheet_item("sheet-row-2", "https://studio.example/gameplay/b"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "role_family_needs_manual_review"
    assert row["googleSheetsBucketIntent"] == "possible_role_family"
    assert evidence["googleSheetsRoleBucketAuditCounts"]["role_family_needs_manual_review"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["possible_role_family"] == 1


def test_dedup_evidence_reports_weak_google_sheets_title_company_grouping() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Engineer",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/team/a"),
                    _sheet_item("sheet-row-2", "https://studio.example/team/b"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "role_family_needs_manual_review"
    assert row["googleSheetsBucketIntent"] == "weak_title_company_grouping"
    assert "title_tokens:1" in row["googleSheetsBucketIntentEvidence"]
    assert evidence["googleSheetsBucketIntentCounts"]["weak_title_company_grouping"] == 1


def test_dedup_evidence_audits_parser_normalized_role_title() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="nDreams 4",
                company="11 bit studios 3",
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/jobs/a"),
                    _sheet_item("sheet-row-2", "https://studio.example/jobs/b"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsRoleBucketAudit"] == "parser_normalized_role_title"
    assert row["googleSheetsBucketIntent"] == "parser_normalized_bucket"
    assert "pollution:title_numeric_suffix" in row["googleSheetsRoleBucketAuditEvidence"]
    assert evidence["googleSheetsRoleBucketAuditCounts"]["parser_normalized_role_title"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["parser_normalized_bucket"] == 1


def test_dedup_evidence_reports_google_sheets_role_category_bucket() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Product-management",
                company="eBay",
                sourceBundleCount=3,
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://ebay.example/jobs/product-1"),
                    _sheet_item("sheet-row-2", "https://ebay.example/jobs/product-2"),
                    _sheet_item("sheet-row-3", "https://ebay.example/jobs/product-3"),
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["googleSheetsBundleShape"] == "role_category_bucket"
    assert row["googleSheetsBucketIntent"] == "likely_spreadsheet_taxonomy_bucket"
    assert row["suspectedCause"] == "spreadsheet_role_bucket_needs_review"
    assert "role_bucket_title" in row["googleSheetsBundleEvidence"]
    assert "google_sheets_shape:role_category_bucket" in row["causeEvidence"]
    assert "google_sheets_intent:likely_spreadsheet_taxonomy_bucket" in row["causeEvidence"]
    assert evidence["googleSheetsBundleShapeCounts"]["role_category_bucket"] == 1
    assert evidence["reviewQueueCauseCounts"]["spreadsheet_role_bucket_needs_review"] == 1


def test_dedup_evidence_reports_google_sheets_multi_location_many_urls() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Berlin", "country": "DE"},
                ],
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/jobs/1"),
                    _sheet_item("sheet-row-2", "https://studio.example/jobs/2"),
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["googleSheetsBundleShape"] == "multi_location_many_urls"
    assert "locations:2" in outlier["googleSheetsBundleEvidence"]
    assert evidence["googleSheetsBundleShapeCounts"]["multi_location_many_urls"] == 1


def test_dedup_evidence_reports_google_sheets_spreadsheet_row_collision() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    _sheet_item("sheet-row-1", "https://studio.example/jobs/1"),
                    _sheet_item("sheet-row-1", "https://studio.example/jobs/2"),
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["googleSheetsBundleShape"] == "spreadsheet_row_collision"
    assert evidence["googleSheetsBundleShapeCounts"]["spreadsheet_row_collision"] == 1


def test_dedup_evidence_reports_not_google_sheets_bundle_shape() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "directory-job-1",
                        "jobLink": "https://directory.example/jobs/1",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-a",
                        "sourceJobId": "directory-job-2",
                        "jobLink": "https://directory.example/jobs/2",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["googleSheetsBundleShape"] == "not_google_sheets"
    assert outlier["googleSheetsRoleBucketAudit"] == "not_google_sheets_role_bucket"
    assert outlier["googleSheetsBucketIntent"] == "not_google_sheets_bucket"
    assert evidence["googleSheetsBundleShapeCounts"]["not_google_sheets"] == 1
    assert evidence["googleSheetsRoleBucketAuditCounts"]["not_google_sheets_role_bucket"] == 1
    assert evidence["googleSheetsBucketIntentCounts"]["not_google_sheets_bucket"] == 1


def test_dedup_evidence_counts_google_sheets_shapes_before_sample_capping() -> None:
    rows = [
        _row(
            id=f"job-{index}",
            dedupKey=f"key-{index}",
            title="Product-management",
            company="Studio",
            sourceBundle=[
                _sheet_item(f"sheet-{index}-a", f"https://studio.example/{index}/a"),
                _sheet_item(f"sheet-{index}-b", f"https://studio.example/{index}/b"),
            ],
        )
        for index in range(12)
    ]

    evidence = build_dedup_evidence({"mergedCount": 0}, rows, risky_limit=2)

    assert evidence["googleSheetsBundleShapeCounts"]["role_category_bucket"] == 12
    assert evidence["reviewQueueCauseCounts"]["spreadsheet_role_bucket_needs_review"] == 12
    assert evidence["googleSheetsRoleBucketAuditCounts"]["job_detail_urls_same_role"] == 12
    assert evidence["googleSheetsBucketIntentCounts"]["likely_spreadsheet_taxonomy_bucket"] == 12
    assert len(evidence["reviewQueue"]) == 2


def test_dedup_evidence_reports_empty_google_sheets_role_bucket_audit_counts() -> None:
    evidence = build_dedup_evidence({"mergedCount": 0}, [])

    assert not any(evidence["googleSheetsRoleBucketAuditCounts"].values())
    assert not any(evidence["googleSheetsBucketIntentCounts"].values())
    assert "unknown" in evidence["googleSheetsRoleBucketAuditCounts"]
    assert "unknown" in evidence["googleSheetsBucketIntentCounts"]
