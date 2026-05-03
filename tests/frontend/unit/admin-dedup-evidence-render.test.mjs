import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: fetcher metrics render read-only dedup evidence", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 120000,
      sourceCount: 3,
      outputCount: 2,
      mergedCount: 1,
      dedupEvidence: {
        mergedCount: 1,
        collisionSamplesCount: 1,
        sourceBundleCollisionCount: 2,
        riskyMergeExampleCount: 1,
        mergeReasonCounts: {
          primaryUrl: 1,
          secondaryKey: 0,
          socialKey: 0,
          sparseIdentity: 0,
          unknown: 0
        },
        sourceBundleComposition: {
          provider: 1,
          static: 1,
          social: 0,
          other: 0
        },
        riskReasonCounts: {
          same_title_company_different_location: 1,
          provider_static_duplicate_disagreement: 1,
          missing_provider_ids: 0,
          weak_title_company_only_evidence: 0
        },
        outlierReasonCounts: {
          multi_location_strong_identity: 1,
          location_divergence_without_strong_identity: 2,
          provider_static_disagreement: 1,
          large_other_source_bundle: 1,
          sparse_title_company_bundle: 0,
          unknown: 0
        },
        identityShapeCounts: {
          shared_job_detail_url: 1,
          shared_listing_or_category_url: 2,
          many_unique_urls_same_title: 3,
          provider_id_backed: 4,
          missing_url_and_ids: 0,
          mixed_or_unknown_identity: 0
        },
        identityQualityCounts: {
          provider_id_strong: 4,
          shared_detail_url_strong: 1,
          shared_listing_url_weak: 2,
          many_urls_same_host_weak: 3,
          many_urls_many_hosts_weak: 1,
          other_source_id_untrusted: 1,
          missing_identity: 0,
          unknown: 0
        },
        nonProviderIdentityProvenanceCounts: {
          google_sheets_row_identity: 3,
          url_derived_identity: 2,
          category_or_directory_identity: 1,
          opaque_other_source_identity: 1,
          mixed_non_provider_identity: 1,
          none: 0,
          unknown: 0
        },
        googleSheetsBundleShapeCounts: {
          role_category_bucket: 3,
          company_role_family: 1,
          single_location_many_urls: 2,
          multi_location_many_urls: 1,
          spreadsheet_row_collision: 1,
          not_google_sheets: 4,
          unknown: 0
        },
        googleSheetsRoleBucketAuditCounts: {
          likely_spreadsheet_category_bucket: 3,
          role_family_needs_manual_review: 2,
          job_detail_urls_same_role: 1,
          listing_or_search_url_bucket: 1,
          parser_normalized_role_title: 1,
          not_google_sheets_role_bucket: 4,
          unknown: 0
        },
        googleSheetsBucketIntentCounts: {
          likely_spreadsheet_taxonomy_bucket: 3,
          possible_role_family: 2,
          weak_title_company_grouping: 1,
          listing_or_search_bucket: 1,
          parser_normalized_bucket: 1,
          not_google_sheets_bucket: 4,
          unknown: 0
        },
        googleSheetsWeakGroupingAuditCounts: {
          role_bucket_detail_url_grouping: 3,
          role_bucket_listing_grouping: 1,
          single_token_title_many_urls: 1,
          two_token_title_many_urls: 2,
          concrete_title_many_urls: 1,
          parser_pollution_grouping: 1,
          not_weak_google_sheets_grouping: 4,
          unknown: 0
        },
        reviewQueueCounts: {
          review_many_urls_same_title: 3,
          review_listing_url_bundle: 2,
          review_category_title_bundle: 1,
          review_open_application_bundle: 1,
          review_provider_static_disagreement: 1,
          monitor: 4
        },
        reviewQueueCauseCounts: {
          category_or_department_bucket: 1,
          open_application_family: 1,
          listing_page_bundle: 2,
          spreadsheet_role_bucket_needs_review: 3,
          google_sheets_role_bucket_needs_review: 2,
          parser_or_directory_text_pollution: 1,
          provider_static_disagreement: 1,
          likely_legitimate_multi_role_family: 4,
          unknown: 3
        },
        dedupAuditGate: {
          status: "blocked",
          lifecycleUxReady: false,
          currentRunMergedCount: 1,
          sourceBundleCollisionCount: 2,
          currentRunSourceBundleCollisionCount: 1,
          carriedSourceBundleCollisionCount: 1,
          highRiskReviewQueueCount: 10,
          currentRunHighRiskReviewQueueCount: 2,
          carriedHighRiskReviewQueueCount: 8,
          providerStaticDisagreementCount: 1,
          googleSheetsGenericRoleGuardActive: true,
          carriedCollisionLikelyHistoricalCount: 0,
          blockers: [
            "provider_static_disagreement_needs_review",
            "high_risk_review_queue_causes_need_review"
          ],
          warnings: ["current_run_primary_url_merges_present"],
          examples: [
            {
              title: "Accounting",
              company: "Kforce Inc",
              recommendedReviewAction: "review_listing_url_bundle",
              suspectedCause: "listing_page_bundle",
              sourceBundleCount: 255,
              identityQuality: "shared_listing_url_weak",
              bundleEvidenceOrigin: "carried_from_existing_output"
            }
          ]
        },
        carriedBundleExamples: [
          {
            title: "Accounting",
            company: "Kforce Inc",
            recommendedReviewAction: "review_listing_url_bundle",
            suspectedCause: "listing_page_bundle",
            sourceBundleCount: 255,
            identityQuality: "shared_listing_url_weak",
            bundleEvidenceOrigin: "carried_from_existing_output"
          }
        ],
        topMergedJobs: [
          {
            title: "Senior Engineer",
            company: "Studio One",
            sourceBundleCount: 2,
            sourceClasses: { provider: 1, static: 1, social: 0, other: 0 }
          }
        ],
        topSourceBundleOutliers: [
          {
            title: "Accounting",
            company: "Kforce Inc",
            sourceBundleCount: 255,
            sourceClasses: { provider: 0, static: 0, social: 0, other: 255 },
            outlierReason: "large_other_source_bundle",
            distinctLocationCount: 30,
            uniqueJobLinkCount: 2,
            sharedPrimaryUrl: false,
            providerSourceJobIdCount: 0,
            hasStrongIdentity: false,
            dominantSourceClass: "other",
            identityShape: "shared_listing_or_category_url",
            sharedUrlHost: "kforce.example",
            sharedUrlPath: "/jobs",
            uniqueUrlHostCount: 1,
            uniqueUrlPathPrefixCount: 1,
            titleShape: "category_like",
            identityCaveats: [
              "shared_url_looks_like_listing_or_category",
              "category_like_title",
              "other_source_class_dominant"
            ]
          }
        ],
        reviewQueue: [
          {
            title: "Accounting",
            company: "Kforce Inc",
            sourceBundleCount: 255,
            recommendedReviewAction: "review_listing_url_bundle",
            suspectedCause: "listing_page_bundle",
            causeEvidence: [
              "cause:listing_page_bundle",
              "identity:shared_listing_or_category_url"
            ],
            identityQuality: "shared_listing_url_weak",
            identityQualityEvidence: [
              "quality:shared_listing_url_weak",
              "provider_ids:0",
              "non_provider_ids:0"
            ],
            nonProviderIdentityProvenance: "google_sheets_row_identity",
            nonProviderIdentityEvidence: [
              "provenance:google_sheets_row_identity",
              "dominant_source_name:google_sheets",
              "single_non_provider_source"
            ],
            googleSheetsBundleShape: "role_category_bucket",
            googleSheetsBundleEvidence: [
              "shape:role_category_bucket",
              "source_count:255",
              "unique_urls:2",
              "role_bucket_title"
            ],
            googleSheetsRoleBucketAudit: "listing_or_search_url_bucket",
            googleSheetsRoleBucketAuditEvidence: [
              "audit:listing_or_search_url_bucket",
              "shape:role_category_bucket",
              "paths_listing_or_search"
            ],
            googleSheetsBucketIntent: "listing_or_search_bucket",
            googleSheetsBucketIntentEvidence: [
              "intent:listing_or_search_bucket",
              "shape:role_category_bucket",
              "audit:listing_or_search_url_bucket"
            ],
            googleSheetsWeakGroupingAudit: "role_bucket_listing_grouping",
            googleSheetsWeakGroupingEvidence: [
              "audit:role_bucket_listing_grouping",
              "sheet_rows:255",
              "sheet_row_span:300"
            ],
            identityShape: "shared_listing_or_category_url",
            outlierReason: "large_other_source_bundle",
            identityCaveats: ["shared_url_looks_like_listing_or_category", "category_like_title"],
            sampleSources: ["kforce-a", "kforce-b"]
          }
        ],
        riskyMergeExamples: [
          {
            title: "Designer",
            company: "Studio Two",
            riskReasons: ["same_title_company_different_location"]
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Current Run Merges/i);
  assert.match(metricsEl.innerHTML, /Bundle Collisions/i);
  assert.match(metricsEl.innerHTML, /Risky Merges/i);
  assert.match(metricsEl.innerHTML, /Dedup evidence/i);
  assert.match(metricsEl.innerHTML, /Dedup Audit Gate/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-card/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-status/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-ready/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-chip/i);
  assert.match(metricsEl.innerHTML, /status blocked/i);
  assert.match(metricsEl.innerHTML, /lifecycle UX ready no/i);
  assert.match(metricsEl.innerHTML, /Blockers/i);
  assert.match(metricsEl.innerHTML, /Warnings/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-example/i);
  assert.match(metricsEl.innerHTML, /Classification evidence/i);
  assert.match(metricsEl.innerHTML, /Gate evidence/i);
  assert.match(metricsEl.innerHTML, /current-run collisions 1/i);
  assert.match(metricsEl.innerHTML, /carried collisions 1/i);
  assert.match(metricsEl.innerHTML, /current high-risk 2/i);
  assert.match(metricsEl.innerHTML, /carried high-risk 8/i);
  assert.match(metricsEl.innerHTML, /Google Sheets guard active/i);
  assert.match(metricsEl.innerHTML, /provider static disagreement needs review/i);
  assert.match(metricsEl.innerHTML, /Examples/i);
  assert.match(metricsEl.innerHTML, /<details class="admin-dedup-audit-gate-example">/i);
  assert.match(metricsEl.innerHTML, /Accounting @ Kforce Inc/i);
  assert.match(metricsEl.innerHTML, /carried from existing output/i);
  assert.match(metricsEl.innerHTML, /Classification/i);
  assert.match(metricsEl.innerHTML, /action review listing url bundle/i);
  assert.match(metricsEl.innerHTML, /review unreviewed/i);
  assert.match(metricsEl.innerHTML, /origin carried from existing output/i);
  assert.match(metricsEl.innerHTML, /Dedup carried bundle examples/i);
  assert.match(metricsEl.innerHTML, /primary URL 1/i);
  assert.match(metricsEl.innerHTML, /Carried source-bundle collision rows: 2/i);
  assert.match(metricsEl.innerHTML, /provider 1, static 1/i);
  assert.match(metricsEl.innerHTML, /Dedup risk reasons/i);
  assert.match(metricsEl.innerHTML, /location 1, provider\/static 1/i);
  assert.match(metricsEl.innerHTML, /Dedup outlier reasons/i);
  assert.match(metricsEl.innerHTML, /multi-location strong 1/i);
  assert.match(metricsEl.innerHTML, /large other 1/i);
  assert.match(metricsEl.innerHTML, /Dedup identity shapes/i);
  assert.match(metricsEl.innerHTML, /detail URL 1/i);
  assert.match(metricsEl.innerHTML, /listing\/category URL 2/i);
  assert.match(metricsEl.innerHTML, /many URLs 3/i);
  assert.match(metricsEl.innerHTML, /provider ID 4/i);
  assert.match(metricsEl.innerHTML, /Dedup identity quality/i);
  assert.match(metricsEl.innerHTML, /listing URL weak 2/i);
  assert.match(metricsEl.innerHTML, /same-host URLs weak 3/i);
  assert.match(metricsEl.innerHTML, /other source ID 1/i);
  assert.match(metricsEl.innerHTML, /Dedup non-provider provenance/i);
  assert.match(metricsEl.innerHTML, /google sheets 3/i);
  assert.match(metricsEl.innerHTML, /URL-derived 2/i);
  assert.match(metricsEl.innerHTML, /opaque other 1/i);
  assert.match(metricsEl.innerHTML, /Dedup Google Sheets bundle shapes/i);
  assert.match(metricsEl.innerHTML, /role\/category 3/i);
  assert.match(metricsEl.innerHTML, /single-location URLs 2/i);
  assert.match(metricsEl.innerHTML, /row collision 1/i);
  assert.match(metricsEl.innerHTML, /Dedup Google Sheets role-bucket audit/i);
  assert.match(metricsEl.innerHTML, /spreadsheet category 3/i);
  assert.match(metricsEl.innerHTML, /listing\/search 1/i);
  assert.match(metricsEl.innerHTML, /parser normalized 1/i);
  assert.match(metricsEl.innerHTML, /Dedup Google Sheets bucket intent/i);
  assert.match(metricsEl.innerHTML, /taxonomy bucket 3/i);
  assert.match(metricsEl.innerHTML, /possible role family 2/i);
  assert.match(metricsEl.innerHTML, /weak title\/company 1/i);
  assert.match(metricsEl.innerHTML, /Dedup Google Sheets weak grouping audit/i);
  assert.match(metricsEl.innerHTML, /role detail URLs 3/i);
  assert.match(metricsEl.innerHTML, /role listing\/search 1/i);
  assert.match(metricsEl.innerHTML, /single-token title 1/i);
  assert.match(metricsEl.innerHTML, /two-token title 2/i);
  assert.match(metricsEl.innerHTML, /Dedup review queue/i);
  assert.match(metricsEl.innerHTML, /listing URL 2/i);
  assert.match(metricsEl.innerHTML, /category title 1/i);
  assert.match(metricsEl.innerHTML, /open application 1/i);
  assert.match(metricsEl.innerHTML, /monitor 4/i);
  assert.match(metricsEl.innerHTML, /Dedup review causes/i);
  assert.match(metricsEl.innerHTML, /category 1/i);
  assert.match(metricsEl.innerHTML, /spreadsheet role 3/i);
  assert.match(metricsEl.innerHTML, /sheets role audit 2/i);
  assert.match(metricsEl.innerHTML, /non-provider URL/i);
  assert.match(metricsEl.innerHTML, /parser\/text 1/i);
  assert.match(metricsEl.innerHTML, /likely legitimate 4/i);
  assert.match(metricsEl.innerHTML, /Top source-bundle outliers/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-evidence-table/i);
  assert.match(metricsEl.innerHTML, /Accounting/i);
  assert.match(metricsEl.innerHTML, /Kforce Inc/i);
  assert.match(metricsEl.innerHTML, /large other source bundle/i);
  assert.match(metricsEl.innerHTML, /30 locations/i);
  assert.match(metricsEl.innerHTML, /2 links/i);
  assert.match(metricsEl.innerHTML, /0 provider IDs/i);
  assert.match(metricsEl.innerHTML, /other dominant/i);
  assert.match(metricsEl.innerHTML, /weak identity/i);
  assert.match(metricsEl.innerHTML, /shared listing or category url/i);
  assert.match(metricsEl.innerHTML, /title category like/i);
  assert.match(metricsEl.innerHTML, /shared kforce\.example\/jobs/i);
  assert.match(metricsEl.innerHTML, /caveats shared url looks like listing or category/i);
  assert.match(metricsEl.innerHTML, /Dedup review examples/i);
  assert.match(metricsEl.innerHTML, /review listing url bundle/i);
  assert.match(metricsEl.innerHTML, /listing page bundle; shared listing or category url/i);
  assert.match(metricsEl.innerHTML, /quality shared listing url weak/i);
  assert.match(metricsEl.innerHTML, /provenance google sheets row identity/i);
  assert.match(metricsEl.innerHTML, /sheets role category bucket/i);
  assert.match(metricsEl.innerHTML, /sheets audit listing or search url bucket/i);
  assert.match(metricsEl.innerHTML, /sheets intent listing or search bucket/i);
  assert.match(metricsEl.innerHTML, /sheets weak audit role bucket listing grouping/i);
  assert.match(metricsEl.innerHTML, /identity evidence quality:shared listing url weak/i);
  assert.match(metricsEl.innerHTML, /provenance evidence provenance:google sheets row identity/i);
  assert.match(metricsEl.innerHTML, /sheets evidence shape:role category bucket/i);
  assert.match(metricsEl.innerHTML, /sheets audit evidence audit:listing or search url bucket/i);
  assert.match(metricsEl.innerHTML, /sheets intent evidence intent:listing or search bucket/i);
  assert.match(metricsEl.innerHTML, /sheets weak evidence audit:role bucket listing grouping/i);
  assert.match(metricsEl.innerHTML, /cause evidence cause:listing page bundle/i);
  assert.match(metricsEl.innerHTML, /sources kforce-a \| kforce-b/i);
  assert.match(metricsEl.innerHTML, /Senior Engineer/i);
  assert.match(metricsEl.innerHTML, /Studio One/i);
  assert.match(metricsEl.innerHTML, /Designer/i);
  assert.match(metricsEl.innerHTML, /Studio Two/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});
