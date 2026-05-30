# Baluffo Data Contracts

> - **Status:** Active
> - **Use this when:** changing payload shape, schema fields, saved-job structure, discovery output shape, or UI interaction handles
> - **Canonical for:** data contracts between pipeline, bridge, frontend, and local user data flows
> - **Not canonical for:** subsystem ownership or route wiring
> - **Then inspect:** `src/core/schemas.py`, `src/core/contracts.py`, the owning `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` modules, relevant tests, and the owning runtime docs
> - **Last updated:** 2026-05-18
> - **Also update when changing contract shape:** `src/core/schemas.py`, `src/core/contracts.py`, the owning `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` modules, relevant tests, and any affected UI/runtime docs

This document serves as the absolute boundary and source of truth for data structures passed between the Python pipeline (`src/jobs/`) and the Vanilla JS frontend (`frontend/`).

**CRITICAL:** The frontend expects `camelCase` keys in all `data/*.json` files. The Python backend maps these explicitly through the owning `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` modules.

**Runtime source of truth:** jobs pipeline contract normalization is owned directly by `src/jobs/common/contracts_runtime.py`, `contracts_source_reports.py`, `contracts_task_state.py`, and `contracts_fetch_report.py`; the old `src/jobs/common/contracts.py` re-export shim is not a stable surface. `src/core/schemas.py` defines the Pydantic validation models used at pipeline, bridge, and local-data boundaries, including `CanonicalJobSchema`, `SavedJobSchema`, `LocalSavedJobRowSchema`, `LocalDataActivityRowSchema`, `LocalDataAttachmentRowSchema`, `LocalDataBackupPayloadSchema`, and `ManifestSchema`. `src/core/contracts.py` uses these schemas to validate pipeline payloads before writing `jobs-unified.json`. After the M5 runtime-storage cutover, bridge-managed terminal fetch closeout mirrors the same canonical rows into SQLite `jobs` / `job_sources` and regenerates `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` as compatibility exports; the row contract remains unchanged. Bridge local-data routes keep save-input validation compatibility-lenient and validate persisted/output rows separately. New fields or contract changes require updating this doc and the Pydantic schemas in `src/core/schemas.py`.

## 1. CanonicalJob
Represents a single job posting retrieved from the external sources.

| Field | Type | Description |
|---|---|---|
| `id` | `string` / `number` | A unique identifier, often auto-incremented by the pipeline or derived. |
| `title` | `string` | The title of the job opening. |
| `company` | `string` | The studio or employer name. |
| `city` | `string` | The geographic city or empty if purely remote. |
| `country` | `string` | The localized or ISO country name; `Unknown` is a missing-country placeholder, not contamination. |
| `workType` | `string` | One of `Remote`, `Hybrid`, `Onsite`. |
| `contractType` | `string` | One of `Full-time`, `Internship`, `Temporary`, `Unknown`. |
| `jobLink` | `string` | The canonical URL to apply for the job. |
| `sector` | `string` | The industry sector, e.g., `Game` or `Tech`. |
| `profession` | `string` | The normalized profession key (e.g. `3d-artist`, `gameplay`, `tools`). |
| `companyType` | `string` | The type of company (e.g. `Game`, `Tech`). |
| `description` | `string` | Fallback description text. |
| `source` | `string` | Name of the scraper/board (e.g. `google_sheets_tech`, `greenhouse_static`). |
| `sourceJobId` | `string` | The ID of the job according to the originating ATS / board. |
| `fetchedAt` | `string` (ISO 8601) | When the pipeline successfully fetched this row. |
| `postedAt` | `string` (ISO 8601) | When the employer originally posted the role (if available). |
| `status` | `string` | The job status (e.g. `active`, `likely_removed`, `archived`). |
| `firstSeenAt` | `string` (ISO 8601) | When this pipeline first discovered the job. |
| `lastSeenAt` | `string` (ISO 8601) | The last pipeline run this job was detected as active. |
| `removedAt` | `string` (ISO 8601) | When the pipeline detected a 404 or removal. |
| `lifecycleEvent` | `string` | Additive row-level lifecycle evidence: currently `reappeared`, `preserved`, or empty. |
| `lifecycleReason` | `string` | Additive lifecycle reason when `lifecycleEvent` is present: currently `source_failed`, `source_skipped`, or empty. |
| `dedupKey` | `string` | A unique content hash used for deduplication. |
| `qualityScore` | `number` | The heuristic health of the job details [0-100]. |
| `focusScore` | `number` | Deprecated/internal score [0-100]. |
| `sourceBundleCount` | `number` | The number of exact duplicates collapsed into this canonical row. |
| `sourceBundle` | `Array<Object>` | Stable capped sample of raw ATS payload from duplicate rows. When `sourceBundleCount` exceeds the array length, the row has additional duplicate evidence omitted from the artifact for size safety. |
| `adapter` | `string` | The Python adapter module used (e.g., `static`, `social`, `csv`). |
| `studio` | `string` | The underlying pipeline configuration studio group. |

`jobs-lifecycle-state.json` stores the lifecycle ledger keyed by dedup identity. Each row keeps the
same lifecycle fields (`status`, `firstSeenAt`, `lastSeenAt`, `removedAt`, `lifecycleEvent`,
`lifecycleReason`) plus enough canonical identity to reconstruct a saved-job key for read-only
Saved-page overlays: `title`, `company`, `city`, `country`, `jobLink`, `source`, `sourceJobId`,
and `postedAt`. This ledger is runtime state, not a user-editable Saved-job contract.

The first read-only lifecycle UX slice exposes only conservative frontend filters/labels derived
from these existing fields: `status="likely_removed"` as `Recently removed`,
`lifecycleEvent="reappeared"` as `Reappeared`, and `lifecycleEvent="preserved"` with
`lifecycleReason="source_failed"` as `Preserved because source failed`. `source_skipped`
preservation remains operational-only and is not a user-facing first-slice filter.

### 1.1 Fetch report dedup evidence

Completed fetch reports may include top-level `dedupEvidence`. This section is read-only
diagnostics built after deduplication from final canonical rows, `sourceBundle`, and dedup stats.
It does not change merge policy, job lifecycle retention, source selection, registry state, or any
source-policy behavior. Admin/Ops may surface it to help operators inspect whether merged jobs look
trustworthy before user-facing lifecycle labels are expanded.

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `number` | Dedup evidence payload version. |
| `mergedCount` | `number` | Count of input rows merged away during deduplication. |
| `collisionSamplesCount` | `number` | Count of stored merge collision samples from the dedup pass. |
| `mergeReasonCounts` | `Object` | Counts for `primaryUrl`, `secondaryKey`, `knownMirrorPair`, `socialKey`, `sparseIdentity`, and `unknown`. |
| `currentRunMergeExamples` | `Array<Object>` | Stable capped current-run merge samples from the latest dedup pass, including non-blocking known mirror pairs and unresolved fresh merge blockers. |
| `currentRunMergeExamplesByReason` | `Object` | Stable capped current-run merge samples grouped by `secondaryKey`, `sparseIdentity`, `knownMirrorPair`, `primaryUrl`, and `unknown` for blocker triage. |
| `googleSheetsGenericRoleGuardBlockedCount` | `number` | Count of Google Sheets generic role/category merge attempts blocked by the existing different-primary-URL guard. |
| `googleSheetsGenericRoleGuardBlockedReasonCounts` | `Object` | Report-only split of guard-blocked attempts by `secondaryKey` and `sparseIdentity`. |
| `googleSheetsGenericRoleGuardBlockedSamples` | `Array<Object>` | Capped report-only samples of Google Sheets generic role/category rows that would otherwise have merged by weak identity despite different concrete primary URLs. |
| `sourceBundleCollisionCount` | `number` | Final canonical rows carrying `sourceBundleCount > 1`, including rows whose bundle evidence was carried forward from previous output. |
| `currentRunSourceBundleCollisionCount` | `number` | Source-bundle collision rows created or touched by current-run merges. |
| `carriedSourceBundleCollisionCount` | `number` | Source-bundle collision rows carried from seeded previous output and not touched by current-run merges. |
| `currentRunHighRiskReviewQueueCount` | `number` | High-risk review queue causes from current-run bundle evidence. |
| `carriedHighRiskReviewQueueCount` | `number` | High-risk review queue causes from carried historical bundle evidence. |
| `sourceBundleComposition` | `Object` | Count of bundle entries by source class: `provider`, `static`, `social`, and `other`. |
| `riskReasonCounts` | `Object` | Aggregate risky-row counts by risk reason before sample capping. |
| `outlierReasonCounts` | `Object` | Aggregate source-bundle outlier counts by diagnostic reason before sample capping. |
| `identityShapeCounts` | `Object` | Aggregate source-bundle URL/source identity shape counts before sample capping. |
| `identityQualityCounts` | `Object` | Aggregate source-bundle identity-quality counts before sample capping. |
| `nonProviderIdentityProvenanceCounts` | `Object` | Aggregate non-provider source identity provenance counts before sample capping. |
| `googleSheetsBundleShapeCounts` | `Object` | Aggregate Google Sheets bundle granularity counts before sample capping. |
| `googleSheetsRoleBucketAuditCounts` | `Object` | Aggregate Google Sheets role-bucket audit counts before sample capping. |
| `googleSheetsRoleBucketAudit` | `Object` | Compact lifecycle-readiness audit summary for Google Sheets role-bucket blockers, including current-run, carried, guard-blocked, same-primary-URL, parser/category, unresolved counts, classification counts, and capped examples. |
| `googleSheetsBucketIntentCounts` | `Object` | Aggregate Google Sheets bucket-intent diagnostic counts before sample capping. |
| `googleSheetsWeakGroupingAuditCounts` | `Object` | Aggregate Google Sheets weak title/company grouping audit counts before sample capping. |
| `reviewQueueCounts` | `Object` | Aggregate advisory dedup review queue counts by recommended review action before sample capping. |
| `reviewQueueCauseCounts` | `Object` | Aggregate advisory dedup review counts by suspected root cause before sample capping. |
| `currentRunBlockingReviewQueueCauseCounts` | `Object` | Current-run blocker-only review queue counts by suspected root cause. |
| `carriedBlockingReviewQueueCauseCounts` | `Object` | Carried blocker-family review queue counts by suspected root cause; these warn instead of blocking lifecycle UX by themselves. |
| `currentRunMonitorReviewQueueCauseCounts` | `Object` | Current-run monitor-only review queue counts by suspected root cause. |
| `carriedMonitorReviewQueueCauseCounts` | `Object` | Carried monitor-only review queue counts by suspected root cause. |
| `dedupAuditGate` | `Object` | Read-only lifecycle-readiness gate derived from current-run merges, carried source-bundle collisions, review queue causes, provider/static disagreement, and Google Sheets guard status. |
| `providerStaticDisagreementCounts` | `Object` | Dedicated counts for provider/static disagreement rows: `total`, `currentRun`, and `carried`. |
| `providerStaticDisagreementGateCounts` | `Object` | Dedicated blocker-vs-warning counts for provider/static disagreement rows after carried-safe URL-variant downgrades, carried location-pollution/location-variant downgrades, and local dedup review-state overrides. |
| `providerStaticDisagreementClassificationCounts` | `Object` | Dedicated provider/static disagreement counts by review classification. |
| `providerStaticDisagreementExamples` | `Array<Object>` | Stable capped sample of provider/static disagreement rows for manual review. |
| `providerStaticTitleCompanyCollisionCounts` | `Object` | Dedicated counts for provider/static disagreements classified as `title_company_collision`: `total`, `currentRun`, and `carried`. |
| `providerStaticTitleCompanyCollisionAuditCounts` | `Object` | Dedicated audit counts for carried title/company collision rows: `carried_location_pollution`, `carried_location_variant`, `carried_provider_identity_location_conflict`, `possible_real_multi_location_conflict`, `not_carried`, and `unknown`. |
| `providerStaticTitleCompanyCollisionExamples` | `Array<Object>` | Stable capped sample of title/company collision rows independent of the general provider/static disagreement cap. |
| `carriedBundleExamples` | `Array<Object>` | Stable capped sample of historical carried source-bundle rows that may need review or metadata rebuilding. |
| `carriedBundleReconciliationRecommendation` | `Object` | Optional report-only recommendation to rebuild carried source-bundle metadata in a separate explicit maintenance run. |
| `reviewQueue` | `Array<Object>` | Stable capped sample of source-bundle rows that should be reviewed before lifecycle UX or dedup behavior changes. |
| `topMergedJobs` | `Array<Object>` | Stable capped sample of canonical rows with the largest `sourceBundleCount`. |
| `topSourceBundleOutliers` | `Array<Object>` | Stable capped sample of carried source-bundle outliers, sorted like `topMergedJobs`. |
| `locationDivergenceExamples` | `Array<Object>` | Stable capped sample of source-bundle rows with more than one meaningful location. |
| `riskyMergeExamples` | `Array<Object>` | Stable capped sample of merged rows that need review because evidence is weak or conflicting. |
| `riskyMergeExampleCount` | `number` | Total risky examples before sample capping. |

Sample rows include `id`, `dedupKey`, `title`, `company`, `jobLink`, `locationSummary`,
`sourceBundleCount`, `sourceClasses`, and `sources`. Outlier rows may also include
`outlierReason`, `distinctLocationCount`, `sampleLocations`, `uniqueJobLinkCount`,
`sharedPrimaryUrl`, `sharedUrlHost`, `sharedUrlPath`, `uniqueUrlHostCount`,
`uniqueUrlPathPrefixCount`, `urlHostDiversity`, `urlPathPrefixDiversity`,
`providerSourceJobIdCount`, `hasStrongIdentity`, `dominantSourceClass`,
`identityShape`, `titleShape`, `identityCaveats`, `titleCompanyPollutionSignals`,
`nonProviderIdentityProvenance`, `nonProviderIdentityEvidence`, `googleSheetsBundleShape`,
`googleSheetsBundleEvidence`, `googleSheetsRoleBucketAudit`,
`googleSheetsRoleBucketAuditEvidence`, `googleSheetsBucketIntent`,
`googleSheetsBucketIntentEvidence`, `googleSheetsWeakGroupingAudit`,
`googleSheetsWeakGroupingEvidence`, `suspectedCause`, and `causeEvidence`.
Risky rows also include `riskReasons`, currently including values such as
`same_title_company_different_location`,
`provider_static_duplicate_disagreement`, `missing_provider_ids`, and
`weak_title_company_only_evidence`.

Provider/static disagreement examples include `title`, `company`, `dedupKey`,
`bundleEvidenceOrigin`, `sourceBundleCount`, provider/static source names, provider/static source
job IDs when present, provider/static URLs, provider/static URL host/path-prefix samples,
`identityQuality`, `sharedIdentifierTokens`, `distinctLocationCount`, `sampleLocations`,
`disagreementClassification`, `disagreementClassificationEvidence`, `collisionReviewHint`, and
`disagreementEvidence`. Rows may also include `concreteSharedIdentifierTokens`,
`providerStaticOnly`, `disagreementGateDisposition`, `disagreementGateEvidence`,
`operatorReviewRecommendation`, `operatorReviewReason`, `dedupReviewStatus`,
`dedupReviewNote`, `dedupReviewUpdatedAt`, and `dedupReviewUpdatedBy`.
`operatorReviewRecommendation` values are `safe_duplicate`, `real_blocker`, and
`needs_review`; these are Admin presentation hints only and do not change review-state
actions. Classification values are `same_job_different_urls`,
`provider_redirect_or_canonical_url`, `static_parser_url_variant`, `title_company_collision`,
`stale_carried_bundle`, and `needs_manual_review`. They expose cases where provider and static
bundle entries do not share a primary URL, so operators can review whether the bundle is a safe
duplicate or a carried historical collision before lifecycle labels rely on it.
Provider/static URL variants are only auto-safe when backed by concrete shared job identity;
same host, company slug, career/listing words, or capped display samples are not sufficient.
Greenhouse public job URLs normalize `boards.greenhouse.io/{board}/jobs/{id}` and
`job-boards.greenhouse.io/{board}/jobs/{id}` to the same job identity. Static detail pages may
promote a single explicit Greenhouse apply CTA to `jobLink`; ambiguous multiple apply targets and
open/general application CTAs must remain on the static detail URL for review.
SmartRecruiters provider/static rows may also be downgraded to an auto-safe warning when both
sides are same-board job-detail URLs, the final title exposes a strong slash-separated title alias,
and the row has a single effective location; this is intentionally narrower than fuzzy
cross-language title matching.
`providerStaticTitleCompanyCollisionExamples` is a separate capped sample of only
`title_company_collision` rows so lifecycle gate blockers are visible even when the general
provider/static disagreement sample is filled by other classifications. `collisionReviewHint`
values are `different_locations_same_title_company`,
`same_location_different_provider_static_urls`, `provider_static_location_missing`,
`multiple_sources_need_manual_review`, and `unknown`.
Rows may also include `carriedLocationPollutionAudit` and `carriedLocationPollutionEvidence`.
Audit values are `carried_location_pollution`, `carried_location_variant`,
`carried_provider_identity_location_conflict`, `possible_real_multi_location_conflict`,
`not_carried`, and `unknown`.
`carried_location_variant` covers carried title/company rows whose location labels normalize to
the same city and whose provider/static sides share strong URL or identifier evidence. This audit
`carried_provider_identity_location_conflict` covers carried rows with polluted location labels,
multiple remaining plausible labels, and shared provider/static job identity evidence. This audit
is report-only and exists to distinguish real carried multi-location conflict from polluted,
equivalent, or provider-identity-backed carried location metadata before lifecycle readiness is
decided.

Outlier reason values are diagnostic only:
`multi_location_strong_identity`, `location_divergence_without_strong_identity`,
`provider_static_disagreement`, `large_other_source_bundle`,
`sparse_title_company_bundle`, and `unknown`. They help distinguish likely multi-location
postings, provider/static disagreement, large carried bundles dominated by unclassified source
rows, and weak title/company-only evidence. They do not change merge policy.

Identity shape values are diagnostic only: `shared_job_detail_url`,
`shared_listing_or_category_url`, `many_unique_urls_same_title`, `provider_id_backed`,
`missing_url_and_ids`, and `mixed_or_unknown_identity`. A shared URL is not automatically safe:
`identityCaveats` may mark shared listing/category URLs, category-like titles, open-application
titles, other-source dominance, many unique URLs with the same title, or missing URL/provider-id
evidence. These fields exist to decide whether dedup behavior needs a later investigation; they do
not change dedup or lifecycle behavior.

Review queue rows are advisory samples derived from the same final canonical rows and include
`recommendedReviewAction`. Current action values are `review_many_urls_same_title`,
`review_listing_url_bundle`, `review_category_title_bundle`,
`review_open_application_bundle`, `review_provider_static_disagreement`, and `monitor`.
Only non-monitor rows are included in the capped `reviewQueue` sample. The queue is not persisted
review state and does not provide merge, unmerge, cleanup, lifecycle, source-policy, or registry
controls.

`dedupAuditGate` is the compact operator gate for deciding whether read-only lifecycle UX can
proceed. Its `status` is `pass`, `warning`, or `blocked`; `lifecycleUxReady` is true only when
there are no blocker causes. It includes current-run and carried collision counts, current-run and
carried high-risk review counts, `providerStaticDisagreementCount`,
`providerStaticDisagreementCurrentRunCount`, `providerStaticDisagreementCarriedCount`,
`providerStaticDisagreementBlockedCount`, `providerStaticDisagreementWarningCount`,
`googleSheetsGenericRoleGuardActive`, `carriedCollisionLikelyHistoricalCount`,
`reviewQueueCauseCounts`, origin-split blocker/monitor cause counts,
`currentRunNonPrimaryMergeCounts`, `blockers`, `warnings`, `blockerDetails`,
`warningDetails`, and capped `examples`. `blockerDetails` and `warningDetails` are
operator-facing summaries with `key`, `label`, `count`, `whyBlocked`, `nextAction`, `counts`,
and capped `examples`; they explain the actionable gate families without replacing the stable
raw `blockers` and `warnings` arrays. Carried historical
source-bundle collisions may warn without blocking, while current-run non-primary merges,
current-run high-risk causes, and provider/static disagreement block lifecycle readiness until
reviewed. Narrow current-run `knownMirrorPair` merges are excluded from the fresh non-primary merge
blocker count when they match the reviewed-safe `gracklehq` plus Guerrilla `gamesjobsdirect`
mirror pattern; this is not a generic secondary-key relaxation. This gate is advisory only and
does not add lifecycle labels or change dedup behavior.
When `carriedBundleReconciliationRecommendation.recommendedAction` is
`rebuild_carried_source_bundle_metadata`, it is a report-only recommendation for a separate
explicit maintenance run; this fetch report does not rewrite historical bundle metadata. Carried
provider/static title/company collisions audited as `carried_location_pollution` or
`carried_location_variant` or `carried_provider_identity_location_conflict` may warn without
blocking, while current-run disagreement and carried unresolved provider/static conflict continue
to block lifecycle readiness.

Suspected cause values are diagnostic only: `category_or_department_bucket`,
`open_application_family`, `listing_page_bundle`, `spreadsheet_role_bucket_needs_review`,
`google_sheets_role_bucket_needs_review`, `parser_or_directory_text_pollution`,
`provider_static_disagreement`,
`likely_legitimate_multi_role_family`, and `unknown`.
`causeEvidence` is a compact list of the identity shape, title shape, outlier reason, dominant
source class, caveats, and title/company pollution signals that led to the suspected cause. These
fields prioritize human review; they do not persist review decisions or change dedup behavior.

Identity quality values are diagnostic only: `provider_id_strong`, `shared_detail_url_strong`,
`shared_listing_url_weak`, `many_urls_same_host_weak`, `many_urls_many_hosts_weak`,
`other_source_id_untrusted`, `missing_identity`, and `unknown`. `identityQualityEvidence`
records compact counts and URL/source facts such as provider IDs, non-provider source IDs, URL
count, host count, path-prefix count, dominant source class, and shared URL shape. These fields
separate provider-grade identity from weaker non-provider URL/source identity for audit only.

Non-provider identity provenance values are diagnostic only: `google_sheets_row_identity`,
`url_derived_identity`, `category_or_directory_identity`, `opaque_other_source_identity`,
`mixed_non_provider_identity`, `none`, and `unknown`. `nonProviderIdentityEvidence` records compact
source ID and source-name facts such as dominant source name, source count, non-provider source ID
count, source ID shape samples, source ID prefix count, URL host count, and URL path-prefix count.
These fields explain where weak non-provider identity evidence appears to come from; they do not
promote those IDs to provider-grade dedup identity.

Google Sheets bundle shape values are diagnostic only: `role_category_bucket`,
`company_role_family`, `single_location_many_urls`, `multi_location_many_urls`,
`spreadsheet_row_collision`, `not_google_sheets`, and `unknown`. `googleSheetsBundleEvidence`
records compact bundle facts such as source count, unique URL count, URL host/path-prefix counts,
sample URL paths, source ID shapes, title/location shape, and role-bucket caveats. These fields
explain spreadsheet bundle granularity; they do not change dedup behavior and do not make Google
Sheets row IDs provider-grade identity.

Google Sheets role-bucket audit values are diagnostic only:
`likely_spreadsheet_category_bucket`, `role_family_needs_manual_review`,
`job_detail_urls_same_role`, `listing_or_search_url_bucket`,
`parser_normalized_role_title`, `not_google_sheets_role_bucket`, and `unknown`.
`googleSheetsRoleBucketAuditEvidence` records compact source, URL path, title token, source ID
shape, location, and pollution facts. These fields explain spreadsheet role-bucket provenance;
they do not change dedup identity strength or merge policy.

`googleSheetsRoleBucketAudit` is an additive summary for lifecycle-readiness triage. It reports
total/current-run/carried role-bucket counts, Google Sheets generic role/category guard-blocked
attempts with different concrete primary URLs, allowed same-primary-URL cases, likely historical
collisions, likely parser/category buckets, unresolved current-run role buckets, classification
counts, and capped examples. Example classifications are `fixed_by_generic_role_guard`,
`allowed_same_primary_url`, `historical_carried_bundle`, `unresolved_current_run_role_bucket`,
`parser_or_sheet_category_noise`, and `needs_narrow_dedup_guard`. The summary does not add review
actions or change dedup output behavior; it only clarifies whether high-risk Google Sheets evidence
is current-run blocker pressure or carried historical warning evidence.

Google Sheets bucket-intent values are diagnostic only:
`likely_spreadsheet_taxonomy_bucket`, `possible_role_family`,
`weak_title_company_grouping`, `listing_or_search_bucket`, `parser_normalized_bucket`,
`not_google_sheets_bucket`, and `unknown`. `googleSheetsBucketIntentEvidence` records compact
source count, unique URL count, URL host/path-prefix count, location count, title/company token
counts, URL path shape, generic role-bucket title markers, and pollution signals. These fields
help operators distinguish likely intentional spreadsheet taxonomy buckets from plausible role
families and weak title/company grouping; they do not change dedup identity strength or merge
policy.

Google Sheets weak grouping audit values are diagnostic only:
`role_bucket_detail_url_grouping`, `role_bucket_listing_grouping`,
`single_token_title_many_urls`, `two_token_title_many_urls`, `concrete_title_many_urls`,
`parser_pollution_grouping`, `not_weak_google_sheets_grouping`, and `unknown`.
`googleSheetsWeakGroupingEvidence` records compact source count, unique URL count, URL
host/path-prefix count, Google Sheets row-id count and span, title/company token counts,
location count, URL path shape, title tokens, path tokens, source ID samples, and pollution
signals. These fields help explain whether weak Google Sheets title/company grouping looks like
intentional spreadsheet bucketing, plausible role-family grouping, parser pollution, or a row-id
cluster that needs manual review. They do not change dedup behavior or lifecycle labels.

`mergedCount` and `mergeReasonCounts` describe the current dedup pass. They can be zero while
`sourceBundleCollisionCount` is nonzero because canonical rows may carry forward historical
`sourceBundle` evidence from earlier runs. `currentRunMergeExamples` is a separate advisory sample
surface for the latest merge pass so fresh blockers are still visible when broader carried-bundle
diagnostics dominate the capped review samples. `currentRunMergeExamplesByReason` groups that same
row shape into capped `secondaryKey`, `sparseIdentity`, `knownMirrorPair`, `primaryUrl`, and
`unknown` buckets so operators can distinguish broad identity-key blockers from reviewed mirror
pairs.

## 2. Desktop Local Data

Desktop local data is file-backed under the configured data root. Repo/source runs default to `data/local-user-data/`; Windows packaged desktop defaults to `%APPDATA%\Baluffo\local-user-data\`. Legacy packaged `ship\data\local-user-data\` is a first-launch migration source only. `src/local_data_store.py` remains the stable import surface, while persisted row shapes are defined by the dedicated local-data schemas in `src/core/schemas.py`.

### 2.1 Persisted saved-job row

This is the canonical stored/output row shape returned by desktop local-data GET routes and used inside backup payloads.

| Field | Type | Description |
|---|---|---|
| `profileId` | `string` | Owning local profile ID. |
| `jobKey` | `string` | Primary key `job_<hash>` derived from normalized job identity. |
| `title` | `string` | Job title. |
| `company` | `string` | Company or studio name. |
| `sector` | `string` | Normalized sector value. |
| `companyType` | `string` | Company type such as `Game` or `Tech`. |
| `city` | `string` | City display text. |
| `country` | `string` | Country display text. |
| `workType` | `string` | `Remote`, `Hybrid`, `Onsite`, or similar display value. |
| `contractType` | `string` | Contract label such as `Full-time`, `Internship`, or `Unknown`. |
| `jobLink` | `string` | Sanitized application URL. |
| `profession` | `string` | Profession key or display label. |
| `isCustom` | `boolean` | True for user-created local rows. |
| `customSourceLabel` | `string` | User-facing badge/source label for custom rows. |
| `reminderAt` | `string` (ISO 8601) | Optional reminder timestamp. |
| `contactedAt` | `string` (ISO 8601) | Optional contacted timestamp. |
| `updatedBy` | `string` | Optional user/editor marker. |
| `pipelinePhase` | `string` | Canonical phase: `bookmark`, `applied`, `screening`, `assignment`, `interview_1`, `interview_2`, `final`, or `offer`. |
| `outcomeStatus` | `string` | Canonical outcome: `active`, `rejected`, `withdrawn`, `ghosted`, `closed`, or `accepted`. |
| `applicationStatus` | `string` | Legacy compatibility mirror derived from `pipelinePhase` and `outcomeStatus`; new code must not read this as source of truth. |
| `phaseTimestamps` | `object<string, string>` | Current valid phase-to-timestamp map for pipeline phases only; `bookmark` is always present after normalization. Backward override transitions clear timestamps for phases after the restored phase. |
| `outcomeTimestamps` | `object<string, string>` | Outcome-to-timestamp map for terminal outcomes. |
| `notes` | `string` | Freeform local notes. |
| `attachmentsCount` | `number` | Current count of persisted local attachments for the row. |
| `savedAt` | `string` (ISO 8601) | Bookmark/create timestamp. |
| `updatedAt` | `string` (ISO 8601) | Last mutation timestamp. |
| `contentUpdatedAt` | `string` (ISO 8601) | Last saved-job content/edit timestamp. |
| `trackingUpdatedAt` | `string` (ISO 8601) | Last phase/outcome tracking timestamp. |
| `notesUpdatedAt` | `string` (ISO 8601) | Last notes timestamp, empty when notes have not been edited. |
| `lastActivityAt` | `string` (ISO 8601) | Latest activity row timestamp touching this saved job. |

`/desktop-local-data/saved-jobs/save` remains compatibility-lenient: the POST input validator still accepts legacy fields such as `snapshot`, `status`, `attachments`, `signature`, and `keySalt` through `SavedJobSchema`. Those fields are accepted for input compatibility, but they are not the canonical persisted/output row contract.

### 2.2 Persisted activity row

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Activity row ID, currently `log_<random>`. |
| `profileId` | `string` | Owning local profile ID. |
| `type` | `string` | Event type such as `job_saved`, `status_changed`, or attachment events. |
| `jobKey` | `string` | Related saved-job key when present. |
| `title` | `string` | Job title snapshot for history display. |
| `company` | `string` | Company snapshot for history display. |
| `createdAt` | `string` (ISO 8601) | Event timestamp. |
| `details` | `object` | Event-specific metadata. |

Tracking activity details remain additive and backward-compatible. `phase_reverted` rows keep the generic phase/status fields and may also include `revertedFromPhase`, `restoredPhase`, `removedPhaseTimestampFor`, and `restoredPhaseTimestamp`. `outcome_reverted` rows keep the generic outcome/status fields and may also include `revertedFromOutcome`, `restoredOutcome`, and `restoredOutcomeTimestamp`.

### 2.3 Persisted attachment row

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Attachment ID, currently `att_<random>` or imported ID. |
| `profileId` | `string` | Owning local profile ID. |
| `jobKey` | `string` | Related saved-job key. |
| `name` | `string` | Original attachment filename. |
| `type` | `string` | MIME type. |
| `size` | `number` | Attachment size in bytes. |
| `createdAt` | `string` (ISO 8601) | Attachment creation/import timestamp. |
| `path` | `string` | Relative on-disk filename under the user attachment directory. |

Saved-job removal is intentionally row-scoped. `removeSavedJobForUser` hard-removes the
saved-job row and does not delete attachment rows for the same `profileId` + `jobKey`.
The Saved page confirmation/Undo flow restores the removed saved-job snapshot with the
same `jobKey`, so any preserved attachment rows become visible again after Undo. File or
metadata cleanup must go through the explicit attachment delete surface, not saved-job
remove.

### 2.4 Backup export/import payload v3

Desktop backup export writes schema version `3` and remains profile-scoped. Importers accept v1, v2, and v3 payloads; legacy saved-job rows are normalized into split `pipelinePhase`/`outcomeStatus` tracking on import.

| Field | Type | Description |
|---|---|---|
| `version` | `number` | Always `3` for the current writer. |
| `schemaVersion` | `number` | Always `3` for the current writer. |
| `exportedAt` | `string` (ISO 8601) | Export timestamp. |
| `includesFiles` | `boolean` | Whether attachment file contents were embedded. |
| `counts.savedJobs` | `number` | Count of exported saved-job rows. |
| `counts.customJobs` | `number` | Count of exported custom saved-job rows. |
| `counts.historyEvents` | `number` | Count of exported activity rows. |
| `counts.attachments` | `number` | Count of exported attachment rows. |
| `counts.sourcePolicyReviewPairs` | `number` | Count of exported local source-policy review-state pairs. |
| `counts.sourcePolicyRecommendationPairs` | `number` | Count of exported source-policy recommendation pairs. |
| `profile` | `object` | Profile metadata with `id`, `name`, and `email`. |
| `savedJobs` | `Array<LocalSavedJobRow>` | Saved-job rows using the canonical persisted shape above. |
| `attachments` | `Array<LocalDataBackupAttachment>` | Attachment metadata rows; each row may include `blobDataUrl` when `includesFiles` is true. |
| `activityLog` | `Array<LocalDataActivityRow>` | Activity/history rows. |
| `sourcePolicy` | `object` | Optional desktop bridge-backed source-policy portability payload. |

`blobDataUrl` is additive inside backup attachment rows. It is only populated when the caller exports with files included, and importers must remain tolerant of metadata-only backups without file contents.

Desktop bridge-backed backups may include additive `sourcePolicy.reviewState`,
`sourcePolicy.recommendations`, and `sourcePolicy.warnings`. `reviewState` is the normalized
`source-policy-review-state.json` artifact, `recommendations` is the normalized
`source-policy-recommendations.json` artifact, and `warnings` contains non-fatal portability
diagnostics. Missing artifacts export as normalized empty artifacts. Malformed artifacts do not
fail export/import; they are skipped or exported empty with warnings. This source-policy backup
payload is explicit local backup/import portability only: source-policy review state, including
`force_pause`, must not be included in `source-sync.json`, active/pending/rejected registry
buckets, tombstones, or remote sync payloads.

### 2.5 Stable JS runtime contract

The canonical browser/desktop local-data runtime surface is defined by `frontend/local-data/runtime-contract.js`. `window.JobAppLocalData` in desktop mode must satisfy `LOCAL_DATA_RUNTIME_METHODS` exactly.

Stable method groups:

- Auth/session: `isReady`, `getCurrentUser`, `onAuthStateChanged`, `signIn`, `signOut`
- Saved jobs and tracking: `saveJobForUser`, `removeSavedJobForUser`, `getSavedJobKeys`, `subscribeSavedJobs`, `generateJobKey`, `canTransitionPhase`, `updateApplicationStatus`, `updateApplicationTracking`, `updateJobNotes`
- Attachments: `buildAttachmentPath`, `listAttachmentsForJob`, `addAttachmentForJob`, `getAttachmentBlob`, `getAttachmentOpenUrl`, `getAttachmentDownloadUrl`, `deleteAttachmentForJob`
- Activity/backup/admin: `listActivityForUser`, `exportProfileData`, `getBackupExportUrl`, `importProfileData`, `getAdminOverview`, `wipeAccountAdmin`

---

## 3. UI Interaction Contract (`data-ui`)

To decouple UI logic from presentation/styling, Baluffo uses `data-ui` attributes as the "canonical handles" for all interactive elements.

### The Strategy
1. **Registry**: All `data-ui` tokens MUST be registered in `frontend/shared/ui/selectors.js`.
2. **HTML Implementation**: Elements in `.html` templates should include the attribute: `data-ui="token-name"`.
3. **JS selection**: Use the `ui(token)` helper from `selectors.js` to query elements: `document.querySelector(ui(G_TOKENS.myToken))`.

### Guidelines for AI Agents
- **NEVER** use class names or IDs for querying if a `data-ui` attribute is available.
- If you add a new interactive element, add a corresponding token to `selectors.js` and apply it via `data-ui`.

---

## 4. Workspace HUD Contract (`LATEST_MANIFEST.json`)

The orchestrator generates a machine-readable HUD in `_out/LATEST_MANIFEST.json` after every run. AI agents SHOULD read this file first to understand the current workspace state.

### Schema (v1)
```json
{
  "last_run_id": "string (YYYYMMDD_HHMMSS)",
  "last_run_time": "string (timestamp)",
  "status": "string (success|failure)",
  "summary": "string (human-readable status)",
  "src_hash": "string (SHA256 of src/)",
  "artifacts_root": "string (path to run directory)",
  "artifacts": {
    "exe": "string (path to portable exe artifact)",
    "ship": "string (path to ship bundle artifact)",
    "smoke_report": "string (relative path to report.json)",
    "py_tests_status": "string (not_run|passed|failed)",
    "node_tests_status": "string (not_run|passed|failed)",
    "py_tests_ok": "boolean",
    "node_tests_ok": "boolean"
  }
}
```

`py_tests_ok` and `node_tests_ok` remain legacy HUD booleans. New code and AI agents should prefer `py_tests_status` and `node_tests_status` when present so `not_run` is distinguishable from `failed`.

---

## 5. Source Registry and Sync

Baluffo's source registry has tracked seed defaults plus local runtime bucket files.
The seed files are reviewable defaults bundled with the app; runtime files are local
operational state and are ignored by Git. After the M6 runtime-storage cutover,
bridge-owned registry authority is SQLite when `sourceRegistry=sqlite`; the files
below remain compatibility/debug exports and JSON fallback inputs.

| File | Purpose |
|---|---|
| `data/defaults/source-registry-active.seed.json` | Tracked default active source registry |
| `data/defaults/source-registry-pending.seed.json` | Tracked default pending source registry |
| `data/source-registry-active.json` | Ignored runtime active sources ready for fetch/sync; overrides the seed when present |
| `data/source-registry-pending.json` | Ignored runtime pending sources in the probe/standby pool; overrides the seed when present |
| `data/source-approval-state.json` | Ignored runtime approval counters and timestamps |
| `data/source-registry-rejected.json` | Local rejected sources; a normal registry bucket, not a delete sentinel |
| `data/source-registry-tombstones.json` | Local delete ledger keyed by `source_identity()` |

When `sourceRegistry=json`, registry reads use the runtime active/pending file when
it exists, then the tracked seed file, then the in-code fallback. When
`sourceRegistry=sqlite`, registry GET routes, POST mutations, and tombstone
load/save publish through generation-scoped SQLite rows first and regenerate the
active/pending/rejected/tombstone exports afterward. Seed files are never mutated.
Direct CLI JSON writes or JSON/SQLite drift trigger JSON fallback/rollback rather
than silent SQLite overwrite.

### Canonical registry row

Registry rows are normalized around these canonical fields:

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Canonical source identity derived from `source_identity()` |
| `registryState` | `string` | One of `active`, `pending`, or `rejected` |
| `pendingReason` | `string` | Why the row is pending or rejected; empty for active rows |
| `stateChangedAt` | `string` (ISO 8601) | Timestamp of the last transition into the current registry state |
| `stateChangedBy` | `string` | Actor or route that performed the transition |
| `lastPromotedAt` | `string` (ISO 8601) | Last time the row was promoted into `active` |
| `lastDemotedAt` | `string` (ISO 8601) | Last time the row was demoted into `pending` or `rejected` |
| `hiddenFromDefault` | `boolean` | Optional pending-row flag for recoverable rows hidden from default review views |
| `duplicateOfSourceId` | `string` | Optional pointer to the active winner when a duplicate-family row is demoted |

Legacy lifecycle fields such as `candidateState`, `approvedAt`, `approvedBy`, `liveAt`, `quarantinedAt`, and `quarantineReason` remain populated for compatibility, but they should be treated as compatibility mirrors rather than the canonical source of truth.

### Registry conflict live evidence

`/registry/conflicts` may include additive live source-check evidence on conflict cards and
rows. When the latest adjudication covers every row in a conflict family and each probe
completed, bridge read-time conflict ranking may use `liveJobsFound` for the effective
winner and safe-automation decision. The stored registry value is preserved as
`registryJobsFound`; `jobsFound` on that read-time row becomes the effective count used
by the conflict card. Cards include `effectiveWinnerSource="live_adjudication"` when
completed live evidence changes the winner, otherwise `effectiveWinnerSource="registry"`.

These fields are read-time diagnostics and must not mutate registry bucket files,
source-state files, sync snapshots, tombstones, saved jobs, or source URLs by themselves.
If adjudication is missing, partial, running, or failed, conflict ranking falls back to
registry counts.

`data/registry-conflict-adjudication.json` also acts as the local progress artifact
while `/registry/conflicts/check-sources` is running. Running payloads keep
`families: []` and expose only additive diagnostic fields: `heartbeatAt`,
`taskProgress`, and `progress`. `taskProgress` carries UI-level phase data
(`active`, `phaseKey`, `phaseLabel`, `mode`, `ratio`, `counts`, `targetLabel`,
`targetUrl`, and `updatedAt`). `progress` carries registry-specific counters,
the current family/source being checked, `lastProgressAt`, and a capped
`recentEvents` list. Terminal success or failure writes `active: false` and may
include full `families`; running progress must not drive winner overrides,
safe automation, demotion, sync, or registry row mutation.

### Sync snapshot v2

Remote sync snapshots now use schema version `2` and are built from canonical per-source rows.

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `number` | Always `2` for the current writer |
| `generatedAt` | `string` (ISO 8601) | Snapshot build time |
| `source` | `object` | Snapshot origin metadata |
| `active` | `Array<Object>` | Active registry rows |
| `pending` | `Array<Object>` | Pending registry rows |

`rejected` rows are intentionally excluded from remote snapshots. Snapshot readers still accept legacy v1 input and infer the transition metadata needed to merge into the canonical model. Tombstones are never included in remote snapshots.

### Admin/Ops registry sync confidence

`/ops/health` may include additive `kpis.registrySync` for Admin/Ops observability. It is derived from existing local registry buckets, tombstones, sync runtime state, and sync run history; it must not be copied into source rows or used to auto-promote, demote, reject, tombstone, hide, delete, or merge sources.

| Field | Type | Description |
|---|---|---|
| `activeCount` | `number` | Local active registry rows. |
| `pendingCount` | `number` | Local pending registry rows. |
| `rejectedCount` | `number` | Local rejected rows; these stay local-only for sync. |
| `tombstoneCount` | `number` | Local tombstone records; these are never serialized remotely. |
| `hiddenPendingCount` | `number` | Pending rows hidden from default review views. |
| `deferredPendingCount` | `number` | Pending rows with deferred/backlog markers. |
| `duplicatePendingCount` | `number` | Pending rows marked as duplicate variants. |
| `lastSyncAt` | `string` | Latest known pull/push/sync-run timestamp, or empty when unavailable. |
| `lastSyncStatus` | `string` | Latest sync result such as `ok`, `error`, `remote_conflict`, `rate_limited`, or `never`. |
| `remoteActiveCount` | `number` | Active count from the latest sync summary when available. |
| `remotePendingCount` | `number` | Pending count from the latest sync summary when available. |
| `pulledCount` | `number` | Latest observed pull operation/count from existing sync history. |
| `pushedCount` | `number` | Latest observed push operation/count from existing sync history. |
| `ignoredRejectedCount` | `number` | Rejected local rows intentionally ignored by remote sync. |
| `ignoredTombstonedCount` | `number` | Tombstones intentionally ignored by remote sync. |
| `conflictCount` | `number` | Latest detected remote conflict indicator count. |
| `localOnlyCount` | `number` | Local-only rejected plus tombstoned rows. |
| `remoteOnlyCount` | `number` | Remote-only row count when existing sync diagnostics provide it; otherwise `0`. |
| `invalidRowsCount` | `number` | Invalid non-object rows found while deriving local bucket counts. |

---

## 6. Runtime Configuration

### Frontend Config
The `frontend-runtime-config.js` is generated by `scripts/build_frontend_runtime_config.py`. It bridges the gap between Python-side configuration and the browser.

### Bridge Config
The Admin Bridge (`src/admin_bridge.py`) follows a strict precedence:
1. CLI Arguments
2. Environment Variables (`BALUFFO_*`)
3. `baluffo.config.local.json` (machine-local overrides)
4. `baluffo.config.json` (committed defaults)

`baluffo.config.json` is the committed app/runtime configuration owner. Tooling-specific
configuration such as `opencode.json` remains separate; do not move MCP/editor keys into
the runtime config or runtime keys into tool config.

---

## 7. Configuration Schema

### baluffo.config.json

| Key | Default | Description |
|-----|---------|-------------|
| **bridge.host** | `"127.0.0.1"` | Admin bridge listen host |
| **bridge.port** | `8877` | Admin bridge listen port |
| **bridge.log_format** | `"human"` | Log format (`human` or `jsonl`) |
| **bridge.log_level** | `"info"` | Log level (`info` or `debug`) |
| **bridge.quiet_requests** | `false` | Suppress request logging |
| **storage.data_dir** | `"data"` | Runtime data directory for repo/source runs; Windows packaged desktop defaults to `%APPDATA%\Baluffo\` unless `--data-dir` or `BALUFFO_DATA_DIR` is set |
| **storage.source_discovery_config_path** | `"data/source-discovery-config.json"` | Source discovery settings path |
| **storage.source_discovery_log_path** | `"data/source-discovery.log"` | Source discovery log path |
| **storage.social_sources_config_path** | `"data/social-sources-config.json"` | Social source settings path |
| **security.github_app_enabled_default** | `true` | GitHub App sync enabled by default |
| **sync.packaged_config_path** | `"packaging/github-app-sync-config.json"` | GitHub App sync config |
| **sync.local_enabled_default** | `true` | Source sync enabled by default |
| **sync.default_repo** | `""` | Default sync repo (e.g. `owner/repo`) |
| **sync.default_branch** | `"main"` | Default sync branch |
| **sync.default_path** | `"baluffo/source-sync.json"` | Default sync file path |
| **sync.build_key_derivation_default** | `"embedded"` | Key derivation mode |
| **desktop.site_port** | `8080` | Local site port |
| **desktop.bridge_port** | `8877` | Desktop mode bridge port |
| **desktop.bridge_host** | `"127.0.0.1"` | Desktop mode bridge host |
| **desktop.open_path** | `"jobs.html"` | Desktop startup page |
| **desktop.title** | `"Baluffo"` | Desktop window title |

**Config precedence:** CLI args → Environment (`BALUFFO_*`) → `baluffo.config.local.json` → `baluffo.config.json` → code defaults

**Machine-local overrides:** Use `baluffo.config.local.json` for settings that must not be committed.

---

## 8. Source discovery contract

Source discovery writes `data/source-discovery-report.json` and `data/source-discovery-candidates.json`. Treat the following as stable until a dedicated plan.

**Pydantic validation:** Report summary shape is defined and validated at the discovery output boundary. See **src/source_discovery/schemas.py** for `DiscoveryReportSummarySchema` and `DiscoveryReportSchema`. The orchestrator validates the summary with `DiscoveryReportSummarySchema.model_validate(report["summary"])` before writing the report; invalid shape raises `ValidationError`. The snapshot test `test_discovery_report_snapshot_contract` also validates the summary so the contract is enforced in CI.

### Stable public APIs (`src/source_discovery`)

Do not change signatures or remove without a dedicated plan:

- `run_discovery(...)`
- `discover_gamesmap_candidates(...)`
- `probe_candidate(...)`, `async_probe_candidate(...)`, `validate_candidate_for_probe(...)`
- `parse_gamesmap_detail_page(...)`, `parse_gamesmap_index_entries(...)`, `build_static_candidate_from_page(...)`

`src/source_discovery/orchestrator.py` remains the public run surface and test patch seam for `run_discovery(...)`; helper modules such as `orchestrator_runtime.py`, `orchestrator_generation.py`, `orchestrator_probe.py`, and `orchestrator_finalize.py` are implementation detail behind that contract. Gamesmap helper ownership now lives in `gamesmap_{cache,parsing,candidates}.py`, discovery reporting/helper ownership lives in `reporting_{progress,candidates,backlog}.py`, and web-search helper ownership lives in `web_search_{fetch,extract,candidates}.py`, with `gamesmap.py`, `reporting.py`, and `web_search.py` kept as stable import surfaces.

### Data contracts

- **source-discovery-report.json** and **source-discovery-candidates.json** must remain shape-compatible.
- **source-discovery-report.json** now includes top-level `runId` for lifecycle evidence. The matching lifecycle row in `data/admin-task-lifecycle.json` owns discovery identity and liveness while the task is active.
- **GameDevMap audit metadata** may appear as top-level `gamedevmapAuditSummary` and `summary.gamedevmapAudit`. These are additive report diagnostics for the resumable GameDevMap audit/cache path, including cache hit, timing, active split, recovered/browser counts, artifact size, and failure buckets. `gamedevmap.activeAuditEnabled` is no longer a supported source-discovery input; enabled GameDevMap discovery uses active-audit artifact rows. These fields are not candidate registry fields and must not be copied into active, pending, or rejected source rows.
- **Directory audit metadata** may appear as top-level `directoryAuditSummaries` and `summary.directoryAudits` when enabled Gameprog, Gamesmap, sheet-directory, or web-search audits run or reuse a fresh artifact in the current process. Gameprog, sheet-directory, and the combined seed-careers/web-search audit run by default when their stages are enabled; Gamesmap still also requires `gamesmap.enabled=true`. `activeAuditEnabled` is no longer a supported source-discovery input; enabled Gameprog, Gamesmap, sheet-directory, and web-search stages use audit-artifact rows. HTTP recovery lanes also run by default for Gameprog, Gamesmap, sheet-directory, and web-search unless the owning `activeAuditRecoveryEnabled=false` setting is set. `activeAuditRecoveryUrlLimit` defaults to `6` for these adapters, invalid or non-positive values fall back to `6`, and resolved values are included in audit signatures so budget changes rebuild artifacts. These additive diagnostics include cache hit, completion, audit duration, candidate/failure counts, timing totals, artifact size, top failure buckets, adapter-owned boundary counts, HTTP recovery counts (`recoveryFetchAttempts`, `recoveryPagesFetched`, `recoveredProviderCandidates`, `recoveredStaticCandidates`, `recoveryFailures`) when an adapter recovery lane runs, web-search-only link/query sample diagnostics, and web-search browser-recovery counts. They are not candidate registry fields and must not be copied into active, pending, or rejected source rows.
- **Report summary** must retain: counts, stage maps (`generatedCountByStage`, `survivedDedupeCountByStage`, `probedCountByStage`, `queuedCountByStage`), `lossAccounting`, `adapterCounts`, `methodCounts`.
- **Runtime lifecycle metadata:** discovery/fetch runtime may include `runtime.lifecycle.owner`, `runtime.lifecycle.ownerPid`, and `runtime.lifecycle.heartbeatAt`. These fields are additive and used by the bridge to project or reattach Current Runs without mutating terminal report truth.
- **Candidates file semantics:** `data/source-discovery-candidates.json` is the persisted discovery review queue. It may contain both queued candidates and deferred review rows; consumers must use `deferred` / `deferReason` instead of assuming every row is queue-ready.
- **M5 review snapshot:** `data/m5-strategic-backlog.json` is a derived review artifact built from discovery output. It is additive and must not replace `data/source-discovery-candidates.json` as the canonical discovery ledger.
- **Additive candidate metadata** may include lifecycle and ranking fields such as `candidateState`, `rankScore`, `rankReasons`, `promotionLane`, `approvedAt`, `approvedBy`, `liveAt`, `quarantinedAt`, `quarantineReason`, `deferCount`, `firstDeferredAt`, and `lastDeferredAt`.
- **Discovery review metadata** is derived observability only. Candidate rows may include additive review fields such as `sourceIdentity`, `duplicateOfActiveSource`, `duplicateOfPendingSource`, `providerDetected`, `providerFamily`, `lastProbeStatus`, `lastProbeError`, `browserFallbackRecommended`, and `promotionRecommendation`. Reports may include top-level `candidateReview` with recommendation counts and compact ranked candidate lanes. These fields help Admin review candidates and must not trigger automatic promote, hide, reject, tombstone, provider migration, or source deletion behavior by themselves.
- **Provider migration advisory metadata** is also derived observability only. Candidate rows may include additive advisory fields such as `currentAdapter`, `currentUrl`, `detectedProviderFamily`, `detectedProviderUrl`, `detectedProviderId`, `existingProviderSourceId`, `existingProviderSourceState`, `staticSourceState`, `migrationConfidence`, `migrationReasons`, and `recommendedAction`. `candidateReview.providerMigration` may group compact read-only lanes for provider migration candidates, already-covered static sources, add-provider-source candidates, unsupported providers, needs-probe rows, and keep-static / insufficient-evidence rows. The source-policy soak report may compute fallback staging diagnostics from current discovery candidates when the discovery report lacks `candidateReview.providerMigration`; fields such as `stagingDiagnosticsSource`, `computedStageableProviderCandidateCount`, `computedWouldStageProviderCandidateCount`, and `computedStagingBlockerCounts` are read-only "would stage" evidence and do not mean registry rows were written. These fields must not add, promote, hide, reject, tombstone, delete, sync, or migrate source rows.
- **Automatic provider candidate staging** may create provider-backed discovery candidates from strong static/generic provider evidence without Admin action. While a row is only in the discovery candidate stream it uses `candidateState="staged_provider_candidate"` plus advisory fields such as `createdFromAdvisory`, `migrationSourceIdentity`, `migrationReasons`, and `migrationConfidence`; it must not claim `registryState="pending"` until written to `data/source-registry-pending.json`. Pending provider rows use `pendingReason="provider_migration_candidate"` and remain subject to the existing probe and discovery auto-approval policy. Static source rows remain unchanged by staging.
- **Provider coverage validation** is provider-fetch-only evidence. A staged provider row is considered `validated_provider` after its own provider adapter fetch succeeds with `keptCount > 0`; static `jobsFound`, static fetch output, Scrapy/static, community, social, and generic rows must not validate provider coverage. Validation proves only that the provider source is real and usable. It must not delete, hide, reject, tombstone, skip, replace, or mark the original static source redundant.
- **Hidden pending rows** remain recoverable pending rows. `candidateState="hidden"` / `hiddenFromDefault=true` means default review views may omit them; explicit review views may request them.
- **Candidates** and **failures** objects must retain the fields asserted in `test_discovery_report_snapshot_contract`.
- Any contract change requires: updated snapshot fixture (`tests/fixtures/source_discovery_report_snapshot.json`), doc update, and a focused PR.

---

## 9. Admin task progress contract

### Admin task lifecycle ledger

`data/admin-task-lifecycle.json` is the canonical Admin task lifecycle ledger for fetch, discovery, sync, and jobs pipeline runs.

Ledger payload shape:

- `schemaVersion`: integer ledger schema version.
- `updatedAt`: ISO timestamp for the last ledger write.
- `rows`: array of task lifecycle rows.

Each lifecycle row keeps:

- `schemaVersion`, `runId`, `taskType`, `parentRunId`, `parentTaskType`
- `status`: one of `queued`, `running`, `succeeded`, `failed`, `canceled`, `orphaned`
- `stage`, `startedAt`, `heartbeatAt`, `finishedAt`, `terminalReason`
- `ownerKind`, `ownerPid`
- `progress`, `summary`

Lifecycle invariants:

- `queued` and `running` rows must have empty `finishedAt`.
- `succeeded`, `failed`, `canceled`, and `orphaned` rows must have non-empty `finishedAt`.
- Reports may enrich `progress` and `summary`, but `admin-task-lifecycle.json` owns active-vs-terminal state.
- `admin-task-state.json` and `admin-run-history.json` are legacy compatibility/migration artifacts and must not be treated as lifecycle authority.
- Production bridge startup must not import lifecycle rows from legacy history/state files. Legacy lifecycle reconciliation is reserved for explicit migration or test tooling.
- Terminal report evidence with `finishedAt` wins over stale active progress, task-state heartbeat, or copied live metadata during lifecycle projection.
- Live task projection must not use `admin-task-state.json` as liveness authority or progress evidence. Lifecycle rows decide active/terminal state; report/task evidence files supply display progress.
- Fetch, discovery, sync, and pipeline lifecycle state is written to `admin-task-lifecycle.json`; `admin-run-history.json` is not updated by normal task launch/completion flows.

Fetcher and discovery reports may include a shared `taskProgress` object for the admin loading bars. This is the preferred progress contract for the frontend.

Discovery `taskProgress.counts` may include additive subtask diagnostics while a stage is running. GameDevMap active-audit discovery reports use `subtaskKey="gamedevmap_active_audit"` plus `activeAudit*` counters such as completed/total URLs, batch, phase, homepage fetch, recovery fetch, recovery analysis, candidate, and failure counts. These fields are display-only progress diagnostics and must not drive registry, candidate, dedupe, or source-health decisions.

Sync `taskProgress.counts` may include additive sharded-push diagnostics while `phaseKey="remote_write"`: `shardCount`, `changedShardCount`, `completedShardCount`, `verifiedShardCount`, `currentShardIndex`, `currentShardLabel`, `shardsPushedBytes`, `totalShardBytes`, `manifestCommitted`, and `gcDeletedCount`. These fields are display-only heartbeat/progress diagnostics; the source-sync manifest, shard payloads, and final sync result metrics remain the authority for committed remote state.

### Stable fields

| Field | Type | Description |
|---|---|---|
| `active` | `boolean` | True while the task should render as in progress. |
| `phaseKey` | `string` | Stable machine-readable phase token such as `executing_sources` or `probing_candidates`. |
| `phaseLabel` | `string` | Human-readable phase label shown in the admin UI. |
| `mode` | `string` | Either `indeterminate` or `determinate`. |
| `ratio` | `number` | `0..1` progress ratio when `mode` is `determinate`; ignored otherwise. |
| `counts` | `object` | Display-only task metrics used to enrich the label, not to redefine primary progress semantics. |

### Frontend contract

- Controllers consume `taskProgress` and pass raw report state plus optional log-derived phase hints into the domain layer.
- The domain layer is responsible for mapping `taskProgress` into the rendered progress view.
- The shared progress renderer only renders the derived view model; it must not infer phases or ratios from raw report counters.
- Raw report counters remain useful for details, but the primary loading-bar state comes from `taskProgress`.
- `/ops/task-live/<taskType>` is the detailed Admin Ops live task contract for fetch, discovery, and sync. Its `recentEvents` rows are normalized by `src/shared/live_task.py` and include `schemaVersion`, `event`, `timestamp`, `level`, `taskType`, `runId`, `workItemId`, `phaseKey`, and `message`.
- The live task `event` token is additive and stable for diagnostics. It uses an explicit row `event` when present, then `phaseKey`, then `live_task_event`; existing consumers should continue to rely on the compatibility fields they already read.
- Do not introduce task-specific parallel live event formats for fetch, discovery, or sync. Extend `taskProgress`, `workItems`, and `recentEvents` through the shared normalizers.

### Lifecycle identity contract

- `runId` is the only lifecycle identity for long-running admin tasks.
- Fetch evidence surfaces:
  - `data/jobs-fetch-report.json`
  - `data/jobs-fetch-tasks.json`
- Discovery evidence surfaces:
  - `data/source-discovery-report.json`
- `data/admin-run-history.json` is a legacy history surface keyed by `runId`. It is not read or written by normal lifecycle projection.
- `data/jobs-fetch-tasks.json` now carries top-level `runId`, `startedAt`, `finishedAt`, and `heartbeatAt`.
- Fetch report runtime may include `runtime.lifecycle.owner`, `runtime.lifecycle.ownerPid`, and `runtime.lifecycle.heartbeatAt`.
- Any new task-lifecycle artifact must preserve `runId` end to end instead of relying on timestamps.

### Lifecycle cleanup

- For a clean post-migration debug baseline, use:
  - `python scripts/reset_admin_task_lifecycle.py --data-dir data`
- This command resets only current lifecycle/debug artifacts and keeps `admin-run-history.json` in the current runId-only shape.

---

## 9. Fetch report diagnostic breakdowns

`summary.needsReviewBreakdown` is the shaped zero-kept static diagnostic view. It does not promise to equal every raw `needs_review` marker in `sources`.

`summary.okCleanSources` and `summary.okWithWarningSources` are additive counters over source-report rows whose `status` remains `ok`. They distinguish clean successful sources from successful sources carrying warning/error diagnostic text without changing source status semantics.

Source rows may include `loss.canonicalDropReasons` for rows rejected before canonical output. The stable reasons include structural drops (`missing_title`, `missing_company`, `missing_job_link`, `invalid_url`, `invalid_payload`) and sanitizer drops (`non_job_static_page`, `google_sheets_category_row`, `sector_gate_filtered`). The `sector_gate_filtered` reason is additive when the `BALUFFO_STRICT_GAME_ONLY=1` environment variable is set; it records rows dropped by the opt-in sector-gate output filter. All drop-reason diagnostics are additive report visibility only and do not add fields to `jobs-unified.json`, `jobs-unified-light.json`, or `jobs-unified.csv`.

Google Sheets source detail stats may also include additive title-hydration diagnostics: `title_hydration_candidates`, `title_hydration_feed_fetches`, `title_hydration_cache_hits`, `title_hydration_repaired`, `title_hydration_missed`, `title_hydration_errors`, and `title_hydration_ms`. These describe provider-feed title repair attempts and do not add canonical job output fields.

`summary.sizeGuardrails` is an additive output-size diagnostic. It does not change the output file contract: `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` are still written with the same row fields. Unified JSON files are compact serialized; report/debug JSON remains pretty-printed.

| Field | Type | Description |
|---|---|---|
| `json.bytes` | `number` | Current `jobs-unified.json` byte size. |
| `json.limitBytes` | `number` | Full JSON warning limit, currently `80_000_000`. |
| `json.exceeded` | `boolean` | True when the full JSON byte size is over its limit. |
| `lightJson.bytes` | `number` | Current `jobs-unified-light.json` byte size. |
| `lightJson.limitBytes` | `number` | Light JSON warning limit, currently `60_000_000`. |
| `lightJson.exceeded` | `boolean` | True when the light JSON byte size is over its limit. |
| `csv.bytes` | `number` | Current `jobs-unified.csv` byte size. |
| `csv.limitBytes` | `number` | CSV warning limit, currently `50_000_000`. |
| `csv.exceeded` | `boolean` | True when the CSV byte size is over its limit. |

`summary.sizeGuardrailExceeded` remains the aggregate compatibility flag and is true when any `summary.sizeGuardrails.*.exceeded` value is true.

`summary.coverageScope` and `runtime.coverageScope` are additive fetch-report metadata. The first-run bootstrap route writes `"bootstrap_sheets"` to both fields for sheet-limited output; normal full fetch/pipeline reports should omit the field or replace it with their own full-coverage scope. Bootstrap-scoped reports must not be used as full-fetch output-drop or reliability baselines.

### Source health triage

Fetch reports and normalized bridge fetch-report payloads may include top-level `sourceHealth`.
This additive field is derived from existing `sources` rows and is for Admin/Ops visibility only;
it must not be copied into source registry rows or used to auto-promote, demote, hide,
tombstone, suppress, reject, or delete sources.

| Field | Type | Description |
|---|---|---|
| `totalSources` | `number` | Count of meaningful source report rows. |
| `okSources` | `number` | Rows with `status="ok"`. |
| `failedSources` | `number` | Rows with `status="error"`. |
| `excludedSources` | `number` | Rows with `status="excluded"`. |
| `skippedSources` | `number` | Excluded rows with an operational `exclusionReason`. |
| `dynamicRedundantStaticSources` | `number` | Static rows skipped at runtime because a linked provider has repeated validated coverage. |
| `zeroKeptSources` | `number` | Non-excluded rows with `keptCount=0`. |
| `zeroKeptNeedsReviewSources` | `number` | Zero-kept rows not classified as a known legitimate empty/no-openings result. |
| `browserFallbackRecommendedSources` | `number` | Rows with `browserFallbackRecommended=true`. |
| `sourcesNeedingAttention` | `array` | Compact top rows with failure, browser-fallback, or zero-kept review signals. |
| `zeroKeptNeedsReview` | `array` | Compact zero-kept rows that need operator review. |
| `browserFallbackRecommended` | `array` | Compact rows recommended for browser fallback. |
| `dynamicRedundantStatic` | `array` | Compact runtime-only static suppression rows, including the provider that covered the static source. |
| `slowestSources` | `array` | Compact source rows sorted by `durationMs` descending. |
| `topProductiveSources` | `array` | Compact source rows sorted by `keptCount` descending. |
| `topFailureBuckets` | `array` | `{key,count,examples}` rows derived from source `failureBucket`. |
| `topClassifications` | `array` | `{key,count,examples}` rows derived from source `classification`. |

Compact source-health rows use only existing source-report fields: `name`, `adapter`, `status`,
`keptCount`, `fetchedCount`, `durationMs`, `failureBucket`, `classification`,
`zeroKeptClassification`, `browserFallbackRecommended`, `error`, and `exclusionReason`.
Runtime-only dynamic redundant-static rows may also include additive diagnostics:
`coveredByProviderSourceId`, `coveredByProviderAdapter`, `providerCoverageStatus`,
`providerCoverageConsecutiveSuccesses`, `providerCoverageLatestKeptCount`, and
`migrationSourceIdentity`. These diagnostics are not registry fields.

`failureBucket="no_openings"` and `zeroKeptClassification="legit_empty"` require
explicit empty-state evidence, such as `emptyConfirmed`, `classification="empty_confirmed"`,
or an explicit no-openings extractor hint. A source with fetched/raw rows that all drop
during canonicalization, for example because every row is missing `jobLink`, remains
`needs_review` instead of being treated as a legitimate empty/no-openings source.

### Provider coverage validation

Fetch reports, normalized bridge payloads, `/ops/health` KPI payloads, and fetcher metrics may
include top-level `providerCoverage`. This additive field is derived from existing source-state
rows for staged/provider-migration provider sources. It is read-only diagnostic evidence for a
later redundant-static decision; it must not mutate static sources, `REDUNDANT_STATIC_IF_PROVIDER`,
source registry rows, tombstones, sync state, saved jobs, or local user data.

Provider coverage fields may also appear additively on source-state rows for provider-family rows
that include provider-migration metadata such as `migrationSourceIdentity`. Non-provider families,
including static, `scrapy_static`, community, social, and generic rows, must not update provider
coverage fields.

| Field | Type | Description |
|---|---|---|
| `providerCoverageStatus` | `string` | One of `untested`, `probing`, `validated_provider`, `unstable_provider`, `failed_provider`, or `needs_review`. |
| `providerCoverageFirstSuccessAt` | `string` | First provider-fetch success timestamp for this staged provider source. |
| `providerCoverageLastSuccessAt` | `string` | Latest provider-fetch success timestamp for this staged provider source. |
| `providerCoverageSuccessCount` | `number` | Total provider-fetch successes with `keptCount > 0`. |
| `providerCoverageConsecutiveSuccesses` | `number` | Consecutive provider-fetch successes with `keptCount > 0`. |
| `providerCoverageConsecutiveFailures` | `number` | Consecutive provider-fetch failures. Excluded/cache-skip rows preserve this value. |
| `providerCoverageLatestKeptCount` | `number` | Latest provider-fetch kept count from a real provider attempt. |
| `providerCoverageLatestError` | `string` | Latest provider-fetch error for failed/unstable provider rows. |
| `providerCoverageSourceBundleOverlapCount` | `number` | Optional diagnostic count of output rows whose source bundle references this provider source. |
| `providerReplacementReadiness` | `string` | Diagnostic only: `none`, `candidate`, or `ready_later`. `ready_later` means repeated provider success may be reviewed in a future explicit redundant-static slice; it does not mutate static sources. |
| `migrationSourceIdentity` | `string` | Static/generic source identity that produced the provider migration evidence. |
| `migrationSourceName` | `string` | Optional static/generic source display name linked by provider staging or explicit Admin migration identity backfill. |
| `migrationConfidence` | `number` | Optional confidence score from explicit Admin migration identity backfill evidence. |
| `migrationReasons` | `Array<string>` | Optional evidence tokens recorded by explicit Admin migration identity backfill. |
| `migrationLinkedAt` | `string` | Timestamp for explicit Admin migration identity backfill. |
| `migrationLinkedBy` | `string` | Actor that wrote the migration identity link. Admin backfill uses `admin_provider_link_backfill`; clear is allowed only for links owned by that actor. |
| `migrationLinkSource` | `string` | Optional source of the Admin backfill recommendation, such as `provider_coverage_link_backfill`. |

`providerCoverage` summary payload:

| Field | Type | Description |
|---|---|---|
| `totalProviderCandidates` | `number` | Count of provider-migration source-state rows with coverage status. |
| `statusCounts` | `object` | Counts by `providerCoverageStatus`. |
| `probingProviders` | `Array<Object>` | Compact staged/provider rows that are untested or probing. |
| `validatedProviders` | `Array<Object>` | Compact staged/provider rows validated by provider fetch evidence. |
| `unstableOrFailedProviders` | `Array<Object>` | Compact staged/provider rows with provider failures. |
| `needsReviewProviders` | `Array<Object>` | Compact provider rows that fetched successfully but kept zero jobs. |
| `readyLaterProviders` | `Array<Object>` | Compact rows with diagnostic `providerReplacementReadiness="ready_later"`. No source mutation is implied. |

### Provider coverage gaps

The source-policy soak report may include `sections.providerCoverageGaps`. This section is
read-only advisory evidence derived from discovery candidates, active/pending/rejected registry
rows, fetch report source rows, and source-state provider coverage. It must not mutate registry
rows, source sync, loader selection, provider validation, dynamic static suppression, source-policy
review state, or `REDUNDANT_STATIC_IF_PROVIDER`.

Section payload:

| Field | Type | Description |
|---|---|---|
| `bucketCounts` | `object` | Counts by provider coverage gap bucket. |
| `totalGapCount` | `number` | Total examples counted across gap buckets. |
| `unsupportedProviderDetected` | `object` | Count and capped examples for unsupported ATS families such as Oracle HCM. |
| `providerDetectedNeedsProbe` | `object` | Count and capped examples for provider-shaped rows that still need probing. |
| `stagedProviderNotFetched` | `object` | Count and capped examples for pending provider migration candidates with no fetch evidence. |
| `fetchedButNotValidated` | `object` | Count and capped examples for fetched provider migration candidates that are not `validated_provider`. |
| `validatedProviderMissingMigrationSourceIdentity` | `object` | Count and capped examples for successful per-provider fetches that cannot link back to a static/generic source; aggregate provider loader rows without a per-provider identity are excluded. |
| `staticStillActiveDespiteValidatedProvider` | `object` | Count and capped examples for active static rows linked to validated providers without current suppression evidence. |

Bucket objects include `count` and `examples`. Example rows are compact diagnostics and may include
source/provider identities and names, blocker reason, detected provider family/url/id, current
adapter, registry bucket/state, latest fetch status, kept count, provider coverage status,
consecutive success count, and `migrationSourceIdentity`.

### Provider coverage next action

The source-policy soak report may include `sections.providerCoverageNextAction`. This section is a
single read-only triage recommendation for AI/operator workflows. It is derived from
`providerMigrationActivation`, `providerCoverageGaps`, and `providerCoverageLinkBackfill`; it must
not mutate discovery artifacts, registry rows, source sync, Admin review state, migration identity
links, loader selection, or runtime fetch behavior.

Section payload:

| Field | Type | Description |
|---|---|---|
| `action` | `string` | One of `refresh_discovery_staging_evidence`, `fetch_staged_provider_candidates`, `debug_provider_validation`, `review_one_migration_link`, `resolve_link_ambiguity`, `plan_unsupported_provider_family`, or `none`. |
| `priority` | `number` | Numeric priority where `1` is the highest triage action and `0` means no action is recommended. |
| `rationale` | `string` | Concise explanation for why this action was selected from the current evidence. |
| `evidenceCounts` | `object` | Compact counts that explain the decision, including staging source/counts, pending/fetched/validated counts, review/blocked link counts, and provider coverage gap counts. |
| `safeLocalCommands` | `Array<string>` | Optional local evidence-refresh commands the AI/operator may run against ignored runtime artifacts. These commands do not imply committed artifact changes. For `refresh_discovery_staging_evidence`, the preferred command is the focused `scripts/provider_migration_staging_refresh.py --apply-pending` path, which updates only provider-migration discovery diagnostics/candidates/pending rows; full discovery remains a fallback when candidate artifacts are missing or malformed. |
| `requiresHumanApproval` | `boolean` | `true` only when the next step would require an explicit Admin or registry mutation, such as applying one migration identity link. |
| `blockedBy` | `Array<string>` | Machine-readable blockers or approval requirements that explain why the action cannot proceed as an automatic mutation. |

When `action="fetch_staged_provider_candidates"`, `evidenceCounts` may include
`pendingProviderMigrationAdapters` and `pendingProviderMigrationSourceLoaders`. The recommended
fetch command must use `--include-pending-provider-migration`; that flag is validation-only and
makes pending provider migration rows fetchable in memory without approving, promoting, syncing,
hiding, rejecting, suppressing, or applying migration links.

When `action="debug_provider_validation"`, `evidenceCounts.providerValidationDiagnostics` may break
pending provider migration rows into `zeroKeptFetched`, `fetchError`, `notFetched`,
`missingDetailEvidence`, and `validated`. The safe command should stay targeted to the pending
provider loaders and may add `--force-refresh-all`; it remains validation-only and does not mutate
registry/Admin state.

`review_one_migration_link` may only be selected when at least one
`providerCoverageLinkBackfill.reviewCandidates[]` row has `apiEligible=true`. Only actionable
blocked link candidates may select `resolve_link_ambiguity`; provider-shaped self-link diagnostics
must not drive the next action by themselves.

### Provider coverage link backfill review surface

Fetch reports, normalized bridge payloads, `/ops/health` KPI payloads, and fetcher metrics may
include top-level `providerCoverageLinkBackfill`. This section is report-only and derived from
current provider coverage, discovery candidates, source-state evidence, and current soak data. It
does not mutate source registry rows, source sync, or migration identity links.

The section may expose both actionable and blocked review queues:

| Field | Type | Description |
|---|---|---|
| `candidateLinkCount` | `number` | Total provider/static candidate links found for the backfill review surface. |
| `reviewCandidates` | `Array<Object>` | Review-surface rows. Only rows with `apiEligible=true` expose apply actions in Admin. |
| `blockedCount` | `number` | Count of candidate links that are not yet reviewable. |
| `blockedReasonCounts` | `object` | Aggregate blocker counts across blocked candidates. |
| `disambiguationBlockerCounts` | `object` | Aggregate lower-level blocker counts across blocked candidates, grouped by `disambiguationBlockers`. |
| `blockedCandidates` | `Array<Object>` | Read-only blocked candidate rows with blocker reasons, disambiguation blockers, and evidence. |
| `actionableBlockedCount` | `number` | Count of blocked candidates that still need operator or report-disambiguation work. |
| `nonActionableBlockedCount` | `number` | Count of blocked candidates retained only as diagnostics. |
| `actionableBlockedCandidates` | `Array<Object>` | Blocked candidates allowed to drive `resolve_link_ambiguity`. |
| `nonActionableBlockedCandidates` | `Array<Object>` | Blocked candidates that must not drive the next action by themselves. |
| `actionableBlockedReasonCounts` | `object` | Aggregate actionability reasons for actionable blocked candidates. |
| `nonActionableBlockedReasonCounts` | `object` | Aggregate actionability reasons for diagnostic blocked candidates. |
| `blockedExamples` | `Array<Object>` | Capped stable sample of blocked candidates for read-only rendering. |
| `disambiguationBlockedExamples` | `Array<Object>` | Capped stable sample of blocked candidates used to explain the lower-level disambiguation blocker split. |
| `linkedCandidates` | `Array<Object>` | Already-linked provider/static rows shown for read-only visibility. |

`candidateLinkCount` may be non-zero even when `reviewCandidates` is empty. In that case, the
blocked-candidate surface explains why the review queue is empty. High-level `blockedReasonCounts`
capture the queue blocker, while `disambiguationBlockerCounts` explain the lower-level evidence gap
for each blocked candidate.

Provider-to-provider or provider-shaped self-links are blocked diagnostics, not actionable review
rows. They may appear under `blockedCandidates` and `nonActionableBlockedCandidates` with blockers
such as `provider_shaped_self_link` or `provider_shaped_static_identity` and `apiEligible=false`,
but they must not appear in `reviewCandidates` as applyable rows or drive `resolve_link_ambiguity`
by themselves.

Blocked candidate rows may include source-state evidence fields such as `lastStatus`,
`lastKeptCount`, `lastSuccessfulAt`, `lastFetchedAt`, `providerCoverageStatus`,
`providerCoverageConsecutiveSuccesses`, `providerCoverageLatestKeptCount`,
`providerReplacementReadiness`, `evidenceScore`, and `evidenceReasons`. The lower-level
`disambiguationBlockers` taxonomy keeps the distinction between missing source-state history
(`no_source_state_history`), unhealthy source-state (`source_state_not_ok`), insufficient provider
success history (`insufficient_provider_success_history`), multiple static candidates with equal
history (`multiple_static_candidates_with_equal_history`), and static-only evidence
(`static_only_evidence_present`).

Provider/static rows only graduate into `reviewCandidates` when the source-state status is
`validated_provider` and the consecutive-success count reaches the review threshold. Rows that
have source-state history but are not yet validated stay in the blocked surface.

Dynamic redundant-static suppression is a reversible runtime skip layered on top of provider
coverage. One successful provider fetch validates provider usability. Two or more consecutive
successful provider fetches may cause the matching static loader to emit an excluded source report
with `exclusionReason="dynamic_redundant_provider"` during normal default fetches. Explicit source
selection bypasses this skip. This must not mutate source registry rows, tombstones, sync state,
saved/local data, or `REDUNDANT_STATIC_IF_PROVIDER`.

Fetch reports, normalized bridge payloads, `/ops/health` KPI payloads, and fetcher metrics may
include top-level `staticSuppressionPolicy`. This runtime-only summary records eligible
provider/static pairs and whether the current run suppressed, warning-suppressed, or paused each
pair. Current loader-selection decisions use only the latest prior `jobs-fetch-report.json`
`staticSuppressionPolicy`, falling back to prior `providerStaticOverlap`; current-run evidence is
written for the next run. Missing prior evidence does not block suppression. Prior
`insufficient_history` suppresses with a warning. Prior `needs_review`, `provider_unstable`,
`staticOnlyCount > 0`, or `auditReasons` containing `static_only_jobs_detected` pauses suppression
and lets the static source run normally.

| Field | Type | Description |
|---|---|---|
| `eligibleCount` | `number` | Provider/static pairs meeting base runtime suppression eligibility. |
| `suppressedCount` | `number` | Eligible pairs skipped with `exclusionReason="dynamic_redundant_provider"`. |
| `pausedCount` | `number` | Eligible pairs allowed to run because prior audit evidence may be unsafe. |
| `warningCount` | `number` | Eligible pairs skipped while recording non-blocking warning evidence. |
| `suppressedPairs` | `Array<Object>` | Compact suppressed pair rows. |
| `pausedPairs` | `Array<Object>` | Compact paused pair rows. |
| `warningPairs` | `Array<Object>` | Compact warning-suppressed pair rows. |

Pair rows include `staticSourceId`, `staticSourceName`, `providerSourceId`,
`providerSourceName`, `decision`, `reason`, `lastAuditStatus`, `auditReasons`,
`staticOnlyCount`, `overlapCount`, `providerCoverageStatus`,
`providerCoverageConsecutiveSuccesses`, and `providerCoverageLatestKeptCount`.

### Provider/static overlap audit

Fetch reports, normalized bridge payloads, `/ops/health` KPI payloads, and fetcher metrics may
include top-level `providerStaticOverlap`. This additive field is read-only diagnostics derived
from dynamic redundant-static source rows, provider coverage state, prior static source-state
counts, and current output `sourceBundle` evidence when available. The audit must not force-run
suppressed static sources or mutate static registry rows. The latest prior audit is advisory
evidence for the next run's runtime suppression policy; it never creates permanent redundancy
rules and never deletes, hides, rejects, demotes, or tombstones static sources.

| Field | Type | Description |
|---|---|---|
| `suppressedStaticCount` | `number` | Static source rows skipped at runtime with `exclusionReason="dynamic_redundant_provider"`. |
| `auditedPairCount` | `number` | Count of provider/static pairs included in the audit. |
| `safePairCount` | `number` | Pairs with repeated validated provider coverage and no static-only contradiction. |
| `needsReviewPairCount` | `number` | Pairs with provider instability, static-only evidence, or malformed/incomplete evidence. |
| `insufficientHistoryPairCount` | `number` | Pairs lacking prior static kept-count or overlap evidence. |
| `staticOnlyJobCount` | `number` | Current output rows whose `sourceBundle` references the static source but not the covering provider. |
| `providerOnlyJobCount` | `number` | Current output rows whose `sourceBundle` references the covering provider but not the static source. |
| `overlapJobCount` | `number` | Current output rows whose `sourceBundle` references both static and provider sources. |
| `pairs` | `Array<Object>` | Compact pair rows with static/provider identities, coverage fields, counts, `auditStatus`, and `auditReasons`. |

`auditStatus` values are `safe`, `needs_review`, `insufficient_history`, `provider_unstable`,
and `not_audited`. `safe` means only that the current reversible suppression appears supported by
available evidence. Static cleanup, deletion, hiding, or permanent redundancy rules remain a later
explicit evidence-backed milestone.

### Redundant static proposals

Fetch reports, normalized bridge payloads, `/ops/health` KPI payloads, and fetcher metrics may
include top-level `redundantStaticProposals`. This additive field is read-only advisory diagnostics
derived only from existing `staticSuppressionPolicy`, `providerStaticOverlap`, and
`providerCoverage` evidence. It is not a loader-selection input, does not change runtime
suppression eligibility, does not create source report rows, and must not mutate source registry
rows, tombstones, sync state, saved/local data, or `REDUNDANT_STATIC_IF_PROVIDER`.

Proposal candidates are only evaluated provider/static pairs already present in the current
suppression policy or overlap audit. Unlinked static registry rows with no evaluated provider/static
pair produce no proposal. `keep_static` means a provider/static pair was evaluated and the evidence
does not support treating the static source as redundant; it is not a broad static registry scan.

`redundantStaticProposals` summary payload:

| Field | Type | Description |
|---|---|---|
| `totalProposalCount` | `number` | Count of evidence-backed proposal rows. |
| `safeRedundantCount` | `number` | Rows proposing `safe_redundant_static`. |
| `keepStaticCount` | `number` | Rows proposing `keep_static`. |
| `needsMoreHistoryCount` | `number` | Rows proposing `needs_more_history`. |
| `needsReviewCount` | `number` | Rows proposing `needs_review`. |
| `providerUnstableCount` | `number` | Rows proposing `provider_unstable`. |
| `staticOnlyDetectedCount` | `number` | Rows proposing `static_only_jobs_detected`. |
| `proposals` | `Array<Object>` | Compact advisory proposal rows. |

Proposal rows include `staticSourceId`, `staticSourceName`, `providerSourceId`,
`providerSourceName`, `proposal`, `confidence`, `reasons`, `recommendedAction`,
`destructiveActionAllowed`, `lastAuditStatus`, `providerCoverageStatus`,
`providerCoverageConsecutiveSuccesses`, `providerCoverageLatestKeptCount`, `staticOnlyCount`, and
`overlapCount`. `confidence` is a number from `0.0` to `1.0`.
`destructiveActionAllowed` is always `false`.

`proposal` values are `safe_redundant_static`, `keep_static`, `needs_more_history`,
`needs_review`, `provider_unstable`, and `static_only_jobs_detected`. `recommendedAction` values are
`keep_runtime_suppression`, `keep_static_active`, `collect_more_history`, `review_pair`, and
`pause_suppression`. These are diagnostic recommendations only. `safe_redundant_static` does not
mean delete, hide, reject, demote, tombstone, or permanently suppress a source; static cleanup and
permanent rules remain later explicit evidence-backed milestones.

### Conservative static cleanup proposals

The source-policy soak report may include
`sections.conservativeStaticCleanupProposals`. This section is report-only and is derived from
`source-policy-recommendations.json`, current suppression/overlap evidence, source-sync
cleanliness, and effective runtime registry rows. It does not mutate registry rows, seed defaults,
tombstones, rejected rows, source sync, or `REDUNDANT_STATIC_IF_PROVIDER`.

Cleanup proposal rows use `recommendedAction="move_static_to_hidden_pending"`,
`destructiveActionAllowed=false`, and `requiresExplicitAdminAction=true`. They are only proposals
for a later explicit Admin action; no current report may hide, demote, reject, tombstone, delete, or
permanently suppress a source.

Rows include the provider/static identity, clean-run counters, static-only evidence counters,
source-sync cleanliness, suppression evidence status, evidence reasons, and blockers. A pair is
proposal-eligible only when repeated stable-safe evidence exists, source-sync is clean, static-only
evidence is absent, the static source is currently active/static, and dynamic suppression has been
observed or its absence is explained by suppression-eligibility diagnostics.

The section may also include additive summary fields:

- `proposalGeneratedAt`: ISO timestamp for the proposal artifact used for the report
- `proposalReportRunId`: fetch run id used to anchor the proposal evidence
- `proposalFreshnessStatus`: `fresh` or `stale` depending on the proposal artifact age
- `proposalFreshnessAgeSeconds`: age of the proposal artifact at report time
- `proposalStaleThresholdSeconds`: configured staleness threshold for proposal evidence
- `proposalReadinessHash`: stable hash over the proposal freshness/readiness inputs
- `staleCount`: number of proposal rows that are stale rather than actionable
- `blockedReasonCounts`: aggregate blocker counts across blocked candidates
- `proposalReadyExamples`: capped stable sample of proposal-ready rows
- `blockedExamples`: capped stable sample of blocked rows

Each row may include `proposalDisposition="proposal_ready"` or `proposalDisposition="blocked"` to
simplify read-only rendering. Rows also carry additive readiness evidence:

- `proposalReadiness`: `actionable`, `stale`, or `blocked`
- `proposalReadinessReason`: short explanation for the readiness state
- `proposalReadinessEvidence`: compact evidence tokens for auditing and rendering

`proposalCount=0` is not failure by itself; blocked candidates, stale proposal evidence, and their
blocker counts are the evidence to inspect before any later reversible cleanup action is
considered. Proposal freshness only explains whether a row is still actionable. It does not
authorize the later reversible cleanup action on its own.

### Static registry scope conflicts

The source-policy soak report may include `sections.staticRegistryScopeConflicts`. This section is
report-only and scans effective active static registry rows for pages whose host differs from the
row's listing host. It cross-checks those off-listing hosts against other active source/provider
coverage and may include kept-output host evidence from `jobs-unified.json` when present.

Rows are diagnostics and proposals only. They must not mutate seed defaults, runtime registries,
tombstones, rejected rows, source sync, suppression rules, timeout settings, fetch behavior, or
`REDUNDANT_STATIC_IF_PROVIDER`.

Section payload:

| Field | Type | Description |
|---|---|---|
| `summary` | `object` | Aggregate counts for scanned static rows and conflict classifications. |
| `conflicts` | `Array<Object>` | Static scope conflict proposal rows. |
| `patchProposals` | `Array<Object>` | Dry-run patch proposal rows for review-only safe-looking conflicts. |
| `examples` | `Array<Object>` | Capped stable sample for Markdown/operator review. |

Conflict rows include `sourceId`, `sourceName`, `adapter`, `listingHost`,
`offListingHosts`, `offListingHostPages`, `coveredOffListingHosts`,
`uncoveredOffListingHosts`, `coverageRows`, `keptOutputHostBreakdown`, `classification`,
`recommendedAction`, `reasons`, `destructiveActionAllowed`, `requiresExplicitAdminAction`, and
`behaviorChangeAllowed`.

`classification` values are `shadowed_cross_host`, `needs_split_source`,
`manual_scope_review`, and `zero_kept_review`. Recommended actions are
`narrow_static_scope_after_review`, `create_or_link_source_after_review`, and
`review_scope_manually`. `destructiveActionAllowed` and `behaviorChangeAllowed` are always
`false`; `requiresExplicitAdminAction` is always `true`.

`patchProposals` may include rows only for `shadowed_cross_host` conflicts whose off-listing hosts
are all covered by other active registry rows. Patch proposals include `sourceId`, `sourceName`,
`proposedAction="narrow_static_scope"`, `removePages`, `keepPages`, `preserveFields`,
`applyAllowed`, `requiresExplicitAdminAction`, `destructiveActionAllowed`, and
`behaviorChangeAllowed`. `applyAllowed`, `destructiveActionAllowed`, and `behaviorChangeAllowed`
are always `false`; proposal rows are dry-run review evidence only.

The soak report CLI may expose an explicit `--apply-static-scope-proposal SOURCE_ID` maintenance
mode. That mode is not part of normal report generation: it requires an exact source id, only
accepts a matching dry-run `shadowed_cross_host` proposal, updates only the local runtime active
registry row's `pages`, preserves source identity fields, and writes
`static-scope-apply-audit.json` under the requested output directory. It must not apply
`zero_kept_review`, `needs_split_source`, or `manual_scope_review` rows and must not edit seed
defaults, source sync, tombstones, rejected rows, timeout settings, or fetch behavior.

### Source-policy recommendation artifact

Completed fetch runs may update `data/source-policy-recommendations.json`, or the same filename
under the active pipeline output directory. This generated local artifact accumulates bounded
history from completed-run `redundantStaticProposals` so provider/static evidence can be reviewed
over time. It is not an input to loader selection, runtime suppression, registry sync, source
cleanup, or any Admin action, and it must not mutate source registries, tombstones, sync state,
saved/local data, static sources, or `REDUNDANT_STATIC_IF_PROVIDER`.

Artifact payload:

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `string` | Artifact contract version. |
| `updatedAt` | `string` | Latest completed fetch timestamp used to update the artifact. |
| `summary` | `object` | Aggregate counts for current recommendation rows. |
| `pairs` | `Array<Object>` | Evidence-backed provider/static recommendation rows. |

Summary fields are `totalPairs`, `stableSafeCount`, `needsReviewCount`,
`staticOnlyDetectedCount`, `unstableProviderCount`, and `moreHistoryCount`.

Pair rows include `staticSourceId`, `staticSourceName`, `providerSourceId`,
`providerSourceName`, `currentRecommendation`, `currentRecommendedAction`, `confidence`,
`firstSeenAt`, `lastSeenAt`, `safeRunCount`, `consecutiveSafeRunCount`,
`needsReviewRunCount`, `staticOnlyDetectedRunCount`, `providerUnstableRunCount`,
`needsMoreHistoryRunCount`, `lastProposal`, `lastAuditStatus`, `destructiveActionAllowed`, and
`history`. `history` retains at most the latest 10 observations per pair.
`destructiveActionAllowed` is always `false`.

History rows include `observedAt`, `proposal`, `recommendedAction`, `confidence`,
`lastAuditStatus`, `providerCoverageStatus`, `providerCoverageConsecutiveSuccesses`,
`providerCoverageLatestKeptCount`, `staticOnlyCount`, `overlapCount`, and `reasons`.

`currentRecommendation` values are `stable_safe_redundant`, `needs_review`,
`static_only_detected`, `needs_more_history`, and `keep_static`. `stable_safe_redundant` means the
artifact has observed repeated safe runtime-suppression evidence; it does not mean delete, hide,
reject, demote, tombstone, permanently suppress, or otherwise modify a source. Permanent
source-policy actions remain later explicit evidence-backed milestones.

Fetch reports may include top-level `sourcePolicyRecommendationExport` with `status`,
`artifactPath`, `reviewStatePath`, `updatedPairCount`, `reviewStatePairCount`,
`manualForcePausedCount`, and optional `warning` / `reviewStateWarning`, plus
`outputs.sourcePolicyRecommendations` and `outputs.sourcePolicyReviewState`. Corrupt, malformed, or
missing prior artifacts must not fail the fetch; they are treated as empty prior evidence and
surfaced through the export diagnostic when useful.

### Source-policy review state artifact

Admin may update `data/source-policy-review-state.json`, or the same filename under the active
pipeline output directory, to record local review state for source-policy recommendations. This
artifact is local, reversible, and non-destructive. It must not delete, hide, reject, demote,
tombstone, or mutate source rows; it must not mutate registry sync state or
`REDUNDANT_STATIC_IF_PROVIDER`; and it must not create permanent redundancy rules.
It may be preserved by explicit desktop local-data backup/import, but remains local-only for
source sync. No remote machine should inherit `force_pause` or other review state through
`source-sync.json`.

Artifact payload:

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `string` | Review-state contract version. |
| `updatedAt` | `string` | Last Admin action timestamp. |
| `summary` | `object` | Counts for local review and override state. |
| `pairs` | `object` | Map keyed by normalized static/provider identity. |

Pair rows include `staticSourceId`, `staticSourceName`, `providerSourceId`,
`providerSourceName`, `reviewState`, `manualSuppressionOverride`, `snoozedUntil`, `notes`,
`updatedAt`, and `updatedBy`. `reviewState` values are `new`, `acknowledged`, `reviewed`, and
`snoozed`. `manualSuppressionOverride` values are `none` and `force_pause`; there is no
`force_suppress` or allow-suppression override.

`acknowledged`, `reviewed`, and `snoozed` affect Admin/Ops visibility only. `snoozedUntil` is
local UI/review metadata and does not change loader selection. `force_pause` is the only runtime
override: during default fetches it conservatively pauses dynamic static suppression for the
matching provider/static pair so the static source runs normally. Clearing the override returns the
pair to normal `staticSuppressionPolicy` behavior.

### Dedup review state artifact

Admin may update `data/dedup-review-state.json`, or the same filename under the active pipeline
output directory, to record local review decisions for provider/static disagreement rows surfaced
through `dedupEvidence`. This artifact is local, reversible, and read-only with respect to dedup
behavior: it does not rewrite `jobs-unified.json`, does not change dedup merge rules, does not
add lifecycle labels, and does not mutate registries, sync state, tombstones, or
`REDUNDANT_STATIC_IF_PROVIDER`.

Artifact payload:

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `string` | Review-state contract version. |
| `updatedAt` | `string` | Last Admin action timestamp. |
| `summary` | `object` | Counts for local disagreement review state. |
| `pairs` | `object` | Map keyed by normalized disagreement identity. |

Pair rows include `reviewKey`, `title`, `company`, `dedupKey`, `bundleEvidenceOrigin`,
`disagreementClassification`, `providerSourceJobIds`, `staticSourceJobIds`, `providerSources`,
`staticSources`, `providerUrls`, `staticUrls`, `sharedIdentifierTokens`, `distinctLocationCount`,
`sampleLocations`, `identityQuality`, `carriedLocationPollutionAudit`, `reviewStatus`,
`reviewedAt`, `reviewedBy`, and `reviewNote`.

`reviewStatus` values are:

- `reviewed_safe`: downgrade that exact disagreement row from gate blocker to warning
- `confirmed_blocking`: keep that exact disagreement row blocking while recording operator review

Clearing a review removes the pair from the local artifact and restores default gate behavior.
Ops health and Admin fetch/report surfaces expose a read-only dedup review-state summary block
with the artifact path, any missing/malformed read warning, the total reviewed pair count, the
`reviewed_safe` count, the `confirmed_blocking` count, and the remaining unresolved blocking
count derived from the merged review evidence. This is operator-visible evidence, not a hidden
implementation detail.
Read-only gate fields such as `disagreementGateDisposition`, `disagreementGateEvidence`,
`providerStaticDisagreementGateCounts`, and `dedupAuditGate` may reflect this local review state in
saved fetch reports and bridge read-time payloads.

### Job lifecycle summary

Fetch reports and normalized bridge fetch-report payloads may include top-level `lifecycleSummary`.
This additive field is derived during final jobs lifecycle application. It explains why missing
previously-seen jobs were or were not transitioned, and must not be used to mutate saved jobs,
local user data, source registry rows, tombstones, sync state, or source-family configuration.

| Field | Type | Description |
|---|---|---|
| `activeCount` | `number` | Tracked lifecycle rows currently active after the run. |
| `newCount` | `number` | Jobs first seen in this run. |
| `reappearedCount` | `number` | Previously removed or archived jobs seen again and restored to active. |
| `likelyRemovedCount` | `number` | Tracked lifecycle rows currently marked `likely_removed`. |
| `archivedCount` | `number` | Tracked lifecycle rows currently archived. |
| `preservedBecauseSourceFailedCount` | `number` | Missing jobs preserved because their source failed or timed out. |
| `preservedBecauseSourceSkippedCount` | `number` | Missing jobs preserved because their source was skipped, excluded, not selected, or needed review/browser fallback. |
| `eligibleMissingSourceCount` | `number` | Source rows with trustworthy missing-job evidence in the run. |
| `ineligibleMissingSourceCount` | `number` | Source rows present but not eligible to mark jobs removed. |

| Field | Type | Description |
|---|---|---|
| `byShape` | `object` | Counts and examples by diagnostic shape. |
| `topByWallTime` | `array` | Slowest included static rows. |
| `topByFrequency` | `array` | Shapes ordered by frequency and duration. |
| `rawMarkerCount` | `number` | Count of source rows where `classification`, `failureBucket`, or `zeroKeptClassification` is `needs_review`. |
| `includedCount` | `number` | Count of rows included in the shaped zero-kept static breakdown. |

---

## 10. Social experiment report contract

The fetch report may include a top-level `socialSummary` block for the M6 social/community pilot.

### Purpose

This block is additive and exists so the jobs fetch report, bridge ops health, and manual review artifact can describe the same measured experiment.

### Definitions

- `official-board origin` means first-party company boards and structured ATS/company-page ingestion only.
- `not official-board origin` means community sheets, aggregators, repost feeds, and social sources.
- `uniqueKeptCount` is measured post-dedup on final canonical output rows.
- `officialBoardOverlapCount` means a canonical job appears in both social and official-board origin paths.

### Fetch report shape

| Field | Type | Description |
|---|---|---|
| `pilotWindowStartAt` | `string` | Start of the measured pilot window. |
| `pilotWindowEndAt` | `string` | End of the measured pilot window. |
| `scheduledRunCount` | `number` | Number of scheduled fetch runs included in the pilot window. |
| `keptCount` | `number` | Total social rows kept in the run window. |
| `uniqueKeptCount` | `number` | Kept social jobs whose final canonical row is unique to social after dedup. |
| `officialBoardOverlapCount` | `number` | Kept social jobs that also appear in official-board ingestion paths. |
| `duplicateCount` | `number` | Kept social rows removed as duplicates. |
| `duplicateRate` | `number` | `duplicateCount / keptCount` for the run window. |
| `lowConfidenceDropped` | `number` | Social rows dropped because confidence was below threshold. |
| `sampleSize` | `number` | Reviewed social sample size, or `0` when no review data exists. |
| `reviewedCount` | `number` | Rows in the review artifact that have a true/false positive judgment. |
| `falsePositiveCount` | `number` | Reviewed rows marked as false positive. |
| `falsePositiveRate` | `number` | `falsePositiveCount / reviewedCount` for the reviewed sample. |
| `reviewArtifactPath` | `string` | Path to `data/social-experiment-review.json` or the run-local equivalent. |
| `channels` | `object` | Per-channel summaries for `reddit` and `mastodon`. |

### Review artifact

- The pipeline writes a deterministic candidate sample to `data/social-experiment-review.json`.
- The sample is stable-sorted by canonical job id or dedup key, then truncated to the first 50 eligible social-kept rows.
- Human review fills in `reviewDecision` and `reviewNotes` for the candidate rows.
- If no review data exists yet, the report should emit `sampleSize = 0`, `reviewedCount = 0`, `falsePositiveCount = 0`, and `falsePositiveRate = 0`.

### Bridge visibility

The bridge ops health payload mirrors a compact `kpis.socialExperiment` view from the fetch report so operators can review the experiment without reading the raw report file.

---

## 11. Fetch regression reconciliation contract

The fetch report may include a top-level `healthSummary` reconciliation pair for the parser-regression lane.

### Fields

| Field | Type | Description |
|---|---|---|
| `siteChangedDiagnosedCount` | `number` | Number of top-level source rows diagnosed as `site_changed`. |
| `parserRegressionQueueCount` | `number` | Number of rows written to `jobs-parser-regression-queue.json`. |

### Queue artifact

- The canonical parser-regression artifact path is exposed at `outputs.parserRegressionQueue`.
- `listingChanged` remains the source/report field; the queue artifact projects it as `listingFingerprintChanged` for review readability.
- For normalized fetch-report rows, static sources diagnosed as `site_changed` preserve `listingUrl`, `pages`, and `sourceId` so the regression lane can recover `oldUrl` even when detail payloads are empty.
- For normalized fetch-report rows, aggregate provider sources diagnosed as `site_changed` preserve `providerUrl` so `greenhouse_boards` and `workable_sources` can still enter the regression lane when no listing URL surface exists.
- Admission to the lane is diagnosis-driven from the top-level source row. URL/status/fingerprint fields are enrichment and ordering data only.
