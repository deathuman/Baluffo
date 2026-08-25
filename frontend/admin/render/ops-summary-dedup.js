/**
 * Dedup gate, audit, review, merge-example, role-bucket, and dedup-lists
 * helpers extracted from ``ops-summary.js``.
 *
 * No coordinator import.  Imports source-policy and provider-static
 * helpers from sibling leaf modules.
 *
 * @module ops-summary-dedup
 */

import { escapeHtml } from "../../shared/ui/index.js?v=6";
import { formatDuration } from "./ops-shared.js";
import { formatDedupSourceClasses } from "./ops-summary-source-policy.js";
import {
  formatProviderStaticDisagreementRows,
  formatProviderStaticTitleCompanyCollisionRows,
  formatProviderStaticDisagreementCounts,
  formatProviderStaticDisagreementGateCounts,
  formatProviderStaticDisagreementClassificationCounts,
  formatProviderStaticTitleCompanyCollisionCounts,
  formatProviderStaticTitleCompanyCollisionAuditCounts,
  visibleProviderStaticRows
} from "./ops-summary-provider-static.js";

// ── dedup count helpers ────────────────────────────────────────────

function countsObject(value) {
  return value && typeof value === "object" ? value : {};
}

function formatNonZeroCounts(entries) {
  const labels = Object.entries(entries)
    .filter(([, value]) => Number(value || 0) > 0)
    .map(([label, value]) => `${label} ${Number(value).toLocaleString()}`);
  return labels.length ? labels.join(", ") : "none";
}

function formatDedupRiskReasonCounts(reasonCounts) {
  const counts = countsObject(reasonCounts);
  return formatNonZeroCounts({
    location: counts.same_title_company_different_location,
    "provider/static": counts.provider_static_duplicate_disagreement,
    "missing provider IDs": counts.missing_provider_ids,
    "weak title/company": counts.weak_title_company_only_evidence
  });
}

function formatDedupOutlierReasonCounts(reasonCounts) {
  const counts = countsObject(reasonCounts);
  return formatNonZeroCounts({
    "multi-location strong": counts.multi_location_strong_identity,
    "location weak": counts.location_divergence_without_strong_identity,
    "provider/static": counts.provider_static_disagreement,
    "large other": counts.large_other_source_bundle,
    sparse: counts.sparse_title_company_bundle,
    unknown: counts.unknown
  });
}

function formatDedupIdentityShapeCounts(shapeCounts) {
  const counts = countsObject(shapeCounts);
  return formatNonZeroCounts({
    "detail URL": counts.shared_job_detail_url,
    "listing/category URL": counts.shared_listing_or_category_url,
    "many URLs": counts.many_unique_urls_same_title,
    "provider ID": counts.provider_id_backed,
    "missing URL/IDs": counts.missing_url_and_ids,
    "mixed/unknown": counts.mixed_or_unknown_identity
  });
}

function formatDedupReviewQueueCounts(queueCounts) {
  const counts = countsObject(queueCounts);
  return formatNonZeroCounts({
    "many URLs": counts.review_many_urls_same_title,
    "listing URL": counts.review_listing_url_bundle,
    "category title": counts.review_category_title_bundle,
    "open application": counts.review_open_application_bundle,
    "provider/static": counts.review_provider_static_disagreement,
    monitor: counts.monitor
  });
}

function formatDedupReviewQueueCauseCounts(causeCounts) {
  const counts = countsObject(causeCounts);
  return formatNonZeroCounts({
    category: counts.category_or_department_bucket,
    "open application": counts.open_application_family,
    "listing page": counts.listing_page_bundle,
    "spreadsheet role": counts.spreadsheet_role_bucket_needs_review,
    "sheets role audit": counts.google_sheets_role_bucket_needs_review,
    "non-provider URL": counts.non_provider_url_identity_needs_review,
    "parser/text": counts.parser_or_directory_text_pollution,
    "provider/static": counts.provider_static_disagreement,
    "likely legitimate": counts.likely_legitimate_multi_role_family,
    unknown: counts.unknown
  });
}

function formatDedupIdentityQualityCounts(qualityCounts) {
  const counts = countsObject(qualityCounts);
  return formatNonZeroCounts({
    "provider ID": counts.provider_id_strong,
    "detail URL": counts.shared_detail_url_strong,
    "listing URL weak": counts.shared_listing_url_weak,
    "same-host URLs weak": counts.many_urls_same_host_weak,
    "many-host URLs weak": counts.many_urls_many_hosts_weak,
    "other source ID": counts.other_source_id_untrusted,
    missing: counts.missing_identity,
    unknown: counts.unknown
  });
}

function formatDedupNonProviderIdentityProvenanceCounts(provenanceCounts) {
  const counts = countsObject(provenanceCounts);
  return formatNonZeroCounts({
    "google sheets": counts.google_sheets_row_identity,
    "URL-derived": counts.url_derived_identity,
    "category/directory": counts.category_or_directory_identity,
    "opaque other": counts.opaque_other_source_identity,
    mixed: counts.mixed_non_provider_identity,
    none: counts.none,
    unknown: counts.unknown
  });
}

function formatDedupGoogleSheetsBundleShapeCounts(shapeCounts) {
  const counts = countsObject(shapeCounts);
  return formatNonZeroCounts({
    "role/category": counts.role_category_bucket,
    "company role family": counts.company_role_family,
    "single-location URLs": counts.single_location_many_urls,
    "multi-location URLs": counts.multi_location_many_urls,
    "row collision": counts.spreadsheet_row_collision,
    "not sheets": counts.not_google_sheets,
    unknown: counts.unknown
  });
}

function formatDedupGoogleSheetsRoleBucketAuditCounts(auditCounts) {
  const counts = countsObject(auditCounts);
  return formatNonZeroCounts({
    "spreadsheet category": counts.likely_spreadsheet_category_bucket,
    "manual role review": counts.role_family_needs_manual_review,
    "detail URLs": counts.job_detail_urls_same_role,
    "listing/search": counts.listing_or_search_url_bucket,
    "parser normalized": counts.parser_normalized_role_title,
    "not sheets role": counts.not_google_sheets_role_bucket,
    unknown: counts.unknown
  });
}

/**
 * @param {GoogleSheetsRoleBucketAuditPayload|null|undefined} audit
 */
function formatDedupGoogleSheetsRoleBucketAuditSummary(audit) {
  const summary = audit && typeof audit === "object" ? audit : {};
  const classificationCounts = summary?.classificationCounts && typeof summary.classificationCounts === "object"
    ? summary.classificationCounts
    : {};
  const countText = formatNonZeroCounts({
    total: summary?.totalRoleBucketCount,
    "current-run": summary?.currentRunRoleBucketCount,
    carried: summary?.carriedHistoricalRoleBucketCount,
    "guard-blocked different URL": summary?.blockedByDifferentPrimaryUrlCount,
    "allowed same URL": summary?.allowedSamePrimaryUrlCount,
    historical: summary?.likelyHistoricalCollisionCount,
    "parser/category": summary?.likelyParserCategoryBucketCount,
    unresolved: summary?.unresolvedRoleBucketCount
  });
  const classificationText = formatNonZeroCounts({
    "fixed by guard": classificationCounts?.fixed_by_generic_role_guard,
    "allowed same URL": classificationCounts?.allowed_same_primary_url,
    "historical carried": classificationCounts?.historical_carried_bundle,
    "unresolved current-run": classificationCounts?.unresolved_current_run_role_bucket,
    "parser/category noise": classificationCounts?.parser_or_sheet_category_noise,
    "needs narrow guard": classificationCounts?.needs_narrow_dedup_guard
  });
  const examples = Array.isArray(summary?.examples) ? summary.examples.slice(0, 10) : [];
  const exampleHtml = examples.length
    ? `
      <table class="admin-dedup-evidence-table">
        <thead><tr><th>Class</th><th>Title</th><th>Company</th><th>Origin</th><th>Evidence</th></tr></thead>
        <tbody>${examples.map(row => {
          const classification = String(row?.classification || "unknown").replaceAll("_", " ");
          const title = String(row?.title || row?.targetTitle || "Untitled");
          const company = String(row?.company || row?.targetCompany || "Unknown company");
          const origin = String(row?.bundleEvidenceOrigin || "unknown").replaceAll("_", " ");
          const evidence = Array.isArray(row?.evidence) ? row.evidence.slice(0, 6).join(", ").replaceAll("_", " ") : "none";
          return `
            <tr>
              <td>${escapeHtml(classification)}</td>
              <td>${escapeHtml(title)}</td>
              <td>${escapeHtml(company)}</td>
              <td>${escapeHtml(origin)}</td>
              <td>${escapeHtml(evidence)}</td>
            </tr>
          `;
        }).join("")}</tbody>
      </table>
    `
    : escapeHtml("No Google Sheets role-bucket audit examples.");
  return `
    <div><strong>Summary</strong>: ${escapeHtml(countText)}</div>
    <div><strong>Classifications</strong>: ${escapeHtml(classificationText)}</div>
    ${exampleHtml}
  `;
}

function formatDedupGoogleSheetsBucketIntentCounts(intentCounts) {
  const counts = countsObject(intentCounts);
  return formatNonZeroCounts({
    "taxonomy bucket": counts.likely_spreadsheet_taxonomy_bucket,
    "possible role family": counts.possible_role_family,
    "weak title/company": counts.weak_title_company_grouping,
    "listing/search": counts.listing_or_search_bucket,
    "parser normalized": counts.parser_normalized_bucket,
    "not sheets": counts.not_google_sheets_bucket,
    unknown: counts.unknown
  });
}

function formatDedupGoogleSheetsWeakGroupingAuditCounts(auditCounts) {
  const counts = countsObject(auditCounts);
  return formatNonZeroCounts({
    "role detail URLs": counts.role_bucket_detail_url_grouping,
    "role listing/search": counts.role_bucket_listing_grouping,
    "single-token title": counts.single_token_title_many_urls,
    "two-token title": counts.two_token_title_many_urls,
    "concrete title": counts.concrete_title_many_urls,
    "parser pollution": counts.parser_pollution_grouping,
    "not weak sheets": counts.not_weak_google_sheets_grouping,
    unknown: counts.unknown
  });
}

// ── dedup table helpers ─────────────────────────────────────────────

/**
 * @param {Array<DedupMergeExampleRow>|null|undefined} rows
 * @param {string} emptyText
 */
function formatDedupMergedRows(rows, emptyText) {
  const mergedRows = Array.isArray(rows) ? rows : [];
  if (!mergedRows.length) return escapeHtml(emptyText);
  const body = mergedRows
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const classes = formatDedupSourceClasses(row?.sourceClasses);
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>${escapeHtml(classes)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Sources</th><th>Classes</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupOutlierRows(rows, emptyText) {
  const outlierRows = Array.isArray(rows) ? rows : [];
  if (!outlierRows.length) return escapeHtml(emptyText);
  const body = outlierRows
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const classes = formatDedupSourceClasses(row?.sourceClasses);
      const reason = String(row?.outlierReason || "unknown").replaceAll("_", " ");
      const locations = Number(row?.distinctLocationCount || 0);
      const links = Number(row?.uniqueJobLinkCount || 0);
      const providerIds = Number(row?.providerSourceJobIdCount || 0);
      const dominant = String(row?.dominantSourceClass || "unknown");
      const strong = row?.hasStrongIdentity ? "strong identity" : "weak identity";
      const shared = row?.sharedPrimaryUrl ? ", shared URL" : "";
      const identityShape = String(row?.identityShape || "mixed_or_unknown_identity").replaceAll("_", " ");
      const titleShape = String(row?.titleShape || "empty_or_unknown").replaceAll("_", " ");
      const caveats = Array.isArray(row?.identityCaveats) ? row.identityCaveats : [];
      const caveatText = caveats.length
        ? `; caveats ${caveats.join(", ").replaceAll("_", " ")}`
        : "";
      const sharedUrl = row?.sharedUrlHost || row?.sharedUrlPath
        ? `; shared ${String(row?.sharedUrlHost || "")}${String(row?.sharedUrlPath || "")}`
        : "";
      const urlShape = `${identityShape}; title ${titleShape}; hosts ${Number(row?.uniqueUrlHostCount || 0).toLocaleString()}, prefixes ${Number(row?.uniqueUrlPathPrefixCount || 0).toLocaleString()}`;
      const detail = `${reason}; ${locations.toLocaleString()} locations, ${links.toLocaleString()} links, ${providerIds.toLocaleString()} provider IDs, ${dominant} dominant, ${strong}${shared}; ${urlShape}${sharedUrl}${caveatText}`;
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>${escapeHtml(classes)}</td>
          <td>${escapeHtml(detail)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Sources</th><th>Classes</th><th>Outlier evidence</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupRiskRows(rows, emptyText) {
  const riskRows = Array.isArray(rows) ? rows : [];
  if (!riskRows.length) return escapeHtml(emptyText);
  const body = riskRows
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const reasons = Array.isArray(row?.riskReasons) ? row.riskReasons : [];
      const reasonText = reasons.length ? reasons.join(", ").replaceAll("_", " ") : "review";
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${escapeHtml(reasonText)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Reason</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupReviewEvidenceLine(label, value) {
  const text = String(value ?? "").trim();
  if (!text || text === "none") return "";
  return `<div class="admin-dedup-review-evidence-line"><strong>${escapeHtml(label)}</strong> ${escapeHtml(text)}</div>`;
}

/**
 * @param {Array<DedupReviewQueueRow>|null|undefined} rows
 * @param {string} emptyText
 */
function formatDedupReviewQueueRows(rows, emptyText) {
  const queueRows = Array.isArray(rows) ? rows : [];
  if (!queueRows.length) return escapeHtml(emptyText);
  const body = queueRows
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const machine = value => String(value || "unknown").replaceAll("_", " ");
      const list = value => (Array.isArray(value) ? value.slice(0, 5).map(item => machine(item)).join(", ") : "none");
      const sources = Array.isArray(row?.sampleSources) ? row.sampleSources : Array.isArray(row?.sources) ? row.sources : [];
      const evidenceLines = [
        formatDedupReviewEvidenceLine("Suspected cause", machine(row?.suspectedCause)),
        formatDedupReviewEvidenceLine("Identity shape", machine(row?.identityShape)),
        formatDedupReviewEvidenceLine("Identity quality", machine(row?.identityQuality)),
        formatDedupReviewEvidenceLine("Provenance", machine(row?.nonProviderIdentityProvenance)),
        formatDedupReviewEvidenceLine("Sheets bundle shape", machine(row?.googleSheetsBundleShape)),
        formatDedupReviewEvidenceLine("Sheets role-bucket audit", machine(row?.googleSheetsRoleBucketAudit)),
        formatDedupReviewEvidenceLine("Sheets bucket intent", machine(row?.googleSheetsBucketIntent)),
        formatDedupReviewEvidenceLine("Sheets weak grouping audit", machine(row?.googleSheetsWeakGroupingAudit)),
        formatDedupReviewEvidenceLine("Outlier reason", machine(row?.outlierReason)),
        formatDedupReviewEvidenceLine("Caveats", list(row?.identityCaveats)),
        formatDedupReviewEvidenceLine("Cause evidence", list(row?.causeEvidence)),
        formatDedupReviewEvidenceLine("Identity evidence", list(row?.identityQualityEvidence)),
        formatDedupReviewEvidenceLine("Provenance evidence", list(row?.nonProviderIdentityEvidence)),
        formatDedupReviewEvidenceLine("Sheets evidence", list(row?.googleSheetsBundleEvidence)),
        formatDedupReviewEvidenceLine("Sheets audit evidence", list(row?.googleSheetsRoleBucketAuditEvidence)),
        formatDedupReviewEvidenceLine("Sheets intent evidence", list(row?.googleSheetsBucketIntentEvidence)),
        formatDedupReviewEvidenceLine("Sheets weak audit evidence", list(row?.googleSheetsWeakGroupingEvidence))
      ].filter(Boolean).join("");
      const sourceText = sources.length ? sources.slice(0, 3).join(" | ") : "none";
      return `
        <tr>
          <td>${escapeHtml(action)}</td>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>
            <details class="admin-dedup-review-evidence">
              <summary>Evidence</summary>
              <div class="admin-dedup-review-evidence-body">${evidenceLines || '<span class="muted">none</span>'}</div>
            </details>
            ${formatDedupReviewEvidenceLine("Sources", sourceText)}
          </td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Action</th><th>Title</th><th>Company</th><th>Sources</th><th>Evidence</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

// ── gate card / review-state helpers ────────────────────────────────

function formatDedupAuditGateDetailCounts(counts) {
  const values = counts && typeof counts === "object" ? counts : {};
  const labels = Object.entries(values)
    .filter(([, value]) => Number(value || 0) > 0)
    .slice(0, 8)
    .map(([key, value]) => `${key.replaceAll("_", " ").replaceAll(".", " ")} ${Number(value || 0).toLocaleString()}`);
  return labels.length ? labels.join(", ") : "none";
}

function fallbackDedupGateDetails(auditGate, keys, type) {
  const labels = {
    current_run_non_primary_merges_need_review: "Current-run non-primary merges",
    provider_static_disagreement_needs_review: "Provider/static disagreements",
    high_risk_review_queue_causes_need_review: "Current-run high-risk review queue",
    current_run_primary_url_merges_present: "Current-run primary URL merges",
    carried_provider_static_location_pollution_present: "Carried provider/static location pollution",
    carried_provider_static_auto_safe_variants_present: "Carried provider/static auto-safe variants",
    carried_provider_static_reviewed_safe_present: "Carried provider/static reviewed-safe rows",
    carried_high_risk_review_queue_causes_present: "Carried high-risk review queue",
    monitor_review_queue_diagnostics_present: "Monitor-only review diagnostics",
    carried_source_bundle_collisions_present: "Carried source-bundle collisions"
  };
  const countsByKey = {
    current_run_non_primary_merges_need_review: Number(auditGate?.currentRunNonPrimaryMergeCounts?.blocking || 0),
    provider_static_disagreement_needs_review: Number(auditGate?.providerStaticDisagreementBlockedCount || 0),
    high_risk_review_queue_causes_need_review: Number(auditGate?.currentRunBlockingReviewQueueCount || 0),
    current_run_primary_url_merges_present: Number(auditGate?.currentRunMergedCount || 0),
    carried_high_risk_review_queue_causes_present: Number(auditGate?.carriedBlockingReviewQueueCount || 0),
    monitor_review_queue_diagnostics_present: Number(auditGate?.monitorReviewQueueCount || 0),
    carried_source_bundle_collisions_present: Number(auditGate?.carriedCollisionLikelyHistoricalCount || 0)
  };
  return keys.map(key => ({
    key,
    label: labels[key] || key.replaceAll("_", " "),
    count: countsByKey[key] || 0,
    whyBlocked: type === "blocker"
      ? "This legacy gate payload marks the family as blocking but does not include detailed cause metadata."
      : "This legacy gate payload marks the family as warning-only.",
    nextAction: "Open supporting diagnostics for examples and rerun with a report that includes gate detail fields.",
    counts: {},
    examples: []
  }));
}

function formatDedupAuditGateExampleDetails(row) {
  const title = String(row?.title || "Untitled");
  const company = String(row?.company || "Unknown company");
  const classification = String(row?.disagreementClassification || row?.suspectedCause || "unknown").replaceAll("_", " ");
  const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
  const status = String(row?.dedupReviewStatus || row?.disagreementGateDisposition || "unreviewed").replaceAll("_", " ");
  const reviewUpdatedBy = String(row?.dedupReviewUpdatedBy || "");
  const reviewUpdatedAt = String(row?.dedupReviewUpdatedAt || "");
  const origin = String(row?.bundleEvidenceOrigin || "unknown").replaceAll("_", " ");
  const gateDisposition = String(row?.disagreementGateDisposition || "").replaceAll("_", " ");
  const sourceBundleCount = Number(row?.sourceBundleCount || 0).toLocaleString();
  const causeEvidence = Array.isArray(row?.disagreementClassificationEvidence)
    ? row.disagreementClassificationEvidence
    : Array.isArray(row?.causeEvidence)
      ? row.causeEvidence
      : [];
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence)
    ? row.disagreementGateEvidence
    : Array.isArray(row?.reviewEvidence)
      ? row.reviewEvidence
      : [];
  const summary = `${title} @ ${company} — ${classification}, ${status}`;
  const metaChips = [
    `classification ${classification}`,
    `action ${action}`,
    `review ${status}${reviewUpdatedBy ? ` by ${reviewUpdatedBy}` : ""}${reviewUpdatedAt ? ` at ${reviewUpdatedAt}` : ""}`,
    `origin ${origin}`,
    gateDisposition ? `gate ${gateDisposition}` : "",
    `sources ${sourceBundleCount}`
  ].filter(Boolean);
  return `
    <details class="admin-dedup-audit-gate-example">
      <summary><span class="admin-dedup-audit-gate-example-summary">${escapeHtml(summary)}</span></summary>
      <div class="admin-dedup-audit-gate-example-body">
        <div class="admin-dedup-audit-gate-example-chips">
          ${metaChips.map(label => `<span class="admin-dedup-audit-gate-chip">${escapeHtml(label)}</span>`).join("")}
        </div>
        <div class="admin-dedup-audit-gate-example-section">
          <div class="admin-dedup-audit-gate-example-label">Classification evidence</div>
          <div>${escapeHtml(causeEvidence.slice(0, 5).join(", ").replaceAll("_", " ") || "none")}</div>
        </div>
        <div class="admin-dedup-audit-gate-example-section">
          <div class="admin-dedup-audit-gate-example-label">Gate evidence</div>
          <div>${escapeHtml(gateEvidence.slice(0, 5).join(", ").replaceAll("_", " ") || "none")}</div>
        </div>
      </div>
    </details>
  `;
}

function formatDedupAuditGateDetailCard(detail, type) {
  const item = detail && typeof detail === "object" ? detail : {};
  const label = String(item?.label || item?.key || "Unknown issue");
  const count = Number(item?.count || 0);
  const why = String(item?.whyBlocked || "No explanation available.");
  const action = String(item?.nextAction || "Inspect supporting diagnostics.");
  const counts = formatDedupAuditGateDetailCounts(item?.counts);
  const examples = Array.isArray(item?.examples) ? item.examples.slice(0, 10) : [];
  return `
    <article class="admin-dedup-audit-gate-detail admin-dedup-audit-gate-detail-${escapeHtml(type)}">
      <div class="admin-dedup-audit-gate-detail-head">
        <strong>${escapeHtml(label)}</strong>
        <span class="admin-dedup-audit-gate-chip">${count.toLocaleString()}</span>
      </div>
      <div class="admin-dedup-audit-gate-detail-meta">${escapeHtml(counts)}</div>
      <div class="admin-dedup-audit-gate-detail-copy"><strong>Why ${type === "blocker" ? "blocked" : "visible"}</strong>: ${escapeHtml(why)}</div>
      <div class="admin-dedup-audit-gate-detail-copy"><strong>Next action</strong>: ${escapeHtml(action)}</div>
      <div class="admin-dedup-audit-gate-examples">
        <div><strong>Examples</strong></div>
        ${examples.length ? examples.map(row => formatDedupAuditGateExampleDetails(row)).join("") : `<div class="admin-dedup-audit-gate-empty">${escapeHtml("No capped examples for this family.")}</div>`}
      </div>
    </article>
  `;
}

function formatDedupAuditGateCard(gate) {
  const auditGate = gate && typeof gate === "object" ? gate : {};
  const status = String(auditGate?.status || "unknown").replaceAll("_", " ");
  const ready = auditGate?.lifecycleUxReady === true ? "yes" : "no";
  const blockers = Array.isArray(auditGate?.blockers) ? auditGate.blockers : [];
  const warnings = Array.isArray(auditGate?.warnings) ? auditGate.warnings : [];
  const blockerDetails = Array.isArray(auditGate?.blockerDetails) && auditGate.blockerDetails.length
    ? auditGate.blockerDetails
    : fallbackDedupGateDetails(auditGate, blockers, "blocker");
  const warningDetails = Array.isArray(auditGate?.warningDetails) && auditGate.warningDetails.length
    ? auditGate.warningDetails
    : fallbackDedupGateDetails(auditGate, warnings, "warning");
  const blockerSummary = blockerDetails.length
    ? `${blockerDetails.length.toLocaleString()} blocking issue${blockerDetails.length === 1 ? "" : "s"}`
    : "no blocking issues";
  const warningSummary = warningDetails.length
    ? `${warningDetails.length.toLocaleString()} warning issue${warningDetails.length === 1 ? "" : "s"}`
    : "no warning issues";
  const gateChips = {
    "current-run merges": auditGate?.currentRunMergedCount,
    "current-run collisions": auditGate?.currentRunSourceBundleCollisionCount,
    "carried collisions": auditGate?.carriedSourceBundleCollisionCount ?? auditGate?.sourceBundleCollisionCount,
    "historical-like": auditGate?.carriedCollisionLikelyHistoricalCount,
    "raw high-risk diagnostics": auditGate?.highRiskReviewQueueCount,
    "current high-risk": auditGate?.currentRunHighRiskReviewQueueCount,
    "carried high-risk": auditGate?.carriedHighRiskReviewQueueCount,
    "blocking review": auditGate?.blockingReviewQueueCount,
    "current blocking": auditGate?.currentRunBlockingReviewQueueCount,
    "carried blocking": auditGate?.carriedBlockingReviewQueueCount,
    "monitor diagnostics": auditGate?.monitorReviewQueueCount,
    "current monitor": auditGate?.currentRunMonitorReviewQueueCount,
    "carried monitor": auditGate?.carriedMonitorReviewQueueCount,
    "provider/static": auditGate?.providerStaticDisagreementCount,
    "provider/static current": auditGate?.providerStaticDisagreementCurrentRunCount,
    "provider/static carried": auditGate?.providerStaticDisagreementCarriedCount,
    "Sheets role unresolved": auditGate?.googleSheetsRoleBucketUnresolvedCount,
    "Sheets guard-blocked": auditGate?.googleSheetsRoleBucketGuardBlockedCount,
    "Sheets historical": auditGate?.googleSheetsRoleBucketHistoricalCount
  };
  const gateMetricsHtml = `
    <div class="admin-dedup-audit-gate-chips">
      ${auditGate?.googleSheetsGenericRoleGuardActive === true
        ? '<span class="admin-dedup-audit-gate-chip">Google Sheets guard active</span>'
        : ""}
      ${Object.entries(gateChips)
        .filter(([, value]) => Number(value || 0) > 0)
        .map(([label, value]) => `<span class="admin-dedup-audit-gate-chip">${escapeHtml(label)} ${Number(value).toLocaleString()}</span>`)
        .join("")}
    </div>
  `;
  return `
    <section class="admin-ops-schedule-item admin-ops-full-row admin-dedup-audit-gate-card">
      <div class="admin-dedup-audit-gate-header">
        <div class="admin-dedup-audit-gate-title">
          <strong>Dedup Audit Gate</strong>
          <span class="admin-dedup-audit-gate-status">status ${escapeHtml(status)}</span>
          <span class="admin-dedup-audit-gate-ready">lifecycle UX ready ${escapeHtml(ready)}</span>
          <span class="admin-dedup-audit-gate-chip">${escapeHtml(blockerSummary)}</span>
          <span class="admin-dedup-audit-gate-chip">${escapeHtml(warningSummary)}</span>
        </div>
        <div class="admin-dedup-audit-gate-summary">
          ${escapeHtml(blockerDetails.length ? "Lifecycle UX is paused by the blocking issues below." : "No blocking issues are reported by the gate.")}
        </div>
      </div>
      <div class="admin-dedup-audit-gate-detail-section">
        <div><strong>Blocking Issues</strong></div>
        ${blockerDetails.length ? blockerDetails.map(detail => formatDedupAuditGateDetailCard(detail, "blocker")).join("") : `<div class="admin-dedup-audit-gate-empty">${escapeHtml("No blocking issues.")}</div>`}
      </div>
      <div class="admin-dedup-audit-gate-detail-section">
        <div><strong>Warnings</strong></div>
        ${warningDetails.length ? warningDetails.map(detail => formatDedupAuditGateDetailCard(detail, "warning")).join("") : `<div class="admin-dedup-audit-gate-empty">${escapeHtml("No warning issues.")}</div>`}
      </div>
      ${formatOpsMetricsDetails("Gate metrics", gateMetricsHtml, "admin-dedup-audit-gate-metrics")}
    </section>
  `;
}

function formatDedupAuditGateExamples(rows, emptyText) {
  const examples = Array.isArray(rows) ? rows : [];
  if (!examples.length) return escapeHtml(emptyText);
  return examples
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const cause = String(row?.suspectedCause || "unknown").replaceAll("_", " ");
      const quality = String(row?.identityQuality || "unknown").replaceAll("_", " ");
      const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const origin = String(row?.bundleEvidenceOrigin || "").replaceAll("_", " ");
      const originText = origin ? `, ${origin}` : "";
      return escapeHtml(`${title} @ ${company} (${cause}, ${quality}, ${action}${originText})`);
    })
    .join(" | ");
}

function providerStaticBlockerCountsFromGate(gate) {
  const auditGate = gate && typeof gate === "object" ? gate : {};
  const details = Array.isArray(auditGate?.blockerDetails) ? auditGate.blockerDetails : [];
  const providerDetail = details.find(detail => detail?.key === "provider_static_disagreement_needs_review");
  const counts = providerDetail?.counts && typeof providerDetail.counts === "object" ? providerDetail.counts : {};
  return {
    current: Number(counts?.currentRunBlocked || 0),
    carried: Number(counts?.carriedBlocked || 0),
    total: Number(counts?.blocked || auditGate?.providerStaticDisagreementBlockedCount || 0)
  };
}

function formatDedupReviewStateSummary(summary, readWarning = "", gate = {}) {
  const state = summary && typeof summary === "object" ? summary : {};
  const artifactPath = String(state?.artifactPath || "data/dedup-review-state.json");
  const status = String(state?.status || (readWarning ? "warning" : "ok")).replaceAll("_", " ");
  const warning = String(state?.readWarning || readWarning || "").replaceAll("_", " ");
  const reviewedPairs = Number(state?.reviewedPairCount || 0);
  const reviewedSafe = Number(state?.reviewedSafeCount || 0);
  const confirmedBlocking = Number(state?.confirmedBlockingCount || 0);
  const unresolvedBlocking = Number(state?.unresolvedBlockingCount || 0);
  const providerStaticBlockers = providerStaticBlockerCountsFromGate(gate);
  const warningAction = warning
    ? [
        `warning ${warning}`,
        `Review-state file missing/malformed (${warning})`,
        providerStaticBlockers.total > 0
          ? `provider/static blockers current-run ${providerStaticBlockers.current.toLocaleString()}, carried ${providerStaticBlockers.carried.toLocaleString()}`
          : "",
        providerStaticBlockers.current > 0
          ? "restoring old review state alone will not clear current-run blockers"
          : "",
        providerStaticBlockers.carried > 0
          ? "restore or re-review carried rows to downgrade reviewed-safe blockers"
          : ""
      ].filter(Boolean).join("; ")
    : "";
  return [
    `path ${artifactPath}`,
    `status ${status}`,
    `reviewed pairs ${reviewedPairs.toLocaleString()}`,
    `reviewed safe ${reviewedSafe.toLocaleString()}`,
    `confirmed blocking ${confirmedBlocking.toLocaleString()}`,
    `unresolved blocking ${unresolvedBlocking.toLocaleString()}`,
    warningAction || (warning ? `warning ${warning}` : "")
  ].filter(Boolean).join(", ");
}

function formatCurrentRunMergeExamples(rows, emptyText) {
  const examples = Array.isArray(rows) ? rows : [];
  if (!examples.length) return escapeHtml(emptyText);
  return examples
    .slice(0, 10)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const source = String(row?.incomingSource || "unknown");
      const reason = String(row?.mergeReason || "unknown").replaceAll("_", " ");
      const review = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const nonBlockingReason = String(row?.nonBlockingReason || "").replaceAll("_", " ");
      const suffix = nonBlockingReason ? `, ${nonBlockingReason}` : "";
      return escapeHtml(`${title} @ ${company} (${reason}, ${source}, ${review}${suffix})`);
    })
    .join(" | ");
}

// ── dedup lists builder ─────────────────────────────────────────────

function formatOpsMetricsDetails(summary, html, className = "") {
  return `
    <details class="admin-ops-metrics-details ${escapeHtml(className)}">
      <summary>${escapeHtml(summary)}</summary>
      <div class="admin-ops-metrics-details-body">
        ${html}
      </div>
    </details>
  `;
}

/**
 * @param {FetcherMetricsPayload|null|undefined} metrics
 * @param {Object} [options]
 */
function buildDedupListsContent(metrics, options = {}) {
  const latest = metrics?.latestRun || {};
  const dedupEvidence = latest?.dedupEvidence && typeof latest.dedupEvidence === "object" ? latest.dedupEvidence : {};
  const dedupReviewStateSummary = latest?.dedupReviewStateSummary && typeof latest.dedupReviewStateSummary === "object"
    ? latest.dedupReviewStateSummary
    : {};
  const dedupReviewStateReadWarning = String(latest?.dedupReviewStateReadWarning || "");
  const mergeReasonCounts = dedupEvidence?.mergeReasonCounts && typeof dedupEvidence.mergeReasonCounts === "object"
    ? dedupEvidence.mergeReasonCounts
    : {};
  const sourceBundleComposition = dedupEvidence?.sourceBundleComposition && typeof dedupEvidence.sourceBundleComposition === "object"
    ? dedupEvidence.sourceBundleComposition
    : {};
  const riskReasonCounts = dedupEvidence?.riskReasonCounts && typeof dedupEvidence.riskReasonCounts === "object"
    ? dedupEvidence.riskReasonCounts
    : {};
  const outlierReasonCounts = dedupEvidence?.outlierReasonCounts && typeof dedupEvidence.outlierReasonCounts === "object"
    ? dedupEvidence.outlierReasonCounts
    : {};
  const identityShapeCounts = dedupEvidence?.identityShapeCounts && typeof dedupEvidence.identityShapeCounts === "object"
    ? dedupEvidence.identityShapeCounts
    : {};
  const identityQualityCounts = dedupEvidence?.identityQualityCounts && typeof dedupEvidence.identityQualityCounts === "object"
    ? dedupEvidence.identityQualityCounts
    : {};
  const nonProviderIdentityProvenanceCounts = dedupEvidence?.nonProviderIdentityProvenanceCounts && typeof dedupEvidence.nonProviderIdentityProvenanceCounts === "object"
    ? dedupEvidence.nonProviderIdentityProvenanceCounts
    : {};
  const googleSheetsBundleShapeCounts = dedupEvidence?.googleSheetsBundleShapeCounts && typeof dedupEvidence.googleSheetsBundleShapeCounts === "object"
    ? dedupEvidence.googleSheetsBundleShapeCounts
    : {};
  const googleSheetsRoleBucketAuditCounts = dedupEvidence?.googleSheetsRoleBucketAuditCounts && typeof dedupEvidence.googleSheetsRoleBucketAuditCounts === "object"
    ? dedupEvidence.googleSheetsRoleBucketAuditCounts
    : {};
  const googleSheetsRoleBucketAudit = dedupEvidence?.googleSheetsRoleBucketAudit && typeof dedupEvidence.googleSheetsRoleBucketAudit === "object"
    ? dedupEvidence.googleSheetsRoleBucketAudit
    : {};
  const googleSheetsBucketIntentCounts = dedupEvidence?.googleSheetsBucketIntentCounts && typeof dedupEvidence.googleSheetsBucketIntentCounts === "object"
    ? dedupEvidence.googleSheetsBucketIntentCounts
    : {};
  const googleSheetsWeakGroupingAuditCounts = dedupEvidence?.googleSheetsWeakGroupingAuditCounts && typeof dedupEvidence.googleSheetsWeakGroupingAuditCounts === "object"
    ? dedupEvidence.googleSheetsWeakGroupingAuditCounts
    : {};
  const reviewQueueCounts = dedupEvidence?.reviewQueueCounts && typeof dedupEvidence.reviewQueueCounts === "object"
    ? dedupEvidence.reviewQueueCounts
    : {};
  const reviewQueueCauseCounts = dedupEvidence?.reviewQueueCauseCounts && typeof dedupEvidence.reviewQueueCauseCounts === "object"
    ? dedupEvidence.reviewQueueCauseCounts
    : {};
  const dedupAuditGate = dedupEvidence?.dedupAuditGate && typeof dedupEvidence.dedupAuditGate === "object"
    ? dedupEvidence.dedupAuditGate
    : {};
  const providerStaticDisagreementCounts = dedupEvidence?.providerStaticDisagreementCounts && typeof dedupEvidence.providerStaticDisagreementCounts === "object"
    ? dedupEvidence.providerStaticDisagreementCounts
    : {};
  const providerStaticDisagreementGateCounts = dedupEvidence?.providerStaticDisagreementGateCounts && typeof dedupEvidence.providerStaticDisagreementGateCounts === "object"
    ? dedupEvidence.providerStaticDisagreementGateCounts
    : {};
  const providerStaticDisagreementClassificationCounts = dedupEvidence?.providerStaticDisagreementClassificationCounts && typeof dedupEvidence.providerStaticDisagreementClassificationCounts === "object"
    ? dedupEvidence.providerStaticDisagreementClassificationCounts
    : {};
  const providerStaticTitleCompanyCollisionCounts = dedupEvidence?.providerStaticTitleCompanyCollisionCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionCounts
    : {};
  const providerStaticTitleCompanyCollisionAuditCounts = dedupEvidence?.providerStaticTitleCompanyCollisionAuditCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts
    : {};
  const providerStaticDisagreementRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples)
    ? dedupEvidence.providerStaticDisagreementExamples
    : [];
  const providerStaticTitleCompanyCollisionRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples)
    ? dedupEvidence.providerStaticTitleCompanyCollisionExamples
    : [];
  const topMergedSummary = formatDedupMergedRows(
    dedupEvidence?.topMergedJobs,
    "No merged canonical jobs in the latest fetch report."
  );
  const topOutlierSummary = formatDedupOutlierRows(
    dedupEvidence?.topSourceBundleOutliers,
    "No carried source-bundle collision outliers in the latest fetch report."
  );
  const riskyMergeSummary = formatDedupRiskRows(
    dedupEvidence?.riskyMergeExamples,
    "No risky merge examples in the latest fetch report."
  );
  const reviewQueueSummary = formatDedupReviewQueueRows(
    dedupEvidence?.reviewQueue,
    "No dedup review queue examples in the latest fetch report."
  );
  const providerStaticDisagreementSummary = formatProviderStaticDisagreementRows(
    providerStaticDisagreementRows,
    "No provider/static disagreement examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStatic" }
  );
  const providerStaticTitleCompanyCollisionSummary = formatProviderStaticTitleCompanyCollisionRows(
    providerStaticTitleCompanyCollisionRows,
    "No provider/static title/company collision examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStaticTitleCompany" }
  );
  const supportingHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup current-run merge examples</strong>: ${formatCurrentRunMergeExamples(dedupEvidence?.currentRunMergeExamples, "No current-run merge examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static disagreements</strong>: ${escapeHtml(formatProviderStaticDisagreementCounts(providerStaticDisagreementCounts))}. Gate: ${escapeHtml(formatProviderStaticDisagreementGateCounts(providerStaticDisagreementGateCounts))}. Classifications: ${escapeHtml(formatProviderStaticDisagreementClassificationCounts(providerStaticDisagreementClassificationCounts))}. ${providerStaticDisagreementSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static title-company collisions</strong>: ${escapeHtml(formatProviderStaticTitleCompanyCollisionCounts(providerStaticTitleCompanyCollisionCounts))}. Audit: ${escapeHtml(formatProviderStaticTitleCompanyCollisionAuditCounts(providerStaticTitleCompanyCollisionAuditCounts))}. ${providerStaticTitleCompanyCollisionSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup carried bundle examples</strong>: ${formatDedupAuditGateExamples(dedupEvidence?.carriedBundleExamples, "No carried bundle examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup source composition</strong>: ${escapeHtml(formatDedupSourceClasses(sourceBundleComposition))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup risk reasons</strong>: ${escapeHtml(formatDedupRiskReasonCounts(riskReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup outlier reasons</strong>: ${escapeHtml(formatDedupOutlierReasonCounts(outlierReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity shapes</strong>: ${escapeHtml(formatDedupIdentityShapeCounts(identityShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity quality</strong>: ${escapeHtml(formatDedupIdentityQualityCounts(identityQualityCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup non-provider provenance</strong>: ${escapeHtml(formatDedupNonProviderIdentityProvenanceCounts(nonProviderIdentityProvenanceCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bundle shapes</strong>: ${escapeHtml(formatDedupGoogleSheetsBundleShapeCounts(googleSheetsBundleShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit</strong>: ${escapeHtml(formatDedupGoogleSheetsRoleBucketAuditCounts(googleSheetsRoleBucketAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit summary</strong>: ${formatDedupGoogleSheetsRoleBucketAuditSummary(googleSheetsRoleBucketAudit)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bucket intent</strong>: ${escapeHtml(formatDedupGoogleSheetsBucketIntentCounts(googleSheetsBucketIntentCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets weak grouping audit</strong>: ${escapeHtml(formatDedupGoogleSheetsWeakGroupingAuditCounts(googleSheetsWeakGroupingAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup action queue</strong>: ${escapeHtml(formatDedupReviewQueueCounts(reviewQueueCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup diagnostic causes</strong>: ${escapeHtml(formatDedupReviewQueueCauseCounts(reviewQueueCauseCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top merged jobs</strong>: ${topMergedSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top source-bundle outliers</strong>: ${topOutlierSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review examples</strong>: ${reviewQueueSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Risky merge examples</strong>: ${riskyMergeSummary}</div>
  `;
  return {
    html: `
      <section class="admin-ops-metrics-section admin-ops-metrics-section-dedup">
        <div class="admin-ops-metrics-section-head">
          <div>
            <h4>Dedup Lists</h4>
            <p>Read-only gate, review-state, and blocker evidence before lifecycle UX.</p>
          </div>
        </div>
        <div class="admin-ops-metrics-section-body">
          <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup evidence</strong>: read-only diagnostics. Current-run merges by reason: ${escapeHtml(formatNonZeroCounts({
            "primary URL": mergeReasonCounts?.primaryUrl,
            "secondary key": mergeReasonCounts?.secondaryKey,
            "known mirror pair": mergeReasonCounts?.knownMirrorPair,
            "social key": mergeReasonCounts?.socialKey,
            "sparse identity": mergeReasonCounts?.sparseIdentity,
            unknown: mergeReasonCounts?.unknown
          }))}. Carried source-bundle collision rows: ${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}.</div>
          ${formatDedupAuditGateCard(dedupAuditGate)}
          <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review-state</strong>: ${escapeHtml(formatDedupReviewStateSummary(dedupReviewStateSummary, dedupReviewStateReadWarning, dedupAuditGate))}</div>
          ${formatOpsMetricsDetails("Dedup supporting diagnostics", supportingHtml, "admin-ops-dedup-details")}
        </div>
      </section>
    `,
    rowGroups: {
      providerStatic: visibleProviderStaticRows(providerStaticDisagreementRows),
      providerStaticTitleCompany: visibleProviderStaticRows(providerStaticTitleCompanyCollisionRows)
    }
  };
}

function wireDedupReviewActions(container, rowGroups, onDedupReviewAction) {
  if (typeof onDedupReviewAction !== "function") return;
  container.querySelectorAll("[data-dedup-review-action]").forEach(button => {
    button.addEventListener("click", () => {
      const action = String(button.getAttribute("data-dedup-review-action") || "");
      const tableKey = String(button.getAttribute("data-dedup-review-table") || "");
      const rowIndex = Number(button.getAttribute("data-dedup-review-row") || -1);
      const row = Array.isArray(rowGroups?.[tableKey]) ? rowGroups[tableKey][rowIndex] : null;
      if (!row || !action) return;
      onDedupReviewAction(row, action);
    });
  });
}

export {
  formatNonZeroCounts,
  formatDedupRiskReasonCounts,
  formatDedupOutlierReasonCounts,
  formatDedupIdentityShapeCounts,
  formatDedupReviewQueueCounts,
  formatDedupReviewQueueCauseCounts,
  formatDedupIdentityQualityCounts,
  formatDedupNonProviderIdentityProvenanceCounts,
  formatDedupGoogleSheetsBundleShapeCounts,
  formatDedupGoogleSheetsRoleBucketAuditCounts,
  formatDedupGoogleSheetsRoleBucketAuditSummary,
  formatDedupGoogleSheetsBucketIntentCounts,
  formatDedupGoogleSheetsWeakGroupingAuditCounts,
  formatDedupMergedRows,
  formatDedupOutlierRows,
  formatDedupRiskRows,
  formatDedupReviewQueueRows,
  formatDedupAuditGateCard,
  formatDedupAuditGateExamples,
  formatDedupReviewStateSummary,
  formatCurrentRunMergeExamples,
  formatOpsMetricsDetails,
  buildDedupListsContent,
  wireDedupReviewActions
};
