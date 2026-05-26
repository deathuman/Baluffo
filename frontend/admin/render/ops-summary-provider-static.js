/**
 * Provider/static disagreement row formatters extracted from ``ops-summary.js``.
 *
 * No coordinator import.  Every function is a pure string renderer that
 * takes the relevant payload slice and returns HTML.
 *
 * @module ops-summary-provider-static
 */

import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js?v=6";

function humanizeProviderStaticValue(value, fallback = "unknown") {
  const text = String(value || "").trim();
  return text ? text.replaceAll("_", " ") : fallback;
}

function formatProviderStaticList(values, limit = 2) {
  const items = Array.isArray(values) ? values.filter(Boolean).slice(0, limit) : [];
  return items.length ? items.join(" | ") : "none";
}

function providerStaticRecommendationLabel(row) {
  const reviewStatus = String(row?.dedupReviewStatus || "");
  if (reviewStatus === "reviewed_safe") return "Safe duplicate";
  if (reviewStatus === "confirmed_blocking") return "Real blocker";
  const recommendation = String(row?.operatorReviewRecommendation || "");
  if (recommendation === "safe_duplicate") return "Safe duplicate";
  if (recommendation === "real_blocker") return "Real blocker";
  if (recommendation === "needs_review") return "Needs review";
  const disposition = String(row?.disagreementGateDisposition || "");
  if (disposition === "warning") return "Safe duplicate";
  return "Needs review";
}

function providerStaticReasonLabel(row) {
  const reviewStatus = String(row?.dedupReviewStatus || "");
  if (reviewStatus === "reviewed_safe") return "Manually reviewed as safe.";
  if (reviewStatus === "confirmed_blocking") return "Manually confirmed as a real blocker.";
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence) ? row.disagreementGateEvidence : [];
  if (gateEvidence.some(item => String(item || "").startsWith("auto_safe_"))) {
    return "Strong provider/static identity; safe URL variant.";
  }
  if (gateEvidence.some(item => String(item || "") === "carried_location_pollution")) {
    return "Historical location text pollution; warning only.";
  }
  const reason = String(row?.operatorReviewReason || "");
  const labels = {
    auto_safe_provider_static_variant: "Strong provider/static identity; safe URL variant.",
    carried_location_pollution_warning: "Historical location text pollution; warning only.",
    different_locations_same_title_company: "Same title/company appears across different locations.",
    manual_confirmed_blocking: "Manually confirmed as a real blocker.",
    manual_reviewed_safe: "Manually reviewed as safe.",
    warning_not_blocking: "Already warning-only.",
    static_parser_url_variant_blocked: "Looks like a static URL variant but lacks concrete shared job identity.",
    provider_redirect_or_canonical_url_blocked: "Looks like a canonical URL variant but lacks concrete shared job identity.",
    same_job_different_urls_blocked: "Provider and static have different URLs and need human review.",
    title_company_collision_blocked: "Title/company collision needs review.",
    needs_manual_review_blocked: "Evidence is incomplete or ambiguous."
  };
  return labels[reason] || humanizeProviderStaticValue(reason, "Needs human review.");
}

function providerStaticReviewStatus(row) {
  const status = humanizeProviderStaticValue(row?.dedupReviewStatus, "unreviewed");
  const updatedBy = String(row?.dedupReviewUpdatedBy || "");
  const updatedAt = String(row?.dedupReviewUpdatedAt || "");
  return `${status}${updatedBy ? ` by ${updatedBy}` : ""}${updatedAt ? ` at ${updatedAt}` : ""}`;
}

function formatProviderStaticRawEvidence(row) {
  const classificationEvidence = Array.isArray(row?.disagreementClassificationEvidence)
    ? row.disagreementClassificationEvidence
    : [];
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence)
    ? row.disagreementGateEvidence
    : [];
  const disagreementEvidence = Array.isArray(row?.disagreementEvidence)
    ? row.disagreementEvidence
    : [];
  const auditEvidence = Array.isArray(row?.carriedLocationPollutionEvidence)
    ? row.carriedLocationPollutionEvidence
    : [];
  const raw = [
    `classification evidence ${classificationEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    `gate evidence ${gateEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    `disagreement evidence ${disagreementEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    auditEvidence.length ? `audit evidence ${auditEvidence.slice(0, 8).join(", ").replaceAll("_", " ")}` : ""
  ].filter(Boolean).join("; ");
  return `
    <details class="admin-dedup-raw-evidence">
      <summary>Raw evidence</summary>
      <div>${escapeHtml(raw)}</div>
    </details>
  `;
}

function formatProviderStaticEvidenceBlock(label, sources, ids, urls) {
  const rows = [
    ["Source", formatProviderStaticList(sources)],
    ["Job IDs", formatProviderStaticList(ids)],
    ["URLs", formatProviderStaticList(urls)]
  ];
  return `
    <section class="admin-dedup-provider-static-evidence-block">
      <h5>${escapeHtml(label)}</h5>
      ${rows.map(([rowLabel, value]) => `
        <div class="admin-dedup-provider-static-evidence-row">
          <span>${escapeHtml(rowLabel)}</span>
          <code>${escapeHtml(value)}</code>
        </div>
      `).join("")}
    </section>
  `;
}

function isProviderStaticBlockedRow(row) {
  return String(row?.disagreementGateDisposition || "").toLowerCase() === "blocked";
}

function isProviderStaticAutoSafeVariantRow(row) {
  const disposition = String(row?.disagreementGateDisposition || "").toLowerCase();
  if (disposition !== "warning") return false;
  const reviewStatus = String(row?.dedupReviewStatus || "").toLowerCase();
  if (reviewStatus === "confirmed_blocking") return false;
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence) ? row.disagreementGateEvidence : [];
  const hasAutoSafeEvidence = gateEvidence.some(item => String(item || "").startsWith("auto_safe_"));
  const recommendation = String(row?.operatorReviewRecommendation || "").toLowerCase();
  const reason = String(row?.operatorReviewReason || "").toLowerCase();
  return hasAutoSafeEvidence || recommendation === "safe_duplicate" || reason === "auto_safe_provider_static_variant";
}

function visibleProviderStaticRows(rows, limit = 5) {
  const allRows = Array.isArray(rows) ? rows : [];
  const candidateRows = allRows.filter(row => !isProviderStaticAutoSafeVariantRow(row));
  const cappedWarningSlots = Math.max(0, Number(limit) || 0);
  const blockedCount = candidateRows.filter(isProviderStaticBlockedRow).length;
  const warningLimit = Math.max(0, cappedWarningSlots - blockedCount);
  let warningsAdded = 0;
  return candidateRows.filter(row => {
    if (isProviderStaticBlockedRow(row)) return true;
    if (warningsAdded >= warningLimit) return false;
    warningsAdded += 1;
    return true;
  });
}

function renderDedupReviewActionButtons(tableKey, rowIndex, showActions) {
  if (!showActions) return "";
  return `
    <div class="admin-inline-actions" data-dedup-review-actions="${escapeHtml(tableKey)}:${Number(rowIndex)}">
      <button type="button" class="admin-pill-button"${tooltipAttrs("Downgrade this exact disagreement from blocker to warning.")} data-dedup-review-action="reviewed_safe" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Safe duplicate</button>
      <button type="button" class="admin-pill-button"${tooltipAttrs("Keep this exact disagreement blocking and record that it was reviewed.")} data-dedup-review-action="confirmed_blocking" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Real blocker</button>
      <button type="button" class="admin-pill-button"${tooltipAttrs("Remove the manual decision and let the report classify it again.")} data-dedup-review-action="clear_review" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Reset review</button>
      <span class="admin-muted">Local review only: no merge, registry, source, or job data is changed.</span>
    </div>
  `;
}

function formatProviderStaticGuidedRows(rows, emptyText, options = {}) {
  const disagreementRows = Array.isArray(rows) ? rows : [];
  if (!disagreementRows.length) return escapeHtml(emptyText);
  const showActions = typeof options?.onReviewAction === "function";
  const tableKey = String(options?.tableKey || "providerStatic");
  const visibleRows = visibleProviderStaticRows(disagreementRows);
  const hiddenSafeCount = disagreementRows.filter(isProviderStaticAutoSafeVariantRow).length;
  const hiddenSafeSummary = hiddenSafeCount
    ? `<div class="admin-muted">Hidden safe provider/static URL variants: ${Number(hiddenSafeCount).toLocaleString()}.</div>`
    : "";
  if (!visibleRows.length) {
    return `${escapeHtml(emptyText)}${hiddenSafeSummary ? ` ${hiddenSafeSummary}` : ""}`;
  }
  const cards = visibleRows
    .map((row, rowIndex) => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const origin = humanizeProviderStaticValue(row?.bundleEvidenceOrigin);
      const quality = humanizeProviderStaticValue(row?.identityQuality);
      const disposition = humanizeProviderStaticValue(row?.disagreementGateDisposition, "blocked");
      const providerSources = Array.isArray(row?.providerSources) ? row.providerSources : [];
      const staticSources = Array.isArray(row?.staticSources) ? row.staticSources : [];
      const providerUrls = Array.isArray(row?.providerUrls) ? row.providerUrls : [];
      const staticUrls = Array.isArray(row?.staticUrls) ? row.staticUrls : [];
      const providerIds = Array.isArray(row?.providerSourceJobIds) ? row.providerSourceJobIds : [];
      const staticIds = Array.isArray(row?.staticSourceJobIds) ? row.staticSourceJobIds : [];
      const tokens = Array.isArray(row?.concreteSharedIdentifierTokens)
        ? row.concreteSharedIdentifierTokens
        : Array.isArray(row?.sharedIdentifierTokens)
          ? row.sharedIdentifierTokens
          : [];
      const locations = Array.isArray(row?.sampleLocations) ? row.sampleLocations : [];
      const classification = humanizeProviderStaticValue(row?.disagreementClassification, "needs manual review");
      const hint = humanizeProviderStaticValue(row?.collisionReviewHint, "");
      const audit = humanizeProviderStaticValue(row?.carriedLocationPollutionAudit, "");
      const statusChips = [
        `gate ${disposition}`,
        `review ${providerStaticReviewStatus(row)}`,
        `origin ${origin}`,
        `classification ${classification}`,
        hint ? `hint ${hint}` : "",
        audit ? `audit ${audit}` : "",
        `quality ${quality}`,
        `sources ${Number(row?.sourceBundleCount || 0).toLocaleString()}`,
        `locations ${Number(row?.distinctLocationCount || 0).toLocaleString()}${locations.length ? ` (${locations.slice(0, 2).join(" | ")})` : ""}`,
        tokens.length ? `shared job token ${tokens.slice(0, 2).join(", ")}` : "shared job token none"
      ].filter(Boolean);
      return `
        <article class="admin-dedup-provider-static-card">
          <div class="admin-dedup-provider-static-card-head">
            <div>
              <h5>${escapeHtml(title)}</h5>
              <div class="admin-dedup-provider-static-company">${escapeHtml(company)}</div>
            </div>
            <div class="admin-dedup-provider-static-recommendation">
              <span>${escapeHtml(providerStaticRecommendationLabel(row))}</span>
              <p>${escapeHtml(providerStaticReasonLabel(row))}</p>
            </div>
          </div>
          <div class="admin-dedup-provider-static-chips">
            ${statusChips.map(chip => `<span>${escapeHtml(chip)}</span>`).join("")}
          </div>
          <div class="admin-dedup-provider-static-evidence-grid">
            ${formatProviderStaticEvidenceBlock("Provider evidence", providerSources, providerIds, providerUrls)}
            ${formatProviderStaticEvidenceBlock("Static evidence", staticSources, staticIds, staticUrls)}
          </div>
          ${formatProviderStaticRawEvidence(row)}
          ${renderDedupReviewActionButtons(tableKey, rowIndex, showActions)}
        </article>
      `;
    })
    .join("");
  return `
    ${hiddenSafeSummary}
    <div class="admin-dedup-provider-static-list">${cards}</div>
  `;
}

function formatProviderStaticDisagreementRows(rows, emptyText, options = {}) {
  return formatProviderStaticGuidedRows(rows, emptyText, {
    ...options,
    tableKey: String(options?.tableKey || "providerStatic")
  });
}

function formatProviderStaticTitleCompanyCollisionRows(rows, emptyText, options = {}) {
  return formatProviderStaticGuidedRows(rows, emptyText, {
    ...options,
    tableKey: String(options?.tableKey || "providerStaticTitleCompany")
  });
}

function formatProviderStaticDisagreementCounts(disagreementCounts) {
  const counts = disagreementCounts && typeof disagreementCounts === "object" ? disagreementCounts : {};
  return [
    `total ${Number(counts?.total || 0).toLocaleString()}`,
    `current ${Number(counts?.currentRun || 0).toLocaleString()}`,
    `carried ${Number(counts?.carried || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticDisagreementGateCounts(gateCounts) {
  const counts = gateCounts && typeof gateCounts === "object" ? gateCounts : {};
  return [
    `blocked ${Number(counts?.blocked || 0).toLocaleString()}`,
    `warning ${Number(counts?.warning || 0).toLocaleString()}`,
    `current blocked ${Number(counts?.currentRunBlocked || 0).toLocaleString()}`,
    `carried blocked ${Number(counts?.carriedBlocked || 0).toLocaleString()}`,
    `carried warning ${Number(counts?.carriedWarning || 0).toLocaleString()}`,
    `auto-safe ${Number(counts?.autoSafeWarning || 0).toLocaleString()}`,
    `reviewed safe ${Number(counts?.reviewedSafeWarning || 0).toLocaleString()}`,
    `confirmed blocking ${Number(counts?.confirmedBlocking || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticDisagreementClassificationCounts(classificationCounts) {
  const counts = classificationCounts && typeof classificationCounts === "object" ? classificationCounts : {};
  return [
    `same job/different URLs ${Number(counts?.same_job_different_urls || 0).toLocaleString()}`,
    `canonical/redirect ${Number(counts?.provider_redirect_or_canonical_url || 0).toLocaleString()}`,
    `static URL variant ${Number(counts?.static_parser_url_variant || 0).toLocaleString()}`,
    `title/company collision ${Number(counts?.title_company_collision || 0).toLocaleString()}`,
    `stale carried ${Number(counts?.stale_carried_bundle || 0).toLocaleString()}`,
    `manual review ${Number(counts?.needs_manual_review || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticTitleCompanyCollisionCounts(collisionCounts) {
  const counts = collisionCounts && typeof collisionCounts === "object" ? collisionCounts : {};
  return [
    `total ${Number(counts?.total || 0).toLocaleString()}`,
    `current ${Number(counts?.currentRun || 0).toLocaleString()}`,
    `carried ${Number(counts?.carried || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticTitleCompanyCollisionAuditCounts(auditCounts) {
  const counts = auditCounts && typeof auditCounts === "object" ? auditCounts : {};
  return [
    `location pollution ${Number(counts?.carried_location_pollution || 0).toLocaleString()}`,
    `location variants ${Number(counts?.carried_location_variant || 0).toLocaleString()}`,
    `provider identity location conflicts ${Number(counts?.carried_provider_identity_location_conflict || 0).toLocaleString()}`,
    `possible real conflict ${Number(counts?.possible_real_multi_location_conflict || 0).toLocaleString()}`,
    `not carried ${Number(counts?.not_carried || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

export {
  humanizeProviderStaticValue,
  formatProviderStaticList,
  providerStaticRecommendationLabel,
  providerStaticReasonLabel,
  providerStaticReviewStatus,
  formatProviderStaticRawEvidence,
  formatProviderStaticEvidenceBlock,
  isProviderStaticBlockedRow,
  isProviderStaticAutoSafeVariantRow,
  visibleProviderStaticRows,
  renderDedupReviewActionButtons,
  formatProviderStaticGuidedRows,
  formatProviderStaticDisagreementRows,
  formatProviderStaticTitleCompanyCollisionRows,
  formatProviderStaticDisagreementCounts,
  formatProviderStaticDisagreementGateCounts,
  formatProviderStaticDisagreementClassificationCounts,
  formatProviderStaticTitleCompanyCollisionCounts,
  formatProviderStaticTitleCompanyCollisionAuditCounts
};
