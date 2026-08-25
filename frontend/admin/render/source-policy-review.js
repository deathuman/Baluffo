import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js?v=6";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { formatDateTime, stableOpsSignature } from "./ops-shared.js";

const SOURCE_POLICY_REVIEW_FILTERS = Object.freeze([
  { key: "all", label: "All" },
  { key: "needs_action", label: "Needs action" },
  { key: "stable_safe", label: "Stable safe" },
  { key: "static_only_detected", label: "Static-only detected" },
  { key: "provider_unstable", label: "Provider unstable" },
  { key: "force_paused", label: "Force-paused" },
  { key: "snoozed", label: "Snoozed" },
  { key: "reviewed", label: "Reviewed" }
]);

const FILTER_KEYS = new Set(SOURCE_POLICY_REVIEW_FILTERS.map(filter => filter.key));
const ACTION_TOKEN = UI_TOKENS.admin.sourcePolicyActionBtn;
const FILTER_TOKEN = UI_TOKENS.admin.sourcePolicyFilterBtn;
const MIGRATION_LINK_ACTION_TOKEN = UI_TOKENS.admin.sourcePolicyMigrationLinkActionBtn;
const ADMIN_MIGRATION_LINK_ACTOR = "admin_provider_link_backfill";

function normalizeFilterKey(value) {
  const key = String(value || "all");
  return FILTER_KEYS.has(key) ? key : "all";
}

function stringValue(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function numberValue(value) {
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function listValue(value) {
  return Array.isArray(value) ? value : [];
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function formatMachineLabel(value, fallback = "unknown") {
  return stringValue(value, fallback).replaceAll("_", " ");
}

function formatPercent(value) {
  const confidence = Math.max(0, Math.min(1, Number(value) || 0));
  return `${Math.round(confidence * 100)}%`;
}

function renderMetaSpans(items, { inlineCount = 5 } = {}) {
  const spans = items
    .filter(item => item.value !== null && item.value !== undefined)
    .map(item => `<span><strong>${escapeHtml(item.label)}</strong> ${escapeHtml(String(item.value))}</span>`);
  const inline = spans.slice(0, inlineCount).join("");
  const extra = spans.slice(inlineCount);
  if (!extra.length) return inline;
  return `
    ${inline}
    <details class="admin-source-policy-more-details">
      <summary>More details (${extra.length.toLocaleString()})</summary>
      <div class="admin-source-policy-meta admin-source-policy-meta-extra">${extra.join("")}</div>
    </details>
  `;
}

function formatOptionalDate(value) {
  const text = stringValue(value);
  return text ? formatDateTime(text) : "None";
}

function getPairs(payload) {
  if (Array.isArray(payload?.recommendations?.pairs)) return payload.recommendations.pairs;
  if (Array.isArray(payload?.pairs)) return payload.pairs;
  return [];
}

function getMigrationLinkReviewCandidates(payload) {
  const linkBackfill = objectValue(payload?.providerCoverageLinkBackfill);
  return listValue(linkBackfill.reviewCandidates).filter(row => row && typeof row === "object");
}

function getMigrationLinkBlockedCandidates(payload) {
  const linkBackfill = objectValue(payload?.providerCoverageLinkBackfill);
  return listValue(linkBackfill.blockedCandidates).filter(row => row && typeof row === "object");
}

function getMigrationLinkLinkedCandidates(payload) {
  const linkBackfill = objectValue(payload?.providerCoverageLinkBackfill);
  return listValue(linkBackfill.linkedCandidates).filter(row => row && typeof row === "object");
}

function getSuppressionEligibilityRows(payload) {
  const eligibility = objectValue(payload?.suppressionEligibility);
  return listValue(eligibility.missingLinkedStaticRows).filter(row => row && typeof row === "object");
}

function selectedStaticSourceId(candidate) {
  return stringValue(
    candidate?.selectedStaticSourceId,
    stringValue(
      candidate?.staticSourceId,
      stringValue(candidate?.migrationSourceIdentity, stringValue(candidate?.recommendedApiPayload?.staticSourceId))
    )
  );
}

function selectedStaticSourceName(candidate) {
  return stringValue(
    candidate?.selectedStaticSourceName,
    stringValue(
      candidate?.staticSourceName,
      stringValue(
        candidate?.migrationSourceName,
        stringValue(candidate?.recommendedApiPayload?.staticSourceName, selectedStaticSourceId(candidate))
      )
    )
  );
}

function isProviderShapedStaticId(candidate) {
  const staticId = selectedStaticSourceId(candidate);
  const providerId = stringValue(candidate?.providerSourceId, stringValue(candidate?.recommendedApiPayload?.providerSourceId));
  return Boolean(staticId && (staticId === providerId || !staticId.startsWith("static:")));
}

function hasMigrationLinkBlockers(candidate) {
  return listValue(candidate?.blockers).filter(Boolean).length > 0;
}

function migrationLinkRecommendedAction(candidate) {
  return stringValue(candidate?.recommendedApiPayload?.recommendedAction, stringValue(candidate?.recommendedAction));
}

function isRejectedMigrationLinkRecommendation(candidate) {
  const action = migrationLinkRecommendedAction(candidate);
  return action === "ambiguous_static_match" || action === "insufficient_evidence";
}

function currentMigrationLinkState(candidate) {
  const explicitState = objectValue(candidate?.currentProviderLinkState);
  if (Object.keys(explicitState).length) return explicitState;
  return {
    providerBucket: stringValue(candidate?.providerBucket),
    migrationSourceIdentity: stringValue(candidate?.migrationSourceIdentity),
    migrationLinkedBy: stringValue(candidate?.migrationLinkedBy),
    adminBackfillOwned: candidate?.adminBackfillOwned === true
  };
}

function migrationLinkActionUnavailableReason(candidate) {
  if (isProviderShapedStaticId(candidate)) return "Not applicable: selected static identity is provider-shaped.";
  if (hasMigrationLinkBlockers(candidate)) return "No safe link action available: blockers are present.";
  if (isRejectedMigrationLinkRecommendation(candidate)) return "No safe link action available for this recommendation.";
  return "No safe link action available.";
}

export function getSourcePolicyReviewActions(row) {
  const reviewState = stringValue(row?.reviewState, "new");
  const override = stringValue(row?.manualSuppressionOverride, "none");
  const actions = [];
  if (reviewState !== "acknowledged" && reviewState !== "reviewed") {
    actions.push({ key: "acknowledge", label: "Acknowledge" });
  }
  if (reviewState === "new" || reviewState === "acknowledged") {
    actions.push({ key: "reviewed", label: "Reviewed" });
  }
  if (reviewState !== "snoozed") {
    actions.push({ key: "snooze", label: "Snooze 7d" });
  }
  if (override === "force_pause") {
    actions.push({ key: "clear_override", label: "Clear override" });
  } else {
    actions.push({ key: "force_pause", label: "Force pause" });
  }
  return actions;
}

export function getMigrationLinkReviewActions(candidate) {
  const linkState = currentMigrationLinkState(candidate);
  const staticId = selectedStaticSourceId(candidate);
  const actions = [];
  const adminOwnedLink = Boolean(
    linkState.adminBackfillOwned
    || (
      stringValue(linkState.migrationLinkedBy) === ADMIN_MIGRATION_LINK_ACTOR
      && stringValue(linkState.migrationSourceIdentity) === staticId
    )
  );
  if (adminOwnedLink && stringValue(linkState.migrationSourceIdentity) === staticId) {
    actions.push({ key: "clear_migration_identity_link", label: "Clear link" });
  }
  const applyEligible = Boolean(
    candidate?.apiEligible === true
    && objectValue(candidate?.recommendedApiPayload).action
    && !hasMigrationLinkBlockers(candidate)
    && !isRejectedMigrationLinkRecommendation(candidate)
    && !isProviderShapedStaticId(candidate)
    && !adminOwnedLink
  );
  if (applyEligible) {
    actions.push({ key: "apply_migration_identity_link", label: "Apply link" });
  }
  return actions;
}

export function getMigrationLinkLinkedActions(candidate) {
  const staticId = selectedStaticSourceId(candidate);
  const adminOwnedLink = Boolean(
    candidate?.adminBackfillOwned === true
    && stringValue(candidate?.migrationLinkedBy) === ADMIN_MIGRATION_LINK_ACTOR
    && stringValue(candidate?.migrationSourceIdentity) === staticId
  );
  return adminOwnedLink && staticId
    ? [{ key: "clear_migration_identity_link", label: "Clear link" }]
    : [];
}

export function filterSourcePolicyReviewPairs(rows, filterKey) {
  const key = normalizeFilterKey(filterKey);
  const pairs = Array.isArray(rows) ? rows : [];
  if (key === "all") return pairs;
  return pairs.filter(row => {
    const recommendation = stringValue(row?.currentRecommendation);
    const reviewState = stringValue(row?.reviewState, "new");
    const override = stringValue(row?.manualSuppressionOverride, "none");
    const staticOnlyCount = numberValue(row?.staticOnlyDetectedRunCount);
    const providerUnstableCount = numberValue(row?.providerUnstableRunCount);
    const lastProposal = stringValue(row?.lastProposal);
    if (key === "needs_action") return reviewState === "new" || reviewState === "acknowledged";
    if (key === "stable_safe") return recommendation === "stable_safe_redundant";
    if (key === "static_only_detected") return recommendation === "static_only_detected" || staticOnlyCount > 0;
    if (key === "provider_unstable") return providerUnstableCount > 0 || lastProposal === "provider_unstable";
    if (key === "force_paused") return override === "force_pause";
    if (key === "snoozed") return reviewState === "snoozed";
    if (key === "reviewed") return reviewState === "reviewed";
    return true;
  });
}

function renderFilterButtons(rows, selectedFilter) {
  return SOURCE_POLICY_REVIEW_FILTERS.map(filter => {
    const active = filter.key === selectedFilter ? " active" : "";
    const count = filterSourcePolicyReviewPairs(rows, filter.key).length;
    return `
      <button
        type="button"
        class="btn back-btn admin-source-policy-filter-btn${active}"
        data-ui="${FILTER_TOKEN}"
        data-source-policy-filter="${escapeHtml(filter.key)}"
      >${escapeHtml(filter.label)} (${count.toLocaleString()})</button>
    `;
  }).join("");
}

function getSelectedStaticIds(reviewEl) {
  try {
    const parsed = JSON.parse(reviewEl?.dataset?.sourcePolicySelected || "[]");
    return Array.isArray(parsed) ? parsed.map(id => String(id)).filter(Boolean) : [];
  } catch {
    return [];
  }
}

export function renderSourcePolicyBulkToolbar(selectedCount = 0) {
  return `
    <div class="admin-source-policy-bulk-bar" role="group" aria-label="Bulk review actions">
      <span class="muted" data-source-policy-bulk-count>${selectedCount.toLocaleString()} selected</span>
      <button
        type="button"
        class="btn back-btn admin-source-policy-bulk-btn"
        data-source-policy-bulk-action="acknowledge"
        ${selectedCount ? "" : "disabled"}
      >Acknowledge selected</button>
      <button
        type="button"
        class="btn back-btn admin-source-policy-bulk-btn"
        data-source-policy-bulk-action="snooze"
        ${selectedCount ? "" : "disabled"}
      >Snooze selected 7d</button>
    </div>
  `;
}

function renderSourcePolicyReviewRow(row, index, { checked = false } = {}) {
  const staticName = stringValue(row?.staticSourceName, stringValue(row?.staticSourceId, "Unknown static source"));
  const staticId = stringValue(row?.staticSourceId, "unknown-static");
  const providerName = stringValue(row?.providerSourceName, stringValue(row?.providerSourceId, "Unknown provider"));
  const providerId = stringValue(row?.providerSourceId, "unknown-provider");
  const recommendation = formatMachineLabel(row?.currentRecommendation);
  const action = formatMachineLabel(row?.currentRecommendedAction);
  const reviewState = formatMachineLabel(row?.reviewState, "new");
  const override = formatMachineLabel(row?.manualSuppressionOverride, "none");
  const lastProposal = formatMachineLabel(row?.lastProposal);
  const lastAuditStatus = formatMachineLabel(row?.lastAuditStatus);
  const rowActions = getSourcePolicyReviewActions(row);
  const actionButtons = rowActions.map(rowAction => `
    <button
      type="button"
      class="btn back-btn admin-source-policy-action-btn"
      data-ui="${ACTION_TOKEN}"
      data-source-policy-action="${escapeHtml(rowAction.key)}"
      data-source-policy-index="${index}"
      ${tooltipAttrs(`${rowAction.label}: apply this source-policy review decision.`)}
    >${escapeHtml(rowAction.label)}</button>
  `).join("");
  return `
    <div class="admin-source-policy-row" data-source-policy-index="${index}">
      <div class="admin-source-policy-row-main admin-source-policy-row-main-selectable">
        <label class="admin-source-policy-select-label">
          <input
            type="checkbox"
            class="admin-source-policy-select-box"
            data-source-policy-static-id="${escapeHtml(staticId)}"
            ${checked ? "checked" : ""}
          />
        </label>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(staticName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(staticId)}</div>
        </div>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(providerName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(providerId)}</div>
        </div>
      </div>
      <div class="admin-source-policy-meta">
        ${renderMetaSpans([
          { label: "Recommendation", value: recommendation },
          { label: "Action", value: action },
          { label: "Confidence", value: formatPercent(row?.confidence) },
          { label: "Review", value: reviewState },
          { label: "Override", value: override },
          { label: "Snoozed until", value: formatOptionalDate(row?.snoozedUntil) },
          { label: "Safe runs", value: numberValue(row?.safeRunCount).toLocaleString() },
          { label: "Safe streak", value: numberValue(row?.consecutiveSafeRunCount).toLocaleString() },
          { label: "Static-only runs", value: numberValue(row?.staticOnlyDetectedRunCount).toLocaleString() },
          { label: "Provider unstable runs", value: numberValue(row?.providerUnstableRunCount).toLocaleString() },
          { label: "Last proposal", value: lastProposal },
          { label: "Last audit", value: lastAuditStatus }
        ])}
      </div>
      <div class="admin-source-policy-actions">${actionButtons}</div>
    </div>
  `;
}

function rawEvidenceList(items) {
  const values = listValue(items).map(item => stringValue(item)).filter(Boolean);
  if (!values.length) return "None";
  return values.map(item => formatMachineLabel(item)).join(", ");
}

function formatMigrationLinkDisambiguationBlockerCounts(counts) {
  const values = objectValue(counts);
  const entries = Object.entries(values)
    .map(([key, value]) => [String(key), numberValue(value)])
    .filter(([, value]) => value > 0)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
  if (!entries.length) return "none";
  return entries.map(([key, value]) => `${formatMachineLabel(key)} ${value.toLocaleString()}`).join(", ");
}

function formatMigrationLinkDisambiguationBlockedExamples(rows) {
  const examples = Array.isArray(rows) ? rows : [];
  if (!examples.length) return "none";
  return examples.slice(0, 5).map(example => {
    const providerName = stringValue(example?.providerSourceName, stringValue(example?.providerSourceId, "Unknown provider"));
    const staticName = selectedStaticSourceName(example);
    const blockers = listValue(example?.disambiguationBlockers).map(item => stringValue(item).replaceAll("_", " ")).filter(Boolean);
    return `${providerName} / ${staticName} (${blockers.join(", ") || "none"})`;
  }).join(" | ");
}

function renderMigrationLinkReviewCandidate(candidate, index) {
  const providerName = stringValue(candidate?.providerSourceName, stringValue(candidate?.providerSourceId, "Unknown provider"));
  const providerId = stringValue(candidate?.providerSourceId, "unknown-provider");
  const staticName = selectedStaticSourceName(candidate);
  const staticId = selectedStaticSourceId(candidate);
  const staticUrl = stringValue(candidate?.selectedStaticUrl);
  const tier = stringValue(candidate?.confidenceTier, "medium");
  const copy = tier === "high"
    ? "High-confidence exact-evidence candidate."
    : "Medium-confidence candidate. Review evidence before applying.";
  const sourceState = objectValue(candidate?.sourceStateEvidence || candidate);
  const ignoredAlternatives = listValue(candidate?.ignoredAlternatives);
  const actions = getMigrationLinkReviewActions(candidate);
  const actionButtons = actions.map(action => `
    <button
      type="button"
      class="btn back-btn admin-source-policy-migration-link-action-btn"
      data-ui="${MIGRATION_LINK_ACTION_TOKEN}"
      data-source-policy-migration-link-action="${escapeHtml(action.key)}"
      data-source-policy-migration-link-kind="review"
      data-source-policy-migration-link-index="${index}"
      ${tooltipAttrs(`${action.label}: update this migration-link review state.`)}
    >${escapeHtml(action.label)}</button>
  `).join("");
  const unavailableReason = migrationLinkActionUnavailableReason(candidate);
  return `
    <div class="admin-source-policy-row admin-source-policy-migration-link-row" data-source-policy-migration-link-index="${index}">
      <div class="admin-source-policy-row-main">
        <div>
          <div class="admin-source-policy-name">${escapeHtml(providerName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(providerId)}</div>
        </div>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(staticName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(staticId)}</div>
          ${staticUrl ? `<div class="admin-source-policy-id">${escapeHtml(staticUrl)}</div>` : ""}
        </div>
      </div>
      <div class="admin-source-policy-copy">${escapeHtml(copy)}</div>
      <div class="admin-source-policy-meta">
        ${renderMetaSpans([
          { label: "Confidence", value: formatPercent(candidate?.confidence) },
          { label: "Tier", value: formatMachineLabel(tier) },
          { label: "API eligible", value: candidate?.apiEligible === true ? "Yes" : "No" },
          { label: "Why not high", value: stringValue(candidate?.whyNotHighConfidence, "None") },
          { label: "Evidence", value: rawEvidenceList(candidate?.evidenceReasons) },
          { label: "Last kept", value: numberValue(sourceState.lastKeptCount).toLocaleString() },
          { label: "Last status", value: formatMachineLabel(sourceState.lastStatus) },
          { label: "Evidence score", value: numberValue(sourceState.evidenceScore).toLocaleString() },
          { label: "Ignored alternatives", value: ignoredAlternatives.length.toLocaleString() }
        ])}
      </div>
      <div class="admin-source-policy-actions">${actionButtons || `<span class="muted">${escapeHtml(unavailableReason)}</span>`}</div>
    </div>
  `;
}

function renderBlockedMigrationLinkCandidate(candidate, index) {
  const providerName = stringValue(candidate?.providerSourceName, stringValue(candidate?.providerSourceId, "Unknown provider"));
  const providerId = stringValue(candidate?.providerSourceId, "unknown-provider");
  const staticName = selectedStaticSourceName(candidate);
  const staticId = selectedStaticSourceId(candidate);
  const staticUrl = stringValue(candidate?.selectedStaticUrl, stringValue(candidate?.staticUrl));
  const sourceState = objectValue(candidate?.sourceStateEvidence || candidate);
  const blockers = listValue(candidate?.blockers);
  const evidenceReasons = listValue(candidate?.evidenceReasons);
  const disambiguationBlockers = listValue(candidate?.disambiguationBlockers);
  const ignoredAlternatives = listValue(candidate?.ignoredAlternatives);
  const providerCoverageStatus = stringValue(sourceState.providerCoverageStatus);
  const providerCoverageConsecutiveSuccesses = numberValue(sourceState.providerCoverageConsecutiveSuccesses);
  const providerCoverageLatestKeptCount = numberValue(sourceState.providerCoverageLatestKeptCount);
  return `
    <div class="admin-source-policy-row admin-source-policy-migration-link-row" data-source-policy-migration-link-index="${index}">
      <div class="admin-source-policy-row-main">
        <div>
          <div class="admin-source-policy-name">${escapeHtml(providerName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(providerId)}</div>
        </div>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(staticName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(staticId)}</div>
          ${staticUrl ? `<div class="admin-source-policy-id">${escapeHtml(staticUrl)}</div>` : ""}
        </div>
      </div>
      <div class="admin-source-policy-copy">Blocked candidate. Review the blocker evidence before any link is applied.</div>
      <div class="admin-source-policy-meta">
        ${renderMetaSpans([
          { label: "Confidence", value: formatPercent(candidate?.confidence) },
          { label: "API eligible", value: candidate?.apiEligible === true ? "Yes" : "No" },
          { label: "Blockers", value: rawEvidenceList(blockers) },
          { label: "Evidence", value: rawEvidenceList(evidenceReasons) },
          { label: "Disambiguation", value: rawEvidenceList(disambiguationBlockers) },
          { label: "Last kept", value: numberValue(sourceState.lastKeptCount).toLocaleString() },
          { label: "Last status", value: formatMachineLabel(sourceState.lastStatus) },
          { label: "Last successful", value: formatMachineLabel(sourceState.lastSuccessfulAt) },
          { label: "Last fetched", value: formatMachineLabel(sourceState.lastFetchedAt) },
          { label: "Evidence score", value: numberValue(sourceState.evidenceScore).toLocaleString() },
          { label: "Coverage status", value: formatMachineLabel(providerCoverageStatus) },
          { label: "Coverage successes", value: providerCoverageConsecutiveSuccesses.toLocaleString() },
          { label: "Coverage latest kept", value: providerCoverageLatestKeptCount.toLocaleString() },
          { label: "Ignored alternatives", value: ignoredAlternatives.length.toLocaleString() }
        ])}
      </div>
      <div class="admin-source-policy-actions"><span class="muted">Read-only blocked candidate.</span></div>
    </div>
  `;
}

function renderLinkedMigrationIdentityRow(candidate, index) {
  const providerName = stringValue(candidate?.providerSourceName, stringValue(candidate?.providerSourceId, "Unknown provider"));
  const providerId = stringValue(candidate?.providerSourceId, "unknown-provider");
  const staticName = selectedStaticSourceName(candidate);
  const staticId = selectedStaticSourceId(candidate);
  const actions = getMigrationLinkLinkedActions(candidate);
  const actionButtons = actions.map(action => `
    <button
      type="button"
      class="btn back-btn admin-source-policy-migration-link-action-btn"
      data-ui="${MIGRATION_LINK_ACTION_TOKEN}"
      data-source-policy-migration-link-action="${escapeHtml(action.key)}"
      data-source-policy-migration-link-kind="linked"
      data-source-policy-migration-link-index="${index}"
      ${tooltipAttrs(`${action.label}: update this linked migration identity.`)}
    >${escapeHtml(action.label)}</button>
  `).join("");
  return `
    <div class="admin-source-policy-row admin-source-policy-migration-link-row" data-source-policy-migration-link-index="${index}">
      <div class="admin-source-policy-row-main">
        <div>
          <div class="admin-source-policy-name">${escapeHtml(providerName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(providerId)}</div>
        </div>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(staticName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(staticId)}</div>
        </div>
      </div>
      <div class="admin-source-policy-copy">
        Linked migration identity. Suppression evidence requires repeated successful provider fetches; one validated fetch may not be enough.
      </div>
      <div class="admin-source-policy-meta">
        ${renderMetaSpans([
          { label: "Bucket", value: formatMachineLabel(candidate?.providerBucket) },
          { label: "Linked by", value: formatMachineLabel(candidate?.migrationLinkedBy) },
          { label: "Admin-owned", value: candidate?.adminBackfillOwned === true ? "Yes" : "No" },
          { label: "Coverage", value: formatMachineLabel(candidate?.providerCoverageStatus) },
          { label: "Success streak", value: numberValue(candidate?.providerCoverageConsecutiveSuccesses).toLocaleString() },
          { label: "Latest kept", value: numberValue(candidate?.providerCoverageLatestKeptCount).toLocaleString() },
          { label: "Readiness", value: formatMachineLabel(candidate?.providerReplacementReadiness) }
        ])}
      </div>
      <div class="admin-source-policy-actions">${actionButtons || '<span class="muted">Linked, but not clearable by this Admin action.</span>'}</div>
    </div>
  `;
}

function renderMigrationLinkReviewSection(candidates, linkedCandidates) {
  const rows = Array.isArray(candidates) ? candidates : [];
  const linkedRows = Array.isArray(linkedCandidates) ? linkedCandidates : [];
  const content = rows.length
    ? rows.map((candidate, index) => renderMigrationLinkReviewCandidate(candidate, index)).join("")
    : '<div class="muted">No migration link review candidates are available.</div>';
  const linkedContent = linkedRows.length
    ? linkedRows.map((candidate, index) => renderLinkedMigrationIdentityRow(candidate, index)).join("")
    : '<div class="muted">No linked migration identities are available.</div>';
  return `
    <div class="admin-source-policy-migration-link-review">
      <h4>Migration Link Review</h4>
      <div class="admin-source-policy-list">${content}</div>
      <h4>Linked Migration Identities</h4>
      <div class="admin-source-policy-copy">
        Admin-owned links stay visible here so they can be cleared after fetch/soak evidence.
      </div>
      <div class="admin-source-policy-list">${linkedContent}</div>
    </div>
  `;
}

function renderBlockedMigrationLinkSection(blockedCandidates, linkBackfill = {}) {
  const rows = Array.isArray(blockedCandidates) ? blockedCandidates : [];
  const disambiguationSummary = formatMigrationLinkDisambiguationBlockerCounts(linkBackfill?.disambiguationBlockerCounts);
  const disambiguationExamples = formatMigrationLinkDisambiguationBlockedExamples(
    linkBackfill?.disambiguationBlockedExamples || rows
  );
  const content = rows.length
    ? rows.map((candidate, index) => renderBlockedMigrationLinkCandidate(candidate, index)).join("")
    : '<div class="muted">No blocked migration link candidates are available.</div>';
  return `
    <div class="admin-source-policy-migration-link-review">
      <h4>Blocked Migration Link Candidates</h4>
      <div class="admin-source-policy-copy">
        These provider/static pairs are evidence-backed but not yet reviewable. Apply actions stay limited to API-eligible review candidates.
        Disambiguation blockers: ${escapeHtml(disambiguationSummary)}. Examples: ${escapeHtml(disambiguationExamples)}.
      </div>
      <div class="admin-source-policy-list">${content}</div>
    </div>
  `;
}

function renderSuppressionEligibilityRow(row) {
  const providerName = stringValue(row?.providerSourceName, stringValue(row?.providerSourceId, "Unknown provider"));
  const providerId = stringValue(row?.providerSourceId, "unknown-provider");
  const staticName = stringValue(row?.migrationSourceName, stringValue(row?.staticSourceName, stringValue(row?.migrationSourceIdentity, "Unknown static source")));
  const staticId = stringValue(row?.migrationSourceIdentity, stringValue(row?.staticSourceId, "unknown-static"));
  const selectedLabel = row?.linkedStaticFoundInSelectedSources || row?.foundInSourceRows || row?.linkedStaticSelected ? "yes" : "no";
  const defaultLoaderLabel = row?.foundInDefaultLoaders ? "yes" : "no";
  const cacheLabel = row?.excludedByCadenceOrCache ? "yes" : "no";
  const onlySourcesLabel = row?.onlySourcesMode ? "yes" : "no";
  const registryBucket = row?.linkedStaticRegistryBucket || row?.registryBucket;
  const registryState = row?.linkedStaticRegistryState || row?.registryState;
  const staticAdapter = row?.linkedStaticAdapter || row?.adapter;
  const hiddenLabel = row?.linkedStaticHiddenFromDefault || row?.hiddenFromDefault ? "yes" : "no";
  const pendingReason = row?.linkedStaticPendingReason || row?.pendingReason;
  const duplicateOfSourceId = row?.linkedStaticDuplicateOfSourceId || row?.duplicateOfSourceId;
  const expectedLoaderName = row?.expectedStaticLoaderName || row?.expectedLoaderName;
  const generatedLoaderName = row?.generatedStaticLoaderName;
  const actualSourceRowName = row?.actualSourceRowName || row?.loaderName;
  const possibleLoaderNames = Array.isArray(row?.possibleLoaderNames) ? row.possibleLoaderNames.join(", ") : "";
  return `
    <div class="admin-source-policy-row admin-source-policy-suppression-eligibility-row">
      <div class="admin-source-policy-row-main">
        <div>
          <div class="admin-source-policy-name">${escapeHtml(providerName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(providerId)}</div>
        </div>
        <div>
          <div class="admin-source-policy-name">${escapeHtml(staticName)}</div>
          <div class="admin-source-policy-id">${escapeHtml(staticId)}</div>
        </div>
      </div>
      <div class="admin-source-policy-copy">
        Provider ready, static not selected. Runtime suppression can emit a row only when the linked static source is selected in the current fetch.
      </div>
      <div class="admin-source-policy-meta">
        ${renderMetaSpans([
          { label: "Selection reason", value: formatMachineLabel(row?.selectionReason || row?.reason) },
          { label: "Readiness", value: formatMachineLabel(row?.providerReplacementReadiness) },
          { label: "Coverage", value: formatMachineLabel(row?.providerCoverageStatus) },
          { label: "Success streak", value: numberValue(row?.providerCoverageConsecutiveSuccesses).toLocaleString() },
          { label: "Latest kept", value: numberValue(row?.providerCoverageLatestKeptCount).toLocaleString() },
          { label: "Bucket", value: formatMachineLabel(registryBucket) },
          { label: "Registry state", value: formatMachineLabel(registryState) },
          { label: "Adapter", value: formatMachineLabel(staticAdapter) },
          { label: "Hidden", value: hiddenLabel },
          { label: "Pending reason", value: formatMachineLabel(pendingReason) },
          { label: "Duplicate of", value: stringValue(duplicateOfSourceId, "none") },
          { label: "Loader match", value: formatMachineLabel(row?.loaderNameMatchStatus) },
          { label: "Expected loader", value: stringValue(expectedLoaderName, "unknown") },
          { label: "Generated loader", value: stringValue(generatedLoaderName, "unknown") },
          { label: "Possible loaders", value: stringValue(possibleLoaderNames, "none") },
          { label: "Actual source row", value: stringValue(actualSourceRowName, "none") },
          { label: "Loader not generated", value: formatMachineLabel(row?.loaderNotGeneratedReason) },
          { label: "Selected", value: selectedLabel },
          { label: "Default loader", value: defaultLoaderLabel },
          { label: "Cache/cadence", value: cacheLabel },
          { label: "Only sources", value: onlySourcesLabel }
        ])}
      </div>
    </div>
  `;
}

function renderSuppressionEligibilitySection(rows) {
  const diagnostics = Array.isArray(rows) ? rows : [];
  if (!diagnostics.length) return "";
  return `
    <div class="admin-source-policy-suppression-eligibility">
      <h4>Suppression Eligibility Visibility</h4>
      <div class="admin-source-policy-copy">
        These diagnostics are read-only. They explain why a ready linked provider did not emit a dynamic redundant-static suppression row in the latest fetch.
      </div>
      <div class="admin-source-policy-list">${diagnostics.map(renderSuppressionEligibilityRow).join("")}</div>
    </div>
  `;
}

export function renderAdminSourcePolicyReview(reviewEl, payload, options = {}) {
  if (!reviewEl) return;
  const rows = getPairs(payload);
  const linkBackfill = objectValue(payload?.providerCoverageLinkBackfill);
  const migrationLinkCandidates = getMigrationLinkReviewCandidates(payload);
  const blockedMigrationLinkCandidates = getMigrationLinkBlockedCandidates(payload);
  const linkedMigrationCandidates = getMigrationLinkLinkedCandidates(payload);
  const suppressionEligibilityRows = getSuppressionEligibilityRows(payload);
  const selectedFilter = normalizeFilterKey(options?.selectedFilter);
  const filteredRows = filterSourcePolicyReviewPairs(rows, selectedFilter);
  const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
  const signature = stableOpsSignature({
    selectedFilter,
    rows,
    migrationLinkCandidates,
    blockedMigrationLinkCandidates,
    migrationLinkDisambiguationBlockerCounts: linkBackfill.disambiguationBlockerCounts || {},
    migrationLinkDisambiguationBlockedExamples: linkBackfill.disambiguationBlockedExamples || [],
    linkedMigrationCandidates,
    suppressionEligibilityRows
  });
  if (canPatchInPlace && reviewEl.dataset.sourcePolicyReviewSig === signature) return;
  if (canPatchInPlace) reviewEl.dataset.sourcePolicyReviewSig = signature;

  const filteredEntries = rows
    .map((row, index) => ({ row, index }))
    .filter(entry => filteredRows.includes(entry.row));
  const selectedIds = getSelectedStaticIds(reviewEl);
  const emptyText = rows.length
    ? "No recommendation pairs match this filter."
    : "No source-policy recommendations are available yet.";
  reviewEl.innerHTML = `
    <div class="admin-source-policy-copy">
      Recommendations are advisory. Review state is local to this machine, included in explicit backups, and not source-synced. Force pause is reversible and conservative. No action deletes or hides sources. Admin review is optional for normal app improvement.
    </div>
    <div class="saved-custom-filter-actions admin-source-policy-filters" role="group" aria-label="Source policy review filter">
      ${renderFilterButtons(rows, selectedFilter)}
    </div>
    ${renderSourcePolicyBulkToolbar(
      filteredEntries.filter(entry => selectedIds.includes(stringValue(entry.row?.staticSourceId))).length
    )}
    <div class="admin-source-policy-list">
      ${filteredEntries.length
        ? filteredEntries.map(entry => renderSourcePolicyReviewRow(entry.row, entry.index, {
            checked: selectedIds.includes(stringValue(entry.row?.staticSourceId))
          })).join("")
        : `<div class="muted">${escapeHtml(emptyText)}</div>`}
    </div>
    ${renderMigrationLinkReviewSection(migrationLinkCandidates, linkedMigrationCandidates)}
    ${renderBlockedMigrationLinkSection(blockedMigrationLinkCandidates, linkBackfill)}
    ${renderSuppressionEligibilitySection(suppressionEligibilityRows)}
  `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
  const syncBulkBar = () => {
    const count = reviewEl.querySelectorAll(".admin-source-policy-select-box:checked").length;
    reviewEl.querySelectorAll("[data-source-policy-bulk-count]").forEach(el => {
      el.textContent = `${count.toLocaleString()} selected`;
    });
    reviewEl.querySelectorAll(".admin-source-policy-bulk-btn").forEach(btn => {
      if (count > 0) btn.removeAttribute("disabled");
      else btn.setAttribute("disabled", "");
    });
  };
  const persistSelection = () => {
    if (!canPatchInPlace) return;
    const checked = [...reviewEl.querySelectorAll(".admin-source-policy-select-box:checked")]
      .map(box => stringValue(box.dataset.sourcePolicyStaticId));
    reviewEl.dataset.sourcePolicySelected = JSON.stringify(checked);
  };
  reviewEl.querySelectorAll(".admin-source-policy-select-box").forEach(box => {
    box.addEventListener("change", () => {
      persistSelection();
      syncBulkBar();
    });
  });
  reviewEl.querySelectorAll(".admin-source-policy-bulk-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const action = stringValue(btn.dataset.sourcePolicyBulkAction);
      persistSelection();
      const currentIds = getSelectedStaticIds(reviewEl);
      const selectedRows = filteredEntries
        .filter(entry => currentIds.includes(stringValue(entry.row?.staticSourceId)))
        .map(entry => entry.row);
      if (!action || !selectedRows.length || typeof options.onSourcePolicyBulkAction !== "function") return;
      options.onSourcePolicyBulkAction(action, selectedRows);
    });
  });
  reviewEl.querySelectorAll(ui(FILTER_TOKEN)).forEach(btn => {
    btn.addEventListener("click", () => {
      const filter = normalizeFilterKey(btn.dataset.sourcePolicyFilter);
      if (typeof options.onSourcePolicyFilter === "function") {
        options.onSourcePolicyFilter(filter);
      }
    });
  });
  reviewEl.querySelectorAll(ui(ACTION_TOKEN)).forEach(btn => {
    btn.addEventListener("click", () => {
      const index = Number(btn.dataset.sourcePolicyIndex || -1);
      const row = rows[index];
      const action = stringValue(btn.dataset.sourcePolicyAction);
      if (row && action && typeof options.onSourcePolicyAction === "function") {
        options.onSourcePolicyAction(row, action);
      }
    });
  });
  reviewEl.querySelectorAll(ui(MIGRATION_LINK_ACTION_TOKEN)).forEach(btn => {
    btn.addEventListener("click", () => {
      const index = Number(btn.dataset.sourcePolicyMigrationLinkIndex || -1);
      const kind = stringValue(btn.dataset.sourcePolicyMigrationLinkKind, "review");
      const sourceRows = kind === "linked" ? linkedMigrationCandidates : migrationLinkCandidates;
      const candidate = sourceRows[index];
      const action = stringValue(btn.dataset.sourcePolicyMigrationLinkAction);
      if (candidate && action && typeof options.onMigrationLinkAction === "function") {
        options.onMigrationLinkAction(candidate, action);
      }
    });
  });
}
