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

function renderSourcePolicyReviewRow(row, index) {
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
      <div class="admin-source-policy-row-main">
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
        <span><strong>Recommendation</strong> ${escapeHtml(recommendation)}</span>
        <span><strong>Action</strong> ${escapeHtml(action)}</span>
        <span><strong>Confidence</strong> ${escapeHtml(formatPercent(row?.confidence))}</span>
        <span><strong>Review</strong> ${escapeHtml(reviewState)}</span>
        <span><strong>Override</strong> ${escapeHtml(override)}</span>
        <span><strong>Snoozed until</strong> ${escapeHtml(formatOptionalDate(row?.snoozedUntil))}</span>
        <span><strong>Safe runs</strong> ${numberValue(row?.safeRunCount).toLocaleString()}</span>
        <span><strong>Safe streak</strong> ${numberValue(row?.consecutiveSafeRunCount).toLocaleString()}</span>
        <span><strong>Static-only runs</strong> ${numberValue(row?.staticOnlyDetectedRunCount).toLocaleString()}</span>
        <span><strong>Provider unstable runs</strong> ${numberValue(row?.providerUnstableRunCount).toLocaleString()}</span>
        <span><strong>Last proposal</strong> ${escapeHtml(lastProposal)}</span>
        <span><strong>Last audit</strong> ${escapeHtml(lastAuditStatus)}</span>
      </div>
      <div class="admin-source-policy-actions">${actionButtons}</div>
    </div>
  `;
}

function renderEvidenceList(items) {
  const values = listValue(items).map(item => stringValue(item)).filter(Boolean);
  if (!values.length) return "None";
  return values.map(item => escapeHtml(formatMachineLabel(item))).join(", ");
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
        <span><strong>Confidence</strong> ${escapeHtml(formatPercent(candidate?.confidence))}</span>
        <span><strong>Tier</strong> ${escapeHtml(formatMachineLabel(tier))}</span>
        <span><strong>API eligible</strong> ${candidate?.apiEligible === true ? "Yes" : "No"}</span>
        <span><strong>Why not high</strong> ${escapeHtml(stringValue(candidate?.whyNotHighConfidence, "None"))}</span>
        <span><strong>Evidence</strong> ${renderEvidenceList(candidate?.evidenceReasons)}</span>
        <span><strong>Last kept</strong> ${numberValue(sourceState.lastKeptCount).toLocaleString()}</span>
        <span><strong>Last status</strong> ${escapeHtml(formatMachineLabel(sourceState.lastStatus))}</span>
        <span><strong>Evidence score</strong> ${numberValue(sourceState.evidenceScore).toLocaleString()}</span>
        <span><strong>Ignored alternatives</strong> ${ignoredAlternatives.length.toLocaleString()}</span>
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
        <span><strong>Confidence</strong> ${escapeHtml(formatPercent(candidate?.confidence))}</span>
        <span><strong>API eligible</strong> ${candidate?.apiEligible === true ? "Yes" : "No"}</span>
        <span><strong>Blockers</strong> ${renderEvidenceList(blockers)}</span>
        <span><strong>Evidence</strong> ${renderEvidenceList(evidenceReasons)}</span>
        <span><strong>Disambiguation</strong> ${renderEvidenceList(disambiguationBlockers)}</span>
        <span><strong>Last kept</strong> ${numberValue(sourceState.lastKeptCount).toLocaleString()}</span>
        <span><strong>Last status</strong> ${escapeHtml(formatMachineLabel(sourceState.lastStatus))}</span>
        <span><strong>Last successful</strong> ${escapeHtml(formatMachineLabel(sourceState.lastSuccessfulAt))}</span>
        <span><strong>Last fetched</strong> ${escapeHtml(formatMachineLabel(sourceState.lastFetchedAt))}</span>
        <span><strong>Evidence score</strong> ${numberValue(sourceState.evidenceScore).toLocaleString()}</span>
        <span><strong>Coverage status</strong> ${escapeHtml(formatMachineLabel(providerCoverageStatus))}</span>
        <span><strong>Coverage successes</strong> ${providerCoverageConsecutiveSuccesses.toLocaleString()}</span>
        <span><strong>Coverage latest kept</strong> ${providerCoverageLatestKeptCount.toLocaleString()}</span>
        <span><strong>Ignored alternatives</strong> ${ignoredAlternatives.length.toLocaleString()}</span>
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
        <span><strong>Bucket</strong> ${escapeHtml(formatMachineLabel(candidate?.providerBucket))}</span>
        <span><strong>Linked by</strong> ${escapeHtml(formatMachineLabel(candidate?.migrationLinkedBy))}</span>
        <span><strong>Admin-owned</strong> ${candidate?.adminBackfillOwned === true ? "Yes" : "No"}</span>
        <span><strong>Coverage</strong> ${escapeHtml(formatMachineLabel(candidate?.providerCoverageStatus))}</span>
        <span><strong>Success streak</strong> ${numberValue(candidate?.providerCoverageConsecutiveSuccesses).toLocaleString()}</span>
        <span><strong>Latest kept</strong> ${numberValue(candidate?.providerCoverageLatestKeptCount).toLocaleString()}</span>
        <span><strong>Readiness</strong> ${escapeHtml(formatMachineLabel(candidate?.providerReplacementReadiness))}</span>
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
      <div class="admin-source-policy-copy">
        Apply or clear one reviewed provider/static migration identity link at a time. This links coverage evidence only; it does not delete, hide, reject, demote, tombstone, or force-suppress any source.
      </div>
      <div class="admin-source-policy-list">${content}</div>
      <h4>Linked Migration Identities</h4>
      <div class="admin-source-policy-copy">
        Linked providers stay visible here so Admin-owned links can be cleared after fetch/soak. Suppression evidence requires repeated successful provider fetches; one validated fetch may not be enough.
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
        <span><strong>Selection reason</strong> ${escapeHtml(formatMachineLabel(row?.selectionReason || row?.reason))}</span>
        <span><strong>Readiness</strong> ${escapeHtml(formatMachineLabel(row?.providerReplacementReadiness))}</span>
        <span><strong>Coverage</strong> ${escapeHtml(formatMachineLabel(row?.providerCoverageStatus))}</span>
        <span><strong>Success streak</strong> ${numberValue(row?.providerCoverageConsecutiveSuccesses).toLocaleString()}</span>
        <span><strong>Latest kept</strong> ${numberValue(row?.providerCoverageLatestKeptCount).toLocaleString()}</span>
        <span><strong>Bucket</strong> ${escapeHtml(formatMachineLabel(registryBucket))}</span>
        <span><strong>Registry state</strong> ${escapeHtml(formatMachineLabel(registryState))}</span>
        <span><strong>Adapter</strong> ${escapeHtml(formatMachineLabel(staticAdapter))}</span>
        <span><strong>Hidden</strong> ${escapeHtml(hiddenLabel)}</span>
        <span><strong>Pending reason</strong> ${escapeHtml(formatMachineLabel(pendingReason))}</span>
        <span><strong>Duplicate of</strong> ${escapeHtml(stringValue(duplicateOfSourceId, "none"))}</span>
        <span><strong>Loader match</strong> ${escapeHtml(formatMachineLabel(row?.loaderNameMatchStatus))}</span>
        <span><strong>Expected loader</strong> ${escapeHtml(stringValue(expectedLoaderName, "unknown"))}</span>
        <span><strong>Generated loader</strong> ${escapeHtml(stringValue(generatedLoaderName, "unknown"))}</span>
        <span><strong>Possible loaders</strong> ${escapeHtml(stringValue(possibleLoaderNames, "none"))}</span>
        <span><strong>Actual source row</strong> ${escapeHtml(stringValue(actualSourceRowName, "none"))}</span>
        <span><strong>Loader not generated</strong> ${escapeHtml(formatMachineLabel(row?.loaderNotGeneratedReason))}</span>
        <span><strong>Selected</strong> ${escapeHtml(selectedLabel)}</span>
        <span><strong>Default loader</strong> ${escapeHtml(defaultLoaderLabel)}</span>
        <span><strong>Cache/cadence</strong> ${escapeHtml(cacheLabel)}</span>
        <span><strong>Only sources</strong> ${escapeHtml(onlySourcesLabel)}</span>
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
    <div class="admin-source-policy-list">
      ${filteredEntries.length
        ? filteredEntries.map(entry => renderSourcePolicyReviewRow(entry.row, entry.index)).join("")
        : `<div class="muted">${escapeHtml(emptyText)}</div>`}
    </div>
    ${renderMigrationLinkReviewSection(migrationLinkCandidates, linkedMigrationCandidates)}
    ${renderBlockedMigrationLinkSection(blockedMigrationLinkCandidates, linkBackfill)}
    ${renderSuppressionEligibilitySection(suppressionEligibilityRows)}
  `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
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
