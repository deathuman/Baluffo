import { escapeHtml } from "../../shared/ui/index.js";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { formatDateTime, stableOpsSignature } from "./ops-shared.js";

export const SOURCE_POLICY_REVIEW_FILTERS = Object.freeze([
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

export function renderAdminSourcePolicyReview(reviewEl, payload, options = {}) {
  if (!reviewEl) return;
  const rows = getPairs(payload);
  const selectedFilter = normalizeFilterKey(options?.selectedFilter);
  const filteredRows = filterSourcePolicyReviewPairs(rows, selectedFilter);
  const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
  const signature = stableOpsSignature({
    selectedFilter,
    rows
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
      Recommendations are advisory. Force pause is reversible and conservative. No action deletes or hides sources. Admin review is optional for normal app improvement.
    </div>
    <div class="saved-custom-filter-actions admin-source-policy-filters" role="group" aria-label="Source policy review filter">
      ${renderFilterButtons(rows, selectedFilter)}
    </div>
    <div class="admin-source-policy-list">
      ${filteredEntries.length
        ? filteredEntries.map(entry => renderSourcePolicyReviewRow(entry.row, entry.index)).join("")
        : `<div class="muted">${escapeHtml(emptyText)}</div>`}
    </div>
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
}
