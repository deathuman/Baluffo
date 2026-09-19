/**
 * Registry conflict renderer — Adjudication / safe-automation toolbars and suppressed-board evidence.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-actions
 */

import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js";
import { stringValue } from "../../shared/format-utils.js";
import { CHECK_TOKEN } from "./registry-conflicts-constants.js";
import {
  adjudicationValue,
  eligibleSafeAutomations,
  formatFieldValue,
  numberValue,
  objectValue,
  renderRunningAdjudicationStatus
} from "./registry-conflicts-model.js";

function renderSafeAutomationToolbar(visibleConflicts, disabled = false) {
  const eligible = eligibleSafeAutomations(visibleConflicts);
  if (!eligible.length) return "";
  const actions = new Map();
  eligible.forEach(row => {
    const action = stringValue(row.safeAutomation.action, "auto_demote_same_adapter_provider_alias");
    if (!actions.has(action)) {
      actions.set(action, {
        action,
        label: row.safeAutomation.label || "Apply safe demotions",
        route: row.safeAutomation.route || "/registry/conflicts/auto-demote-safe",
        targetIds: []
      });
    }
    actions.get(action).targetIds.push(...row.safeAutomation.targetIds);
  });
  const totalTargetCount = eligible.flatMap(row => row.safeAutomation.targetIds).length;
  const buttons = [...actions.values()].map(entry => `
    <button
      type="button"
      class="btn back-btn admin-registry-conflict-safe-automation-btn"
      data-registry-conflict-safe-automation-card-index="-1"
      data-registry-conflict-safe-automation-action="${escapeHtml(entry.action)}"
      data-registry-conflict-safe-automation-route="${escapeHtml(entry.route)}"
      data-registry-conflict-safe-automation-ids="${escapeHtml(entry.targetIds.join(","))}"
      ${tooltipAttrs(`${entry.label}: apply this safe automation to ${entry.targetIds.length.toLocaleString()} visible source rows.`)}
      ${disabled ? "disabled" : ""}
    >${escapeHtml(entry.label)} · ${entry.targetIds.length.toLocaleString()}</button>
  `).join("");
  return `
    <div class="admin-registry-conflict-action-group">
      <div>
        <div class="admin-registry-conflict-action-title">Safe automation</div>
        <div class="admin-registry-conflict-action-summary">${eligible.length.toLocaleString()} visible conflict family can be auto-demoted safely; ${totalTargetCount.toLocaleString()} row target.</div>
      </div>
      <div class="admin-registry-conflict-actions">${buttons}</div>
    </div>
  `;
}

function renderAdjudicationToolbar(payload, visibleConflicts, checkingConflicts) {
  const adjudication = adjudicationValue(payload);
  const running = checkingConflicts || stringValue(adjudication?.status) === "running";
  const applyAutopilot = Boolean(adjudication?.applyAutopilot);
  const checkedAt = stringValue(adjudication?.finishedAt, "");
  const demoted = Number(adjudication?.demoted || 0);
  const recommended = Number(objectValue(adjudication?.summary)?.recommendedDemotion || 0);
  const disabled = running || !visibleConflicts.length;
  const neverChecked = !checkedAt && !demoted && !recommended;
  const startHereBadge = !running && neverChecked && visibleConflicts.length > 0
    ? '<span class="admin-registry-conflict-triage-badge">Recommended first step</span>'
    : "";
  const checkLabel = running && !applyAutopilot ? "Checking conflicts..." : "Check conflicting sources";
  const applyLabel = running && applyAutopilot
    ? "Applying recommendations..."
    : "Apply high-confidence recommendations";
  const checkTooltip = running
    ? "Conflict source check is already running."
    : !visibleConflicts.length
      ? "No visible conflicts to check."
      : "Check whether visible conflict sources can be resolved safely.";
  const applyTooltip = running && applyAutopilot
    ? "Conflict recommendation apply is already running."
    : running
      ? "Conflict source check is already running."
      : !visibleConflicts.length
      ? "No visible conflicts to apply."
      : "Apply only high-confidence conflict recommendations.";
  const statusCopy = running
    ? renderRunningAdjudicationStatus(adjudication)
    : `${checkedAt ? `Last checked ${escapeHtml(formatFieldValue("finishedAt", checkedAt))}; ` : "No conflict source check has run yet. "}${demoted.toLocaleString()} demoted, ${recommended.toLocaleString()} recommended.`;
  return `
    <div class="admin-registry-conflict-action-group${startHereBadge ? " admin-registry-conflict-action-group-start" : ""}">
      <div>
        <div class="admin-registry-conflict-action-title">Conflict source checks ${startHereBadge}</div>
        <div class="admin-registry-conflict-action-summary">
          ${statusCopy}
        </div>
      </div>
      <div class="admin-registry-conflict-actions">
        <button
          type="button"
          class="btn back-btn"
          data-ui="${CHECK_TOKEN}"
          data-registry-conflict-apply-autopilot="false"
          ${tooltipAttrs(checkTooltip)}
          ${disabled ? "disabled" : ""}
        >${escapeHtml(checkLabel)}</button>
        <button
          type="button"
          class="btn back-btn"
          data-ui="${CHECK_TOKEN}"
          data-registry-conflict-apply-autopilot="true"
          ${tooltipAttrs(applyTooltip)}
          ${disabled ? "disabled" : ""}
        >${escapeHtml(applyLabel)}</button>
      </div>
    </div>
  `;
}

function renderRegistryConflictActionStrip(payload, visibleConflicts, checkingConflicts) {
  return `
    <div class="admin-registry-conflict-action-strip">
      ${renderAdjudicationToolbar(payload, visibleConflicts, checkingConflicts)}
      ${renderSafeAutomationToolbar(visibleConflicts, checkingConflicts)}
    </div>
  `;
}

function renderSuppressedIndependentProviderBoards(payload) {
  const audit = objectValue(payload?.suppressedIndependentProviderBoards);
  const summary = objectValue(audit?.summary);
  const families = Array.isArray(audit?.families) ? audit.families : [];
  const familyCount = numberValue(summary?.familyCount || families.length);
  const rowCount = numberValue(summary?.rowCount);
  if (!familyCount || !families.length) return "";
  const rows = families
    .slice(0, 12)
    .map(family => {
      const sourceIds = Array.isArray(family?.sourceIds) ? family.sourceIds : [];
      const sourceText = sourceIds.length ? sourceIds.join(" | ") : "none";
      const adapter = stringValue(family?.adapter, "provider");
      const reason = stringValue(family?.evidenceReason, "independent job-set evidence")
        .replaceAll("_", " ");
      return `
        <div class="admin-registry-conflict-triage-card">
          <span class="admin-registry-conflict-triage-badge">${escapeHtml(stringValue(family?.familyKey, "unknown family"))}</span>
          <p>${escapeHtml(adapter)} sources suppressed from duplicate review: ${escapeHtml(sourceText)}.</p>
          <p>${escapeHtml(reason)}</p>
        </div>
      `;
    })
    .join("");
  return `
    <details class="admin-registry-conflict-detail admin-registry-conflict-suppressed-independent">
      <summary>${escapeHtml(familyCount.toLocaleString())} independent provider board ${familyCount === 1 ? "family" : "families"} suppressed · ${escapeHtml(rowCount.toLocaleString())} source ${rowCount === 1 ? "row" : "rows"}</summary>
      <div class="admin-registry-conflict-detail-body">
        ${rows}
      </div>
    </details>
  `;
}

export {
  renderRegistryConflictActionStrip,
  renderSuppressedIndependentProviderBoards
};
