/**
 * Registry conflict renderer — Per-row meta, rationale, actions, adjudication, and diff rendering.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-rows
 */

import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js";
import { stringValue } from "../../shared/format-utils.js";
import { ACTION_TOKEN } from "./registry-conflicts-constants.js";
import {
  familyAdjudicationValue,
  formatFieldValue,
  listValue,
  objectValue
} from "./registry-conflicts-model.js";

function renderRationaleChip(item) {
  const label = stringValue(item?.label, "Signal");
  const value = stringValue(item?.value, "—");
  return `
    <span class="admin-registry-conflict-rationale-chip">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(value)}</span>
    </span>
  `;
}

function renderRowActions(cardIndex, rowIndex, row) {
  const actions = listValue(row?.actions);
  if (!actions.length) {
    return `<span class="muted">No direct action.</span>`;
  }
  return actions
    .map((action, actionIndex) => {
      const label = stringValue(action?.label, stringValue(action?.action, "Action"));
      return `
        <button
          type="button"
          class="btn back-btn admin-registry-conflict-action-btn"
          data-ui="${ACTION_TOKEN}"
          data-registry-conflict-card-index="${cardIndex}"
          data-registry-conflict-row-index="${rowIndex}"
          data-registry-conflict-action-index="${actionIndex}"
          ${tooltipAttrs(`${label}: apply this registry conflict action.`)}
        >${escapeHtml(label)}</button>
      `;
    })
    .join("");
}

function getRowMetaItems(row) {
  // Canonical counters only (alias collapse Phase 5): the bridge wire payload
  // no longer carries the legacy alias spellings.
  const lastKeptCount = row?.lastKeptCount;
  const jobsFound = row?.jobsFound ?? row?.sampleCount ?? row?.lastJobsFound ?? lastKeptCount;
  const registryJobsFound = row?.registryJobsFound;
  const liveJobsFound = row?.liveJobsFound;
  return [
    { label: "State", value: stringValue(row?.registryState, stringValue(row?.candidateState, "unknown")), compact: true },
    { label: "Transition", value: stringValue(row?.transitionReason, "—"), compact: false },
    { label: "Health", value: stringValue(row?.health, "unknown"), compact: true },
    { label: "Health reason", value: stringValue(row?.healthReason, "—"), compact: false },
    { label: "Last success", value: formatFieldValue("lastSuccessfulFetchAt", row?.lastSuccessfulFetchAt), compact: true },
    { label: "Last seen", value: formatFieldValue("lastSeenInFetchAt", row?.lastSeenInFetchAt), compact: false },
    {
      label: row?.liveJobsFound === undefined ? "Jobs found" : "Effective jobs found",
      value: jobsFound === undefined || jobsFound === null ? "—" : stringValue(jobsFound, "0"),
      compact: true
    },
    {
      label: "Registry jobs found",
      value: registryJobsFound === undefined || registryJobsFound === null ? null : stringValue(registryJobsFound, "0"),
      compact: true
    },
    {
      label: "Live jobs found",
      value: liveJobsFound === undefined || liveJobsFound === null ? null : stringValue(liveJobsFound, "0"),
      compact: true
    },
    {
      label: "Last jobs kept",
      value: lastKeptCount === undefined || lastKeptCount === null ? "—" : stringValue(lastKeptCount, "0"),
      compact: true
    },
    { label: "Failure count", value: stringValue(row?.consecutiveFailures, "0"), compact: false },
    { label: "Zero-job streak", value: stringValue(row?.consecutiveZeroKept, "0"), compact: false }
  ];
}

function renderRowMetaItems(items) {
  return items
    .filter(item => item.value !== null)
    .map(item => `<span><strong>${escapeHtml(item.label)}</strong> ${escapeHtml(String(item.value))}</span>`)
    .join("");
}

function renderRowMeta(row, options = {}) {
  const compactOnly = Boolean(options?.compactOnly);
  const items = getRowMetaItems(row).filter(item => !compactOnly || item.compact);
  return renderRowMetaItems(items);
}

function renderRowMoreDetails(row) {
  const items = getRowMetaItems(row).filter(item => !item.compact && item.value !== null);
  if (!items.length) return "";
  return `
    <details class="admin-registry-conflict-row-details">
      <summary>More row evidence</summary>
      <div class="admin-registry-conflict-meta admin-registry-conflict-meta-secondary">
        ${renderRowMetaItems(items)}
      </div>
    </details>
  `;
}

function renderAdjudicationProbe(probe) {
  const status = probe?.ok ? "ok" : stringValue(probe?.error, "failed");
  return `
    <div class="admin-registry-conflict-triage-card">
      <span class="admin-registry-conflict-triage-badge">${escapeHtml(stringValue(probe?.name, stringValue(probe?.sourceId, "source")))}</span>
      <span>${escapeHtml(status)} · HTTP ${Number(probe?.httpStatus || 0).toLocaleString()} · jobs ${Number(probe?.jobsFound || 0).toLocaleString()}</span>
      <span>final ${escapeHtml(stringValue(probe?.finalUrl, "-"))}</span>
      ${probe?.newestJobDate ? `<span>newest ${escapeHtml(formatFieldValue("newestJobDate", probe.newestJobDate))}</span>` : ""}
    </div>
  `;
}

function renderAdjudicationDecision(decision) {
  const overlap = objectValue(decision?.overlap);
  return `
    <div class="admin-registry-conflict-triage-card">
      <span class="admin-registry-conflict-triage-badge">${escapeHtml(stringValue(decision?.status, "needs_review"))} · ${escapeHtml(stringValue(decision?.confidence, "low"))}</span>
      <span>${escapeHtml(stringValue(decision?.sourceId, "source"))}: ${escapeHtml(stringValue(decision?.reason, "No reason available."))}</span>
      <span>overlap ${Number(overlap?.ratio || 0).toLocaleString(undefined, { maximumFractionDigits: 3 })}</span>
    </div>
  `;
}

function renderAdjudicationCardInner(card) {
  const adjudication = familyAdjudicationValue(card);
  if (!Object.keys(adjudication).length) return "";
  const probes = listValue(adjudication?.probes);
  const decisions = listValue(adjudication?.decisions);
  return `
    <div class="admin-registry-conflict-triage-card">
      <span class="admin-registry-conflict-triage-badge">Adjudication · ${escapeHtml(stringValue(adjudication?.status, "checked"))} · winner ${escapeHtml(stringValue(adjudication?.winnerSourceId, "unknown"))}</span>
    </div>
    ${probes.map(renderAdjudicationProbe).join("")}
    ${decisions.map(renderAdjudicationDecision).join("")}
  `;
}

function renderConflictRow(row, cardIndex, rowIndex, role) {
  const title = stringValue(row?.name, "Unnamed source");
  const identifier = stringValue(row?.id || row?.sourceId || row?.sourceStateName, "unknown");
  const rowClass = role === "winner"
    ? "admin-registry-conflict-row admin-registry-conflict-row-winner"
    : "admin-registry-conflict-row";
  return `
    <div class="${rowClass}" data-registry-conflict-card-index="${cardIndex}" data-registry-conflict-row-index="${rowIndex}">
      <div class="admin-registry-conflict-row-main">
        <div>
          <div class="admin-registry-conflict-name">${escapeHtml(title)}</div>
          <div class="admin-registry-conflict-id">${escapeHtml(identifier)}</div>
        </div>
        <div class="admin-registry-conflict-role">${escapeHtml(role)}</div>
      </div>
      <div class="admin-registry-conflict-meta">${renderRowMeta(row, { compactOnly: true })}</div>
      ${renderRowMoreDetails(row)}
      <div class="admin-registry-conflict-actions">${renderRowActions(cardIndex, rowIndex, row)}</div>
    </div>
  `;
}

function renderConflictDiff(cardIndex, diff, winner) {
  const loserName = stringValue(diff?.loserName, stringValue(diff?.loserId, "loser"));
  const fields = listValue(diff?.fields);
  const rows = fields.length
    ? fields
        .map(field => {
          const fieldKey = stringValue(field?.key, stringValue(field?.label, "field"));
          return `
            <tr>
              <td class="admin-registry-conflict-diff-field">${escapeHtml(stringValue(field?.label, fieldKey))}</td>
              <td class="admin-registry-conflict-diff-value">${escapeHtml(
                formatFieldValue(fieldKey, field?.winnerValue)
              )}</td>
              <td class="admin-registry-conflict-diff-value">${escapeHtml(
                formatFieldValue(fieldKey, field?.loserValue)
              )}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="3" class="muted">No differing fields.</td></tr>`;
  const winnerLabel = stringValue(winner?.name, stringValue(winner?.id || winner?.sourceId, "winner"));
  const fieldCount = fields.length;
  return `
    <details class="admin-registry-conflict-diff" data-registry-conflict-card-index="${cardIndex}">
      <summary>Diffs · ${fieldCount.toLocaleString()} fields · ${escapeHtml(loserName)} vs ${escapeHtml(winnerLabel)}</summary>
      <div class="admin-registry-conflict-diff-body">
        <table class="admin-registry-conflict-diff-table">
          <thead>
            <tr>
              <th>Field</th>
              <th>Winner</th>
              <th>Loser</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </details>
  `;
}

export {
  renderRationaleChip,
  renderAdjudicationCardInner,
  renderConflictRow,
  renderConflictDiff
};
