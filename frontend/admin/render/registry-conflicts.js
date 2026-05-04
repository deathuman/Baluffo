import { escapeHtml } from "../../shared/ui/index.js";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { formatDateTime, stableOpsSignature } from "./ops-shared.js";

const ACTION_TOKEN = UI_TOKENS.admin.registryConflictActionBtn;

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function listValue(value) {
  return Array.isArray(value) ? value : [];
}

function stringValue(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function formatFieldValue(key, value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (Array.isArray(value) || (value && typeof value === "object")) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  const text = String(value);
  if (key.toLowerCase().endsWith("at")) {
    return formatDateTime(text);
  }
  return text;
}

function getConflictCards(payload) {
  if (Array.isArray(payload?.conflicts)) return payload.conflicts;
  if (Array.isArray(payload?.rows)) return payload.rows;
  return [];
}

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
          data-registry-conflict-card-index="${cardIndex}"
          data-registry-conflict-row-index="${rowIndex}"
          data-registry-conflict-action-index="${actionIndex}"
        >${escapeHtml(label)}</button>
      `;
    })
    .join("");
}

function renderRowMeta(row) {
  const items = [
    ["State", stringValue(row?.registryState, stringValue(row?.candidateState, "unknown"))],
    ["Transition", stringValue(row?.transitionReason, "—")],
    ["Health", stringValue(row?.health, "unknown")],
    ["Health reason", stringValue(row?.healthReason, "—")],
    ["Last success", formatFieldValue("lastSuccessfulFetchAt", row?.lastSuccessfulFetchAt)],
    ["Last seen", formatFieldValue("lastSeenInFetchAt", row?.lastSeenInFetchAt)],
    ["Last jobs kept", stringValue(row?.lastJobsKept ?? row?.lastKeptCount, "0")],
    ["Failure count", stringValue(row?.failureCount ?? row?.consecutiveFailures, "0")],
    ["Zero-job streak", stringValue(row?.zeroJobStreak ?? row?.consecutiveZeroKept, "0")]
  ];
  return items
    .map(([label, value]) => `<span><strong>${escapeHtml(label)}</strong> ${escapeHtml(String(value))}</span>`)
    .join("");
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
        <div class="admin-registry-conflict-id">${escapeHtml(role)}</div>
      </div>
      <div class="admin-registry-conflict-meta">${renderRowMeta(row)}</div>
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
  return `
    <details class="admin-registry-conflict-diff" open data-registry-conflict-card-index="${cardIndex}">
      <summary>${escapeHtml(loserName)} vs ${escapeHtml(winnerLabel)}</summary>
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

function renderConflictCard(card, cardIndex) {
  const winner = objectValue(card?.winner);
  const rows = listValue(card?.rows);
  const rationale = listValue(card?.winnerRationale);
  const diffs = listValue(card?.diffs);
  const familyKey = stringValue(card?.familyKey, "unknown family");
  const winnerName = stringValue(winner?.name, stringValue(winner?.id || winner?.sourceId, "winner"));
  const rowCount = Number(card?.rowCount || rows.length || 0);
  return `
    <section class="admin-registry-conflict-card" data-registry-conflict-card="${cardIndex}">
      <div class="admin-registry-conflict-card-head">
        <div>
          <div class="admin-registry-conflict-family">${escapeHtml(familyKey)}</div>
          <div class="admin-registry-conflict-summary">${escapeHtml(
            `${rowCount.toLocaleString()} rows · winner ${winnerName}`
          )}</div>
        </div>
        <div class="admin-registry-conflict-summary">
          ${escapeHtml(stringValue(winner?.health, "unknown"))}
          ${winner?.healthReason ? ` · ${escapeHtml(stringValue(winner.healthReason))}` : ""}
        </div>
      </div>
      <div class="admin-registry-conflict-rationale">
        ${rationale.length ? rationale.map(renderRationaleChip).join("") : `<span class="muted">No rationale available.</span>`}
      </div>
      <div class="admin-registry-conflict-rows">
        ${rows.length
          ? rows.map((row, rowIndex) => renderConflictRow(row, cardIndex, rowIndex, rowIndex === 0 ? "winner" : "loser")).join("")
          : `<div class="muted">No conflict rows available.</div>`}
      </div>
      <div class="admin-registry-conflict-diffs">
        ${diffs.length
          ? diffs.map(diff => renderConflictDiff(cardIndex, diff, winner)).join("")
          : `<div class="muted">No side-by-side diff available.</div>`}
      </div>
    </section>
  `;
}

export function renderAdminRegistryConflicts(reviewEl, payload, options = {}) {
  if (!reviewEl) return;
  const conflicts = getConflictCards(payload);
  const summary = objectValue(payload?.summary);
  const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
  const signature = stableOpsSignature({
    summary,
    conflicts
  });
  if (canPatchInPlace && reviewEl.dataset.registryConflictsSig === signature) return;
  if (canPatchInPlace) reviewEl.dataset.registryConflictsSig = signature;

  const conflictCount = Number(summary?.conflictCount || conflicts.length || 0);
  reviewEl.innerHTML = `
    <div class="admin-registry-conflicts-copy">
      Registry conflicts are read from the current registry snapshot and the latest jobs source-state history. Winner selection follows the duplicate-family score order and the row actions reuse the existing registry lifecycle routes.
    </div>
    <div class="admin-registry-conflicts-list">
      ${conflicts.length
        ? conflicts.map((card, index) => renderConflictCard(card, index)).join("")
        : `<div class="muted">${escapeHtml(
            conflictCount
              ? "Registry conflict summary available, but no cards were rendered."
              : "No duplicate-family registry conflicts are currently queued."
          )}</div>`}
    </div>
  `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
  reviewEl.querySelectorAll(ui(ACTION_TOKEN)).forEach(button => {
    button.addEventListener("click", () => {
      const cardIndex = Number(button.dataset.registryConflictCardIndex || -1);
      const rowIndex = Number(button.dataset.registryConflictRowIndex || -1);
      const actionIndex = Number(button.dataset.registryConflictActionIndex || -1);
      const card = conflicts[cardIndex];
      const row = card?.rows?.[rowIndex];
      const action = row?.actions?.[actionIndex];
      if (row && action && typeof options.onRegistryConflictAction === "function") {
        options.onRegistryConflictAction(row, action, card);
      }
    });
  });
}
