/**
 * Registry conflict renderer — filter toolbar, conflict cards, review groups, and the load-more footer.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-view
 */

import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js";
import { stringValue } from "../../shared/format-utils.js";
import {
  adjudicationValue,
  listValue,
  objectValue,
  safeAutomationValue
} from "./registry-conflicts-model.js";
import {
  renderAdjudicationCardInner,
  renderConflictDiff,
  renderConflictRow,
  renderRationaleChip
} from "./registry-conflicts-rows.js";

function renderTriageFilterOption(bucket, activeFilter) {
  const token = stringValue(bucket?.bucket, "ambiguous_manual_review");
  const label = stringValue(bucket?.label, token);
  const count = Number(bucket?.count || 0);
  const selected = activeFilter === token;
  return `
    <option
      value="${escapeHtml(token)}"
      ${selected ? "selected" : ""}
      ${tooltipAttrs(stringValue(bucket?.description, label)).trim()}
    >${escapeHtml(label)} · ${count.toLocaleString()}</option>
  `;
}

function renderReviewFilterOption(queue, activeFilter) {
  const token = stringValue(queue?.queue, "p3_low_signal_manual");
  const label = stringValue(queue?.label, token);
  const count = Number(queue?.count || 0);
  const selected = activeFilter === token;
  return `
    <option
      value="${escapeHtml(token)}"
      ${selected ? "selected" : ""}
      ${tooltipAttrs(stringValue(queue?.description, label)).trim()}
    >${escapeHtml(label)} · ${count.toLocaleString()}</option>
  `;
}

function renderAllTriageFilterOption(total, activeFilter) {
  const allSelected = activeFilter === "all";
  return `
    <option value="all" ${allSelected ? "selected" : ""}>All · ${total.toLocaleString()}</option>
  `;
}

function renderAllReviewFilterOption(total, activeFilter) {
  const allSelected = activeFilter === "all";
  return `
    <option value="all" ${allSelected ? "selected" : ""}>All queues · ${total.toLocaleString()}</option>
  `;
}

function renderConflictFilterToolbar(triage, review, activeTriageFilter, activeReviewFilter, activeSearchQuery) {
  const triageTotal = Number(triage?.summary?.totalConflictCount || 0);
  const reviewTotal = Number(review?.summary?.totalConflictCount || 0);
  const buckets = listValue(triage?.buckets);
  const queues = listValue(review?.queues);
  return `
    <div class="admin-registry-conflict-toolbar" aria-label="Registry conflict filters">
      <div class="admin-registry-conflict-filter-group" role="group" aria-label="Triage filter">
        <label class="admin-registry-conflict-filter-label" for="admin-registry-conflict-triage-filter">Triage</label>
        <select
          id="admin-registry-conflict-triage-filter"
          class="admin-registry-conflict-filter-select"
          data-registry-conflict-filter-bucket="${escapeHtml(activeTriageFilter)}"
        >
          ${renderAllTriageFilterOption(triageTotal, activeTriageFilter)}
          ${buckets.map(bucket => renderTriageFilterOption(bucket, activeTriageFilter)).join("")}
        </select>
      </div>
      <div class="admin-registry-conflict-filter-group" role="group" aria-label="Review queue filter">
        <label class="admin-registry-conflict-filter-label" for="admin-registry-conflict-review-filter">Review queue</label>
        <select
          id="admin-registry-conflict-review-filter"
          class="admin-registry-conflict-review-filter-select"
          data-registry-conflict-review-filter-queue="${escapeHtml(activeReviewFilter)}"
        >
          ${renderAllReviewFilterOption(reviewTotal, activeReviewFilter)}
          ${queues.map(queue => renderReviewFilterOption(queue, activeReviewFilter)).join("")}
        </select>
      </div>
      <div class="admin-registry-conflict-filter-group" role="group" aria-label="Search conflicts">
        <label class="admin-registry-conflict-filter-label" for="admin-registry-conflict-search">Search</label>
        <input
          id="admin-registry-conflict-search"
          type="search"
          class="admin-registry-conflict-search-input"
          placeholder="Family or source ID"
          value="${escapeHtml(stringValue(activeSearchQuery))}"
        />
      </div>
    </div>
  `;
}

function renderSafeAutomationCard(card, cardIndex, disabled = false) {
  const safeAutomation = safeAutomationValue(card);
  if (!safeAutomation.eligible) return "";
  return `
    <div class="admin-registry-conflict-triage-card">
      <span class="admin-registry-conflict-triage-badge">Safe automation available</span>
      <span>${escapeHtml(safeAutomation.reason)}</span>
      <button
        type="button"
        class="btn back-btn admin-registry-conflict-safe-automation-btn"
        data-registry-conflict-safe-automation-card-index="${cardIndex}"
        data-registry-conflict-safe-automation-action="${escapeHtml(safeAutomation.action)}"
        data-registry-conflict-safe-automation-ids="${escapeHtml(safeAutomation.targetIds.join(","))}"
        ${tooltipAttrs(`${safeAutomation.label}: ${safeAutomation.reason || "apply this safe registry-conflict automation."}`)}
        ${disabled ? "disabled" : ""}
      >${escapeHtml(safeAutomation.label)}</button>
    </div>
  `;
}

function renderConflictCard(card, cardIndex, options = {}) {
  const winner = objectValue(card?.winner);
  const rows = listValue(card?.rows);
  const rationale = listValue(card?.winnerRationale);
  const diffs = listValue(card?.diffs);
  const familyKey = stringValue(card?.familyKey, "unknown family");
  const winnerName = stringValue(winner?.name, stringValue(winner?.id || winner?.sourceId, "winner"));
  const rowCount = Number(card?.rowCount || rows.length || 0);
  const triageLabel = stringValue(card?.triageLabel, "Manual review");
  const triageRisk = stringValue(card?.triageRisk, "medium");
  const triageReason = stringValue(card?.triageReason, "No triage reason available.");
  const reviewLabel = stringValue(card?.reviewLabel, "Manual review");
  const reviewReason = stringValue(card?.reviewReason, "No review reason available.");
  const suggestedDisposition = stringValue(card?.suggestedDisposition, "Manual review");
  const suggestedConfidence = stringValue(card?.suggestedConfidence, "low");
  const reviewPriority = Number(card?.reviewPriority ?? 3);
  const winnerHealth = stringValue(winner?.health, "unknown");
  const winnerHealthReason = stringValue(winner?.healthReason, "");
  const effectiveWinnerSource = stringValue(card?.effectiveWinnerSource, "registry");
  const winnerSourceNote = effectiveWinnerSource === "live_adjudication"
    ? `<div class="admin-registry-conflict-inline-note">
        <span class="admin-registry-conflict-triage-badge">Live counts applied</span>
        <span>Winner selected from completed source-check counts; registry counts remain visible on each row.</span>
      </div>`
    : "";
  const detailCount = rationale.length
    + diffs.reduce((total, diff) => total + listValue(diff?.fields).length, 0)
    + listValue(adjudicationValue(card)?.probes).length
    + listValue(adjudicationValue(card)?.decisions).length;
  const loserName = stringValue(
    listValue(rows)[1]?.name,
    stringValue(listValue(rows)[1]?.id || listValue(rows)[1]?.sourceId, "")
  );
  const summaryLine = loserName
    ? `${rowCount.toLocaleString()} rows · winner ${winnerName} vs ${loserName}`
    : `${rowCount.toLocaleString()} rows · winner ${winnerName}`;
  return `
    <section class="admin-registry-conflict-card" data-registry-conflict-card="${cardIndex}" data-conflict-key="${escapeHtml(familyKey)}"${tooltipAttrs("Card background opens the Inspector")}>
      <div class="admin-registry-conflict-card-head">
        <div class="admin-registry-conflict-card-title">
          <div class="admin-registry-conflict-family">${escapeHtml(familyKey)}</div>
          <div class="admin-registry-conflict-summary">${escapeHtml(summaryLine)}</div>
        </div>
        <div class="admin-registry-conflict-card-badges" aria-label="Conflict classification">
          <span class="admin-registry-conflict-triage-badge">${escapeHtml(triageLabel)} · ${escapeHtml(triageRisk)}</span>
          <span class="admin-registry-conflict-triage-badge">P${Number.isFinite(reviewPriority) ? reviewPriority : 3}</span>
          <span class="admin-registry-conflict-triage-badge">${escapeHtml(reviewLabel)} · ${escapeHtml(suggestedConfidence)}</span>
          <span class="admin-registry-conflict-triage-badge">${escapeHtml(winnerHealth)}</span>
        </div>
      </div>
      <div class="admin-registry-conflict-recommendation">
        <strong>${escapeHtml(suggestedDisposition)}</strong>
        <span>${escapeHtml(reviewReason)}</span>
      </div>
      <details class="admin-registry-conflict-detail">
        <summary>Decision details · ${detailCount.toLocaleString()} signals</summary>
        <div class="admin-registry-conflict-detail-body">
          <div class="admin-registry-conflict-triage-card">
            <span class="admin-registry-conflict-triage-badge">${escapeHtml(triageLabel)} · ${escapeHtml(triageRisk)}</span>
            <span>${escapeHtml(triageReason)}</span>
          </div>
          <div class="admin-registry-conflict-triage-card">
            <span class="admin-registry-conflict-triage-badge">${escapeHtml(reviewLabel)} · ${escapeHtml(suggestedConfidence)}</span>
            <span>${escapeHtml(suggestedDisposition)} · ${escapeHtml(reviewReason)}</span>
          </div>
          ${winnerHealthReason ? `<div class="admin-registry-conflict-triage-card">
            <span class="admin-registry-conflict-triage-badge">Winner health</span>
            <span>${escapeHtml(winnerHealthReason)}</span>
          </div>` : ""}
          ${winnerSourceNote}
          <div class="admin-registry-conflict-rationale">
            ${rationale.length ? rationale.map(renderRationaleChip).join("") : `<span class="muted">No rationale available.</span>`}
          </div>
        </div>
      </details>
      ${renderSafeAutomationCard(card, cardIndex, Boolean(options?.disableSafeAutomation))}
      <details class="admin-registry-conflict-detail admin-registry-conflict-adjudication">
        <summary>Adjudication & diffs · ${detailCount.toLocaleString()} signals</summary>
        <div class="admin-registry-conflict-detail-body">
          ${renderAdjudicationCardInner(card)}
          <div class="admin-registry-conflict-diffs">
            ${diffs.length
              ? diffs.map(diff => renderConflictDiff(cardIndex, diff, winner)).join("")
              : `<div class="muted">No side-by-side diff available.</div>`}
          </div>
        </div>
      </details>
      <div class="admin-registry-conflict-rows">
        ${rows.length
          ? rows.map((row, rowIndex) => renderConflictRow(row, cardIndex, rowIndex, rowIndex === 0 ? "winner" : "loser")).join("")
          : `<div class="muted">No conflict rows available.</div>`}
      </div>
    </section>
  `;
}

function renderConflictGroups(conflicts, review, options = {}) {
  const queues = listValue(review?.queues);
  const queueMeta = new Map(queues.map(queue => [stringValue(queue?.queue), queue]));
  const groups = new Map();
  conflicts.forEach((card, index) => {
    const queue = stringValue(card?.reviewQueue, "p3_low_signal_manual");
    if (!groups.has(queue)) groups.set(queue, []);
    groups.get(queue).push({ card, index });
  });
  return [...groups.entries()]
    .map(([queue, rows]) => {
      const meta = queueMeta.get(queue) || { queue, priority: 3, label: queue, description: "" };
      const priority = Number(meta?.priority ?? rows[0]?.card?.reviewPriority ?? 3);
      const open = priority < 2 ? " open" : "";
      return `
        <details
          class="admin-registry-conflict-review-group"
          data-registry-conflict-review-queue="${escapeHtml(queue)}"${open}
        >
          <summary>
            <span>${escapeHtml(stringValue(meta?.label, queue))}</span>
            <span>${rows.length.toLocaleString()} shown · P${priority}</span>
          </summary>
          <div class="admin-registry-conflict-review-group-body">
            ${rows.map(row => renderConflictCard(row.card, row.index, options)).join("")}
          </div>
        </details>
      `;
    })
    .join("");
}

function renderLoadMoreFooter(visibleCount, loadedCount, totalCount) {
  if (!totalCount || loadedCount >= totalCount) return "";
  return `
    <div class="admin-registry-conflict-load-more">
      <span class="muted">Showing ${visibleCount.toLocaleString()} of ${totalCount.toLocaleString()} conflicts.</span>
      <button
        type="button"
        class="btn back-btn admin-registry-conflict-load-more-btn"
        ${tooltipAttrs("Load the next page of conflict cards.")}
      >Show 50 more</button>
    </div>
  `;
}

export {
  renderConflictFilterToolbar,
  renderConflictGroups,
  renderLoadMoreFooter
};
