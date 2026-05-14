import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js?v=6";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { formatDateTime, stableOpsSignature } from "./ops-shared.js";

const ACTION_TOKEN = UI_TOKENS.admin.registryConflictActionBtn;
const CHECK_TOKEN = UI_TOKENS.admin.registryConflictCheckBtn;
const TRIAGE_FILTER_SELECTOR = ".admin-registry-conflict-filter-btn";
const REVIEW_FILTER_SELECTOR = ".admin-registry-conflict-review-filter-btn";
const TRIAGE_FILTER_SELECT_SELECTOR = ".admin-registry-conflict-filter-select";
const REVIEW_FILTER_SELECT_SELECTOR = ".admin-registry-conflict-review-filter-select";
const SAFE_AUTOMATION_SELECTOR = ".admin-registry-conflict-safe-automation-btn";
const TRIAGE_BUCKET_FALLBACKS = [
  {
    bucket: "exact_duplicate_auto_healable",
    label: "Exact duplicate",
    risk: "low",
    description: "Rows share the same canonical source identity."
  },
  {
    bucket: "active_active_likely_duplicate",
    label: "Active-active",
    risk: "high",
    description: "More than one active row exists for this source family."
  },
  {
    bucket: "pending_duplicate_of_active",
    label: "Pending duplicate",
    risk: "medium",
    description: "A pending row matches a family with one active source."
  },
  {
    bucket: "rejected_historical_noise",
    label: "Rejected noise",
    risk: "low",
    description: "Rejected rows are retained as historical registry noise."
  },
  {
    bucket: "ambiguous_manual_review",
    label: "Manual review",
    risk: "medium",
    description: "The conflict needs operator review."
  }
];
const REVIEW_QUEUE_FALLBACKS = [
  {
    queue: "p0_multi_active_provider",
    priority: 0,
    label: "Multiple active providers",
    description: "Multiple active API/provider rows exist for one source family."
  },
  {
    queue: "p1_active_provider_static",
    priority: 1,
    label: "Active provider + static",
    description: "Active provider rows coexist with active static rows."
  },
  {
    queue: "p1_pending_provider_against_active",
    priority: 1,
    label: "Pending provider vs active",
    description: "A pending API/provider candidate is competing with one active source."
  },
  {
    queue: "p2_same_adapter_active_variant",
    priority: 2,
    label: "Same-adapter active variant",
    description: "Multiple active rows use the same non-static source type."
  },
  {
    queue: "p2_static_url_variant_active",
    priority: 2,
    label: "Active static URL variants",
    description: "Multiple active static rows look like URL variants."
  },
  {
    queue: "p2_pending_static_variant",
    priority: 2,
    label: "Pending static variant",
    description: "Pending static rows compete with one active source."
  },
  {
    queue: "p3_pending_only_intake",
    priority: 3,
    label: "Pending-only intake",
    description: "Duplicate candidates are pending only."
  },
  {
    queue: "p3_low_signal_manual",
    priority: 3,
    label: "Low-signal manual review",
    description: "The conflict needs manual review."
  }
];

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

function numberValue(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
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

function progressTimestamp(adjudication) {
  const taskProgress = objectValue(adjudication?.taskProgress);
  return stringValue(adjudication?.heartbeatAt)
    || stringValue(taskProgress?.updatedAt)
    || stringValue(objectValue(adjudication?.progress)?.lastProgressAt);
}

function isStaleProgressTimestamp(timestamp) {
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return false;
  return Date.now() - parsed > 120000;
}

function renderRunningAdjudicationStatus(adjudication) {
  const taskProgress = objectValue(adjudication?.taskProgress);
  const progress = objectValue(adjudication?.progress);
  const counts = objectValue(taskProgress?.counts);
  const phaseLabel = stringValue(taskProgress?.phaseLabel, "Checking conflicts");
  const checkedSources = numberValue(counts?.checkedSources ?? progress?.checkedSourceCount);
  const totalSources = numberValue(counts?.totalSources ?? progress?.totalSourceCount);
  const checkedFamilies = numberValue(counts?.checkedFamilies ?? progress?.checkedFamilyCount);
  const totalFamilies = numberValue(counts?.totalFamilies ?? progress?.totalFamilyCount);
  const ratio = Math.max(0, Math.min(1, numberValue(taskProgress?.ratio)));
  const parts = [escapeHtml(phaseLabel)];
  if (totalSources > 0) {
    const percent = Math.round(ratio * 100);
    parts.push(`${checkedSources.toLocaleString()}/${totalSources.toLocaleString()} sources`);
    if (totalFamilies > 0) {
      parts.push(`${checkedFamilies.toLocaleString()}/${totalFamilies.toLocaleString()} families`);
    }
    parts.unshift(`${percent}%`);
  } else {
    parts.push("waiting for source totals");
  }
  const targetLabel = stringValue(taskProgress?.targetLabel)
    || stringValue(progress?.currentSourceName)
    || stringValue(progress?.currentSourceId);
  const targetUrl = stringValue(taskProgress?.targetUrl) || stringValue(progress?.currentEndpointUrl);
  const currentFamily = stringValue(progress?.currentFamilyKey);
  let currentCopy = "";
  if (targetLabel || targetUrl || currentFamily) {
    const target = targetLabel || targetUrl;
    currentCopy = ` Current: ${escapeHtml(target)}${currentFamily ? ` in ${escapeHtml(currentFamily)}` : ""}.`;
  }
  const timestamp = progressTimestamp(adjudication);
  const staleCopy = isStaleProgressTimestamp(timestamp)
    ? ` <strong>No progress update since ${escapeHtml(formatFieldValue("updatedAt", timestamp))}.</strong>`
    : "";
  return `${parts.join(" · ")}.${currentCopy}${staleCopy}`;
}

function getConflictCards(payload) {
  if (Array.isArray(payload?.conflicts)) return payload.conflicts;
  if (Array.isArray(payload?.rows)) return payload.rows;
  return [];
}

function getTriagePayload(payload, conflicts) {
  const triage = objectValue(payload?.triage);
  const summary = objectValue(triage?.summary);
  const bucketCounts = objectValue(summary?.bucketCounts);
  const fallbackCounts = conflicts.reduce((counts, card) => {
    const bucket = stringValue(card?.triageBucket, "ambiguous_manual_review");
    counts[bucket] = Number(counts[bucket] || 0) + 1;
    return counts;
  }, {});
  const buckets = listValue(triage?.buckets).length
    ? listValue(triage.buckets)
    : TRIAGE_BUCKET_FALLBACKS.map(bucket => ({
        ...bucket,
        count: Number(bucketCounts[bucket.bucket] ?? fallbackCounts[bucket.bucket] ?? 0)
      }));
  return {
    summary: {
      totalConflictCount: Number(summary?.totalConflictCount || conflicts.length || 0),
      bucketCounts: Object.keys(bucketCounts).length ? bucketCounts : fallbackCounts
    },
    buckets: buckets.map(bucket => ({
      bucket: stringValue(bucket?.bucket, "ambiguous_manual_review"),
      label: stringValue(bucket?.label, "Manual review"),
      risk: stringValue(bucket?.risk, "medium"),
      description: stringValue(bucket?.description, ""),
      count: Number(bucket?.count || bucketCounts?.[bucket?.bucket] || fallbackCounts?.[bucket?.bucket] || 0)
    }))
  };
}

function getReviewPayload(payload, conflicts) {
  const review = objectValue(payload?.review);
  const summary = objectValue(review?.summary);
  const queueCounts = objectValue(summary?.queueCounts);
  const fallbackCounts = conflicts.reduce((counts, card) => {
    const queue = stringValue(card?.reviewQueue, "p3_low_signal_manual");
    counts[queue] = Number(counts[queue] || 0) + 1;
    return counts;
  }, {});
  const queues = listValue(review?.queues).length
    ? listValue(review.queues)
    : REVIEW_QUEUE_FALLBACKS.map(queue => ({
        ...queue,
        count: Number(queueCounts[queue.queue] ?? fallbackCounts[queue.queue] ?? 0)
      }));
  return {
    summary: {
      totalConflictCount: Number(summary?.totalConflictCount || conflicts.length || 0),
      priorityCounts: objectValue(summary?.priorityCounts),
      queueCounts: Object.keys(queueCounts).length ? queueCounts : fallbackCounts
    },
    queues: queues.map(queue => ({
      queue: stringValue(queue?.queue, "p3_low_signal_manual"),
      priority: Number(queue?.priority ?? 3),
      label: stringValue(queue?.label, "Manual review"),
      description: stringValue(queue?.description, ""),
      count: Number(queue?.count || queueCounts?.[queue?.queue] || fallbackCounts?.[queue?.queue] || 0)
    }))
  };
}

function sortedConflictCards(conflicts) {
  return [...conflicts].sort((left, right) => {
    const priorityDelta = Number(left?.reviewPriority ?? 3) - Number(right?.reviewPriority ?? 3);
    if (priorityDelta) return priorityDelta;
    const queueDelta = stringValue(left?.reviewQueue, "p3_low_signal_manual")
      .localeCompare(stringValue(right?.reviewQueue, "p3_low_signal_manual"));
    if (queueDelta) return queueDelta;
    return stringValue(left?.familyKey, "unknown family").localeCompare(stringValue(right?.familyKey, "unknown family"));
  });
}

function safeAutomationValue(card) {
  const safeAutomation = objectValue(card?.safeAutomation);
  return {
    eligible: Boolean(safeAutomation?.eligible),
    action: stringValue(safeAutomation?.action, "auto_demote_same_adapter_provider_alias"),
    label: stringValue(safeAutomation?.label, "Auto-demote safe duplicate"),
    reason: stringValue(safeAutomation?.reason, ""),
    route: stringValue(safeAutomation?.route, "/registry/conflicts/auto-demote-safe"),
    targetIds: listValue(safeAutomation?.targetIds).map(id => stringValue(id)).filter(Boolean),
    blockedReasons: listValue(safeAutomation?.blockedReasons).map(reason => stringValue(reason)).filter(Boolean)
  };
}

function adjudicationValue(payload) {
  return objectValue(payload?.adjudication);
}

function familyAdjudicationValue(card) {
  return objectValue(card?.adjudication);
}

function eligibleSafeAutomations(conflicts) {
  return conflicts
    .map((card, index) => ({ card, index, safeAutomation: safeAutomationValue(card) }))
    .filter(row => row.safeAutomation.eligible && row.safeAutomation.targetIds.length);
}

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

function renderConflictFilterToolbar(triage, review, activeTriageFilter, activeReviewFilter) {
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
    </div>
  `;
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
  const lastJobsKept = row?.lastJobsKept ?? row?.lastKeptCount;
  const jobsFound = row?.jobsFound ?? row?.sampleCount ?? row?.lastJobsFound ?? lastJobsKept;
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
      value: lastJobsKept === undefined || lastJobsKept === null ? "—" : stringValue(lastJobsKept, "0"),
      compact: true
    },
    { label: "Failure count", value: stringValue(row?.failureCount ?? row?.consecutiveFailures, "0"), compact: false },
    { label: "Zero-job streak", value: stringValue(row?.zeroJobStreak ?? row?.consecutiveZeroKept, "0"), compact: false }
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
  const status = Boolean(probe?.ok) ? "ok" : stringValue(probe?.error, "failed");
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

function renderAdjudicationCard(card) {
  const adjudication = familyAdjudicationValue(card);
  if (!Object.keys(adjudication).length) return "";
  const probes = listValue(adjudication?.probes);
  const decisions = listValue(adjudication?.decisions);
  return `
    <details class="admin-registry-conflict-detail admin-registry-conflict-adjudication">
      <summary>Adjudication · ${escapeHtml(stringValue(adjudication?.status, "checked"))} · winner ${escapeHtml(stringValue(adjudication?.winnerSourceId, "unknown"))}</summary>
      <div class="admin-registry-conflict-detail-body">
        ${probes.map(renderAdjudicationProbe).join("")}
        ${decisions.map(renderAdjudicationDecision).join("")}
      </div>
    </details>
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
  const detailCount = rationale.length + diffs.reduce((total, diff) => total + listValue(diff?.fields).length, 0);
  return `
    <section class="admin-registry-conflict-card" data-registry-conflict-card="${cardIndex}">
      <div class="admin-registry-conflict-card-head">
        <div class="admin-registry-conflict-card-title">
          <div class="admin-registry-conflict-family">${escapeHtml(familyKey)}</div>
          <div class="admin-registry-conflict-summary">${escapeHtml(
            `${rowCount.toLocaleString()} rows · winner ${winnerName}`
          )}</div>
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
      ${winnerSourceNote}
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
          <div class="admin-registry-conflict-rationale">
            ${rationale.length ? rationale.map(renderRationaleChip).join("") : `<span class="muted">No rationale available.</span>`}
          </div>
        </div>
      </details>
      ${renderAdjudicationCard(card)}
      ${renderSafeAutomationCard(card, cardIndex, Boolean(options?.disableSafeAutomation))}
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
    <div class="admin-registry-conflict-action-group">
      <div>
        <div class="admin-registry-conflict-action-title">Conflict source checks</div>
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
      const open = priority < 3 ? " open" : "";
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

export function renderAdminRegistryConflicts(reviewEl, payload, options = {}) {
  if (!reviewEl) return;
  const conflicts = sortedConflictCards(getConflictCards(payload));
  const summary = objectValue(payload?.summary);
  const triage = getTriagePayload(payload, conflicts);
  const review = getReviewPayload(payload, conflicts);
  const suppressedIndependentProviderBoards = objectValue(payload?.suppressedIndependentProviderBoards);
  const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
  const activeTriageFilter = stringValue(reviewEl?.dataset?.registryConflictTriageFilter, "all");
  const activeReviewFilter = stringValue(reviewEl?.dataset?.registryConflictReviewFilter, "all");
  const triageFilteredConflicts = activeTriageFilter === "all"
    ? conflicts
    : conflicts.filter(card => stringValue(card?.triageBucket, "ambiguous_manual_review") === activeTriageFilter);
  const visibleConflicts = activeReviewFilter === "all"
    ? triageFilteredConflicts
    : triageFilteredConflicts.filter(card => stringValue(card?.reviewQueue, "p3_low_signal_manual") === activeReviewFilter);
  const adjudication = adjudicationValue(payload);
  const checkingConflicts = Boolean(options?.checkingConflicts)
    || stringValue(adjudication?.status) === "running";
  const signature = stableOpsSignature({
    summary,
    triage,
    review,
    adjudication,
    activeTriageFilter,
    activeReviewFilter,
    checkingConflicts,
    conflicts,
    suppressedIndependentProviderBoards
  });
  if (canPatchInPlace && reviewEl.dataset.registryConflictsSig === signature) return;
  if (canPatchInPlace) reviewEl.dataset.registryConflictsSig = signature;

  const conflictCount = Number(summary?.conflictCount || conflicts.length || 0);
  reviewEl.innerHTML = `
    ${renderConflictFilterToolbar(triage, review, activeTriageFilter, activeReviewFilter)}
    ${renderRegistryConflictActionStrip(payload, visibleConflicts, checkingConflicts)}
    ${renderSuppressedIndependentProviderBoards(payload)}
    <div class="admin-registry-conflicts-list">
      ${visibleConflicts.length
        ? renderConflictGroups(visibleConflicts, review, { disableSafeAutomation: checkingConflicts })
        : `<div class="muted">${escapeHtml(
            conflictCount
              ? "No registry conflict cards match the selected triage or review queue."
              : "No duplicate-family registry conflicts are currently queued."
          )}</div>`}
    </div>
  `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
  const applyTriageFilter = bucket => {
    if (canPatchInPlace) {
      reviewEl.dataset.registryConflictTriageFilter = bucket;
      reviewEl.dataset.registryConflictsSig = "";
    }
    renderAdminRegistryConflicts(reviewEl, payload, options);
  };
  const applyReviewFilter = queue => {
    if (canPatchInPlace) {
      reviewEl.dataset.registryConflictReviewFilter = queue;
      reviewEl.dataset.registryConflictsSig = "";
    }
    renderAdminRegistryConflicts(reviewEl, payload, options);
  };
  reviewEl.querySelectorAll(TRIAGE_FILTER_SELECT_SELECTOR).forEach(select => {
    select.addEventListener("change", () => {
      applyTriageFilter(stringValue(select.value, "all"));
    });
  });
  reviewEl.querySelectorAll(REVIEW_FILTER_SELECT_SELECTOR).forEach(select => {
    select.addEventListener("change", () => {
      applyReviewFilter(stringValue(select.value, "all"));
    });
  });
  reviewEl.querySelectorAll(TRIAGE_FILTER_SELECTOR).forEach(button => {
    button.addEventListener("click", () => {
      const bucket = stringValue(button.dataset?.registryConflictFilterBucket, "all");
      applyTriageFilter(bucket);
    });
  });
  reviewEl.querySelectorAll(REVIEW_FILTER_SELECTOR).forEach(button => {
    button.addEventListener("click", () => {
      const queue = stringValue(button.dataset?.registryConflictReviewFilterQueue, "all");
      applyReviewFilter(queue);
    });
  });
  reviewEl.querySelectorAll(SAFE_AUTOMATION_SELECTOR).forEach(button => {
    button.addEventListener("click", () => {
      const cardIndex = Number(button.dataset.registryConflictSafeAutomationCardIndex || -1);
      const ids = stringValue(button.dataset.registryConflictSafeAutomationIds)
        .split(",")
        .map(id => id.trim())
        .filter(Boolean);
      const card = cardIndex >= 0 ? visibleConflicts[cardIndex] : null;
      const safeAutomation = card
        ? safeAutomationValue(card)
        : {
            eligible: true,
            action: stringValue(button.dataset.registryConflictSafeAutomationAction, "auto_demote_same_adapter_provider_alias"),
            label: "Apply safe demotions",
            route: stringValue(button.dataset.registryConflictSafeAutomationRoute, "/registry/conflicts/auto-demote-safe"),
            targetIds: ids,
            blockedReasons: []
          };
      if (typeof options.onRegistryConflictSafeAutomation === "function") {
        options.onRegistryConflictSafeAutomation(
          {
            ...safeAutomation,
            targetIds: ids.length ? ids : safeAutomation.targetIds
          },
          card
        );
      }
    });
  });
  reviewEl.querySelectorAll(ui(CHECK_TOKEN)).forEach(button => {
    button.addEventListener("click", () => {
      const applyAutopilot = String(button.dataset.registryConflictApplyAutopilot || "false") === "true";
      if (typeof options.onRegistryConflictCheck === "function") {
        options.onRegistryConflictCheck({ applyAutopilot });
      }
    });
  });
  reviewEl.querySelectorAll(ui(ACTION_TOKEN)).forEach(button => {
    button.addEventListener("click", () => {
      const cardIndex = Number(button.dataset.registryConflictCardIndex || -1);
      const rowIndex = Number(button.dataset.registryConflictRowIndex || -1);
      const actionIndex = Number(button.dataset.registryConflictActionIndex || -1);
      const card = visibleConflicts[cardIndex];
      const row = card?.rows?.[rowIndex];
      const action = row?.actions?.[actionIndex];
      if (row && action && typeof options.onRegistryConflictAction === "function") {
        options.onRegistryConflictAction(row, action, card);
      }
    });
  });
}
