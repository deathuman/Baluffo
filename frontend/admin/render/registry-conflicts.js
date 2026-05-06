import { escapeHtml } from "../../shared/ui/index.js";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { formatDateTime, stableOpsSignature } from "./ops-shared.js";

const ACTION_TOKEN = UI_TOKENS.admin.registryConflictActionBtn;
const CHECK_TOKEN = UI_TOKENS.admin.registryConflictCheckBtn;
const TRIAGE_FILTER_SELECTOR = ".admin-registry-conflict-filter-btn";
const REVIEW_FILTER_SELECTOR = ".admin-registry-conflict-review-filter-btn";
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

function renderTriageFilterButton(bucket, activeFilter) {
  const token = stringValue(bucket?.bucket, "ambiguous_manual_review");
  const label = stringValue(bucket?.label, token);
  const count = Number(bucket?.count || 0);
  const selected = activeFilter === token;
  return `
    <button
      type="button"
      class="btn back-btn admin-registry-conflict-filter-btn"
      data-registry-conflict-filter-bucket="${escapeHtml(token)}"
      aria-pressed="${selected ? "true" : "false"}"
      title="${escapeHtml(stringValue(bucket?.description, label))}"
    >${escapeHtml(label)} · ${count.toLocaleString()}</button>
  `;
}

function renderReviewFilterButton(queue, activeFilter) {
  const token = stringValue(queue?.queue, "p3_low_signal_manual");
  const label = stringValue(queue?.label, token);
  const count = Number(queue?.count || 0);
  const selected = activeFilter === token;
  return `
    <button
      type="button"
      class="btn back-btn admin-registry-conflict-review-filter-btn"
      data-registry-conflict-review-filter-queue="${escapeHtml(token)}"
      aria-pressed="${selected ? "true" : "false"}"
      title="${escapeHtml(stringValue(queue?.description, label))}"
    >${escapeHtml(label)} · ${count.toLocaleString()}</button>
  `;
}

function renderTriageSummary(triage, activeFilter) {
  const total = Number(triage?.summary?.totalConflictCount || 0);
  const buckets = listValue(triage?.buckets);
  const allSelected = activeFilter === "all";
  return `
    <div class="admin-registry-conflict-triage">
      <div class="admin-registry-conflict-triage-head">
        <div>
          <div class="admin-registry-conflict-family">Triage report</div>
          <div class="admin-registry-conflict-summary">${total.toLocaleString()} conflict families classified.</div>
        </div>
        <button
          type="button"
          class="btn back-btn admin-registry-conflict-filter-btn"
          data-registry-conflict-filter-bucket="all"
          aria-pressed="${allSelected ? "true" : "false"}"
        >All · ${total.toLocaleString()}</button>
      </div>
      <div class="admin-registry-conflict-filters">
        ${buckets.map(bucket => renderTriageFilterButton(bucket, activeFilter)).join("")}
      </div>
    </div>
  `;
}

function renderReviewSummary(review, activeFilter) {
  const total = Number(review?.summary?.totalConflictCount || 0);
  const queues = listValue(review?.queues);
  const allSelected = activeFilter === "all";
  return `
    <div class="admin-registry-conflict-triage">
      <div class="admin-registry-conflict-triage-head">
        <div>
          <div class="admin-registry-conflict-family">Review queue</div>
          <div class="admin-registry-conflict-summary">${total.toLocaleString()} conflict families ranked by operator priority.</div>
        </div>
        <button
          type="button"
          class="btn back-btn admin-registry-conflict-review-filter-btn"
          data-registry-conflict-review-filter-queue="all"
          aria-pressed="${allSelected ? "true" : "false"}"
        >All queues · ${total.toLocaleString()}</button>
      </div>
      <div class="admin-registry-conflict-filters">
        ${queues.map(queue => renderReviewFilterButton(queue, activeFilter)).join("")}
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
    <div class="admin-registry-conflict-adjudication">
      <div class="admin-registry-conflict-triage-card">
        <span class="admin-registry-conflict-triage-badge">Adjudication · ${escapeHtml(stringValue(adjudication?.status, "checked"))}</span>
        <span>winner ${escapeHtml(stringValue(adjudication?.winnerSourceId, "unknown"))}</span>
      </div>
      ${probes.map(renderAdjudicationProbe).join("")}
      ${decisions.map(renderAdjudicationDecision).join("")}
    </div>
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

function renderSafeAutomationCard(card, cardIndex) {
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
      >${escapeHtml(safeAutomation.label)}</button>
    </div>
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
  const triageLabel = stringValue(card?.triageLabel, "Manual review");
  const triageRisk = stringValue(card?.triageRisk, "medium");
  const triageReason = stringValue(card?.triageReason, "No triage reason available.");
  const reviewLabel = stringValue(card?.reviewLabel, "Manual review");
  const reviewReason = stringValue(card?.reviewReason, "No review reason available.");
  const suggestedDisposition = stringValue(card?.suggestedDisposition, "Manual review");
  const suggestedConfidence = stringValue(card?.suggestedConfidence, "low");
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
      <div class="admin-registry-conflict-triage-card">
        <span class="admin-registry-conflict-triage-badge">${escapeHtml(triageLabel)} · ${escapeHtml(triageRisk)}</span>
        <span>${escapeHtml(triageReason)}</span>
      </div>
      <div class="admin-registry-conflict-triage-card">
        <span class="admin-registry-conflict-triage-badge">${escapeHtml(reviewLabel)} · ${escapeHtml(suggestedConfidence)}</span>
        <span>${escapeHtml(suggestedDisposition)} · ${escapeHtml(reviewReason)}</span>
      </div>
      ${renderAdjudicationCard(card)}
      ${renderSafeAutomationCard(card, cardIndex)}
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

function renderSafeAutomationToolbar(visibleConflicts) {
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
    >${escapeHtml(entry.label)} · ${entry.targetIds.length.toLocaleString()}</button>
  `).join("");
  return `
    <div class="admin-registry-conflict-triage">
      <div class="admin-registry-conflict-triage-head">
        <div>
          <div class="admin-registry-conflict-family">Safe automation</div>
          <div class="admin-registry-conflict-summary">${eligible.length.toLocaleString()} visible conflict family can be auto-demoted safely; ${totalTargetCount.toLocaleString()} row target.</div>
        </div>
        <div class="admin-registry-conflict-actions">${buttons}</div>
      </div>
    </div>
  `;
}

function renderAdjudicationToolbar(payload, visibleConflicts, checkingConflicts) {
  const adjudication = adjudicationValue(payload);
  const checkedAt = stringValue(adjudication?.finishedAt, "");
  const demoted = Number(adjudication?.demoted || 0);
  const recommended = Number(objectValue(adjudication?.summary)?.recommendedDemotion || 0);
  const disabled = checkingConflicts || !visibleConflicts.length;
  const checkLabel = checkingConflicts ? "Checking conflicts..." : "Check conflicting sources";
  return `
    <div class="admin-registry-conflict-triage">
      <div class="admin-registry-conflict-triage-head">
        <div>
          <div class="admin-registry-conflict-family">Conflict source checks</div>
          <div class="admin-registry-conflict-summary">
            ${checkedAt ? `Last checked ${escapeHtml(formatFieldValue("finishedAt", checkedAt))}; ` : "No conflict source check has run yet. "}
            ${demoted.toLocaleString()} demoted, ${recommended.toLocaleString()} recommended.
          </div>
        </div>
        <div class="admin-registry-conflict-actions">
          <button
            type="button"
            class="btn back-btn"
            data-ui="${CHECK_TOKEN}"
            data-registry-conflict-apply-autopilot="false"
            ${disabled ? "disabled" : ""}
          >${escapeHtml(checkLabel)}</button>
          <button
            type="button"
            class="btn back-btn"
            data-ui="${CHECK_TOKEN}"
            data-registry-conflict-apply-autopilot="true"
            ${disabled ? "disabled" : ""}
          >Apply high-confidence recommendations</button>
        </div>
      </div>
    </div>
  `;
}

function renderConflictGroups(conflicts, review) {
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
            ${rows.map(row => renderConflictCard(row.card, row.index)).join("")}
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
  const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
  const activeTriageFilter = stringValue(reviewEl?.dataset?.registryConflictTriageFilter, "all");
  const activeReviewFilter = stringValue(reviewEl?.dataset?.registryConflictReviewFilter, "all");
  const triageFilteredConflicts = activeTriageFilter === "all"
    ? conflicts
    : conflicts.filter(card => stringValue(card?.triageBucket, "ambiguous_manual_review") === activeTriageFilter);
  const visibleConflicts = activeReviewFilter === "all"
    ? triageFilteredConflicts
    : triageFilteredConflicts.filter(card => stringValue(card?.reviewQueue, "p3_low_signal_manual") === activeReviewFilter);
  const checkingConflicts = Boolean(options?.checkingConflicts);
  const signature = stableOpsSignature({
    summary,
    triage,
    review,
    adjudication: adjudicationValue(payload),
    activeTriageFilter,
    activeReviewFilter,
    checkingConflicts,
    conflicts
  });
  if (canPatchInPlace && reviewEl.dataset.registryConflictsSig === signature) return;
  if (canPatchInPlace) reviewEl.dataset.registryConflictsSig = signature;

  const conflictCount = Number(summary?.conflictCount || conflicts.length || 0);
  reviewEl.innerHTML = `
    <div class="admin-registry-conflicts-copy">
      Registry conflicts are read from the current registry snapshot and the latest jobs source-state history. Winner selection follows the duplicate-family score order and the row actions reuse the existing registry lifecycle routes.
    </div>
    ${renderTriageSummary(triage, activeTriageFilter)}
    ${renderReviewSummary(review, activeReviewFilter)}
    ${renderAdjudicationToolbar(payload, visibleConflicts, checkingConflicts)}
    ${renderSafeAutomationToolbar(visibleConflicts)}
    <div class="admin-registry-conflicts-list">
      ${visibleConflicts.length
        ? renderConflictGroups(visibleConflicts, review)
        : `<div class="muted">${escapeHtml(
            conflictCount
              ? "No registry conflict cards match the selected triage or review queue."
              : "No duplicate-family registry conflicts are currently queued."
          )}</div>`}
    </div>
  `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
  reviewEl.querySelectorAll(TRIAGE_FILTER_SELECTOR).forEach(button => {
    button.addEventListener("click", () => {
      const bucket = stringValue(button.dataset?.registryConflictFilterBucket, "all");
      if (canPatchInPlace) {
        reviewEl.dataset.registryConflictTriageFilter = bucket;
        reviewEl.dataset.registryConflictsSig = "";
      }
      renderAdminRegistryConflicts(reviewEl, payload, options);
    });
  });
  reviewEl.querySelectorAll(REVIEW_FILTER_SELECTOR).forEach(button => {
    button.addEventListener("click", () => {
      const queue = stringValue(button.dataset?.registryConflictReviewFilterQueue, "all");
      if (canPatchInPlace) {
        reviewEl.dataset.registryConflictReviewFilter = queue;
        reviewEl.dataset.registryConflictsSig = "";
      }
      renderAdminRegistryConflicts(reviewEl, payload, options);
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
