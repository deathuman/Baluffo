/**
 * Registry conflict renderer — Payload normalization, sorting, and conflict classification helpers.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-model
 */

import { stringValue } from "../../shared/format-utils.js";
import { escapeHtml } from "../../shared/ui/index.js";
import {
  REVIEW_QUEUE_FALLBACKS,
  TRIAGE_BUCKET_FALLBACKS
} from "./registry-conflicts-constants.js";
import { formatDateTime } from "./ops-shared.js";

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function listValue(value) {
  return Array.isArray(value) ? value : [];
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

function conflictMatchesSearch(card, query) {
  const needle = stringValue(query).trim().toLowerCase();
  if (!needle) return true;
  const haystack = [
    stringValue(card?.familyKey),
    ...listValue(card?.rows).map(row => `${stringValue(row?.name)} ${stringValue(row?.id || row?.sourceId || row?.sourceStateName)}`)
  ].join(" ").toLowerCase();
  return haystack.includes(needle);
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

export {
  objectValue,
  listValue,
  numberValue,
  formatFieldValue,
  renderRunningAdjudicationStatus,
  getConflictCards,
  getTriagePayload,
  getReviewPayload,
  conflictMatchesSearch,
  sortedConflictCards,
  safeAutomationValue,
  adjudicationValue,
  familyAdjudicationValue,
  eligibleSafeAutomations
};
