import {
  formatTaskProgressCounts,
  normalizeTaskProgressPayload
} from "../shared/task-progress.js";
import { getTaskStateRows } from "../shared/live-task.js";

export function getErrorMessage(err, unknownErrorText = "unknown error") {
  return err?.message || unknownErrorText;
}

export function normalizeLogLevel(level) {
  const value = String(level || "info").toLowerCase();
  if (value === "error") return "log-error";
  if (value === "warn" || value === "warning") return "log-warn";
  if (value === "success") return "log-success";
  if (value === "muted") return "log-muted";
  return "log-info";
}

export function createLogEvent(scope, messageOrEvent, level = "info") {
  if (messageOrEvent && typeof messageOrEvent === "object" && !Array.isArray(messageOrEvent)) {
    return {
      timestamp: String(messageOrEvent.timestamp || new Date().toISOString()),
      level: normalizeLogLevel(messageOrEvent.level || level).replace("log-", ""),
      scope: String(messageOrEvent.scope || scope || "admin"),
      sourceId: String(messageOrEvent.sourceId || ""),
      message: String(messageOrEvent.message || "")
    };
  }
  return {
    timestamp: new Date().toISOString(),
    level: normalizeLogLevel(level).replace("log-", ""),
    scope: String(scope || "admin"),
    sourceId: "",
    message: String(messageOrEvent || "")
  };
}

export function formatLogEventText(event) {
  const prefix = `[${event.scope}]`;
  const source = event.sourceId ? ` [${event.sourceId}]` : "";
  return `${prefix}${source} ${event.message}`.trim();
}

export function getSourceJobsFoundCount(row) {
  const value = Number(
    row?.jobsFound
      ?? row?.sampleCount
      ?? row?._lastKeptCount
      ?? row?.keptCount
      ?? row?.lastKeptCount
      ?? row?._lastFetchedCount
      ?? row?.fetchedCount
      ?? row?.lastFetchedCount
      ?? NaN
  );
  return Number.isFinite(value) ? value : NaN;
}

function normalizeSourceStatusToken(value) {
  const token = String(value || "").trim().toLowerCase();
  if (!token) return "";
  if (token === "n/a" || token === "na" || token === "unknown" || token === "not_run" || token === "not run yet") {
    return "not_run";
  }
  if (token === "success" || token === "healthy") return "ok";
  if (token === "failed" || token === "failure") return "error";
  return token;
}

function coerceReportDetailRow(detail) {
  if (detail && typeof detail === "object" && !Array.isArray(detail)) {
    return detail;
  }
  if (typeof detail !== "string") return null;
  const raw = detail.trim();
  if (!raw.startsWith("{") || !raw.endsWith("}")) return null;

  const candidates = [raw];
  const pyLike = raw
    .replace(/\bNone\b/g, "null")
    .replace(/\bTrue\b/g, "true")
    .replace(/\bFalse\b/g, "false");
  if (pyLike !== raw) candidates.push(pyLike);
  if (!raw.includes("\"")) candidates.push(pyLike.replace(/'/g, "\""));

  for (const attempt of candidates) {
    try {
      const parsed = JSON.parse(attempt);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // Continue trying fallbacks.
    }
  }
  return null;
}

function extractSourceIdFromLoaderName(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (raw.startsWith("static_source::")) {
    return raw.slice("static_source::".length).trim();
  }
  return "";
}

function toSourceMatchKeys(row) {
  const out = new Set();
  const studio = String(row?.studio || "").trim().toLowerCase();
  const name = String(row?.name || "").trim().toLowerCase();
  const id = String(row?.id || "").trim().toLowerCase();
  const loaderSourceId = extractSourceIdFromLoaderName(name);
  if (id) out.add(id);
  if (studio) out.add(studio);
  if (name) out.add(name);
  if (loaderSourceId) out.add(loaderSourceId);
  if (studio && name) out.add(`${studio}|${name}`);
  return Array.from(out);
}

function shouldTryGroupErrorMatch(group) {
  const status = normalizeSourceStatusToken(group?.status);
  return status === "error" && String(group?.error || "").trim().length > 0;
}

function rowMatchesGroupError(row, group) {
  if (!shouldTryGroupErrorMatch(group)) return false;
  const errorText = String(group?.error || "").toLowerCase();
  const tokens = toSourceMatchKeys(row).filter(token => token.length >= 4);
  return tokens.some(token => errorText.includes(token));
}

export function deriveSourceStatus(row) {
  const mergedStatus = normalizeSourceStatusToken(row?._lastStatus);
  if (mergedStatus) return mergedStatus;
  const rowStatus = normalizeSourceStatusToken(row?.status);
  if (rowStatus) return rowStatus;
  if (String(row?.lastProbeError || "").trim()) return "error";
  const jobsFound = getSourceJobsFoundCount(row);
  if (Number.isFinite(jobsFound) && jobsFound > 0) return "ok";
  if (String(row?.lastProbedAt || "").trim()) return "warning";
  return "not_run";
}

export function mergeSourceStatusFromReport(rows, report, mode) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const groups = Array.isArray(report?.sources) ? report.sources : [];
  const candidates = [];
  groups.forEach(group => {
    if (!group || typeof group !== "object") return;
    candidates.push(group);
    const details = Array.isArray(group?.details) ? group.details : [];
    details.forEach(detail => {
      const parsed = coerceReportDetailRow(detail);
      if (parsed) candidates.push(parsed);
    });
  });
  const byKey = new Map();
  candidates.forEach(candidate => {
    toSourceMatchKeys(candidate).forEach(key => {
      if (!byKey.has(key)) byKey.set(key, candidate);
    });
  });
  return sourceRows.map(row => {
    const keys = toSourceMatchKeys(row);
    const direct = keys.map(key => byKey.get(key)).find(Boolean) || null;
    const matched = direct || groups.find(group => rowMatchesGroupError(row, group)) || null;
    if (!matched) return row;
    return {
      ...row,
      _lastStatus: normalizeSourceStatusToken(matched?.status),
      _lastError: String(matched?.error || ""),
      _lastFetchedCount: Number(matched?.fetchedCount || 0),
      _lastKeptCount: Number(matched?.keptCount || 0),
      _mode: mode
    };
  });
}

export function applySourceFilter(rows, activeSourceFilter) {
  const filter = activeSourceFilter || "all";
  if (filter === "all") return rows;
  return (Array.isArray(rows) ? rows : []).filter(row => {
    const status = deriveSourceStatus(row);
    const jobsFound = getSourceJobsFoundCount(row);
    if (filter === "error") return status === "error";
    if (filter === "excluded") return status === "excluded";
    if (filter === "zero") return jobsFound === 0;
    if (filter === "healthy") return status === "ok" || (jobsFound > 0 && status !== "error");
    return true;
  });
}

function parseRunTimestampMs(row) {
  const raw = String(row?.startedAt || row?.finishedAt || "").trim();
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeRunStatus(value) {
  const token = String(value || "").trim().toLowerCase();
  return token || "unknown";
}

function isRunLive(row) {
  return Boolean(row?.active) && !String(row?.finishedAt || "").trim();
}

function isTerminalHistoryRow(row) {
  return Boolean(String(row?.finishedAt || "").trim());
}

function toOpsRunRow(row, nowMs) {
  const startedMs = Date.parse(String(row?.startedAt || ""));
  const live = isRunLive(row);
  const elapsedMs = live && Number.isFinite(startedMs) ? Math.max(0, Number(nowMs || Date.now()) - startedMs) : Number(row?.durationMs || 0);
  const status = live ? "running" : normalizeRunStatus(row?.status);
  return {
    ...row,
    isLive: live,
    elapsedMs,
    displayStatus: status
  };
}

export function normalizeOpsRuns(runs, nowMs = Date.now()) {
  const rows = Array.isArray(runs) ? runs.filter(row => row && typeof row === "object") : [];
  const dedupedRows = [];
  const runIdMatches = new Map();
  const legacyMatches = new Map();
  rows.forEach(row => {
    const runId = String(row?.runId || "").trim();
    const type = String(row?.type || "").trim().toLowerCase();
    const startedAt = String(row?.startedAt || "").trim();
    // Note: finishedAt and status extracted for future use
    const _finishedAt = String(row?.finishedAt || "").trim();
    const _status = String(row?.status || "").trim().toLowerCase();

    let existingIndex;
    if (runId && type) {
      const key = `${type}|${runId}`;
      existingIndex = runIdMatches.get(key);
      if (existingIndex === undefined) {
        runIdMatches.set(key, dedupedRows.length);
        dedupedRows.push(row);
        return;
      }
    } else if (type && startedAt) {
      const key = `${type}|${startedAt}`;
      existingIndex = legacyMatches.get(key);
      if (existingIndex === undefined) {
        legacyMatches.set(key, dedupedRows.length);
        dedupedRows.push(row);
        return;
      }
    } else {
      dedupedRows.push(row);
      return;
    }

    const existing = dedupedRows[existingIndex];
    const existingLive = isRunLive(existing);
    const nextLive = isRunLive(row);
    const existingHasRunId = Boolean(String(existing?.runId || "").trim());
    const nextHasRunId = Boolean(runId);
    if (!existingHasRunId && nextHasRunId) {
      dedupedRows[existingIndex] = row;
      return;
    }
    if (existingLive && !nextLive) {
      dedupedRows[existingIndex] = row;
      return;
    }
    if (existingLive === nextLive && parseRunTimestampMs(row) >= parseRunTimestampMs(existing)) {
      dedupedRows[existingIndex] = row;
    }
  });
  const sorted = [...dedupedRows].sort((a, b) => parseRunTimestampMs(b) - parseRunTimestampMs(a));
  const latestLiveByType = new Map();
  sorted.forEach(row => {
    if (!isRunLive(row)) return;
    const type = String(row?.type || "").trim().toLowerCase();
    if (!type || latestLiveByType.has(type)) return;
    latestLiveByType.set(type, toOpsRunRow(row, nowMs));
  });

  const currentRows = Array.from(latestLiveByType.values())
    .sort((a, b) => parseRunTimestampMs(b) - parseRunTimestampMs(a));

  const completedRows = sorted
    .filter(row => !isRunLive(row) && isTerminalHistoryRow(row))
    .map(row => toOpsRunRow(row, nowMs));

  const visibleCompletedRows = completedRows.slice(0, 2);
  const olderCompletedRows = completedRows.slice(2);
  const hasLiveRuns = currentRows.some(row => Boolean(row?.isLive));
  const liveTypes = currentRows
    .filter(row => Boolean(row?.isLive))
    .map(row => String(row?.type || "").toLowerCase())
    .filter(Boolean);

  return {
    currentRows,
    visibleCompletedRows,
    olderCompletedRows,
    hasLiveRuns,
    liveTypes
  };
}

function normalizeCurrentTaskStateRow(row, nowMs = Date.now()) {
  if (!row || typeof row !== "object" || Array.isArray(row)) return null;
  const taskType = String(row.taskType || row.type || "").trim().toLowerCase();
  if (!taskType) return null;
  const startedAt = String(row.startedAt || "").trim();
  const finishedAt = String(row.finishedAt || "").trim();
  const startedMs = Date.parse(startedAt);
  const active = Boolean(row.active);
  const taskProgress = normalizeTaskProgressContract(row.taskProgress);
  return {
    ...row,
    type: taskType,
    taskType,
    runId: String(row.runId || row.id || "").trim(),
    startedAt,
    finishedAt,
    active,
    isLive: active,
    summary: row.summary && typeof row.summary === "object" && !Array.isArray(row.summary) ? row.summary : {},
    outputs: row.outputs && typeof row.outputs === "object" && !Array.isArray(row.outputs) ? row.outputs : {},
    taskProgress,
    elapsedMs: active && Number.isFinite(startedMs) ? Math.max(0, Number(nowMs || Date.now()) - startedMs) : Number(row.durationMs || 0),
    displayStatus: active ? "running" : String(row.status || "unknown").trim().toLowerCase()
  };
}

export function deriveAdminRunsModel(
  {
    taskState,
    historyRuns
  } = {},
  nowMs = Date.now()
) {
  const taskRows = getTaskStateRows(taskState);
  const normalizedCurrentRows = taskRows
    .map(row => normalizeCurrentTaskStateRow(row, nowMs))
    .filter(row => row && row.isLive);
  const currentByType = new Map();
  normalizedCurrentRows
    .sort((a, b) => parseRunTimestampMs(b) - parseRunTimestampMs(a))
    .forEach(row => {
      const taskType = String(row?.taskType || row?.type || "").trim().toLowerCase();
      if (!taskType || currentByType.has(taskType)) return;
      currentByType.set(taskType, row);
    });

  const completedModel = normalizeOpsRuns(Array.isArray(historyRuns) ? historyRuns : [], nowMs);

  const baseModel = {
    currentRows: Array.from(currentByType.values()),
    visibleCompletedRows: Array.isArray(completedModel.visibleCompletedRows) ? completedModel.visibleCompletedRows : [],
    olderCompletedRows: Array.isArray(completedModel.olderCompletedRows) ? completedModel.olderCompletedRows : [],
    hasLiveRuns: currentByType.size > 0,
    liveTypes: Array.from(currentByType.keys())
  };
  return baseModel;
}

function compactCount(value) {
  return Number(value || 0).toLocaleString();
}

function normalizeTaskProgressContract(progress) {
  return normalizeTaskProgressPayload(progress);
}

function inferDiscoveryPhaseKeyFromLabel(label, fallback = "") {
  const normalized = String(label || "").trim().toLowerCase();
  if (!normalized) return String(fallback || "").trim();
  if (/prob/i.test(normalized)) return "probing_candidates";
  if (/final/i.test(normalized)) return "finalizing";
  if (/scan/i.test(normalized)) return "scanning_sources";
  if (/seed|provider-pattern|web-search|generating/i.test(normalized)) return "generating_candidates";
  return String(fallback || "").trim() || "starting";
}

function deriveTaskProgressView(progress, {
  taskLabel,
  fallbackPhaseLabel = "",
  formatCountsLabel
} = {}) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }
  const phaseLabel = normalized.phaseLabel || fallbackPhaseLabel || "In progress";
  const countsLabel = typeof formatCountsLabel === "function"
    ? String(formatCountsLabel(normalized.counts, normalized) || "").trim()
    : "";
  return {
    active: true,
    determinate: normalized.mode === "determinate",
    ratio: normalized.mode === "determinate" ? normalized.ratio : 0,
    label: `${taskLabel}: ${phaseLabel}${countsLabel ? ` | ${countsLabel}` : ""}`
  };
}

function mapDiscoveryHybridRatio(progress) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) return null;
  const phaseKey = String(normalized.phaseKey || "").trim().toLowerCase();
  const ratio = Number(normalized.ratio || 0);
  switch (phaseKey) {
    case "starting":
      return 0.05;
    case "generating_candidates":
      return 0.28;
    case "scanning_sources":
      return 0.5;
    case "probing_candidates":
      return 0.6 + (Math.max(0, Math.min(1, ratio)) * 0.32);
    case "finalizing":
      return 0.96;
    case "completed":
      return 1;
    default:
      return null;
  }
}

function deriveDiscoveryHybridProgressView(progress, {
  taskLabel,
  fallbackPhaseLabel = "",
  formatCountsLabel
} = {}) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }
  const phaseLabel = normalized.phaseLabel || fallbackPhaseLabel || "In progress";
  const countsLabel = typeof formatCountsLabel === "function"
    ? String(formatCountsLabel(normalized.counts, normalized) || "").trim()
    : "";
  const hybridRatio = mapDiscoveryHybridRatio(normalized);
  const determinate = Number.isFinite(hybridRatio) && hybridRatio !== null;
  return {
    active: true,
    determinate,
    ratio: determinate ? hybridRatio : 0,
    label: `${taskLabel}: ${phaseLabel}${countsLabel ? ` | ${countsLabel}` : ""}`
  };
}

function deriveLegacyFetcherTaskProgress(report, { running = false } = {}) {
  const summary = report?.summary || {};
  const runtime = report?.runtime || {};
  const runtimeSelectedSourceCount = Math.max(0, Number(runtime.selectedSourceCount || 0));
  const summarySourceCount = Math.max(0, Number(summary.sourceCount || 0));
  const successfulSources = Math.max(0, Number(summary.successfulSources || 0));
  const failedSources = Math.max(0, Number(summary.failedSources || 0));
  const excludedSources = Math.max(0, Number(summary.excludedSources || 0));
  const resolvedSources = successfulSources + failedSources + excludedSources;
  const outputCount = Math.max(0, Number(summary.outputCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (resolvedSources > 0 || outputCount > 0));
  if (!active) return null;
  const finished = Boolean(String(report?.finishedAt || "").trim());
  const runtimeCountMatchesResolvedUnit = (
    runtimeSelectedSourceCount > 0
    && resolvedSources <= runtimeSelectedSourceCount
    && (summarySourceCount <= 0 || summarySourceCount <= runtimeSelectedSourceCount)
  );
  const totalSources = finished
    ? Math.max(summarySourceCount, resolvedSources)
    : (runtimeCountMatchesResolvedUnit ? runtimeSelectedSourceCount : 0);
  return {
    active: true,
    phaseKey: finished ? "completed" : "executing_sources",
    phaseLabel: finished ? "Completed" : "Executing sources",
    mode: totalSources > 0 ? "determinate" : "indeterminate",
    ratio: totalSources > 0 ? Math.max(0, Math.min(1, resolvedSources / totalSources)) : 0,
    counts: {
      resolvedSources,
      sourceCount: totalSources || summarySourceCount || resolvedSources,
      outputCount,
      failedSources,
      excludedSources
    }
  };
}

function deriveLegacyDiscoveryTaskProgress(report, { running = false, phaseHint = "" } = {}) {
  const summary = report?.summary || {};
  const phaseLabel = String(phaseHint || summary.phaseLabel || summary.phase || "").trim();
  const foundCount = Math.max(0, Number(summary.foundEndpointCount ?? 0));
  const probedCount = Math.max(0, Number(summary.probedCandidateCount ?? summary.probedCount ?? 0));
  const queuedCount = deriveDiscoveryQueuedCount(report);
  const deferredCount = Math.max(0, Number(summary.discoverableButDeferredCount ?? 0));
  const failedCount = Math.max(0, Number(summary.failedProbeCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (foundCount > 0 || probedCount > 0 || queuedCount > 0 || failedCount > 0 || Boolean(phaseLabel)));
  if (!active) return null;
  const loss = summary?.lossAccounting && typeof summary.lossAccounting === "object"
    ? summary.lossAccounting
    : {};
  const probeTotal = Math.max(
    0,
    Number(loss.generated ?? 0)
      - Number(loss.dedupSkipped ?? 0)
      - Number(loss.validationSkipped ?? 0)
      - Number(loss.lowEvidenceSkipped ?? 0)
      - Number(summary.suppressedStaticCount ?? 0)
  ) || Math.max(0, probedCount, failedCount, queuedCount);
  const finished = Boolean(report?.finishedAt);
  const phaseKey = finished
    ? "completed"
    : (/prob/i.test(phaseLabel) ? "probing_candidates" : (String(summary.phaseKey || summary.phase || "").trim() || "scanning_sources"));
  return {
    active: !finished,
    phaseKey,
    phaseLabel: finished ? "Discovery completed" : (phaseLabel || "Initializing scan"),
    mode: (finished || (phaseKey === "probing_candidates" && probeTotal > 0)) ? "determinate" : "indeterminate",
    ratio: finished ? 1 : (phaseKey === "probing_candidates" && probeTotal > 0 ? Math.max(0, Math.min(1, probedCount / probeTotal)) : 0),
    counts: {
      foundEndpoints: foundCount,
      probedCandidates: probedCount,
      probeTotal,
      queuedCandidates: queuedCount,
      deferredCandidates: deferredCount,
      failedProbes: failedCount
    }
  };
}

export function deriveFetcherTaskProgress(report, { running = false } = {}) {
  const normalized = normalizeTaskProgressContract(report?.taskProgress);
  const finished = Boolean(String(report?.finishedAt || "").trim());
  if (finished) {
    return deriveLegacyFetcherTaskProgress(report, { running: false });
  }
  return normalized || deriveLegacyFetcherTaskProgress(report, { running });
}

export function deriveDiscoveryTaskProgress(report, { running = false, phaseHint = "" } = {}) {
  const rawTaskProgress = report?.taskProgress;
  let normalized = normalizeTaskProgressContract(rawTaskProgress);
  const finished = Boolean(String(report?.finishedAt || "").trim());
  if (normalized && !finished) {
    const rawObj = rawTaskProgress && typeof rawTaskProgress === "object" && !Array.isArray(rawTaskProgress)
      ? rawTaskProgress
      : null;
    if (rawObj && !Object.prototype.hasOwnProperty.call(rawObj, "active")) {
      normalized = { ...normalized, active: true };
    }
  }
  const nextPhaseHint = String(phaseHint || "").trim();
  if (normalized) {
    const phaseKey = String(normalized.phaseKey || "").trim();
    const phaseLabel = String(normalized.phaseLabel || "").trim();
    const shouldUsePhaseHint = !finished && normalized.active && nextPhaseHint && (
      !phaseLabel
      || /^initializing scan$/i.test(phaseLabel)
      || !phaseKey
      || phaseKey === "starting"
    );
    if (shouldUsePhaseHint) {
      return {
        ...normalized,
        phaseKey: inferDiscoveryPhaseKeyFromLabel(nextPhaseHint, phaseKey),
        phaseLabel: nextPhaseHint
      };
    }
    return normalized;
  }
  return deriveLegacyDiscoveryTaskProgress(report, { running, phaseHint });
}

function formatFetcherCountsLabel(counts, progress) {
  return formatTaskProgressCounts("fetch", counts, progress);
}

function formatDiscoveryCountsLabel(counts, progress) {
  return formatTaskProgressCounts("discovery", counts, progress);
}

export function applyOptimisticDiscoveryRun(model, optimisticRun, nowMs = Date.now()) {
  const baseModel = model && typeof model === "object"
    ? model
    : {
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: false,
        liveTypes: []
      };
  void optimisticRun;
  void nowMs;
  return baseModel;
}

export function deriveFetcherProgressModel(report, { running = false } = {}) {
  return deriveTaskProgressView(
    deriveFetcherTaskProgress(report, { running }),
    {
      taskLabel: "Fetcher",
      fallbackPhaseLabel: "Executing sources",
      formatCountsLabel: formatFetcherCountsLabel
    }
  );
}

export function deriveDiscoveryProgressModel(report, { running = false, phaseHint = "" } = {}) {
  return deriveDiscoveryHybridProgressView(
    deriveDiscoveryTaskProgress(report, { running, phaseHint }),
    {
      taskLabel: "Discovery",
      fallbackPhaseLabel: "Initializing scan",
      formatCountsLabel: formatDiscoveryCountsLabel
    }
  );
}

export function applyOptimisticFetchRun(model, optimisticRun, nowMs = Date.now()) {
  const baseModel = model && typeof model === "object"
    ? model
    : {
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: false,
        liveTypes: []
      };
  void optimisticRun;
  void nowMs;
  return baseModel;
}

function hasReportFailureSignal(row) {
  if (!row || typeof row !== "object") return false;
  const status = normalizeSourceStatusToken(row?.status);
  if (status === "error") return true;
  return String(row?.error || "").trim().length > 0;
}

function classifyFetcherFailureBucket(row) {
  const adapter = String(row?.adapter || row?._groupAdapter || "").trim().toLowerCase();
  const error = String(row?.error || "").trim().toLowerCase();
  const low = `${adapter} ${error}`.trim();
  if (
    /fetch_ok_extract_zero|extract[_ -]?zero|no jobs extracted|no jobs parsed|empty extract|parsed 0 jobs|keptcount.?0/.test(low)
  ) {
    return "extract_zero";
  }
  if (
    /blocked|challenge|captcha|cloudflare|access denied|forbidden|http 403|status 403|linkedin 999|http 999/.test(low)
  ) {
    return "blocked_or_challenge";
  }
  if (/timed out|timeout|time out|read timed out/.test(low)) {
    return "timeout";
  }
  if (
    ["ashby", "personio", "greenhouse", "lever", "smartrecruiters", "workable", "recruitee", "pinpoint", "breezy", "jazzhr", "teamtailor"].includes(adapter)
    && /http 429|rate limit|too many requests/.test(low)
  ) {
    return "provider_rate_limited";
  }
  if (
    ["ashby", "personio", "greenhouse", "lever", "smartrecruiters", "workable", "recruitee", "pinpoint", "breezy", "jazzhr", "teamtailor"].includes(adapter)
    && /http 404|not found|bad config|missing board|missing feed|missing slug|missing board_url|missing feed_url|invalid .*host/.test(low)
  ) {
    return "provider_not_found_or_bad_config";
  }
  return "uncategorized";
}

export function deriveFetcherFailureSummary(report) {
  const groups = Array.isArray(report?.sources) ? report.sources : [];
  const buckets = new Map();
  let topLevelFailedSources = 0;
  let detailFailureCount = 0;

  function pushBucketRow(bucketKey, row) {
    const existing = buckets.get(bucketKey) || {
      key: bucketKey,
      count: 0,
      examples: []
    };
    existing.count += 1;
    const name = String(row?.name || row?.studio || row?._groupName || "unknown").trim();
    if (name && !existing.examples.includes(name) && existing.examples.length < 4) {
      existing.examples.push(name);
    }
    buckets.set(bucketKey, existing);
  }

  groups.forEach(group => {
    if (!group || typeof group !== "object") return;
    if (normalizeSourceStatusToken(group?.status) === "error") {
      topLevelFailedSources += 1;
    }
    const parsedDetails = (Array.isArray(group?.details) ? group.details : [])
      .map(coerceReportDetailRow)
      .filter(detail => detail && typeof detail === "object");
    const failingDetails = parsedDetails.filter(detail => hasReportFailureSignal(detail));
    if (failingDetails.length) {
      detailFailureCount += failingDetails.length;
      failingDetails.forEach(detail => {
        pushBucketRow(classifyFetcherFailureBucket({
          ...detail,
          _groupName: String(group?.name || ""),
          _groupAdapter: String(group?.adapter || "")
        }), detail);
      });
      return;
    }
    if (hasReportFailureSignal(group)) {
      pushBucketRow(classifyFetcherFailureBucket(group), group);
    }
  });

  const bucketOrder = [
    "extract_zero",
    "blocked_or_challenge",
    "timeout",
    "provider_rate_limited",
    "provider_not_found_or_bad_config",
    "uncategorized"
  ];

  return {
    topLevelFailedSources,
    detailFailureCount,
    buckets: bucketOrder
      .map(key => buckets.get(key))
      .filter(Boolean)
  };
}

export function deriveDiscoveryQueuedCount(report) {
  const summary = report?.summary || {};
  const summaryQueued = Math.max(0, Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0));
  const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
  const derivedQueued = candidates.filter(row => row && typeof row === "object" && !row.deferred).length;
  return Math.max(summaryQueued, derivedQueued);
}

export function deriveDiscoveryLifecycleCounts(report) {
  const summary = report?.summary || {};
  return {
    validated: Math.max(0, Number(summary.validatedCandidateCount ?? 0)),
    approved: Math.max(0, Number(summary.approvedCandidateCount ?? 0)),
    live: Math.max(0, Number(summary.liveCandidateCount ?? 0)),
    quarantined: Math.max(0, Number(summary.quarantinedCandidateCount ?? 0))
  };
}

export function getOpsPollIntervalMs(hasLiveRuns, idleMs = 10000, liveMs = 2000) {
  return hasLiveRuns ? Number(liveMs) : Number(idleMs);
}
