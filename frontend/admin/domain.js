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
  const raw = String(row?.finishedAt || row?.startedAt || "").trim();
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeRunStatus(value) {
  const token = String(value || "").trim().toLowerCase();
  return token || "unknown";
}

function isRunLive(row) {
  return normalizeRunStatus(row?.status) === "started" && !String(row?.finishedAt || "").trim();
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
  const sorted = [...rows].sort((a, b) => parseRunTimestampMs(b) - parseRunTimestampMs(a));
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
    .filter(row => !isRunLive(row))
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

function compactCount(value) {
  return Number(value || 0).toLocaleString();
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
  const startedAt = String(optimisticRun?.startedAt || "").trim();
  if (!startedAt) return baseModel;

  const currentRows = Array.isArray(baseModel.currentRows) ? [...baseModel.currentRows] : [];
  const visibleCompletedRows = Array.isArray(baseModel.visibleCompletedRows) ? [...baseModel.visibleCompletedRows] : [];
  const olderCompletedRows = Array.isArray(baseModel.olderCompletedRows) ? [...baseModel.olderCompletedRows] : [];
  const allRows = [...currentRows, ...visibleCompletedRows, ...olderCompletedRows];

  const hasLiveDiscovery = currentRows.some(row => (
    String(row?.type || "").trim().toLowerCase() === "discovery"
    && Boolean(row?.isLive)
  ));
  if (hasLiveDiscovery) return baseModel;

  const hasCompletedMatch = allRows.some(row => (
    String(row?.type || "").trim().toLowerCase() === "discovery"
    && String(row?.startedAt || "").trim() === startedAt
    && String(row?.finishedAt || "").trim()
  ));
  if (hasCompletedMatch) return baseModel;

  currentRows.push({
    id: String(optimisticRun?.runId || `optimistic-discovery:${startedAt}`),
    type: "discovery",
    status: "started",
    startedAt,
    finishedAt: "",
    durationMs: 0,
    summary: {},
    isLive: true,
    elapsedMs: Math.max(0, Number(nowMs || Date.now()) - parseRunTimestampMs({ startedAt })),
    displayStatus: "running",
    optimistic: true
  });
  currentRows.sort((a, b) => parseRunTimestampMs(b) - parseRunTimestampMs(a));
  const liveTypes = Array.from(new Set([
    ...currentRows
      .filter(row => Boolean(row?.isLive))
      .map(row => String(row?.type || "").toLowerCase())
      .filter(Boolean),
    ...(Array.isArray(baseModel.liveTypes) ? baseModel.liveTypes : [])
  ]));

  return {
    ...baseModel,
    currentRows,
    visibleCompletedRows,
    olderCompletedRows,
    hasLiveRuns: currentRows.some(row => Boolean(row?.isLive)),
    liveTypes
  };
}

export function deriveFetcherProgressModel(report, { running = false } = {}) {
  const summary = report?.summary || {};
  const runtime = report?.runtime || {};
  const totalSources = Math.max(
    0,
    Number(runtime.selectedSourceCount || 0),
    Number(summary.sourceCount || 0)
  );
  const successfulSources = Math.max(0, Number(summary.successfulSources || 0));
  const failedSources = Math.max(0, Number(summary.failedSources || 0));
  const excludedSources = Math.max(0, Number(summary.excludedSources || 0));
  const resolvedSources = successfulSources + failedSources + excludedSources;
  const outputCount = Math.max(0, Number(summary.outputCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (resolvedSources > 0 || outputCount > 0));
  if (!active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }
  const determinate = totalSources > 0;
  const ratio = determinate ? Math.max(0, Math.min(1, resolvedSources / totalSources)) : 0;
  const resolvedLabel = determinate
    ? `${compactCount(Math.min(resolvedSources, totalSources))}/${compactCount(totalSources)} sources resolved`
    : `${compactCount(resolvedSources)} sources resolved`;
  return {
    active: true,
    determinate,
    ratio,
    label: `Fetcher: ${resolvedLabel} | output ${compactCount(outputCount)} | failed ${compactCount(failedSources)} | excluded ${compactCount(excludedSources)}`
  };
}

export function deriveDiscoveryProgressModel(report, { running = false } = {}) {
  const summary = report?.summary || {};
  const foundCount = Math.max(0, Number(summary.foundEndpointCount ?? 0));
  const probedCount = Math.max(0, Number(summary.probedCandidateCount ?? summary.probedCount ?? 0));
  const queuedCount = Math.max(0, Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0));
  const failedCount = Math.max(0, Number(summary.failedProbeCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (foundCount > 0 || probedCount > 0 || queuedCount > 0 || failedCount > 0));
  if (!active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }

  const determinate = foundCount > 0 || probedCount > 0;
  const total = Math.max(foundCount, probedCount, 1);
  const ratio = determinate ? Math.max(0, Math.min(1, probedCount / total)) : 0;
  const label = determinate
    ? `Discovery: probed ${compactCount(probedCount)}/${compactCount(total)} found so far | queued ${compactCount(queuedCount)} | failed ${compactCount(failedCount)}`
    : `Discovery: scanning candidates | queued ${compactCount(queuedCount)} | failed ${compactCount(failedCount)}`;
  return {
    active: true,
    determinate,
    ratio,
    label
  };
}

export function getOpsPollIntervalMs(hasLiveRuns, idleMs = 10000, liveMs = 2000) {
  return Boolean(hasLiveRuns) ? Number(liveMs) : Number(idleMs);
}
