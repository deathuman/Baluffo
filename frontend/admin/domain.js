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

function toSourceMatchKeys(row) {
  const out = new Set();
  const studio = String(row?.studio || "").trim().toLowerCase();
  const name = String(row?.name || "").trim().toLowerCase();
  if (studio) out.add(studio);
  if (name) out.add(name);
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

export function getOpsPollIntervalMs(hasLiveRuns, idleMs = 10000, liveMs = 2000) {
  return Boolean(hasLiveRuns) ? Number(liveMs) : Number(idleMs);
}
