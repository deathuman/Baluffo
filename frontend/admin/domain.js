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
      ?? row?.fetchedCount
      ?? row?.lastFetchedCount
      ?? NaN
  );
  return Number.isFinite(value) ? value : NaN;
}

export function mergeSourceStatusFromReport(rows, report, mode) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const sources = Array.isArray(report?.sources) ? report.sources : [];
  const byName = new Map(sources.map(row => [String(row?.studio || row?.name || "").toLowerCase(), row]));
  return sourceRows.map(row => {
    const key = String(row?.studio || row?.name || "").toLowerCase();
    const matched = byName.get(key) || null;
    if (!matched) return row;
    return {
      ...row,
      _lastStatus: String(matched?.status || ""),
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
    const status = String(row?._lastStatus || row?.status || "").toLowerCase();
    const jobsFound = getSourceJobsFoundCount(row);
    if (filter === "error") return status === "error";
    if (filter === "excluded") return status === "excluded";
    if (filter === "zero") return jobsFound === 0;
    if (filter === "healthy") return status === "ok" || (jobsFound > 0 && !status);
    return true;
  });
}
