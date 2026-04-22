export function formatDurationCompact(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (value < 1000) return `${value}ms`;
  if (value < 60_000) return `${Math.round(value / 1000)}s`;
  const minutes = Math.floor(value / 60_000);
  const seconds = Math.round((value % 60_000) / 1000);
  return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
}

export function formatStageTopSummary(report) {
  const timing = report?.runtime?.timingSummary || {};
  const stageTop = Array.isArray(timing?.stageTop) ? timing.stageTop : [];
  if (!stageTop.length) return "";
  return stageTop
    .slice(0, 3)
    .map(item => `${String(item?.stage || "unknown")} ${formatDurationCompact(item?.durationMs)}`)
    .join(" | ");
}

export function selectSlowSources(report) {
  const runtimeSlowest = Array.isArray(report?.runtime?.slowestSources) ? report.runtime.slowestSources : [];
  if (runtimeSlowest.length) return runtimeSlowest;
  const sources = Array.isArray(report?.sources) ? report.sources : [];
  return sources
    .filter(source => Number(source?.durationMs || 0) >= 20_000)
    .sort((a, b) => Number(b?.durationMs || 0) - Number(a?.durationMs || 0))
    .slice(0, 5);
}

export async function fetchJobsFetchReportJsonWithRetry(
  fetchJobsFetchReportJson,
  options = {},
  maxAttempts = 3,
  delayMs = 850
) {
  let attempt = 0;
  while (attempt < Math.max(1, Number(maxAttempts) || 1)) {
    attempt += 1;
    const report = await fetchJobsFetchReportJson(options);
    if (report) return report;
    if (attempt < maxAttempts) {
      await new Promise(resolve => {
        window.setTimeout(resolve, Math.max(100, Number(delayMs) || 850));
      });
    }
  }
  return null;
}
