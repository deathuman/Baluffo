export function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (!value) return "0s";
  if (value < 1000) return `${value}ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)}s`;
  return `${(value / 60_000).toFixed(1)}m`;
}

export function formatDateTime(value) {
  const parsed = Date.parse(String(value || ""));
  if (!Number.isFinite(parsed)) return "unknown";
  return new Date(parsed).toLocaleString();
}

export function formatScheduleCell(entry) {
  const interval = Number(entry?.intervalHours || 0);
  const next = formatDateTime(entry?.nextRunAt || "");
  if (interval > 0) return `every ${interval}h, next ${next}`;
  if (String(entry?.note || "") === "manual_task") return "manual task (no interval)";
  return "unknown";
}

export function sanitizeSlowSourceName(value, maxLen = 64) {
  const text = String(value || "")
    .replace(/[^\x20-\x7E]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) return "unknown-source";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3)).trim()}...`;
}

export function formatLastRunCell(lastRun) {
  const type = String(lastRun?.type || "");
  const status = String(lastRun?.status || "");
  const finished = formatDateTime(lastRun?.finishedAt || "");
  if (!type) return "none";
  return `${type} ${status} @ ${finished}`;
}

export function buildRunStatusTooltip(row) {
  const status = String(row?.status || "").toLowerCase();
  if (status !== "warning" && status !== "error") return "";
  const type = String(row?.type || "").toLowerCase();
  const summary = row?.summary || {};
  const parts = [];
  if (type === "discovery") {
    const failed = Number(summary?.failedProbeCount || 0);
    const probed = Number(summary?.probedCandidateCount || 0);
    const queued = Number(summary?.queuedCandidateCount || 0);
    parts.push(`Failed probes: ${failed}`);
    if (probed > 0) parts.push(`Probed: ${probed}`);
    if (queued >= 0) parts.push(`Review queue: ${queued}`);
  } else {
    const failed = Number(summary?.failedSources || 0);
    const sourceCount = Number(summary?.sourceCount || 0);
    const output = Number(summary?.outputCount || 0);
    parts.push(`Failed sources: ${failed}`);
    if (sourceCount > 0) parts.push(`Sources: ${sourceCount}`);
    parts.push(`Output: ${output}`);
  }
  const durationMs = Number(row?.durationMs || 0);
  if (durationMs > 0) parts.push(`Duration: ${formatDuration(durationMs)}`);
  const stamp = formatDateTime(row?.finishedAt || row?.startedAt || "");
  if (stamp && stamp !== "unknown") parts.push(`Finished: ${stamp}`);
  return parts.join(" | ");
}

export function getRunStatusChipClass(status) {
  const token = String(status || "").toLowerCase();
  if (token === "error") return "critical";
  if (token === "warning") return "warning";
  if (token === "running" || token === "started") return "healthy";
  return "healthy";
}

export function formatSignedInt(value) {
  const num = Number(value) || 0;
  return num > 0 ? `+${num}` : `${num}`;
}

export function stableOpsSignature(value) {
  try {
    if (Array.isArray(value)) {
      return JSON.stringify(value.map(item => item || {}));
    }
    return JSON.stringify(value || {});
  } catch {
    return String(value || "");
  }
}

export const FETCHER_FAILURE_BUCKET_LABELS = {
  extract_zero: "Extract Zero",
  blocked_or_challenge: "Blocked/Challenge",
  timeout: "Timeout",
  provider_rate_limited: "Provider Rate Limited",
  provider_not_found_or_bad_config: "Provider Bad Config",
  uncategorized: "Uncategorized"
};
