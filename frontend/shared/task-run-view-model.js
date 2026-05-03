import {
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressCounts,
  formatTaskProgressDetail,
  normalizeTaskProgressPayload
} from "./task-progress.js";

const STALLED_AFTER_MS = 10 * 60 * 1000;

const TASK_TITLES = {
  fetch: "Fetcher",
  discovery: "Discovery",
  sync: "Sync",
  pipeline: "Pipeline"
};

function compactNumber(value) {
  return Math.max(0, Number(value || 0)).toLocaleString();
}

function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (!value) return "0s";
  if (value < 1000) return `${value}ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)}s`;
  return `${(value / 60_000).toFixed(1)}m`;
}

function formatDateTime(value) {
  const parsed = Date.parse(String(value || ""));
  if (!Number.isFinite(parsed)) return "";
  return new Date(parsed).toLocaleString();
}

function parseTimeMs(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function getTaskType(row) {
  return String(row?.taskType || row?.type || "unknown").trim().toLowerCase() || "unknown";
}

function getSummary(row) {
  return row?.summary && typeof row.summary === "object" && !Array.isArray(row.summary)
    ? row.summary
    : {};
}

function deriveElapsedMs(row, nowMs) {
  const startedMs = parseTimeMs(row?.startedAt);
  if ((row?.active || row?.isLive) && startedMs > 0 && !String(row?.finishedAt || "").trim()) {
    return Math.max(0, Number(nowMs || Date.now()) - startedMs);
  }
  return Math.max(0, Number(row?.elapsedMs ?? row?.durationMs ?? 0));
}

function deriveStatus(row, progress, nowMs) {
  const raw = String(row?.displayStatus || row?.status || row?.stage || "").trim().toLowerCase();
  const finished = Boolean(String(row?.finishedAt || "").trim());
  const active = Boolean(row?.active || row?.isLive || progress?.active) && !finished;
  if (!active && !finished && !raw) return "waiting";
  if (finished) {
    if (raw === "error" || raw === "failed" || raw === "failure") return "failed";
    if (raw === "warning" || raw === "completed_with_warnings") return "completed_with_warnings";
    return "completed";
  }
  if (!active && raw === "running") return "running";
  if (!active && raw === "started") return "orphaned";
  const heartbeatMs = Math.max(
    parseTimeMs(row?.heartbeatAt),
    parseTimeMs(row?.runtime?.heartbeatAt),
    parseTimeMs(progress?.updatedAt)
  );
  if (active && heartbeatMs > 0 && Number(nowMs || Date.now()) - heartbeatMs > STALLED_AFTER_MS) {
    return "stalled";
  }
  const phaseKey = String(progress?.phaseKey || "").trim().toLowerCase();
  if (active && (raw === "finishing" || phaseKey === "finalizing" || phaseKey === "finishing")) return "finishing";
  if (active) return "running";
  if (raw === "warning") return "completed_with_warnings";
  if (raw === "error") return "failed";
  return raw || "unknown";
}

function severityForStatus(status) {
  if (status === "failed" || status === "orphaned") return "critical";
  if (status === "stalled" || status === "completed_with_warnings" || status === "finishing") return "warning";
  if (status === "running" || status === "completed" || status === "waiting") return "healthy";
  return "muted";
}

function fallbackProgressLabel(taskType, summary) {
  if (taskType === "fetch") {
    return `output ${compactNumber(summary?.outputCount)} | failed ${compactNumber(summary?.failedSources)}`;
  }
  if (taskType === "discovery") {
    return `queued ${compactNumber(summary?.queuedCandidateCount)} | failed ${compactNumber(summary?.failedProbeCount)}`;
  }
  if (taskType === "sync") {
    const action = String(summary?.action || "").trim();
    const actionLabel = action ? `${action} | ` : "";
    return `${actionLabel}active ${compactNumber(summary?.activeCount)} | pending ${compactNumber(summary?.pendingCount)} | rejected ${compactNumber(summary?.rejectedCount)}`;
  }
  return "";
}

function derivePrimaryLabel(taskType, summary) {
  if (taskType === "fetch") return `${compactNumber(summary?.outputCount)} jobs`;
  if (taskType === "discovery") return `${compactNumber(summary?.queuedCandidateCount)} queued`;
  if (taskType === "sync") {
    const action = String(summary?.action || "").trim();
    return action ? `Sync ${action}` : "Sync";
  }
  return TASK_TITLES[taskType] || "Task";
}

function deriveSecondaryLabel(taskType, summary) {
  if (taskType === "fetch") {
    const failed = Math.max(0, Number(summary?.failedSources || 0));
    return `${failed.toLocaleString()} failed source${failed === 1 ? "" : "s"}`;
  }
  if (taskType === "discovery") {
    const failed = Math.max(0, Number(summary?.failedProbeCount || 0));
    return `${failed.toLocaleString()} failed probe${failed === 1 ? "" : "s"}`;
  }
  if (taskType === "sync") {
    return `active ${compactNumber(summary?.activeCount)} / pending ${compactNumber(summary?.pendingCount)} / rejected ${compactNumber(summary?.rejectedCount)}`;
  }
  return "";
}

function deriveCurrentTarget(row, progress) {
  const direct = String(row?.currentTarget || row?.targetLabel || progress?.targetLabel || "").trim();
  if (direct) return direct;
  const workItems = Array.isArray(row?.workItems) ? row.workItems : [];
  const runningItem = workItems.find(item => String(item?.status || "").trim().toLowerCase() === "running");
  return String(runningItem?.name || runningItem?.id || "").trim();
}

function deriveFailureSummary(taskType, summary) {
  if (taskType === "fetch") {
    const failed = Math.max(0, Number(summary?.failedSources || 0));
    return failed > 0 ? `${failed.toLocaleString()} failed source${failed === 1 ? "" : "s"}` : "";
  }
  if (taskType === "discovery") {
    const failed = Math.max(0, Number(summary?.failedProbeCount || 0));
    return failed > 0 ? `${failed.toLocaleString()} failed probe${failed === 1 ? "" : "s"}` : "";
  }
  if (taskType === "sync" && String(summary?.error || "").trim()) return String(summary.error).trim();
  return "";
}

function deriveWarningSummary(taskType, summary, status) {
  if (status === "stalled") return "No recent heartbeat";
  if (status === "orphaned") return "Task state has no active owner";
  if (taskType === "fetch") {
    const warnings = Math.max(0, Number(summary?.okWithWarningSources || 0));
    return warnings > 0 ? `${warnings.toLocaleString()} source warning${warnings === 1 ? "" : "s"}` : "";
  }
  return "";
}

function deriveRemediationHint(status) {
  if (status === "stalled") return "Check bridge and task logs; verify whether the task heartbeat stopped.";
  if (status === "orphaned") return "Refresh task state and check whether the owning process exited.";
  return "";
}

export function buildTaskRunView(row, { nowMs = Date.now() } = {}) {
  const safeRow = row && typeof row === "object" && !Array.isArray(row) ? row : {};
  const taskType = getTaskType(safeRow);
  const summary = getSummary(safeRow);
  const progress = normalizeTaskProgressPayload(safeRow.taskProgress);
  const status = deriveStatus(safeRow, progress, nowMs);
  const progressDetail = progress
    ? formatTaskProgressDetail(taskType, progress, summary, { includeCounts: status === "running" ? false : true })
    : "";
  const tailBadge = taskType === "fetch" && (safeRow.active || safeRow.isLive)
    ? formatScrapyStaticSourcesTailBadge(safeRow.workItems)
    : "";
  const countsLabel = progress
    ? formatTaskProgressCounts(taskType, progress.counts, progress, summary)
    : "";
  const progressLabel = [progressDetail, tailBadge].filter(Boolean).join(" | ") || fallbackProgressLabel(taskType, summary);
  const elapsedMs = deriveElapsedMs(safeRow, nowMs);
  const durationMs = Math.max(0, Number(safeRow.durationMs || 0));
  const finishedAt = String(safeRow.finishedAt || "").trim();
  const statusLabel = status.replaceAll("_", " ");
  return {
    taskType,
    title: TASK_TITLES[taskType] || "Task",
    status,
    statusLabel,
    severity: severityForStatus(status),
    primaryLabel: derivePrimaryLabel(taskType, summary),
    secondaryLabel: deriveSecondaryLabel(taskType, summary),
    progressLabel,
    progressRatio: progress?.mode === "determinate" ? Math.max(0, Math.min(1, Number(progress?.ratio || 0))) : 0,
    progressMode: progress?.mode || "indeterminate",
    currentTarget: deriveCurrentTarget(safeRow, progress),
    elapsedLabel: formatDuration(elapsedMs),
    durationLabel: durationMs > 0 ? formatDuration(durationMs) : "",
    finishedLabel: formatDateTime(finishedAt),
    warningSummary: deriveWarningSummary(taskType, summary, status),
    failureSummary: deriveFailureSummary(taskType, summary),
    remediationHint: deriveRemediationHint(status),
    diagnosticHints: [countsLabel].filter(Boolean)
  };
}
