import {
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressCounts,
  formatTaskProgressDetail,
  normalizeTaskProgressPayload
} from "./task-progress.js";

const STALLED_AFTER_MS = 10 * 60 * 1000;
const DIAGNOSTIC_LIST_LIMIT = 5;
const DIAGNOSTIC_TEXT_LIMIT = 180;

const TASK_TITLES = {
  fetch: "Fetcher",
  discovery: "Discovery",
  sync: "Sync",
  pipeline: "Pipeline"
};

function compactNumber(value) {
  return Math.max(0, Number(value || 0)).toLocaleString();
}

function hasOwn(source, key) {
  return Object.prototype.hasOwnProperty.call(source || {}, key);
}

function hasSyncLifecycleCounts(summary, progress) {
  const counts = progress?.counts && typeof progress.counts === "object" && !Array.isArray(progress.counts)
    ? progress.counts
    : {};
  return ["activeCount", "pendingCount", "rejectedCount"].some(
    key => hasOwn(counts, key) || hasOwn(summary, key)
  );
}

function syncProgressEvidenceLabel(summary, progress) {
  const counts = progress?.counts && typeof progress.counts === "object" && !Array.isArray(progress.counts)
    ? progress.counts
    : {};
  return formatTaskProgressCounts("sync", counts, progress, summary);
}

function trimDiagnosticText(value, limit = DIAGNOSTIC_TEXT_LIMIT) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}...` : text;
}

function compactPrimitiveMap(value, allowedKeys) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  return allowedKeys.reduce((acc, key) => {
    const raw = source[key];
    if (typeof raw === "number" && Number.isFinite(raw)) {
      acc[key] = raw;
    } else if (typeof raw === "boolean") {
      acc[key] = raw;
    } else if (typeof raw === "string" && raw.trim()) {
      acc[key] = trimDiagnosticText(raw);
    }
    return acc;
  }, {});
}

function compactWorkItem(item) {
  if (!item || typeof item !== "object" || Array.isArray(item)) return null;
  const row = compactPrimitiveMap(item, [
    "id",
    "name",
    "status",
    "stage",
    "phase",
    "source",
    "sourceId",
    "adapter",
    "target",
    "error"
  ]);
  return Object.keys(row).length ? row : null;
}

function compactEvent(item) {
  if (!item || typeof item !== "object" || Array.isArray(item)) return null;
  const row = compactPrimitiveMap(item, [
    "at",
    "timestamp",
    "time",
    "type",
    "level",
    "status",
    "message",
    "detail",
    "source",
    "target"
  ]);
  return Object.keys(row).length ? row : null;
}

function compactAnalysisItem(item) {
  if (!item || typeof item !== "object" || Array.isArray(item)) return null;
  const row = compactPrimitiveMap(item, [
    "id",
    "name",
    "label",
    "title",
    "source",
    "sourceId",
    "adapter",
    "stage",
    "phase",
    "status",
    "target",
    "durationMs",
    "elapsedMs",
    "count",
    "failed",
    "error",
    "message",
    "detail"
  ]);
  return Object.keys(row).length ? row : null;
}

function compactAnalysisList(value) {
  return Array.isArray(value)
    ? value.map(compactAnalysisItem).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
}

function getFirstString(source, keys) {
  const row = source && typeof source === "object" && !Array.isArray(source) ? source : {};
  for (const key of keys) {
    const value = String(row[key] || "").trim();
    if (value) return trimDiagnosticText(value);
  }
  return "";
}

function getFirstTime(source, keys) {
  const value = getFirstString(source, keys);
  const ts = parseTimeMs(value);
  return {
    value,
    ts
  };
}

function timelineSeverity(status, fallback = "muted") {
  const token = String(status || "").trim().toLowerCase();
  if (["error", "failed", "failure", "critical"].includes(token)) return "critical";
  if (["warning", "warn", "stalled", "orphaned"].includes(token)) return "warning";
  if (["running", "active", "completed", "done", "success", "ok"].includes(token)) return "healthy";
  return fallback;
}

function timelineStatusLabel(status, fallback) {
  const token = String(status || fallback || "").trim();
  return token ? trimDiagnosticText(token.replaceAll("_", " ")) : "";
}

function buildTimelineEntries(row, view) {
  const hasPayload = Boolean(row && typeof row === "object" && !Array.isArray(row));
  const safeRow = hasPayload ? row : {};
  const progress = normalizeTaskProgressPayload(safeRow.taskProgress);
  const entries = [];
  let order = 0;

  const addEntry = entry => {
    const label = trimDiagnosticText(entry?.label || entry?.message || "");
    if (!label) return;
    const ts = Number(entry?.timestampMs || 0);
    entries.push({
      source: trimDiagnosticText(entry?.source || "event"),
      timestamp: trimDiagnosticText(entry?.timestamp || ""),
      timestampMs: Number.isFinite(ts) ? ts : 0,
      order,
      type: trimDiagnosticText(entry?.type || ""),
      status: timelineStatusLabel(entry?.status, entry?.type),
      severity: entry?.severity || timelineSeverity(entry?.status),
      label,
      detail: trimDiagnosticText(entry?.detail || "")
    });
    order += 1;
  };

  if (progress) {
    const time = getFirstTime(progress, ["updatedAt", "timestamp", "at"]);
    addEntry({
      source: "progress",
      timestamp: time.value,
      timestampMs: time.ts,
      type: "phase",
      status: progress.phaseKey || progress.phaseLabel || view.status,
      severity: timelineSeverity(view.status, view.severity),
      label: progress.phaseLabel || progress.phaseKey || view.progressLabel,
      detail: view.progressLabel
    });
  }

  (Array.isArray(safeRow.recentEvents) ? safeRow.recentEvents : []).forEach(event => {
    if (!event || typeof event !== "object" || Array.isArray(event)) return;
    const time = getFirstTime(event, ["at", "timestamp", "time", "createdAt", "updatedAt"]);
    const status = getFirstString(event, ["status", "level", "type"]);
    const message = getFirstString(event, ["message", "detail", "type", "status"]);
    addEntry({
      source: "event",
      timestamp: time.value,
      timestampMs: time.ts,
      type: getFirstString(event, ["type", "level"]),
      status,
      severity: timelineSeverity(status),
      label: message,
      detail: getFirstString(event, ["source", "target"])
    });
  });

  (Array.isArray(safeRow.workItems) ? safeRow.workItems : []).forEach(item => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return;
    const status = getFirstString(item, ["status", "stage", "phase"]);
    const token = String(status || "").trim().toLowerCase();
    if (!["running", "active", "failed", "failure", "error", "completed", "complete", "done", "warning"].includes(token)) {
      return;
    }
    const time = getFirstTime(item, ["updatedAt", "finishedAt", "startedAt", "at", "timestamp", "time"]);
    addEntry({
      source: "work item",
      timestamp: time.value,
      timestampMs: time.ts,
      type: getFirstString(item, ["stage", "phase", "adapter"]),
      status,
      severity: timelineSeverity(status),
      label: getFirstString(item, ["name", "id", "sourceId", "source", "target"]),
      detail: getFirstString(item, ["error", "message", "detail"])
    });
  });

  return entries
    .sort((a, b) => {
      if (a.timestampMs && b.timestampMs && a.timestampMs !== b.timestampMs) return a.timestampMs - b.timestampMs;
      if (a.timestampMs && !b.timestampMs) return -1;
      if (!a.timestampMs && b.timestampMs) return 1;
      return a.order - b.order;
    })
    .slice(0, DIAGNOSTIC_LIST_LIMIT)
    .map(({ order: _order, ...entry }) => entry);
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
  const lifecycleStatus = String(row?.lifecycleStatus || "").trim().toLowerCase();
  const raw = String(row?.displayStatus || row?.status || row?.stage || "").trim().toLowerCase();
  const finished = Boolean(String(row?.finishedAt || "").trim());
  const active = Boolean(row?.active || row?.isLive || progress?.active) && !finished;
  if (!active && !finished && !raw) return "waiting";
  if (finished) {
    if (["succeeded", "failed", "orphaned", "canceled"].includes(lifecycleStatus)) {
      return lifecycleStatus;
    }
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
  if (status === "stalled" || status === "completed_with_warnings" || status === "finishing" || status === "canceled") return "warning";
  if (status === "running" || status === "completed" || status === "succeeded" || status === "waiting") return "healthy";
  return "muted";
}

function fallbackProgressLabel(taskType, summary, progress) {
  if (taskType === "fetch") {
    return `output ${compactNumber(summary?.outputCount)} | failed ${compactNumber(summary?.failedSources)}`;
  }
  if (taskType === "discovery") {
    return `queued ${compactNumber(summary?.queuedCandidateCount)} | failed ${compactNumber(summary?.failedProbeCount)}`;
  }
  if (taskType === "sync") {
    if (!hasSyncLifecycleCounts(summary, progress)) {
      return syncProgressEvidenceLabel(summary, progress) || "awaiting progress";
    }
    const action = String(summary?.action || "").trim();
    const actionLabel = action ? `${action} | ` : "";
    return `${actionLabel}active ${compactNumber(summary?.activeCount)} | pending ${compactNumber(summary?.pendingCount)} | rejected ${compactNumber(summary?.rejectedCount)}`;
  }
  if (taskType === "pipeline") {
    const currentStep = Math.max(0, Number(progress?.counts?.currentStep ?? summary?.currentStep ?? 0));
    const totalSteps = Math.max(0, Number(progress?.counts?.totalSteps ?? summary?.totalSteps ?? 0));
    const baseline = Math.max(0, Number(progress?.counts?.baselineOutputCount ?? summary?.baselineOutputCount ?? 0));
    const final = Math.max(0, Number(progress?.counts?.finalOutputCount ?? summary?.finalOutputCount ?? 0));
    if (currentStep > 0 || totalSteps > 0 || baseline > 0 || final > 0) {
      const stepLabel = totalSteps > 0
        ? `step ${compactNumber(currentStep)}/${compactNumber(totalSteps)}`
        : `step ${compactNumber(currentStep)}`;
      return `${stepLabel} | output ${compactNumber(final)} (baseline ${compactNumber(baseline)})`;
    }
  }
  return "";
}

function derivePrimaryLabel(taskType, summary, progress) {
  if (taskType === "fetch") return `${compactNumber(summary?.outputCount)} jobs`;
  if (taskType === "discovery") return `${compactNumber(summary?.queuedCandidateCount)} queued`;
  if (taskType === "sync") {
    const action = String(summary?.action || "").trim();
    return action ? `Sync ${action}` : "Sync";
  }
  if (taskType === "pipeline") return "Pipeline";
  return TASK_TITLES[taskType] || "Task";
}

function deriveSecondaryLabel(taskType, summary, progress) {
  if (taskType === "fetch") {
    const failed = Math.max(0, Number(summary?.failedSources || 0));
    return `${failed.toLocaleString()} failed source${failed === 1 ? "" : "s"}`;
  }
  if (taskType === "discovery") {
    const failed = Math.max(0, Number(summary?.failedProbeCount || 0));
    return `${failed.toLocaleString()} failed probe${failed === 1 ? "" : "s"}`;
  }
  if (taskType === "sync") {
    if (!hasSyncLifecycleCounts(summary, progress)) {
      return syncProgressEvidenceLabel(summary, progress) ? "" : "awaiting progress";
    }
    return `active ${compactNumber(summary?.activeCount)} / pending ${compactNumber(summary?.pendingCount)} / rejected ${compactNumber(summary?.rejectedCount)}`;
  }
  if (taskType === "pipeline") {
    const currentStep = Math.max(0, Number(progress?.counts?.currentStep ?? summary?.currentStep ?? 0));
    const totalSteps = Math.max(0, Number(progress?.counts?.totalSteps ?? summary?.totalSteps ?? 0));
    if (currentStep > 0 || totalSteps > 0) {
      return totalSteps > 0
        ? `step ${compactNumber(currentStep)}/${compactNumber(totalSteps)}`
        : `step ${compactNumber(currentStep)}`;
    }
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
  const nowValue = Number(nowMs || Date.now());
  const heartbeatMs = Math.max(
    parseTimeMs(safeRow?.heartbeatAt),
    parseTimeMs(safeRow?.runtime?.heartbeatAt),
    parseTimeMs(progress?.updatedAt)
  );
  const heartbeatStaleness = Boolean(safeRow?.active || safeRow?.isLive || progress?.active) && heartbeatMs > 0
    ? Math.min(1, Math.max(0, (nowValue - heartbeatMs) / STALLED_AFTER_MS))
    : 0;
  const stallProximity = Boolean(heartbeatStaleness >= 0.75 && ["running", "finishing"].includes(status))
    ? "approaching"
    : null;
  const progressUpdatedAt = String(progress?.updatedAt || "").trim();
  const progressUpdatedMs = parseTimeMs(progressUpdatedAt);
  const progressStale = Boolean(
    progressUpdatedAt
    && ["running", "finishing"].includes(status)
    && progressUpdatedMs > 0
    && (nowValue - progressUpdatedMs) > (STALLED_AFTER_MS / 2)
  );
  const progressDetail = progress
    ? formatTaskProgressDetail(taskType, progress, summary, {
        includeCounts: status === "running" && taskType !== "discovery" ? false : true
      })
    : "";
  const tailBadge = taskType === "fetch" && (safeRow.active || safeRow.isLive)
    ? formatScrapyStaticSourcesTailBadge(safeRow.workItems)
    : "";
  const countsLabel = progress
    ? formatTaskProgressCounts(taskType, progress.counts, progress, summary)
    : "";
  const progressLabel = [
    progressDetail,
    taskType !== "discovery" && countsLabel && !String(progressDetail || "").includes(countsLabel) ? countsLabel : "",
    tailBadge
  ].filter(Boolean).join(" | ") || fallbackProgressLabel(taskType, summary, progress);
  const elapsedMs = deriveElapsedMs(safeRow, nowMs);
  const durationMs = Math.max(0, Number(safeRow.durationMs || 0));
  const finishedAt = String(safeRow.finishedAt || "").trim();
  const statusLabel = status.replaceAll("_", " ");
  const severity = stallProximity === "approaching" ? "warning" : severityForStatus(status);
  return {
    taskType,
    title: TASK_TITLES[taskType] || "Task",
    status,
    statusLabel,
    severity,
    heartbeatStaleness,
    stallProximity,
    heartbeatStalenessLabel: stallProximity === "approaching"
      ? `Heartbeat aging (${Math.round(heartbeatStaleness * 100)}%)`
      : "",
    progressUpdatedAt,
    progressStale,
    progressStaleLabel: progressStale
      ? `Progress stale (last update ${formatDuration(Math.max(0, nowValue - progressUpdatedMs))} ago)`
      : "",
    primaryLabel: derivePrimaryLabel(taskType, summary, progress),
    secondaryLabel: deriveSecondaryLabel(taskType, summary, progress),
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

export function buildTaskRunLogLabel(row, {
  taskType = "",
  running = false,
  nowMs = Date.now(),
  prefix = ""
} = {}) {
  const hasPayload = Boolean(row && typeof row === "object" && !Array.isArray(row));
  const safeRow = hasPayload ? row : {};
  const normalizedTaskType = String(taskType || safeRow.taskType || safeRow.type || "").trim().toLowerCase();
  const finished = Boolean(String(safeRow.finishedAt || "").trim());
  const view = buildTaskRunView({
    ...safeRow,
    ...(normalizedTaskType ? { taskType: normalizedTaskType } : {}),
    active: Boolean(running || safeRow.active || safeRow.isLive) && !finished
  }, { nowMs });
  const title = String(prefix || view.title || TASK_TITLES[view.taskType] || "Task").trim() || "Task";
  const detail = hasPayload
    ? String(view.progressLabel || view.primaryLabel || view.secondaryLabel || "").trim()
    : "";
  const terminal = Boolean(finished || ["completed", "completed_with_warnings", "failed"].includes(view.status));
  const hasFailureSummary = Boolean(String(view.failureSummary || "").trim());
  const levelHint = ["failed", "completed_with_warnings"].includes(view.status) || hasFailureSummary
    ? "warn"
    : (terminal ? "success" : "info");
  return {
    message: detail ? `${title}: ${detail}.` : `${title}: no progress detail available.`,
    levelHint,
    view
  };
}

export function buildTaskRunDiagnostics(row, {
  rowArea = "unknown",
  nowMs = Date.now(),
  generatedAt = new Date().toISOString(),
  runView = null
} = {}) {
  const safeRow = row && typeof row === "object" && !Array.isArray(row) ? row : {};
  const view = runView && typeof runView === "object" && !Array.isArray(runView)
    ? runView
    : buildTaskRunView(safeRow, { nowMs });
  const summaryCounts = compactPrimitiveMap(safeRow.summary, [
    "outputCount",
    "sourceCount",
    "successfulSources",
    "failedSources",
    "okWithWarningSources",
    "queuedCandidateCount",
    "failedProbeCount",
    "foundEndpointCount",
    "generatedCandidateCount",
    "survivedDedupeCandidateCount",
    "probedCandidateCount",
    "discoverableButDeferredCount",
    "currentStageKey",
    "stageIndex",
    "stageTotal",
    "completedStageCount",
    "activeCount",
    "pendingCount",
    "rejectedCount",
    "action",
    "error"
  ]);
  const timing = {
    startedAt: String(safeRow.startedAt || "").trim(),
    finishedAt: String(safeRow.finishedAt || "").trim(),
    heartbeatAt: String(safeRow.heartbeatAt || safeRow.runtime?.heartbeatAt || "").trim(),
    elapsedLabel: view.elapsedLabel || "",
    durationLabel: view.durationLabel || ""
  };
  const diagnosticHints = Array.isArray(view.diagnosticHints)
    ? view.diagnosticHints.map(hint => trimDiagnosticText(hint, 160)).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
  const workItemExamples = Array.isArray(safeRow.workItems)
    ? safeRow.workItems.map(compactWorkItem).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
  const eventExamples = Array.isArray(safeRow.recentEvents)
    ? safeRow.recentEvents.map(compactEvent).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];

  return {
    kind: "admin_run_diagnostics",
    version: 1,
    generatedAt,
    rowArea: String(rowArea || "unknown"),
    taskType: view.taskType || getTaskType(safeRow),
    title: view.title || TASK_TITLES[getTaskType(safeRow)] || "Task",
    runId: String(safeRow.runId || safeRow.id || "").trim(),
    status: view.status || "unknown",
    statusLabel: view.statusLabel || String(safeRow.displayStatus || safeRow.status || "unknown"),
    severity: view.severity || "muted",
    timing,
    primaryLabel: view.primaryLabel || "",
    secondaryLabel: view.secondaryLabel || "",
    progressLabel: view.progressLabel || "",
    progressRatio: Number.isFinite(Number(view.progressRatio)) ? Number(view.progressRatio) : 0,
    progressMode: view.progressMode || "indeterminate",
    currentTarget: trimDiagnosticText(view.currentTarget || ""),
    warningSummary: trimDiagnosticText(view.warningSummary || ""),
    failureSummary: trimDiagnosticText(view.failureSummary || ""),
    remediationHint: trimDiagnosticText(view.remediationHint || ""),
    diagnosticHints,
    summaryCounts,
    workItemExamples,
    eventExamples
  };
}

export function buildTaskRunAnalysis(row, {
  rowArea = "unknown",
  nowMs = Date.now(),
  runView = null
} = {}) {
  const safeRow = row && typeof row === "object" && !Array.isArray(row) ? row : {};
  const view = runView && typeof runView === "object" && !Array.isArray(runView)
    ? runView
    : buildTaskRunView(safeRow, { nowMs });
  const summary = getSummary(safeRow);
  const summaryCounts = compactPrimitiveMap(summary, [
    "outputCount",
    "sourceCount",
    "successfulSources",
    "failedSources",
    "okWithWarningSources",
    "queuedCandidateCount",
    "failedProbeCount",
    "foundEndpointCount",
    "generatedCandidateCount",
    "survivedDedupeCandidateCount",
    "probedCandidateCount",
    "discoverableButDeferredCount",
    "currentStageKey",
    "stageIndex",
    "stageTotal",
    "completedStageCount",
    "activeCount",
    "pendingCount",
    "rejectedCount",
    "action",
    "error"
  ]);
  const diagnosticHints = Array.isArray(view.diagnosticHints)
    ? view.diagnosticHints.map(hint => trimDiagnosticText(hint, 160)).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
  const workItemExamples = Array.isArray(safeRow.workItems)
    ? safeRow.workItems.map(compactWorkItem).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
  const eventExamples = Array.isArray(safeRow.recentEvents)
    ? safeRow.recentEvents.map(compactEvent).filter(Boolean).slice(0, DIAGNOSTIC_LIST_LIMIT)
    : [];
  const slowExamples = [
    ...compactAnalysisList(summary.slowestSources),
    ...compactAnalysisList(summary.slowStages),
    ...compactAnalysisList(summary.slowestStages)
  ].slice(0, DIAGNOSTIC_LIST_LIMIT);

  return {
    kind: "admin_selected_run_analysis",
    version: 1,
    rowArea: String(rowArea || "unknown"),
    taskType: view.taskType || getTaskType(safeRow),
    title: view.title || TASK_TITLES[getTaskType(safeRow)] || "Task",
    runId: String(safeRow.runId || safeRow.id || "").trim(),
    status: view.status || "unknown",
    statusLabel: view.statusLabel || String(safeRow.displayStatus || safeRow.status || "unknown"),
    severity: view.severity || "muted",
    timing: {
      startedAt: String(safeRow.startedAt || "").trim(),
      finishedAt: String(safeRow.finishedAt || "").trim(),
      heartbeatAt: String(safeRow.heartbeatAt || safeRow.runtime?.heartbeatAt || "").trim(),
      elapsedLabel: view.elapsedLabel || "",
      durationLabel: view.durationLabel || ""
    },
    primaryLabel: view.primaryLabel || "",
    secondaryLabel: view.secondaryLabel || "",
    progressLabel: view.progressLabel || "",
    progressRatio: Number.isFinite(Number(view.progressRatio)) ? Number(view.progressRatio) : 0,
    progressMode: view.progressMode || "indeterminate",
    currentTarget: trimDiagnosticText(view.currentTarget || ""),
    warningSummary: trimDiagnosticText(view.warningSummary || ""),
    failureSummary: trimDiagnosticText(view.failureSummary || ""),
    remediationHint: trimDiagnosticText(view.remediationHint || ""),
    diagnosticHints,
    summaryCounts,
    slowExamples,
    workItemExamples,
    eventExamples,
    timelineEntries: buildTimelineEntries(safeRow, view)
  };
}
