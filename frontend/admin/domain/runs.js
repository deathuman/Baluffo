import { getTaskStateRows } from "../../shared/live-task.js";
import { normalizeTaskProgressPayload } from "../../shared/task-progress.js";

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
  return Boolean(row?.active);
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
    finishedAt: live ? "" : String(row?.finishedAt || "").trim(),
    elapsedMs,
    displayStatus: status
  };
}

function normalizeCurrentTaskStateRow(row, nowMs = Date.now()) {
  if (!row || typeof row !== "object" || Array.isArray(row)) return null;
  const taskType = String(row.taskType || row.type || "").trim().toLowerCase();
  if (!taskType) return null;
  const startedAt = String(row.startedAt || "").trim();
  const startedMs = Date.parse(startedAt);
  const active = Boolean(row.active);
  const finishedAt = active ? "" : String(row.finishedAt || "").trim();
  const taskProgress = normalizeTaskProgressPayload(row.taskProgress);
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

export function normalizeOpsRuns(runs, nowMs = Date.now()) {
  const rows = Array.isArray(runs) ? runs.filter(row => row && typeof row === "object") : [];
  const dedupedRows = [];
  const runIdMatches = new Map();
  const legacyMatches = new Map();
  rows.forEach(row => {
    const runId = String(row?.runId || "").trim();
    const type = String(row?.type || "").trim().toLowerCase();
    const startedAt = String(row?.startedAt || "").trim();
    const finishedAt = String(row?.finishedAt || "").trim();
    const status = String(row?.status || "").trim().toLowerCase();

    void finishedAt;
    void status;

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

  return {
    currentRows: Array.from(currentByType.values()),
    visibleCompletedRows: Array.isArray(completedModel.visibleCompletedRows) ? completedModel.visibleCompletedRows : [],
    olderCompletedRows: Array.isArray(completedModel.olderCompletedRows) ? completedModel.olderCompletedRows : [],
    hasLiveRuns: currentByType.size > 0,
    liveTypes: Array.from(currentByType.keys())
  };
}

export function getOpsPollIntervalMs(hasLiveRuns, idleMs = 10000, liveMs = 2000) {
  return hasLiveRuns ? Number(liveMs) : Number(idleMs);
}
