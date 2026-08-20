import { getObjectValue } from "./ops-shape-utils.js";

export function getOpsAbortKey(taskType, runId) {
  return `${String(taskType || "").trim().toLowerCase()}|${String(runId || "").trim()}`;
}

export function buildOptimisticAbortRow(row, taskType, runId, pendingAbort) {
  const existingProgress = getObjectValue(row?.taskProgress || row?.progress);
  const summary = getObjectValue(row?.summary);
  const startedAt = String(row?.startedAt || pendingAbort?.startedAt || "").trim();
  return {
    ...getObjectValue(row),
    id: String(row?.id || runId),
    runId,
    taskType,
    type: taskType,
    active: true,
    isLive: true,
    status: "running",
    displayStatus: "aborting",
    lifecycleStatus: "aborting",
    stage: "aborting",
    startedAt,
    finishedAt: "",
    taskProgress: {
      ...existingProgress,
      active: true,
      phaseKey: "aborting",
      phaseLabel: "Aborting...",
      label: "Aborting...",
      mode: String(existingProgress?.mode || "indeterminate")
    },
    summary: {
      ...summary,
      abortRequestedAt: String(summary?.abortRequestedAt || pendingAbort?.requestedAt || ""),
      abortReason: String(summary?.abortReason || "admin_ops_abort")
    }
  };
}

export function isAbortAcceptedResult(result) {
  return Boolean(
    result?.ok
    || result?.abortAccepted
    || result?.gatewayAccepted
    || result?.accepted
    || Number(result?.status) === 202
  );
}
