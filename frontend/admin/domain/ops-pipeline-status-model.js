import { getObjectValue } from "./ops-shape-utils.js";

export function getTaskRowType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

export function getTaskRowRunId(row) {
  return String(row?.runId || row?.id || "").trim();
}

function getPipelineRunIdFromTaskState(taskStatePayload = {}) {
  const rows = Array.isArray(taskStatePayload?.tasks) ? taskStatePayload.tasks : [];
  const pipelineRow = rows.find(row => getTaskRowType(row) === "pipeline" && String(row?.runId || row?.id || "").trim());
  return String(pipelineRow?.runId || pipelineRow?.id || "").trim();
}

function getPipelineChildSignature(taskStatePayload = {}, pipelineRunId = "") {
  const cleanPipelineRunId = String(pipelineRunId || "").trim();
  const rows = Array.isArray(taskStatePayload?.tasks) ? taskStatePayload.tasks : [];
  const hasMatchingPipelineRow = Boolean(cleanPipelineRunId) && rows.some(row => (
    getTaskRowType(row) === "pipeline"
    && String(row?.runId || row?.id || "").trim() === cleanPipelineRunId
  ));
  return rows
    .filter(row => {
      const type = getTaskRowType(row);
      if (!type || type === "pipeline" || row?.active === false || row?.finishedAt) return false;
      const parentRunId = String(row?.parentRunId || row?.summary?.pipelineRunId || "").trim();
      const parentTaskType = String(row?.parentTaskType || "").trim().toLowerCase();
      return cleanPipelineRunId
        ? parentRunId === cleanPipelineRunId
          || (!parentRunId && parentTaskType === "pipeline")
          || (hasMatchingPipelineRow && !parentRunId && !parentTaskType)
        : parentTaskType === "pipeline" || Boolean(parentRunId);
    })
    .map(row => `${getTaskRowType(row)}|${String(row?.runId || row?.id || "").trim()}`)
    .filter(Boolean)
    .sort()
    .join(",");
}

export function shouldKeepExistingActiveTaskState(existingPayload, pipelineStatusPayload, hasActiveRows) {
  if (!hasActiveRows(existingPayload)) return false;
  if (existingPayload?.source === "pipeline-status") return false;
  const pipelineRunId = getPipelineRunIdFromTaskState(pipelineStatusPayload);
  if (!pipelineRunId) return true;
  const existingRows = Array.isArray(existingPayload?.tasks) ? existingPayload.tasks : [];
  const hasRelatedPipelineRows = existingRows.some(row => {
    const type = getTaskRowType(row);
    const parentRunId = String(row?.parentRunId || row?.summary?.pipelineRunId || "").trim();
    const parentTaskType = String(row?.parentTaskType || "").trim().toLowerCase();
    return (
      type === "pipeline" && String(row?.runId || row?.id || "").trim() === pipelineRunId
    ) || parentRunId === pipelineRunId
      || (!parentRunId && parentTaskType === "pipeline");
  });
  if (!hasRelatedPipelineRows) return true;
  const nextChildSignature = getPipelineChildSignature(pipelineStatusPayload, pipelineRunId);
  if (!nextChildSignature) return true;
  return getPipelineChildSignature(existingPayload, pipelineRunId) === nextChildSignature;
}

export function buildPipelineTaskStatePayload(payload = {}) {
  if (!payload?.active) return null;
  const progress = getObjectValue(payload?.progress);
  const runId = String(payload?.runId || "").trim();
  const startedAt = String(payload?.startedAt || "").trim();
  const stage = String(payload?.stage || progress.phaseKey || "pipeline").trim();
  const progressLabel = String(
    progress.phaseLabel
    || progress.label
    || payload?.progressLabel
    || (stage && stage !== "pipeline" ? `Pipeline ${stage}` : "Pipeline running")
  ).trim();
  const parentSummary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  const activeChildren = Array.isArray(payload?.activeChildren)
    ? payload.activeChildren
        .filter(row => row && typeof row === "object" && row.active !== false)
        .slice(0, 3)
        .map(row => ({
          ...row,
          id: String(row.id || row.runId || ""),
          runId: String(row.runId || row.id || ""),
          taskType: String(row.taskType || row.type || "").trim().toLowerCase(),
          type: String(row.type || row.taskType || "").trim().toLowerCase(),
          active: true,
          status: String(row.status || "running"),
          displayStatus: String(row.displayStatus || row.status || "running"),
          controlPlaneSource: "pipeline-status",
          displayOnly: true,
          summary: {
            ...(row.summary && typeof row.summary === "object" ? row.summary : {}),
            controlPlane: true,
            pipelineRunId: runId
          },
          taskProgress: row.taskProgress && typeof row.taskProgress === "object"
            ? { ...row.taskProgress, active: true }
            : {
                active: true,
                phaseKey: String(row.taskType || row.type || "task").trim().toLowerCase(),
                phaseLabel: "Task running",
                mode: "indeterminate",
                ratio: 0,
                counts: {}
              }
        }))
    : [];
  const parentRow = {
    taskType: "pipeline",
    type: "pipeline",
    runId,
    active: true,
    startedAt,
    status: "running",
    controlPlaneSource: "pipeline-status",
    taskProgress: {
      ...progress,
      phaseKey: progress.phaseKey || stage,
      phaseLabel: progressLabel
    },
    summary: {
      ...parentSummary,
      activeChildTaskType: activeChildren[0]?.taskType || (stage && stage !== "pipeline" ? stage : undefined),
      activeChildRunId: activeChildren[0]?.runId || "",
      activeChildPhaseLabel: activeChildren[0]?.taskProgress?.phaseLabel || "",
      activeChildDisplayLabel: activeChildren[0]?.taskProgress?.phaseLabel
        ? `${String(activeChildren[0]?.taskType || "").trim()}: ${activeChildren[0].taskProgress.phaseLabel}`
        : "",
      stage
    }
  };
  const tasks = [
    ...activeChildren,
    parentRow
  ];
  return {
    tasks,
    count: tasks.length,
    summary: true,
    source: "pipeline-status"
  };
}
