import {
  clearJobsPipelinePolling as clearJobsPipelinePollingFromModule,
  formatBlockingTaskProgressLabel,
  getJobsUpdateTooltip,
  getPipelineRunningLabel,
  JOBS_UPDATE_COPY,
  scheduleJobsPipelineStatusPoll as scheduleJobsPipelineStatusPollFromModule,
  updateJobsPipelineUi as updateJobsPipelineUiFromModule
} from "../pipeline.js?v=10";
import { isActiveTaskStateRow } from "../../../shared/live-task.js";

const BLOCKING_TASK_TYPES = new Set(["pipeline", "fetch", "discovery", "sync"]);
const ABORTABLE_TASK_TYPES = new Set(["pipeline", "fetch", "discovery"]);
const ABORT_TERMINAL_REASON = "user_abort_requested";
const ABORT_REQUEST_TIMEOUT_MS = 20000;
const ABORT_REQUEST_VERIFY_GRACE_MS = 5000;
const PIPELINE_START_TIMEOUT_MS = 18000;
const PIPELINE_ACTIVE_STATUS_GRACE_MS = 45000;
const IDLE_TASK_STATE_RECHECK_MS = 5000;

/**
 * @param {import("../../../shared/types.js").TaskStatePayload|null|undefined} payload
 * @returns {Array<import("../../../shared/types.js").TaskStateRow>}
 */
function getTaskStateRows(payload) {
  return Array.isArray(payload?.tasks) ? payload.tasks : [];
}

/**
 * @param {import("../../../shared/types.js").TaskStateRow|null|undefined} task
 * @returns {string}
 */
function normalizeTaskType(task) {
  return String(task?.taskType || task?.type || "").trim().toLowerCase();
}

function taskRunId(task) {
  return String(task?.runId || task?.id || "").trim();
}

function shouldRecheckIdleTaskState(jobsPipelineUiState) {
  const lastCheckedAt = Number(jobsPipelineUiState.lastTaskStateSummaryCheckedAt || 0);
  return !lastCheckedAt || Date.now() - lastCheckedAt >= IDLE_TASK_STATE_RECHECK_MS;
}

function normalizeAbortTarget(task) {
  const taskType = normalizeTaskType(task);
  const runId = taskRunId(task);
  return taskType && runId ? { taskType, runId } : null;
}

function isAbortableTask(task) {
  return isActiveTaskStateRow(task) && ABORTABLE_TASK_TYPES.has(normalizeTaskType(task)) && taskRunId(task);
}

function taskMatchesAbortTarget(task, target) {
  return Boolean(
    target?.taskType
    && target?.runId
    && normalizeTaskType(task) === target.taskType
    && taskRunId(task) === target.runId
  );
}

function payloadStage(payload) {
  return String(payload?.stage || "").trim().toLowerCase();
}

function payloadTerminalReason(payload) {
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  return String(payload?.terminalReason || summary.terminalReason || "").trim();
}

function isAbortRequestedError(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized.includes("pipeline abort requested")
    || normalized.includes("pipeline child abort requested");
}

function isUserAbortCompletionPayload(payload) {
  return payloadStage(payload) === "canceled"
    || payloadTerminalReason(payload) === ABORT_TERMINAL_REASON
    || isAbortRequestedError(payload?.error);
}

function isAbortPendingPayload(payload) {
  const stage = payloadStage(payload);
  const progress = payload?.progress && typeof payload.progress === "object" ? payload.progress : {};
  return stage === "aborting"
    || stage === "abort_pending_sync"
    || String(progress.phaseKey || "").trim().toLowerCase() === "aborting";
}

/**
 * @param {import("../../../shared/types.js").TaskStateRow} task
 * @returns {{active: boolean, stage: string, startedAt: string, progress?: {label: string}}}
 */
function buildBlockingTaskPayload(task) {
  const taskType = normalizeTaskType(task);
  const taskProgress = task?.taskProgress && typeof task.taskProgress === "object"
    ? task.taskProgress
    : {};
  const summary = task?.summary && typeof task.summary === "object"
    ? task.summary
    : {};
  const syncAction = taskType === "sync"
    ? String(summary.action || taskProgress?.counts?.lastAction || "").trim().toLowerCase()
    : "";
  const payload = {
    active: true,
    stage: taskType === "sync" && syncAction ? `sync_${syncAction}` : (taskType || "pipeline"),
    startedAt: String(task?.startedAt || "")
  };
  if (taskType === "pipeline") {
    const phaseLabel = String(taskProgress.phaseLabel || "").trim();
    if (phaseLabel) {
      payload.progress = { label: phaseLabel };
    }
  }
  return payload;
}

/**
 * @param {import("../../../shared/types.js").TaskStateRow|null|undefined} task
 * @returns {string}
 */
function taskCoverageScope(task) {
  const summary = task?.summary && typeof task.summary === "object" ? task.summary : {};
  const runtime = task?.runtime && typeof task.runtime === "object" ? task.runtime : {};
  return String(task?.coverageScope || summary.coverageScope || runtime.coverageScope || "").trim();
}

/**
 * @param {import("../../../shared/types.js").TaskStateRow|null|undefined} task
 * @returns {boolean}
 */
function isFirstRunBootstrapTask(task) {
  const runId = String(task?.runId || "").trim();
  const taskName = String(task?.task || task?.name || "").trim();
  return runId.startsWith("jobs_bootstrap_")
    || taskName === "jobs_bootstrap"
    || taskCoverageScope(task) === "bootstrap_sheets";
}

/**
 * @param {import("../../../shared/types.js").TaskStatePayload|null|undefined} taskStatePayload
 * @param {string} [trackedRunId]
 * @returns {import("../../../shared/types.js").TaskStateRow|null}
 */
function getBlockingTask(taskStatePayload, trackedRunId = "") {
  const tasks = getTaskStateRows(taskStatePayload);
  const activeTasks = tasks.filter(task => (
    isActiveTaskStateRow(task) && BLOCKING_TASK_TYPES.has(normalizeTaskType(task))
  ));
  if (!activeTasks.length) {
    return null;
  }
  if (trackedRunId) {
    const trackedPipelineTask = activeTasks.find(task => (
      normalizeTaskType(task) === "pipeline" && String(task?.runId || "") === trackedRunId
    ));
    if (trackedPipelineTask) {
      return trackedPipelineTask;
    }
  }
  for (const taskType of ["pipeline", "fetch", "discovery", "sync"]) {
    const match = activeTasks.find(task => normalizeTaskType(task) === taskType);
    if (match) {
      return match;
    }
  }
  return activeTasks[0] || null;
}

function resetPipelineStatusPollFailures(jobsPipelineUiState) {
  if (jobsPipelineUiState) {
    jobsPipelineUiState.statusPollFailureCount = 0;
  }
}

function markPipelineStatusPollFailure(jobsPipelineUiState) {
  if (!jobsPipelineUiState) return 1;
  const nextCount = Math.max(0, Number(jobsPipelineUiState.statusPollFailureCount || 0)) + 1;
  jobsPipelineUiState.statusPollFailureCount = nextCount;
  return nextCount;
}

function rememberActivePipelinePayload(jobsPipelineUiState, payload) {
  if (!jobsPipelineUiState || !payload?.active) return;
  jobsPipelineUiState.lastActivePipelinePayload = { ...payload, active: true };
  jobsPipelineUiState.lastActivePipelineSeenAt = Date.now();
}

function getRecentActivePipelinePayload(jobsPipelineUiState) {
  if (!jobsPipelineUiState) return null;
  const seenAt = Number(jobsPipelineUiState.lastActivePipelineSeenAt || 0);
  const payload = jobsPipelineUiState.lastActivePipelinePayload;
  if (!payload?.active || !seenAt || Date.now() - seenAt > PIPELINE_ACTIVE_STATUS_GRACE_MS) {
    return null;
  }
  return payload;
}

function clearRememberedActivePipeline(jobsPipelineUiState) {
  if (!jobsPipelineUiState) return;
  jobsPipelineUiState.lastActivePipelinePayload = null;
  jobsPipelineUiState.lastActivePipelineSeenAt = 0;
}

function pipelineStartAttachedToast(message) {
  const normalized = String(message || "").trim().toLowerCase();
  return normalized.includes("already running")
    ? "Job update already running."
    : "Job update is running.";
}

function pipelineStartFailureToast(message) {
  const normalized = String(message || "").trim().toLowerCase();
  if (
    normalized.includes("another")
    && normalized.includes("task")
    && normalized.includes("already running")
  ) {
    return "Another background task is still running.";
  }
  if (normalized.includes("already running")) return "Job update already running.";
  return JOBS_UPDATE_COPY.startFailed;
}

export function createJobsPipelineController({
  refs,
  jobsPipelineUiState,
  callJobsBridge,
  getAllJobs,
  showToast,
  setRefreshJobsNeedsAttention,
  refreshJobsAfterPipelineCompletion = null,
  isErrorStage,
  pollDelayMs,
  idlePollDelayMs,
  isContainerRuntimeMode = () => false
}) {
  function updateJobsPipelineUi({
    pipelinePayload = null,
    running = false,
    disabled = false,
    buttonLabel = "",
    progressLabel = "",
    firstRunBootstrapActive = false,
    isError = false,
    abortTask = null
  } = {}) {
    const abortable = isAbortableTask(abortTask) && !jobsPipelineUiState.abortRequested;
    const aborting = Boolean(jobsPipelineUiState.abortRequested || isAbortPendingPayload(pipelinePayload));
    if (!abortable || aborting) {
      jobsPipelineUiState.abortRevealActive = false;
    }
    updateJobsPipelineUiFromModule(refs, {
      pipelinePayload,
      running,
      disabled,
      buttonLabel: aborting ? JOBS_UPDATE_COPY.abortingLabel : buttonLabel,
      progressLabel,
      buttonTooltip: getJobsUpdateTooltip({
        bridgeError: jobsPipelineUiState.updateTooltipBridgeError,
        firstRunBootstrapActive: Boolean(
          firstRunBootstrapActive || jobsPipelineUiState.updateTooltipFirstRunBootstrapActive
        ),
        firstRun: jobsPipelineUiState.updateTooltipFirstRun,
        firstRunKnown: jobsPipelineUiState.updateTooltipFirstRunKnown
      }),
      isError,
      abortable,
      aborting
    });
    jobsPipelineUiState.abortTask = abortable ? normalizeAbortTarget(abortTask) : null;
    syncJobsPipelineAbortButton();
  }

  function resetJobsPipelineAbortState() {
    jobsPipelineUiState.abortRequested = false;
    jobsPipelineUiState.abortRequestedTask = null;
    jobsPipelineUiState.abortRevealActive = false;
    jobsPipelineUiState.abortRequestError = "";
    jobsPipelineUiState.abortRequestErrorAt = 0;
  }

  function syncJobsPipelineAbortButton() {
    const abortButton = refs.jobsPipelineRunBtn?.parentElement?.querySelector?.('[data-ui="jobs-pipeline-abort"]');
    if (!abortButton) return;
    abortButton.onclick = event => {
      event?.preventDefault?.();
      event?.stopPropagation?.();
      requestJobsPipelineAbort().catch(() => {});
    };
  }

  function hasAbortTask() {
    const task = jobsPipelineUiState.abortTask;
    return Boolean(task?.taskType && task?.runId);
  }

  function abortTargetStillActive({ pipelinePayload, taskStatePayload, taskStateKnown, target }) {
    if (!target?.taskType || !target?.runId) return false;
    const pipelineRunId = String(pipelinePayload?.runId || "").trim();
    if (
      target.taskType === "pipeline"
      && Boolean(pipelinePayload?.active)
      && pipelineRunId === target.runId
    ) {
      return true;
    }
    if (!taskStateKnown) return false;
    return getTaskStateRows(taskStatePayload).some(task => (
      isActiveTaskStateRow(task) && taskMatchesAbortTarget(task, target)
    ));
  }

  function reconcileAbortRequest({ pipelinePayload, taskStatePayload, taskStateKnown }) {
    if (!jobsPipelineUiState.abortRequested) return;
    const target = jobsPipelineUiState.abortRequestedTask;
    if (!target?.taskType || !target?.runId) {
      resetJobsPipelineAbortState();
      return;
    }
    const pipelineRunId = String(pipelinePayload?.runId || "").trim();
    const targetIsPipelinePayload = target.taskType === "pipeline"
      && (!pipelineRunId || pipelineRunId === target.runId);
    if (targetIsPipelinePayload && isUserAbortCompletionPayload(pipelinePayload)) {
      resetJobsPipelineAbortState();
      return;
    }
    const targetStillActive = abortTargetStillActive({
      pipelinePayload,
      taskStatePayload,
      taskStateKnown,
      target
    });
    if (!targetStillActive && (target.taskType === "pipeline" || taskStateKnown)) {
      resetJobsPipelineAbortState();
      return;
    }
    if (
      targetStillActive
      && jobsPipelineUiState.abortRequestError
      && Date.now() - Number(jobsPipelineUiState.abortRequestErrorAt || 0) >= ABORT_REQUEST_VERIFY_GRACE_MS
    ) {
      const message = jobsPipelineUiState.abortRequestError;
      resetJobsPipelineAbortState();
      showToast(`Could not abort job update: ${message}`, "error");
    }
  }

  async function requestJobsPipelineAbort() {
    const task = jobsPipelineUiState.abortTask;
    if (!task?.taskType || !task?.runId || jobsPipelineUiState.abortRequested) return;
    const confirmed = typeof globalThis.confirm === "function"
      ? globalThis.confirm("Abort the current job update?")
      : true;
    if (!confirmed) return;
    jobsPipelineUiState.abortRequested = true;
    jobsPipelineUiState.abortRequestedTask = normalizeAbortTarget(task);
    jobsPipelineUiState.abortRevealActive = false;
    jobsPipelineUiState.abortRequestError = "";
    jobsPipelineUiState.abortRequestErrorAt = 0;
    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: JOBS_UPDATE_COPY.abortingLabel,
      pipelinePayload: { active: true, stage: "aborting", startedAt: jobsPipelineUiState.startedAt },
      abortTask: null
    });
    try {
      const result = await callJobsBridge("/tasks/abort", {
        method: "POST",
        body: {
          taskType: task.taskType,
          runId: task.runId,
          reason: "jobs_page_abort_update"
        },
        allowStatuses: [200, 400, 404, 409],
        timeoutMs: ABORT_REQUEST_TIMEOUT_MS
      });
      if (!result?.ok && !result?.abortAccepted) {
        throw new Error(String(result?.error || "abort failed"));
      }
      showToast("Job update abort requested.", "success");
      scheduleJobsPipelineStatusPoll(250);
    } catch (err) {
      jobsPipelineUiState.abortRequestError = String(err?.message || err);
      jobsPipelineUiState.abortRequestErrorAt = Date.now();
      scheduleJobsPipelineStatusPoll(250);
    }
  }

  async function refreshJobsUpdateTooltipFromHealth() {
    try {
      const payload = await callJobsBridge("/ops/dashboard-health?view=summary");
      const alerts = Array.isArray(payload?.alerts) ? payload.alerts : [];
      jobsPipelineUiState.updateTooltipFirstRun = alerts.some(alert => (
        ["fetch_never_run", "pipeline_never_run"].includes(String(alert?.id || "").trim())
      ));
      jobsPipelineUiState.updateTooltipFirstRunKnown = true;
    } catch {
      jobsPipelineUiState.updateTooltipFirstRun = false;
      jobsPipelineUiState.updateTooltipFirstRunKnown = false;
    }
  }

  function clearJobsPipelinePolling() {
    clearJobsPipelinePollingFromModule(jobsPipelineUiState);
  }

  function scheduleJobsPipelineStatusPoll(delayMs) {
    scheduleJobsPipelineStatusPollFromModule(
      jobsPipelineUiState,
      delayMs,
      pollJobsPipelineStatus,
      pollDelayMs
    );
  }

  function attachActivePipelinePayload(payload, { toastMessage = "" } = {}) {
    const runId = String(payload?.runId || jobsPipelineUiState.runId || "");
    const startedAt = String(payload?.startedAt || jobsPipelineUiState.startedAt || "");
    const activePayload = { ...payload, active: true, runId, startedAt };
    jobsPipelineUiState.bridgeOnline = true;
    jobsPipelineUiState.updateTooltipBridgeError = "";
    jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
    resetPipelineStatusPollFailures(jobsPipelineUiState);
    jobsPipelineUiState.active = true;
    jobsPipelineUiState.pendingStart = false;
    jobsPipelineUiState.runId = runId;
    jobsPipelineUiState.startedAt = startedAt;
    rememberActivePipelinePayload(jobsPipelineUiState, activePayload);
    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: getPipelineRunningLabel(activePayload),
      pipelinePayload: activePayload,
      abortTask: {
        active: true,
        taskType: "pipeline",
        runId
      }
    });
    if (toastMessage) showToast(toastMessage, "info");
    scheduleJobsPipelineStatusPoll(pollDelayMs);
  }

  async function verifyActivePipelineAfterStartFailure(message) {
    try {
      const payload = await callJobsBridge("/tasks/run-jobs-pipeline-status", {
        timeoutMs: PIPELINE_START_TIMEOUT_MS
      });
      if (payload?.active) {
        attachActivePipelinePayload(payload, {
          toastMessage: pipelineStartAttachedToast(message)
        });
        return true;
      }
    } catch {
      // Preserve the original start failure when verification cannot prove a live pipeline.
    }
    return false;
  }

  function handlePipelineCompletionStatus(payload) {
    const updatesFound = Boolean(payload?.updatesFound || payload?.refreshRecommended);
    const userAbortCompletion = isUserAbortCompletionPayload(payload);
    if (userAbortCompletion) {
      resetJobsPipelineAbortState();
    }
    const hasError = !userAbortCompletion && Boolean(isErrorStage(payload));
    setRefreshJobsNeedsAttention(updatesFound);
    jobsPipelineUiState.active = false;
    jobsPipelineUiState.pendingStart = false;
    jobsPipelineUiState.runId = "";
    jobsPipelineUiState.startedAt = "";
    clearRememberedActivePipeline(jobsPipelineUiState);
    updateJobsPipelineUi({
      running: false,
      disabled: !jobsPipelineUiState.bridgeOnline,
      buttonLabel: hasError ? "Error" : "",
      pipelinePayload: payload,
      isError: hasError
    });
    if (userAbortCompletion) {
      return;
    }
    const syncWarning = Boolean(payload?.completedWithWarnings || payload?.syncWarning);
    if (syncWarning) {
      showToast(
        updatesFound
          ? `${JOBS_UPDATE_COPY.completedWithUpdates} Source sync needs attention.`
          : JOBS_UPDATE_COPY.completedWithSyncWarning,
        "warn"
      );
    } else if (updatesFound) {
      showToast(JOBS_UPDATE_COPY.completedWithUpdates, "success");
    } else if (payload?.error) {
      showToast(`Job update failed: ${String(payload.error)}`, "error");
    }
    if (updatesFound && typeof refreshJobsAfterPipelineCompletion === "function") {
      refreshJobsAfterPipelineCompletion(payload).catch(() => {});
    }
  }

  async function pollJobsPipelineStatus() {
    try {
      const payload = await callJobsBridge("/tasks/run-jobs-pipeline-status");
      jobsPipelineUiState.bridgeOnline = true;
      jobsPipelineUiState.updateTooltipBridgeError = "";
      resetPipelineStatusPollFailures(jobsPipelineUiState);

      const active = Boolean(payload?.active);
      const runId = String(payload?.runId || "");
      const trackedRunId = String(jobsPipelineUiState.runId || "");
      const shouldLoadTaskState = Boolean(
        !isContainerRuntimeMode?.()
        && (
          active
          || jobsPipelineUiState.active
          || jobsPipelineUiState.pendingStart
          || trackedRunId
          || jobsPipelineUiState.abortRequested
          || !jobsPipelineUiState.taskStateSummaryChecked
          || shouldRecheckIdleTaskState(jobsPipelineUiState)
        )
      );
      let taskStateKnown = false;
      let taskStatePayload = { tasks: [] };
      if (shouldLoadTaskState) {
        try {
          taskStatePayload = await callJobsBridge("/ops/task-state?view=summary");
          taskStateKnown = true;
          jobsPipelineUiState.taskStateSummaryChecked = true;
          jobsPipelineUiState.lastTaskStateSummaryCheckedAt = Date.now();
        } catch {
          taskStateKnown = false;
          if (!active && !jobsPipelineUiState.active && !jobsPipelineUiState.pendingStart) {
            jobsPipelineUiState.taskStateSummaryChecked = true;
            jobsPipelineUiState.lastTaskStateSummaryCheckedAt = Date.now();
          }
        }
      }
      const blockingTask = getBlockingTask(taskStatePayload, trackedRunId);
      reconcileAbortRequest({ pipelinePayload: payload, taskStatePayload, taskStateKnown });
      if (active) {
        attachActivePipelinePayload(payload);
        return;
      }

      if (jobsPipelineUiState.pendingStart) {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: JOBS_UPDATE_COPY.updatingLabel,
          pipelinePayload: payload,
          abortTask: null
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      if (blockingTask) {
        if (trackedRunId || jobsPipelineUiState.active) {
          jobsPipelineUiState.active = true;
        }
        const firstRunBootstrapActive = isFirstRunBootstrapTask(blockingTask);
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = firstRunBootstrapActive;
        const blockingPayload = buildBlockingTaskPayload(blockingTask);
        const blockingProgressLabel = formatBlockingTaskProgressLabel(blockingTask);
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: getPipelineRunningLabel(blockingPayload),
          progressLabel: blockingProgressLabel || String(blockingTask?.taskProgress?.phaseLabel || "").trim(),
          firstRunBootstrapActive,
          pipelinePayload: blockingPayload,
          abortTask: isAbortableTask(blockingTask) ? blockingTask : null
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      if ((trackedRunId && trackedRunId === runId) || jobsPipelineUiState.active) {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        refreshJobsUpdateTooltipFromHealth().catch(() => {});
        handlePipelineCompletionStatus(payload);
      } else {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        if (!jobsPipelineUiState.updateTooltipFirstRunKnown) {
          refreshJobsUpdateTooltipFromHealth()
            .then(() => {
              if (
                jobsPipelineUiState.active
                || jobsPipelineUiState.pendingStart
                || jobsPipelineUiState.runId
              ) {
                return;
              }
              updateJobsPipelineUi({
                running: false,
                disabled: false,
                buttonLabel: "",
                pipelinePayload: payload
              });
            })
            .catch(() => {});
        }
        updateJobsPipelineUi({
          running: false,
          disabled: false,
          buttonLabel: "",
          pipelinePayload: payload
        });
      }
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    } catch (err) {
      markPipelineStatusPollFailure(jobsPipelineUiState);
      jobsPipelineUiState.updateTooltipBridgeError = String(err?.message || err || "bridge unavailable");
      jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
      const recentActivePayload = getRecentActivePipelinePayload(jobsPipelineUiState);
      if (recentActivePayload) {
        jobsPipelineUiState.bridgeOnline = true;
        jobsPipelineUiState.active = true;
        jobsPipelineUiState.pendingStart = false;
        jobsPipelineUiState.runId = String(recentActivePayload.runId || jobsPipelineUiState.runId || "");
        jobsPipelineUiState.startedAt = String(recentActivePayload.startedAt || jobsPipelineUiState.startedAt || "");
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: getPipelineRunningLabel(recentActivePayload),
          progressLabel: "Pipeline status delayed; retrying...",
          pipelinePayload: recentActivePayload,
          abortTask: {
            active: true,
            taskType: "pipeline",
            runId: jobsPipelineUiState.runId
          }
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      jobsPipelineUiState.bridgeOnline = false;
      jobsPipelineUiState.updateTooltipFirstRun = false;
      jobsPipelineUiState.updateTooltipFirstRunKnown = false;
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.abortRequested = false;
      jobsPipelineUiState.abortTask = null;
      jobsPipelineUiState.pendingStart = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
      clearRememberedActivePipeline(jobsPipelineUiState);
      updateJobsPipelineUi({
        running: false,
        disabled: true,
        buttonLabel: JOBS_UPDATE_COPY.idleLabel,
        pipelinePayload: null,
        isError: false
      });
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    }
  }

  function ensureJobsPipelineStatusWatch() {
    updateJobsPipelineUi({
      running: false,
      disabled: true,
      buttonLabel: JOBS_UPDATE_COPY.idleLabel,
      pipelinePayload: null
    });
    pollJobsPipelineStatus().catch(() => {});
  }

  async function triggerJobsPipelineRun() {
    if (!refs.jobsPipelineRunBtn) return;
    if (jobsPipelineUiState.abortRequested) return;
    if (jobsPipelineUiState.active || hasAbortTask()) {
      if (hasAbortTask()) {
        await requestJobsPipelineAbort();
      }
      return;
    }
    if (refs.jobsPipelineRunBtn.disabled) return;

    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: JOBS_UPDATE_COPY.updatingLabel,
      pipelinePayload: null
    });
    try {
      jobsPipelineUiState.active = true;
      jobsPipelineUiState.pendingStart = true;
      jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = new Date().toISOString();
      const payload = await callJobsBridge("/tasks/run-jobs-pipeline", {
        method: "POST",
        allowStatuses: [409],
        timeoutMs: PIPELINE_START_TIMEOUT_MS,
        body: {
          jobsPageLoadedCount: Array.isArray(getAllJobs()) ? getAllJobs().length : 0
        }
      });
      const started = Boolean(payload?.started);
      if (!started) {
        const message = String(payload?.error || "pipeline did not start");
        if (await verifyActivePipelineAfterStartFailure(message)) return;
        throw new Error(message);
      }
      attachActivePipelinePayload({
        ...payload,
        active: true,
        startedAt: String(payload?.startedAt || new Date().toISOString())
      });
      showToast(JOBS_UPDATE_COPY.startedToast, "success");
    } catch (err) {
      const message = String(err?.message || JOBS_UPDATE_COPY.startFailed);
      if (await verifyActivePipelineAfterStartFailure(message)) return;
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.pendingStart = false;
      jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
      updateJobsPipelineUi({
        running: false,
        disabled: true,
        buttonLabel: "Error",
        pipelinePayload: null,
        isError: true
      });
      showToast(pipelineStartFailureToast(message), "error");
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    }
  }

  return {
    updateJobsPipelineUi,
    clearJobsPipelinePolling,
    scheduleJobsPipelineStatusPoll,
    handlePipelineCompletionStatus,
    pollJobsPipelineStatus,
    ensureJobsPipelineStatusWatch,
    triggerJobsPipelineRun
  };
}
