import {
  clearJobsPipelinePolling as clearJobsPipelinePollingFromModule,
  formatBlockingTaskProgressLabel,
  getJobsUpdateTooltip,
  getPipelineRunningLabel,
  JOBS_UPDATE_COPY,
  scheduleJobsPipelineStatusPoll as scheduleJobsPipelineStatusPollFromModule,
  updateJobsPipelineUi as updateJobsPipelineUiFromModule
} from "../pipeline.js?v=9";

const BLOCKING_TASK_TYPES = new Set(["pipeline", "fetch", "discovery", "sync"]);

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
    Boolean(task?.active) && BLOCKING_TASK_TYPES.has(normalizeTaskType(task))
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

export function createJobsPipelineController({
  refs,
  jobsPipelineUiState,
  callJobsBridge,
  getAllJobs,
  showToast,
  setRefreshJobsNeedsAttention,
  isErrorStage,
  pollDelayMs,
  idlePollDelayMs
}) {
  function updateJobsPipelineUi({
    pipelinePayload = null,
    running = false,
    disabled = false,
    buttonLabel = "",
    progressLabel = "",
    firstRunBootstrapActive = false,
    isError = false
  } = {}) {
    updateJobsPipelineUiFromModule(refs, {
      pipelinePayload,
      running,
      disabled,
      buttonLabel,
      progressLabel,
      buttonTooltip: getJobsUpdateTooltip({
        bridgeError: jobsPipelineUiState.updateTooltipBridgeError,
        firstRunBootstrapActive: Boolean(
          firstRunBootstrapActive || jobsPipelineUiState.updateTooltipFirstRunBootstrapActive
        ),
        firstRun: jobsPipelineUiState.updateTooltipFirstRun,
        firstRunKnown: jobsPipelineUiState.updateTooltipFirstRunKnown
      }),
      isError
    });
  }

  async function refreshJobsUpdateTooltipFromHealth() {
    try {
      const payload = await callJobsBridge("/ops/dashboard-health");
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

  function handlePipelineCompletionStatus(payload) {
    const updatesFound = Boolean(payload?.updatesFound || payload?.refreshRecommended);
    const hasError = Boolean(isErrorStage(payload));
    setRefreshJobsNeedsAttention(updatesFound);
    jobsPipelineUiState.active = false;
    jobsPipelineUiState.pendingStart = false;
    jobsPipelineUiState.runId = "";
    jobsPipelineUiState.startedAt = "";
    updateJobsPipelineUi({
      running: false,
      disabled: !jobsPipelineUiState.bridgeOnline,
      buttonLabel: hasError ? "Error" : "",
      pipelinePayload: payload,
      isError: hasError
    });
    if (updatesFound) {
      showToast(JOBS_UPDATE_COPY.completedWithUpdates, "success");
    } else if (payload?.error) {
      showToast(`Job update failed: ${String(payload.error)}`, "error");
    }
  }

  async function pollJobsPipelineStatus() {
    try {
      const [pipelineStatusResult, taskStateResult] = await Promise.allSettled([
        callJobsBridge("/tasks/run-jobs-pipeline-status"),
        callJobsBridge("/ops/task-state")
      ]);
      if (pipelineStatusResult.status !== "fulfilled") {
        throw pipelineStatusResult.reason;
      }
      const payload = pipelineStatusResult.value;
      const taskStatePayload = taskStateResult.status === "fulfilled"
        ? taskStateResult.value
        : { tasks: [] };
      jobsPipelineUiState.bridgeOnline = true;
      jobsPipelineUiState.updateTooltipBridgeError = "";
      resetPipelineStatusPollFailures(jobsPipelineUiState);

      const active = Boolean(payload?.active);
      const runId = String(payload?.runId || "");
      const trackedRunId = String(jobsPipelineUiState.runId || "");
      const blockingTask = getBlockingTask(taskStatePayload, trackedRunId);
      if (active) {
        jobsPipelineUiState.active = true;
        jobsPipelineUiState.pendingStart = false;
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        jobsPipelineUiState.runId = runId || jobsPipelineUiState.runId;
        jobsPipelineUiState.startedAt = String(payload?.startedAt || jobsPipelineUiState.startedAt || "");
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: getPipelineRunningLabel({
            ...payload,
            startedAt: jobsPipelineUiState.startedAt
          }),
          pipelinePayload: payload
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }

      if (jobsPipelineUiState.pendingStart) {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: JOBS_UPDATE_COPY.updatingLabel,
          pipelinePayload: payload
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
          pipelinePayload: blockingPayload
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      if ((trackedRunId && trackedRunId === runId) || jobsPipelineUiState.active) {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        await refreshJobsUpdateTooltipFromHealth();
        handlePipelineCompletionStatus(payload);
      } else {
        jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
        await refreshJobsUpdateTooltipFromHealth();
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
      jobsPipelineUiState.bridgeOnline = false;
      jobsPipelineUiState.updateTooltipBridgeError = String(err?.message || err || "bridge unavailable");
      jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
      jobsPipelineUiState.updateTooltipFirstRun = false;
      jobsPipelineUiState.updateTooltipFirstRunKnown = false;
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.pendingStart = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
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
    if (!refs.jobsPipelineRunBtn || refs.jobsPipelineRunBtn.disabled || jobsPipelineUiState.active) return;

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
        body: {
          jobsPageLoadedCount: Array.isArray(getAllJobs()) ? getAllJobs().length : 0
        }
      });
      const started = Boolean(payload?.started);
      if (!started) {
        throw new Error(String(payload?.error || "pipeline did not start"));
      }
      jobsPipelineUiState.bridgeOnline = true;
      jobsPipelineUiState.updateTooltipBridgeError = "";
      jobsPipelineUiState.updateTooltipFirstRunBootstrapActive = false;
      resetPipelineStatusPollFailures(jobsPipelineUiState);
      jobsPipelineUiState.active = true;
      jobsPipelineUiState.pendingStart = false;
      jobsPipelineUiState.runId = String(payload?.runId || "");
      jobsPipelineUiState.startedAt = String(payload?.startedAt || new Date().toISOString());
      updateJobsPipelineUi({
        running: true,
        disabled: true,
        buttonLabel: getPipelineRunningLabel({
          ...payload,
          startedAt: jobsPipelineUiState.startedAt
        }),
        pipelinePayload: payload
      });
      showToast(JOBS_UPDATE_COPY.startedToast, "success");
      scheduleJobsPipelineStatusPoll(pollDelayMs);
    } catch (err) {
      const message = String(err?.message || JOBS_UPDATE_COPY.startFailed);
      const normalizedMessage = message.toLowerCase();
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
      showToast(
        normalizedMessage.includes("already running")
          ? "Job update already running."
          : normalizedMessage.includes("another fetch/discovery/sync task is already running")
            ? "Another background task is still running."
            : JOBS_UPDATE_COPY.startFailed,
        "error"
      );
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
