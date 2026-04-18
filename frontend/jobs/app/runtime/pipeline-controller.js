import {
  clearJobsPipelinePolling as clearJobsPipelinePollingFromModule,
  formatBlockingTaskProgressLabel,
  getPipelineRunningLabel,
  scheduleJobsPipelineStatusPoll as scheduleJobsPipelineStatusPollFromModule,
  updateJobsPipelineUi as updateJobsPipelineUiFromModule
} from "../pipeline.js";

const BLOCKING_TASK_TYPES = new Set(["pipeline", "fetch", "discovery", "sync"]);

function getTaskStateRows(payload) {
  return Array.isArray(payload?.tasks) ? payload.tasks : [];
}

function normalizeTaskType(task) {
  return String(task?.taskType || task?.type || "").trim().toLowerCase();
}

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
    isError = false
  } = {}) {
    updateJobsPipelineUiFromModule(refs, {
      pipelinePayload,
      running,
      disabled,
      buttonLabel,
      progressLabel,
      isError
    });
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
      showToast("Pipeline completed. Refresh jobs to load updated listings.", "success");
    } else if (payload?.error) {
      showToast(`Pipeline failed: ${String(payload.error)}`, "error");
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

      const active = Boolean(payload?.active);
      const runId = String(payload?.runId || "");
      const trackedRunId = String(jobsPipelineUiState.runId || "");
      const blockingTask = getBlockingTask(taskStatePayload, trackedRunId);
      if (active) {
        jobsPipelineUiState.active = true;
        jobsPipelineUiState.pendingStart = false;
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
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: "Starting Pipeline...",
          pipelinePayload: payload
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      if (blockingTask) {
        if (trackedRunId || jobsPipelineUiState.active) {
          jobsPipelineUiState.active = true;
        }
        const blockingPayload = buildBlockingTaskPayload(blockingTask);
        const blockingProgressLabel = formatBlockingTaskProgressLabel(blockingTask);
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: getPipelineRunningLabel(blockingPayload),
          progressLabel: blockingProgressLabel || String(blockingTask?.taskProgress?.phaseLabel || "").trim(),
          pipelinePayload: blockingPayload
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }
      if ((trackedRunId && trackedRunId === runId) || jobsPipelineUiState.active) {
        handlePipelineCompletionStatus(payload);
      } else {
        updateJobsPipelineUi({
          running: false,
          disabled: false,
          buttonLabel: "",
          pipelinePayload: payload
        });
      }
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    } catch {
      jobsPipelineUiState.bridgeOnline = false;
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.pendingStart = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
      updateJobsPipelineUi({
        running: false,
        disabled: true,
        buttonLabel: "Error",
        pipelinePayload: null,
        isError: true
      });
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    }
  }

  function ensureJobsPipelineStatusWatch() {
    updateJobsPipelineUi({
      running: false,
      disabled: true,
      buttonLabel: "Checking...",
      pipelinePayload: null
    });
    pollJobsPipelineStatus().catch(() => {});
  }

  async function triggerJobsPipelineRun() {
    if (!refs.jobsPipelineRunBtn || refs.jobsPipelineRunBtn.disabled || jobsPipelineUiState.active) return;

    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: "Starting Pipeline...",
      pipelinePayload: null
    });
    try {
      jobsPipelineUiState.active = true;
      jobsPipelineUiState.pendingStart = true;
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
      showToast("Jobs pipeline started.", "success");
      scheduleJobsPipelineStatusPoll(pollDelayMs);
    } catch (err) {
      const message = String(err?.message || "Could not start jobs pipeline.");
      const normalizedMessage = message.toLowerCase();
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.pendingStart = false;
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
          ? "Pipeline already running."
          : normalizedMessage.includes("another fetch/discovery/sync task is already running")
            ? "Another background task is still running."
            : "Could not start jobs pipeline.",
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
