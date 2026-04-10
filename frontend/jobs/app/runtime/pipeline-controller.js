import {
  clearJobsPipelinePolling as clearJobsPipelinePollingFromModule,
  getPipelineRunningLabel,
  scheduleJobsPipelineStatusPoll as scheduleJobsPipelineStatusPollFromModule,
  updateJobsPipelineUi as updateJobsPipelineUiFromModule
} from "../pipeline.js";

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
      showToast("Pipeline completed. Refresh jobs to load new updates.", "success");
    } else if (payload?.error) {
      showToast(`Pipeline failed: ${String(payload.error)}`, "error");
    }
  }

  async function pollJobsPipelineStatus() {
    try {
      const payload = await callJobsBridge("/tasks/run-jobs-pipeline-status");
      jobsPipelineUiState.bridgeOnline = true;

      const active = Boolean(payload?.active);
      const runId = String(payload?.runId || "");
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

      const trackedRunId = String(jobsPipelineUiState.runId || "");
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
      showToast(message.toLowerCase().includes("409") ? "Pipeline already running." : "Could not start jobs pipeline.", "error");
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
