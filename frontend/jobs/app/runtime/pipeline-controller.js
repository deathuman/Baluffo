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
  function updateJobsPipelineUi({ running = false, disabled = false, buttonLabel = "", progressLabel = "", isError = false } = {}) {
    updateJobsPipelineUiFromModule(refs, { running, disabled, buttonLabel, progressLabel, isError });
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
    jobsPipelineUiState.runId = "";
    jobsPipelineUiState.startedAt = "";
    updateJobsPipelineUi({
      running: false,
      disabled: !jobsPipelineUiState.bridgeOnline,
      buttonLabel: hasError ? "Error" : "",
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
        jobsPipelineUiState.runId = runId || jobsPipelineUiState.runId;
        jobsPipelineUiState.startedAt = String(payload?.startedAt || jobsPipelineUiState.startedAt || "");
        updateJobsPipelineUi({
          running: true,
          disabled: true,
          buttonLabel: getPipelineRunningLabel({
            ...payload,
            startedAt: jobsPipelineUiState.startedAt
          })
        });
        scheduleJobsPipelineStatusPoll(pollDelayMs);
        return;
      }

      const trackedRunId = String(jobsPipelineUiState.runId || "");
      if ((trackedRunId && trackedRunId === runId) || jobsPipelineUiState.active) {
        handlePipelineCompletionStatus(payload);
      } else {
        updateJobsPipelineUi({
          running: false,
          disabled: false,
          buttonLabel: ""
        });
      }
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    } catch {
      jobsPipelineUiState.bridgeOnline = false;
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
      updateJobsPipelineUi({
        running: false,
        disabled: true,
        buttonLabel: "Error",
        isError: true
      });
      scheduleJobsPipelineStatusPoll(idlePollDelayMs);
    }
  }

  function ensureJobsPipelineStatusWatch() {
    updateJobsPipelineUi({
      running: false,
      disabled: true,
      buttonLabel: "Checking..."
    });
    pollJobsPipelineStatus().catch(() => {});
  }

  async function triggerJobsPipelineRun() {
    if (!refs.jobsPipelineRunBtn || refs.jobsPipelineRunBtn.disabled || jobsPipelineUiState.active) return;

    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: "Starting Pipeline..."
    });
    try {
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
      jobsPipelineUiState.runId = String(payload?.runId || "");
      jobsPipelineUiState.startedAt = String(payload?.startedAt || new Date().toISOString());
      updateJobsPipelineUi({
        running: true,
        disabled: true,
        buttonLabel: getPipelineRunningLabel({
          ...payload,
          startedAt: jobsPipelineUiState.startedAt
        })
      });
      showToast("Jobs pipeline started.", "success");
      scheduleJobsPipelineStatusPoll(pollDelayMs);
    } catch (err) {
      const message = String(err?.message || "Could not start jobs pipeline.");
      jobsPipelineUiState.active = false;
      jobsPipelineUiState.runId = "";
      jobsPipelineUiState.startedAt = "";
      updateJobsPipelineUi({
        running: false,
        disabled: true,
        buttonLabel: "Error",
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
