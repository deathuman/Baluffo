import { applyAdminTaskProgress } from "./progress-ui.js";
import { createAdminFetcherLogController } from "./fetcher/logs.js";
import {
  applyFetcherPresetMetadata as applyFetcherPresetMetadataFromModule,
  FETCHER_FALLBACK_MESSAGES,
  FETCHER_PRESET_META,
  getFetcherPresetMeta
} from "./fetcher/presets.js";
import { createAdminFetcherReportController } from "./fetcher/report.js";
import { createAdminFetcherWatchController } from "./fetcher/watch.js";

export { FETCHER_PRESET_META } from "./fetcher/presets.js";

export function createAdminFetcherController({
  state,
  refs,
  getBridge,
  postBridge,
  fetchJobsFetchReportJson,
  writeJobsAutoRefreshSignal,
  showToast,
  getErrorMessage,
  logAdminError,
  setBusyFlag,
  getSourceStatusSetter,
  loadOpsHealthData,
  activeProgressPollIntervalMs = 500,
  jobsAutoRefreshSignalKey,
  jobsFetcherCommand,
  jobsFetcherTaskLabel,
  syncSourceTablesAfterTaskCompletion,
  createLogEvent,
  appendLogRow
}) {
  function setFetcherProgress(view) {
    if (!refs.adminFetcherProgressEl || !refs.adminFetcherProgressBarEl || !refs.adminFetcherProgressLabelEl) {
      return;
    }

    applyAdminTaskProgress(
      refs.adminFetcherProgressEl,
      refs.adminFetcherProgressBarEl,
      refs.adminFetcherProgressLabelEl,
      view
    );
  }

  const logController = createAdminFetcherLogController({
    state,
    refs,
    getBridge,
    createLogEvent,
    appendLogRow,
    setFetcherProgress
  });

  const reportController = createAdminFetcherReportController({
    state,
    refs,
    fetchJobsFetchReportJson,
    writeJobsAutoRefreshSignal,
    showToast,
    setBusyFlag,
    loadOpsHealthData,
    jobsAutoRefreshSignalKey,
    setFetcherProgress,
    appendFetcherLog: logController.appendFetcherLog,
    appendFetcherLogEvent: logController.appendFetcherLogEvent
  });

  const watchController = createAdminFetcherWatchController({
    state,
    setBusyFlag,
    getBridge,
    fetchJobsFetchReportJson,
    activeProgressPollIntervalMs,
    syncSourceTablesAfterTaskCompletion,
    setFetcherProgress,
    loadFetcherLogChunk: logController.loadFetcherLogChunk,
    scheduleFetcherLogPoll: logController.scheduleFetcherLogPoll,
    appendFetcherLog: logController.appendFetcherLog,
    appendFetcherProgressFromReport: reportController.appendFetcherProgressFromReport,
    updateFetcherProgressFromReport: reportController.updateFetcherProgressFromReport,
    emitJobsAutoRefreshSignal: reportController.emitJobsAutoRefreshSignal
  });

  function applyFetcherPresetMetadata() {
    return applyFetcherPresetMetadataFromModule(refs);
  }

  function launchVsCodeUri(uri) {
    const launchLink = document.createElement("a");
    launchLink.href = uri;
    launchLink.style.display = "none";
    document.body.appendChild(launchLink);
    launchLink.click();
    launchLink.remove();
  }

  async function triggerJobsFetcherTask(runOptions = {}) {
    if (state.adminBusyState.fetcherRun || state.adminBusyState.fetcherWatch || state.adminBusyState.fetcherReportLoad || state.adminBusyState.liveFetchRunning) {
      showToast("Fetcher task is already running.", "info");
      return;
    }
    setBusyFlag("fetcherRun", true);
    const preset = String(runOptions?.preset || "default");
    const presetMeta = getFetcherPresetMeta(preset);
    const payload = { ...runOptions };
    let usedFallback = false;
    try {
      const bridgeResponse = await postBridge("/tasks/run-fetcher", payload, {
        allowStatuses: [409],
        returnMeta: true
      });
      const bridgeStatus = Number(bridgeResponse?.status || 200);
      const bridge = bridgeResponse?.data || bridgeResponse || null;
      if (bridge && bridge.started) {
        watchController.attachToActiveFetchRun(bridge);
        const presetLabel = String(bridge?.preset || presetMeta.preset || "default");
        const argsLabel = Array.isArray(bridge?.args) ? bridge.args.join(" ") : "";
        logController.appendFetcherLog(
          `Triggered fetcher via local admin bridge (preset ${presetLabel})${argsLabel ? `, args: ${argsLabel}` : ""}.`
        );
        getSourceStatusSetter()("Triggered local fetcher task via admin bridge.");
        showToast("Fetcher started via admin bridge.", "success");
        loadOpsHealthData().catch(() => {});
        reportController.loadLatestFetcherReport({ silent: true }).catch(() => {});
        return;
      }
      if (bridgeStatus === 409 && bridge?.alreadyRunning) {
        watchController.attachToActiveFetchRun(bridge, { announceStart: false });
        logController.appendFetcherLog("Fetcher already running; attached to the active bridge-managed run.", "info");
        getSourceStatusSetter()("Attached to the active fetcher task via admin bridge.");
        showToast("Fetcher already running. Attached to active run.", "info");
        loadOpsHealthData().catch(() => {});
        reportController.loadLatestFetcherReport({ silent: true }).catch(() => {});
        return;
      }
    } catch {
      if (!usedFallback) {
        logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.bridgeUnavailable, "warn");
        usedFallback = true;
      }
    } finally {
      setBusyFlag("fetcherRun", false);
    }
    if (presetMeta.preset !== "default") {
      logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.presetNeedsBridge, "error");
      showToast("Fetcher preset requires admin bridge.", "error");
      return;
    }
    logController.appendFetcherLog("Preparing jobs fetcher task launch from admin panel.");
    showToast("Attempting fetcher launch...", "info");
    const optimisticRun = {
      runId: `fallback-fetch:${Date.now()}`,
      startedAt: new Date().toISOString()
    };
    watchController.attachToActiveFetchRun(optimisticRun, { announceStart: false });
    const taskArgQuoted = encodeURIComponent(JSON.stringify(jobsFetcherTaskLabel));
    const taskArgRaw = encodeURIComponent(jobsFetcherTaskLabel);
    const taskUris = [
      `vscode://command/workbench.action.tasks.runTask?${taskArgRaw}`,
      `vscode://command/workbench.action.tasks.runTask?${taskArgQuoted}`
    ];

    try {
      launchVsCodeUri(taskUris[0]);
      logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchPrimary(jobsFetcherTaskLabel));
      getSourceStatusSetter()("Triggered VS Code task to run jobs fetcher. Check VS Code terminal for progress.");
      window.setTimeout(() => {
        launchVsCodeUri(taskUris[1]);
        logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchSecondary);
      }, 180);
      logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualHint, "warn");
      showToast("Fetcher task launch requested. Check VS Code.", "info");
    } catch (err) {
      logAdminError("Could not trigger VS Code task", err);
      logController.appendFetcherLog(`Could not trigger VS Code task automatically: ${getErrorMessage(err)}`, "error");
      showToast(`Could not trigger VS Code task. Run ${jobsFetcherCommand}`, "error");
      getSourceStatusSetter()("Could not trigger jobs fetcher task automatically.");
      watchController.clearOptimisticFetchRun();
      watchController.stopFetcherCompletionWatch();
      return;
    }

    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(jobsFetcherCommand)
        .then(() => {
          logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.copiedManualCommand(jobsFetcherCommand));
        })
        .catch(() => {
          logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(jobsFetcherCommand), "warn");
        });
    } else {
      logController.appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(jobsFetcherCommand), "warn");
    }

    reportController.loadLatestFetcherReport({ silent: true }).catch(fetchErr => {
      logAdminError("Could not load fetch report after task trigger", fetchErr);
    });
    watchController.startFetcherCompletionWatch();
  }

  return {
    FETCHER_PRESET_META,
    FETCHER_FALLBACK_MESSAGES,
    getFetcherPresetMeta,
    applyFetcherPresetMetadata,
    setFetcherLogPlaceholder: logController.setFetcherLogPlaceholder,
    clearOptimisticFetchRun: watchController.clearOptimisticFetchRun,
    attachToActiveFetchRun: watchController.attachToActiveFetchRun,
    restartFetcherCompletionWatch: watchController.restartFetcherCompletionWatch,
    getRestorableFetcherRunMeta: watchController.getRestorableFetcherRunMeta,
    appendFetcherLog: logController.appendFetcherLog,
    loadFetcherLivePayload: watchController.loadFetcherLivePayload,
    loadLatestFetcherReport: reportController.loadLatestFetcherReport,
    copyLatestFailureSummary: reportController.copyLatestFailureSummary,
    triggerJobsFetcherTask,
    startFetcherCompletionWatch: watchController.startFetcherCompletionWatch,
    stopFetcherCompletionWatch: watchController.stopFetcherCompletionWatch,
    loadFetcherLogChunk: logController.loadFetcherLogChunk,
    appendFetcherServerLogText: logController.appendFetcherServerLogText
  };
}
