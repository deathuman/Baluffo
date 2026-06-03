import { createAdminDiscoveryLogController } from "./discovery/logs.js";
import { createAdminDiscoveryProgressController } from "./discovery/progress.js";
import { createAdminDiscoveryWatchController } from "./discovery/watch.js?v=1";

export {
  isDiscoveryMobileViewport,
  setDiscoveryLogOpen,
  syncDiscoveryLogDisclosure
} from "./discovery/disclosure.js";

export function createAdminDiscoveryController({
  state,
  refs,
  getBridge,
  postBridge,
  setBusyFlag,
  getErrorMessage,
  logAdminError,
  showToast,
  createLogEvent,
  appendLogRow,
  loadOpsHealthData,
  scheduleOpsHealthPolling,
  activeProgressPollIntervalMs = 500,
  syncSourceTablesAfterTaskCompletion,
  loadDiscoveryData
}) {
  let progressController;

  function updateDiscoveryProgressFromReport(...args) {
    return progressController?.updateDiscoveryProgressFromReport(...args);
  }

  const logController = createAdminDiscoveryLogController({
    state,
    refs,
    getBridge,
    createLogEvent,
    appendLogRow,
    setDiscoveryProgress: view => progressController?.setDiscoveryProgress(view),
    updateDiscoveryProgressFromReport
  });

  progressController = createAdminDiscoveryProgressController({
    state,
    refs,
    getBridge,
    postBridge,
    setBusyFlag,
    getErrorMessage,
    logAdminError,
    showToast,
    loadDiscoveryData,
    appendDiscoveryLog: logController.appendDiscoveryLog,
    appendDiscoveryLogEvent: logController.appendDiscoveryLogEvent
  });

  const watchController = createAdminDiscoveryWatchController({
    state,
    refs,
    getBridge,
    setBusyFlag,
    showToast,
    loadOpsHealthData,
    scheduleOpsHealthPolling,
    activeProgressPollIntervalMs,
    syncSourceTablesAfterTaskCompletion,
    appendDiscoveryLog: logController.appendDiscoveryLog,
    loadDiscoveryLogChunk: logController.loadDiscoveryLogChunk,
    loadLatestDiscoveryReport: progressController.loadLatestDiscoveryReport,
    updateDiscoveryProgressFromReport: progressController.updateDiscoveryProgressFromReport,
    runProgressAppend: progressController.runProgressAppend,
    refreshDiscoveryDataIfNeeded: progressController.refreshDiscoveryDataIfNeeded,
    setDiscoveryProgress: progressController.setDiscoveryProgress
  });

  async function runDiscoveryTask(runOptions = {}) {
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Discovery operation already in progress.", "info");
      return;
    }
    setBusyFlag("discoveryRun", true);
    state.discoveryLogRemoteOffset = 0;
    progressController.updateDiscoveryProgressFromReport(null, { running: true });
    logController.appendDiscoveryLog("Triggering source discovery task...");
    const launchAttemptAtMs = Date.now();
    try {
      const payload = (runOptions && typeof runOptions === "object" && !Array.isArray(runOptions))
        ? { ...runOptions }
        : {};
      const response = await postBridge("/tasks/run-discovery", payload, {
        allowStatuses: [409],
        returnMeta: true
      });
      const responseStatus = Number(response?.status || 200);
      const result = response?.data || response || null;
      if (responseStatus === 409 && result?.alreadyRunning) {
        watchController.attachToActiveDiscoveryRun(result, { announceStart: false });
        logController.appendDiscoveryLog("Discovery already running; attached to the active bridge-managed run.", "info");
        showToast("Source discovery already running. Attached to active run.", "info");
        loadOpsHealthData().catch(() => {});
        progressController.loadLatestDiscoveryReport({ silent: true }).catch(() => {});
        scheduleOpsHealthPolling(250);
        return;
      }
      watchController.attachToActiveDiscoveryRun(result || {});
      const preset = String(result?.preset || payload?.preset || "default").trim().toLowerCase();
      const isUncapped = preset === "uncapped";
      logController.appendDiscoveryLog(isUncapped ? "Source discovery uncapped task started." : "Source discovery task started.", "success");
      showToast(isUncapped ? "Source discovery uncapped run started." : "Source discovery started.", "success");
      loadOpsHealthData().catch(() => {});
      scheduleOpsHealthPolling(250);
    } catch (err) {
      let recovered = false;
      const message = getErrorMessage(err);
      if (/network|empty response|bridge unreachable|fetch/i.test(String(message || ""))) {
        try {
          recovered = await watchController.recoverDiscoveryLaunchAfterTransportError(launchAttemptAtMs);
        } catch (recoveryErr) {
          logAdminError("Discovery launch recovery probe failed", recoveryErr);
        }
      }
      if (recovered) return;
      logController.appendDiscoveryLog(`Could not trigger discovery task: ${getErrorMessage(err)}`, "error");
      showToast("Could not trigger source discovery task.", "error");
      watchController.clearOptimisticDiscoveryRun();
      setBusyFlag("liveDiscoveryRunning", false);
    } finally {
      setBusyFlag("discoveryRun", false);
    }
  }

  function formatManualCheckFailureMessage(checkResult) {
    const code = String(checkResult?.errorCode || "").toLowerCase();
    if (code === "browser_fallback_unavailable") return "Manual source check failed (browser fallback is not installed).";
    if (code === "not_found") return "Manual source check failed (404 not found).";
    if (code === "forbidden") return "Manual source check failed (403 forbidden).";
    if (code === "ssl_error") return "Manual source check failed (SSL certificate/hostname issue).";
    if (code === "dns_error") return "Manual source check failed (DNS/host resolution issue).";
    if (code === "timeout") return "Manual source check failed (timeout).";
    return "Manual source check failed.";
  }

  return {
    populateDiscoveryConfigForm: progressController.populateDiscoveryConfigForm,
    collectDiscoveryConfigPayload: progressController.collectDiscoveryConfigPayload,
    loadDiscoveryConfig: progressController.loadDiscoveryConfig,
    saveDiscoveryConfig: progressController.saveDiscoveryConfig,
    appendDiscoveryLog: logController.appendDiscoveryLog,
    appendDiscoveryLogEvent: logController.appendDiscoveryLogEvent,
    appendDiscoveryServerLogText: logController.appendDiscoveryServerLogText,
    loadDiscoveryLogChunk: logController.loadDiscoveryLogChunk,
    loadDiscoveryLivePayload: watchController.loadDiscoveryLivePayload,
    loadLatestDiscoveryReport: progressController.loadLatestDiscoveryReport,
    setDiscoveryLogPlaceholder: logController.setDiscoveryLogPlaceholder,
    clearOptimisticDiscoveryRun: watchController.clearOptimisticDiscoveryRun,
    attachToActiveDiscoveryRun: watchController.attachToActiveDiscoveryRun,
    restartDiscoveryCompletionWatch: watchController.restartDiscoveryCompletionWatch,
    startDiscoveryCompletionWatch: watchController.startDiscoveryCompletionWatch,
    stopDiscoveryCompletionWatch: watchController.stopDiscoveryCompletionWatch,
    runDiscoveryTask,
    formatManualCheckFailureMessage
  };
}
