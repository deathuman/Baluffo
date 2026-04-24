import {
  clearOptimisticRun,
  createBoundedSignatureSet,
  attachToActiveRun,
  loadTaskLivePayload,
  parseReportTimestampMs,
  pickMeaningfulTaskLivePayload,
  pickTaskLivePayload,
  restartCompletionWatch,
  scheduleAsyncWatchTimer,
  setOptimisticRun,
  shouldApplyTimestampGate,
  startLiveTaskWatch,
  stopLiveTaskWatch
} from "../live-task.js";
import { deriveDiscoveryQueuedCount } from "../../domain/progress.js";

export function createAdminDiscoveryWatchController({
  state,
  getBridge,
  setBusyFlag,
  showToast,
  loadOpsHealthData,
  scheduleOpsHealthPolling,
  activeProgressPollIntervalMs = 500,
  syncSourceTablesAfterTaskCompletion,
  appendDiscoveryLog,
  loadDiscoveryLogChunk,
  updateDiscoveryProgressFromReport,
  runProgressAppend,
  refreshDiscoveryDataIfNeeded,
  setDiscoveryProgress
}) {
  function setOptimisticDiscoveryRun(runMeta) {
    setOptimisticRun(state, "discoveryOptimisticRun", runMeta);
  }

  function clearOptimisticDiscoveryRun() {
    clearOptimisticRun(state, "discoveryOptimisticRun");
  }

  async function loadDiscoveryLivePayload() {
    return loadTaskLivePayload({
      getBridge,
      taskType: "discovery"
    });
  }

  function hasDiscoveryLaunchFailure(report) {
    const failures = Array.isArray(report?.failures) ? report.failures : [];
    return failures.some(item => {
      const stage = String(item?.stage || "").trim().toLowerCase();
      const adapter = String(item?.adapter || "").trim().toLowerCase();
      return stage === "launch" || adapter === "bridge";
    });
  }

  function parseFreshDiscoveryRun(report, launchedAtMs) {
    if (!report || typeof report !== "object" || Array.isArray(report)) return null;
    const startedAt = String(report?.startedAt || "").trim();
    const finishedAt = String(report?.finishedAt || "").trim();
    const startedAtMs = parseReportTimestampMs(startedAt);
    if (startedAtMs <= 0) return null;
    const lowerBound = Math.max(0, Number(launchedAtMs) - 5000);
    const upperBound = Number(launchedAtMs) + 30000;
    if (startedAtMs < lowerBound || startedAtMs > upperBound) return null;
    if (finishedAt) return null;
    if (hasDiscoveryLaunchFailure(report)) return null;
    return {
      runId: String(report?.runId || state.discoveryOptimisticRun?.runId || ""),
      startedAt
    };
  }

  async function recoverDiscoveryLaunchAfterTransportError(launchAttemptAtMs) {
    const [report, logChunk] = await Promise.all([
      getBridge("/discovery/report"),
      loadDiscoveryLogChunk({ reset: true }).catch(() => null)
    ]);
    if (logChunk) {
      state.discoveryLogRemoteOffset = Math.max(0, Number(logChunk?.nextOffset) || 0);
    }
    const recoveredRun = parseFreshDiscoveryRun(report, launchAttemptAtMs);
    if (!recoveredRun) return false;
    setOptimisticDiscoveryRun(recoveredRun);
    appendDiscoveryLog("Discovery launch response was lost, but the run is active. Reattaching to live progress...", "warn");
    showToast("Source discovery started; reattached after a dropped bridge response.", "warning");
    startDiscoveryCompletionWatch();
    loadOpsHealthData().catch(() => {});
    scheduleOpsHealthPolling(250);
    return true;
  }

  function attachToActiveDiscoveryRun(runMeta = null, options = {}) {
    attachToActiveRun({
      isWatching: () => state.adminBusyState.discoveryWatch,
      setOptimisticRun: setOptimisticDiscoveryRun,
      startWatch: () => startDiscoveryCompletionWatch(options)
    }, runMeta);
  }

  function restartDiscoveryCompletionWatch(runMeta = null, options = {}) {
    restartCompletionWatch(stopDiscoveryCompletionWatch, nextRunMeta => attachToActiveDiscoveryRun(nextRunMeta, options), runMeta);
  }

  function shouldApplyDiscoveryLiveProgressGate(report) {
    return shouldApplyTimestampGate(report, {
      optimisticRun: state.discoveryOptimisticRun,
      launchAtMs: state.discoveryLaunchAtMs,
      timestampField: "startedAt",
      skewMs: 60000
    });
  }

  function shouldApplyDiscoveryFinishedGate(report) {
    return shouldApplyTimestampGate(report, {
      optimisticRun: state.discoveryOptimisticRun,
      launchAtMs: state.discoveryLaunchAtMs,
      timestampField: "finishedAt",
      skewMs: 60000
    });
  }

  function startDiscoveryCompletionWatch(options = {}) {
    const announceStart = options?.announceStart !== false;
    stopDiscoveryCompletionWatch();
    startLiveTaskWatch({
      state,
      setBusyFlag,
      watchKey: "discoveryWatch",
      launchAtKey: "discoveryLaunchAtMs",
      optimisticRunKey: "discoveryOptimisticRun",
      logOffsetKey: "discoveryLogRemoteOffset",
      liveStateKey: "discoveryLiveProgressState",
      createLiveState: () => ({
        phaseLabel: "",
        summarySignature: "",
        workItemSignature: "",
        registryRefreshSignature: "",
        candidateCount: 0,
        failureCount: 0,
        serverPhaseLabel: "",
        recentEventSignatures: createBoundedSignatureSet(),
        serverLogSignatures: createBoundedSignatureSet(),
        lastHeartbeatAtMs: 0,
        lastActivityAtMs: Date.now()
      }),
      setProgress: () => updateDiscoveryProgressFromReport(null, { running: true }),
      onStart: announceStart ? () => appendDiscoveryLog("Discovery started. Watching live progress...", "info") : null,
      loadInitialLogChunk: () => loadDiscoveryLogChunk({ reset: true }).catch(() => {}),
      scheduleCompletionPoll: () => scheduleDiscoveryCompletionPoll(0)
    });
  }

  function stopDiscoveryCompletionWatch() {
    stopLiveTaskWatch({
      state,
      setBusyFlag,
      watchKey: "discoveryWatch",
      completionPollTimerKey: "discoveryCompletionPollTimer",
      liveStateKey: "discoveryLiveProgressState",
      setProgress: view => setDiscoveryProgress(view)
    });
  }

  function scheduleDiscoveryCompletionPoll(delayMs) {
    scheduleAsyncWatchTimer({
      state,
      timerKey: "discoveryCompletionPollTimer",
      delayMs,
      task: pollDiscoveryCompletion,
      onError: () => {
        scheduleDiscoveryCompletionPoll(activeProgressPollIntervalMs);
      }
    });
  }

  async function pollDiscoveryCompletion() {
    const now = Date.now();
    const [livePayload] = await Promise.all([
      loadDiscoveryLivePayload().catch(() => null),
      loadDiscoveryLogChunk().catch(() => null)
    ]);
    const identityLivePayload = pickTaskLivePayload(livePayload);
    const meaningfulLivePayload = pickMeaningfulTaskLivePayload(livePayload);
    const liveFinishedMs = parseReportTimestampMs((meaningfulLivePayload || identityLivePayload)?.finishedAt);

    if (meaningfulLivePayload && liveFinishedMs <= 0) {
      if (shouldApplyDiscoveryLiveProgressGate(meaningfulLivePayload)) {
        runProgressAppend(meaningfulLivePayload, now);
      }
      updateDiscoveryProgressFromReport(meaningfulLivePayload, { running: true });
      scheduleDiscoveryCompletionPoll(activeProgressPollIntervalMs);
      return;
    }

    const report = await getBridge("/discovery/report").catch(() => null);
    if (!meaningfulLivePayload && report) {
      if (!String(report?.finishedAt || "").trim()) {
        updateDiscoveryProgressFromReport(report, { running: true });
      }
      refreshDiscoveryDataIfNeeded(report);
    }

    const finishedMs = parseReportTimestampMs(report?.finishedAt);
    if (finishedMs > 0 && shouldApplyDiscoveryFinishedGate(report)) {
      const summary = report?.summary || {};
      const queuedCount = deriveDiscoveryQueuedCount(report);
      const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
      const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
      const failedCount = Number(summary.failedProbeCount || 0);
      updateDiscoveryProgressFromReport(report, { running: false });
      await Promise.resolve(syncSourceTablesAfterTaskCompletion?.({
        taskType: "discovery",
        completionSignature: [
          String(report?.runId || state.discoveryOptimisticRun?.runId || ""),
          String(report?.finishedAt || "")
        ].join("|")
      })).catch(() => {});
      appendDiscoveryLog(
        `Discovery completed: endpoints ${Number(summary.foundEndpointCount ?? 0)}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}, failed ${failedCount}.`,
        failedCount > 0 ? "warn" : "success"
      );
      clearOptimisticDiscoveryRun();
      setBusyFlag("liveDiscoveryRunning", false);
      stopDiscoveryCompletionWatch();
      return;
    }

    scheduleDiscoveryCompletionPoll(activeProgressPollIntervalMs);
  }

  return {
    clearOptimisticDiscoveryRun,
    loadDiscoveryLivePayload,
    recoverDiscoveryLaunchAfterTransportError,
    attachToActiveDiscoveryRun,
    restartDiscoveryCompletionWatch,
    startDiscoveryCompletionWatch,
    stopDiscoveryCompletionWatch
  };
}
