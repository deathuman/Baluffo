import {
  attachToActiveRun,
  clearOptimisticRun,
  createBoundedSignatureSet,
  createLiveTaskPollGuard,
  getLiveTaskPollBackoffDelay,
  getRestorableRunMeta,
  loadTaskLivePayload,
  parseReportTimestampMs,
  pickMeaningfulTaskLivePayload,
  pickTaskLivePayload,
  restartCompletionWatch,
  runGuardedLiveTaskPoll,
  scheduleAsyncWatchTimer,
  setOptimisticRun,
  startLiveTaskWatch,
  stopLiveTaskWatch
} from "../live-task.js";
import {
  formatDurationCompact,
  formatStageTopSummary,
  selectSlowSources
} from "../fetcher-summary.js";
import { buildTaskRunLogLabel } from "../../../shared/task-run-view-model.js";

const FETCHER_INITIAL_LOG_TAIL_LIMIT_CHARS = 8192;
const FETCHER_LIVE_POLL_TIMEOUT_MS = 3500;
const FETCHER_LIVE_POLL_BACKOFF_MAX_MS = 5000;

export function createAdminFetcherWatchController({
  state,
  setBusyFlag,
  getBridge,
  fetchJobsFetchReportJson,
  activeProgressPollIntervalMs = 500,
  syncSourceTablesAfterTaskCompletion,
  setFetcherProgress,
  loadFetcherLogChunk,
  scheduleFetcherLogPoll,
  appendFetcherLog,
  appendFetcherProgressFromReport,
  updateFetcherProgressFromReport,
  emitJobsAutoRefreshSignal
}) {
  function setOptimisticFetchRun(runMeta) {
    setOptimisticRun(state, "fetchOptimisticRun", runMeta);
  }

  function clearOptimisticFetchRun() {
    clearOptimisticRun(state, "fetchOptimisticRun");
  }

  async function loadFetcherLivePayload(options = {}) {
    return loadTaskLivePayload({
      getBridge,
      taskType: "fetch",
      view: "summary",
      requestOptions: options?.requestOptions || {}
    });
  }

  function getRestorableFetcherRunMeta(report = null) {
    return getRestorableRunMeta(report, {
      hasLiveState: Boolean(
        state.fetcherLiveProgressState
        || state.fetchOptimisticRun
        || state.adminBusyState.fetcherWatch
        || state.adminBusyState.liveFetchRunning
      ),
      optimisticRun: state.fetchOptimisticRun,
      launchAtMs: state.fetcherLaunchAtMs
    });
  }

  function attachToActiveFetchRun(runMeta = null, options = {}) {
    attachToActiveRun({
      isWatching: () => state.adminBusyState.fetcherWatch,
      setOptimisticRun: setOptimisticFetchRun,
      startWatch: () => startFetcherCompletionWatch(options)
    }, runMeta);
    if (options?.initialReport && typeof options.initialReport === "object") {
      updateFetcherProgressFromReport(options.initialReport, { running: true });
    }
  }

  function restartFetcherCompletionWatch(runMeta = null, options = {}) {
    restartCompletionWatch(stopFetcherCompletionWatch, nextRunMeta => attachToActiveFetchRun(nextRunMeta, options), runMeta);
  }

  function startFetcherCompletionWatch(options = {}) {
    const announceStart = options?.announceStart !== false;
    stopFetcherCompletionWatch();
    startLiveTaskWatch({
      state,
      setBusyFlag,
      watchKey: "fetcherWatch",
      launchAtKey: "fetcherLaunchAtMs",
      optimisticRunKey: "fetchOptimisticRun",
      logOffsetKey: "fetcherLogRemoteOffset",
      liveStateKey: "fetcherLiveProgressState",
      createLiveState: () => ({
        summarySignature: "",
        workItemSignature: "",
        recentEventSignatures: createBoundedSignatureSet(),
        serverLogSignatures: createBoundedSignatureSet(),
        lastHeartbeatAtMs: 0,
        lastActivityAtMs: Date.now(),
        livePollGuard: createLiveTaskPollGuard({
          baseDelayMs: activeProgressPollIntervalMs,
          maxDelayMs: FETCHER_LIVE_POLL_BACKOFF_MAX_MS
        })
      }),
      setProgress: () => updateFetcherProgressFromReport(null, { running: true }),
      onStart: announceStart ? () => appendFetcherLog("Fetcher started. Watching live progress...", "info") : null,
      loadInitialLogChunk: () => loadFetcherLogChunk({
        reset: true,
        view: "tail",
        limitChars: FETCHER_INITIAL_LOG_TAIL_LIMIT_CHARS
      }).catch(() => {}),
      scheduleLogPoll: () => scheduleFetcherLogPoll(activeProgressPollIntervalMs),
      scheduleCompletionPoll: () => scheduleFetcherCompletionPoll(0)
    });
  }

  function stopFetcherCompletionWatch() {
    stopLiveTaskWatch({
      state,
      setBusyFlag,
      watchKey: "fetcherWatch",
      completionPollTimerKey: "fetcherCompletionPollTimer",
      logPollTimerKey: "fetcherLogPollTimer",
      liveStateKey: "fetcherLiveProgressState",
      setProgress: view => setFetcherProgress(view)
    });
  }

  function scheduleFetcherCompletionPoll(delayMs) {
    scheduleAsyncWatchTimer({
      state,
      timerKey: "fetcherCompletionPollTimer",
      delayMs,
      task: pollFetcherCompletion,
      onError: () => {
        scheduleFetcherCompletionPoll(activeProgressPollIntervalMs);
      }
    });
  }

  async function pollFetcherCompletion() {
    const now = Date.now();
    const liveState = state.fetcherLiveProgressState;
    const liveResult = await runGuardedLiveTaskPoll(
      liveState?.livePollGuard,
      () => loadFetcherLivePayload({
        requestOptions: { timeoutMs: FETCHER_LIVE_POLL_TIMEOUT_MS }
      })
    );
    const livePayload = liveResult?.ok ? liveResult.value : null;
    const identityLivePayload = pickTaskLivePayload(livePayload);
    const meaningfulLivePayload = pickMeaningfulTaskLivePayload(livePayload);
    const liveEnvelope = meaningfulLivePayload || identityLivePayload;
    const liveStartedMs = parseReportTimestampMs(liveEnvelope?.startedAt);
    const liveFinishedMs = parseReportTimestampMs(liveEnvelope?.finishedAt);
    const nextPollDelayMs = Math.max(
      activeProgressPollIntervalMs,
      getLiveTaskPollBackoffDelay(liveState?.livePollGuard, 0)
    );

    if (liveResult && liveResult.ok === false) {
      scheduleFetcherCompletionPoll(nextPollDelayMs);
      return;
    }

    if (meaningfulLivePayload && liveStartedMs >= (state.fetcherLaunchAtMs - 1000)) {
      if (liveFinishedMs <= 0) {
        appendFetcherProgressFromReport(meaningfulLivePayload, now);
        updateFetcherProgressFromReport(meaningfulLivePayload, { running: true });
      }
    }

    let terminalPayload = meaningfulLivePayload || identityLivePayload;
    if (!meaningfulLivePayload || liveFinishedMs > 0) {
      terminalPayload = await fetchJobsFetchReportJson({ live: true }).catch(() => null) || terminalPayload;
      if (terminalPayload && !String(terminalPayload?.finishedAt || "").trim()) {
        state.latestFetcherReportCache = terminalPayload || state.latestFetcherReportCache;
        updateFetcherProgressFromReport(terminalPayload, { running: true });
      }
    }

    const finishedMs = parseReportTimestampMs(terminalPayload?.finishedAt);
    if (finishedMs >= (state.fetcherLaunchAtMs - 1000)) {
      const finalReport = await fetchJobsFetchReportJson().catch(() => null);
      const completedPayload = finalReport || terminalPayload;
      state.latestFetcherReportCache = completedPayload || state.latestFetcherReportCache;
      updateFetcherProgressFromReport(completedPayload, { running: false });
      const completionLog = buildTaskRunLogLabel(completedPayload, {
        taskType: "fetch",
        running: false,
        nowMs: now,
        prefix: "Fetcher completed"
      });
      appendFetcherLog(
        completionLog.message,
        completionLog.levelHint
      );
      const slowSources = selectSlowSources(completedPayload)
        .slice(0, 3)
        .map(source => `${String(source?.name || "unknown")} ${formatDurationCompact(source?.durationMs)}`);
      if (slowSources.length) {
        appendFetcherLog(`Slowest sources: ${slowSources.join(" | ")}`, "muted");
      }
      const slowStages = formatStageTopSummary(completedPayload);
      if (slowStages) {
        appendFetcherLog(`Slowest stages: ${slowStages}`, "muted");
      }
      await Promise.resolve(syncSourceTablesAfterTaskCompletion?.({
        taskType: "fetch",
        completionSignature: [
          String(completedPayload?.runId || state.fetchOptimisticRun?.runId || ""),
          String(completedPayload?.finishedAt || "")
        ].join("|"),
        fetchReport: completedPayload
      })).catch(() => {});
      emitJobsAutoRefreshSignal(completedPayload);
      setBusyFlag("liveFetchRunning", false);
      clearOptimisticFetchRun();
      stopFetcherCompletionWatch();
      return;
    }

    scheduleFetcherCompletionPoll(nextPollDelayMs);
  }

  return {
    clearOptimisticFetchRun,
    loadFetcherLivePayload,
    getRestorableFetcherRunMeta,
    attachToActiveFetchRun,
    restartFetcherCompletionWatch,
    startFetcherCompletionWatch,
    stopFetcherCompletionWatch
  };
}
