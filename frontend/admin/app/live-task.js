export function maybeUnrefTimer(timer) {
  timer?.unref?.();
  return timer;
}

export function parseReportTimestampMs(value) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function setOptimisticRun(state, key, runMeta) {
  const startedAt = String(runMeta?.startedAt || "").trim();
  if (!startedAt) {
    state[key] = null;
    return;
  }
  state[key] = {
    runId: String(runMeta?.runId || ""),
    startedAt
  };
}

export function clearOptimisticRun(state, key) {
  state[key] = null;
}

export function loadLiveTaskLogChunk({
  getBridge,
  path,
  state,
  offsetKey,
  reset = false,
  onText,
  onNextOffset
}) {
  const offset = reset ? 0 : Math.max(0, Number(state[offsetKey]) || 0);
  return getBridge(`${path}?offset=${offset}`).then(payload => {
    if (reset) {
      state[offsetKey] = 0;
    }
    onText?.(String(payload?.text || ""));
    state[offsetKey] = Math.max(0, Number(payload?.nextOffset) || 0);
    onNextOffset?.(payload || null);
    return payload || null;
  });
}

export function startLiveTaskWatch({
  state,
  setBusyFlag,
  watchKey,
  launchAtKey,
  optimisticRunKey,
  logOffsetKey,
  liveStateKey,
  createLiveState,
  setProgress,
  onStart,
  loadInitialLogChunk,
  scheduleLogPoll,
  scheduleCompletionPoll
}) {
  setBusyFlag(watchKey, true);
  state[launchAtKey] = Date.now();
  const optimisticStartedAtMs = parseReportTimestampMs(state[optimisticRunKey]?.startedAt);
  if (optimisticStartedAtMs > 0) {
    state[launchAtKey] = optimisticStartedAtMs;
  }
  state[logOffsetKey] = 0;
  state[liveStateKey] = createLiveState();
  setProgress?.({ active: true });
  onStart?.();
  loadInitialLogChunk?.();
  scheduleLogPoll?.();
  scheduleCompletionPoll?.();
}

export function stopLiveTaskWatch({
  state,
  setBusyFlag,
  watchKey,
  completionPollTimerKey,
  logPollTimerKey,
  liveStateKey,
  setProgress
}) {
  if (completionPollTimerKey && state[completionPollTimerKey]) {
    clearTimeout(state[completionPollTimerKey]);
    state[completionPollTimerKey] = null;
  }
  if (logPollTimerKey && state[logPollTimerKey]) {
    clearTimeout(state[logPollTimerKey]);
    state[logPollTimerKey] = null;
  }
  state[liveStateKey] = null;
  setProgress?.({ active: false });
  setBusyFlag(watchKey, false);
}

export function attachToActiveRun({
  isWatching,
  setOptimisticRun: setOptimisticRunFn,
  startWatch
}, runMeta = null) {
  if (typeof isWatching === "function" && isWatching()) return;
  if (runMeta && typeof runMeta === "object" && !Array.isArray(runMeta)) {
    setOptimisticRunFn?.(runMeta);
  }
  startWatch?.();
}

export function restartCompletionWatch(stopWatch, attachFn, runMeta = null) {
  stopWatch?.();
  attachFn?.(runMeta);
}

export function getRestorableRunMeta(report, {
  hasLiveState = false,
  optimisticRun = null,
  launchAtMs = 0,
  skewMs = 1000
} = {}) {
  if (!hasLiveState) {
    return null;
  }
  const finishedMs = parseReportTimestampMs(report?.finishedAt);
  if (finishedMs >= (Number(launchAtMs) - Number(skewMs))) {
    return null;
  }
  const runId = String(report?.runId || optimisticRun?.runId || "").trim();
  const startedAt = String(report?.startedAt || optimisticRun?.startedAt || "").trim();
  if (!runId && !startedAt) {
    return null;
  }
  return { runId, startedAt };
}

export function shouldApplyTimestampGate(report, {
  optimisticRun = null,
  launchAtMs = 0,
  timestampField = "startedAt",
  skewMs = 1000
} = {}) {
  const runId = String(report?.runId || "").trim();
  const optimisticRunId = String(optimisticRun?.runId || "").trim();
  if (optimisticRunId && runId && runId === optimisticRunId) {
    return true;
  }
  const timestampMs = parseReportTimestampMs(report?.[timestampField]);
  return timestampMs >= (Number(launchAtMs) - Number(skewMs));
}

function isLiveTaskReportActive(report) {
  return Boolean(
    report
    && !String(report.finishedAt || "").trim()
    && Boolean(report?.taskProgress?.active)
    && (String(report.runId || "").trim() || String(report.startedAt || "").trim())
  );
}

export function resetLiveTaskPlaceholder({
  logEl,
  clearOffset,
  setProgress,
  appendLog,
  message,
  level = "muted"
}) {
  if (!logEl) return;
  logEl.innerHTML = "";
  clearOffset?.();
  setProgress?.({ active: false });
  appendLog?.(message, level);
}

export function createRestoreActiveRunWatches({
  loadLatestFetcherReport,
  fetcherController,
  loadLatestDiscoveryReport,
  discoveryController
}) {
  let restorePromise = null;

  return async function restoreActiveRunWatches() {
    if (restorePromise) {
      return restorePromise;
    }

    restorePromise = (async () => {
      const fetchReport = await loadLatestFetcherReport({ silent: true }).catch(() => null);
      const fetchMeta = fetcherController?.getRestorableFetcherRunMeta?.(fetchReport);
      if (fetchMeta) {
        fetcherController?.restartFetcherCompletionWatch?.(fetchMeta);
      }

      const discoveryReport = await loadLatestDiscoveryReport({ silent: true }).catch(() => null);
      if (isLiveTaskReportActive(discoveryReport)) {
        discoveryController?.restartDiscoveryCompletionWatch?.({
          runId: discoveryReport?.runId,
          startedAt: discoveryReport?.startedAt
        });
      }
    })();

    try {
      return await restorePromise;
    } finally {
      restorePromise = null;
    }
  };
}
