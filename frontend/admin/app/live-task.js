import { getLiveTaskWorkItems } from "../../shared/live-task.js";

const DEFAULT_SIGNATURE_TRACKER_CAP = 256;

class BoundedSignatureSet extends Set {
  constructor(limit = DEFAULT_SIGNATURE_TRACKER_CAP) {
    super();
    this.limit = Math.max(1, Number(limit) || DEFAULT_SIGNATURE_TRACKER_CAP);
    this.order = [];
  }

  add(value) {
    const signature = String(value ?? "").trim();
    if (!signature || super.has(signature)) {
      return this;
    }
    super.add(signature);
    this.order.push(signature);
    while (this.size > this.limit && this.order.length > 0) {
      const dropped = String(this.order.shift() || "").trim();
      if (dropped) {
        super.delete(dropped);
      }
    }
    return this;
  }

  clear() {
    this.order = [];
    return super.clear();
  }
}

export function createBoundedSignatureSet(limit = DEFAULT_SIGNATURE_TRACKER_CAP) {
  return new BoundedSignatureSet(limit);
}

function maybeUnrefTimer(timer) {
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

export function loadTaskLivePayload({
  getBridge,
  taskType
}) {
  return getBridge(`/ops/task-live/${encodeURIComponent(String(taskType || "").trim().toLowerCase())}`)
    .then(payload => (payload && typeof payload === "object" ? payload : null));
}

function getTaskLiveWorkItems(payload) {
  return getLiveTaskWorkItems(payload);
}

function getTaskLiveRecentEvents(payload) {
  return Array.isArray(payload?.recentEvents) ? payload.recentEvents : [];
}

function hasTaskLivePayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  if (payload.active) return true;
  if (String(payload.runId || "").trim()) return true;
  if (String(payload.startedAt || "").trim()) return true;
  if (String(payload.finishedAt || "").trim()) return true;
  if (payload.taskProgress && typeof payload.taskProgress === "object" && !Array.isArray(payload.taskProgress)) {
    return true;
  }
  if (getTaskLiveWorkItems(payload).length > 0) return true;
  if (Array.isArray(payload.recentEvents) && payload.recentEvents.length > 0) return true;
  return false;
}

function hasMeaningfulTaskProgress(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  if (payload.active) return true;
  if (String(payload.phaseKey || "").trim()) return true;
  if (String(payload.phaseLabel || "").trim()) return true;
  const counts = payload.counts && typeof payload.counts === "object" && !Array.isArray(payload.counts)
    ? payload.counts
    : {};
  return Object.values(counts).some(value => {
    if (typeof value === "number") return Number(value) > 0;
    if (typeof value === "boolean") return value;
    return Boolean(String(value || "").trim());
  });
}

function hasMeaningfulTaskLivePayload(payload) {
  if (!hasTaskLivePayload(payload)) return false;
  if (hasMeaningfulTaskProgress(payload.taskProgress)) return true;
  if (getTaskLiveWorkItems(payload).length > 0) return true;
  if (getTaskLiveRecentEvents(payload).length > 0) return true;
  return false;
}

export function pickTaskLivePayload(livePayload, fallbackPayload = null) {
  void fallbackPayload;
  return hasTaskLivePayload(livePayload) ? livePayload : null;
}

export function pickMeaningfulTaskLivePayload(livePayload, fallbackPayload = null) {
  if (hasMeaningfulTaskLivePayload(livePayload)) return livePayload;
  return hasMeaningfulTaskLivePayload(fallbackPayload) ? fallbackPayload : null;
}

export function markLiveTaskActivity(liveState, nowMs = Date.now()) {
  if (!liveState || typeof liveState !== "object") return;
  liveState.lastActivityAtMs = Math.max(0, Number(nowMs) || Date.now());
}

function formatCountsSignature(counts) {
  if (!counts || typeof counts !== "object" || Array.isArray(counts)) return "";
  return Object.keys(counts)
    .sort()
    .map(key => `${String(key)}:${String(counts[key] ?? "")}`)
    .join(",");
}

export function buildTaskWorkItemActivitySignature(payload) {
  return getTaskLiveWorkItems(payload).map(item => {
    const progress = item?.progress && typeof item.progress === "object" && !Array.isArray(item.progress)
      ? item.progress
      : {};
    return [
      String(item?.id || item?.name || ""),
      String(item?.status || ""),
      String(item?.startedAt || ""),
      String(item?.finishedAt || ""),
      String(item?.heartbeatAt || ""),
      String(progress.phaseKey || ""),
      String(progress.phaseLabel || ""),
      formatCountsSignature(progress.counts),
      String(progress.targetLabel || ""),
      String(progress.targetUrl || ""),
      String(progress.updatedAt || ""),
      String(item?.error || "")
    ].join("|");
  }).join("||");
}

function appendStructuredTaskEvents({
  payload,
  liveState,
  appendEvent,
  scope
}) {
  if (!liveState || typeof appendEvent !== "function") return false;
  const events = getTaskLiveRecentEvents(payload);
  if (!events.length) return false;
  if (!(liveState.recentEventSignatures instanceof Set)) {
    liveState.recentEventSignatures = createBoundedSignatureSet();
  }
  let sawActivity = false;
  events.forEach(eventLike => {
    const signature = [
      String(eventLike?.timestamp || ""),
      String(eventLike?.phaseKey || ""),
      String(eventLike?.workItemId || eventLike?.sourceId || ""),
      String(eventLike?.message || "")
    ].join("|");
    if (!signature || liveState.recentEventSignatures.has(signature)) {
      return;
    }
    liveState.recentEventSignatures.add(signature);
    appendEvent({
      ...eventLike,
      scope: String(eventLike?.scope || scope || "admin"),
      sourceId: String(eventLike?.sourceId || eventLike?.workItemId || "")
    });
    sawActivity = true;
  });
  return sawActivity;
}

export function appendLiveTaskActivity({
  payload,
  liveState,
  nowMs,
  appendEvent,
  scope,
  summarySignature = "",
  workItemSignature = "",
  onSummaryChange,
  onHeartbeat,
  heartbeatIntervalMs = 60000
}) {
  if (!liveState || typeof liveState !== "object") return false;
  let sawActivity = appendStructuredTaskEvents({
    payload,
    liveState,
    appendEvent,
    scope
  });
  const nextSummarySignature = String(summarySignature || "");
  if (nextSummarySignature !== String(liveState.summarySignature || "")) {
    liveState.summarySignature = nextSummarySignature;
    sawActivity = true;
    onSummaryChange?.();
  }
  const nextWorkItemSignature = String(workItemSignature || "");
  if (nextWorkItemSignature !== String(liveState.workItemSignature || "")) {
    liveState.workItemSignature = nextWorkItemSignature;
    sawActivity = true;
  }
  if (sawActivity) {
    markLiveTaskActivity(liveState, nowMs);
  }
  const heartbeatInterval = Math.max(1000, Number(heartbeatIntervalMs) || 60000);
  const clockNowMs = Math.max(0, Number(nowMs) || Date.now());
  const idleMs = clockNowMs - Number(liveState.lastActivityAtMs || 0);
  if (idleMs < heartbeatInterval) return sawActivity;
  if ((clockNowMs - Number(liveState.lastHeartbeatAtMs || 0)) < heartbeatInterval) return sawActivity;
  liveState.lastHeartbeatAtMs = clockNowMs;
  onHeartbeat?.();
  return true;
}

export function scheduleAsyncWatchTimer({
  state,
  timerKey,
  delayMs,
  task,
  onError
}) {
  state[timerKey] = maybeUnrefTimer(setTimeout(
    () => Promise.resolve()
      .then(task)
      .catch(err => {
        onError?.(err);
      }),
    Math.max(0, Number(delayMs) || 0)
  ));
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

function isRestorableLiveTaskRun(report) {
  return Boolean(
    report
    && !String(report.finishedAt || "").trim()
    && (
      Boolean(report?.active)
      || Boolean(report?.taskProgress?.active)
      || String(report?.runId || "").trim()
      || String(report?.startedAt || "").trim()
    )
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
  loadFetcherLivePayload,
  loadLatestFetcherReport,
  fetcherController,
  loadDiscoveryLivePayload,
  loadLatestDiscoveryReport,
  discoveryController
}) {
  let restorePromise = null;

  return async function restoreActiveRunWatches() {
    if (restorePromise) {
      return restorePromise;
    }

    restorePromise = (async () => {
      const fetchLivePayload = await loadFetcherLivePayload?.().catch(() => null);
      if (isRestorableLiveTaskRun(fetchLivePayload)) {
        fetcherController?.attachToActiveFetchRun?.({
          runId: fetchLivePayload?.runId,
          startedAt: fetchLivePayload?.startedAt
        }, {
          announceStart: false
        });
        await loadLatestFetcherReport?.({ silent: true, hydrateActiveProgress: true }).catch(() => null);
      } else {
        const fetchReport = await loadLatestFetcherReport({ silent: true }).catch(() => null);
        const fetchMeta = fetcherController?.getRestorableFetcherRunMeta?.(fetchReport);
        if (fetchMeta) {
          fetcherController?.attachToActiveFetchRun?.(fetchMeta, {
            announceStart: false
          });
          await loadLatestFetcherReport?.({ silent: true, hydrateActiveProgress: true }).catch(() => null);
        } else if (isRestorableLiveTaskRun(fetchReport)) {
          fetcherController?.attachToActiveFetchRun?.({
            runId: fetchReport?.runId,
            startedAt: fetchReport?.startedAt
          }, {
            announceStart: false
          });
          await loadLatestFetcherReport?.({ silent: true, hydrateActiveProgress: true }).catch(() => null);
        }
      }

      const discoveryLivePayload = await loadDiscoveryLivePayload?.().catch(() => null);
      if (isRestorableLiveTaskRun(discoveryLivePayload)) {
        discoveryController?.attachToActiveDiscoveryRun?.({
          runId: discoveryLivePayload?.runId,
          startedAt: discoveryLivePayload?.startedAt
        }, {
          announceStart: false
        });
      } else {
        const discoveryReport = await loadLatestDiscoveryReport({ silent: true }).catch(() => null);
        if (isRestorableLiveTaskRun(discoveryReport)) {
          discoveryController?.attachToActiveDiscoveryRun?.({
            runId: discoveryReport?.runId,
            startedAt: discoveryReport?.startedAt
          }, {
            announceStart: false
          });
        }
      }
    })();

    try {
      return await restorePromise;
    } finally {
      restorePromise = null;
    }
  };
}
