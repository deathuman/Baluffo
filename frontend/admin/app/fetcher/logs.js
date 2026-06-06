import {
  loadLiveTaskLogChunk,
  markLiveTaskActivity,
  resetLiveTaskPlaceholder,
  scheduleAsyncWatchTimer
} from "../live-task.js";

export function createAdminFetcherLogController({
  state,
  refs,
  getBridge,
  createLogEvent,
  appendLogRow,
  setFetcherProgress
}) {
  function appendFetcherLogEvent(eventLike, fallbackLevel = "muted") {
    if (!refs.adminFetcherLogEl) return;
    const event = (eventLike && typeof eventLike === "object" && !Array.isArray(eventLike))
      ? {
          timestamp: String(eventLike.timestamp || new Date().toISOString()),
          level: String(eventLike.level || fallbackLevel || "muted"),
          scope: String(eventLike.scope || "fetch"),
          sourceId: String(eventLike.sourceId || eventLike.workItemId || ""),
          message: String(eventLike.message || "")
        }
      : createLogEvent("fetch", String(eventLike || ""), fallbackLevel);
    appendLogRow(refs.adminFetcherLogEl, event);
  }

  function appendFetcherLog(message, level = "info") {
    if (!refs.adminFetcherLogEl) return;
    const event = createLogEvent("fetcher", message, level);
    appendLogRow(refs.adminFetcherLogEl, event);
  }

  function normalizeFetcherServerLine(rawLine) {
    const trimmed = String(rawLine || "").trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(/\s+/g, " ").trim();
    if (/^triggered fetcher via local admin bridge/i.test(normalized)) return null;
    if (/^fetcher started\. watching live progress/i.test(normalized)) return null;
    const level = /\b(error|failed|timeout|dns|ssl|forbidden|traceback|exception)\b/i.test(normalized)
      ? "warn"
      : /\bwarn|retry|excluded\b/i.test(normalized)
        ? "muted"
        : "muted";
    return {
      message: normalized,
      level
    };
  }

  function appendFetcherServerLogText(text) {
    const payload = String(text || "");
    if (!payload) return;
    payload.split(/\r?\n/).forEach(line => {
      const trimmed = String(line || "").trim();
      if (!trimmed) return;
      const match = trimmed.match(/^\[([^\]]+)\]\s*(.*)$/);
      const normalizedLine = normalizeFetcherServerLine(match ? match[2] : trimmed);
      if (!normalizedLine) return;
      if (state.fetcherLiveProgressState?.serverLogSignatures?.has(normalizedLine.message)) return;
      state.fetcherLiveProgressState?.serverLogSignatures?.add(normalizedLine.message);
      markLiveTaskActivity(state.fetcherLiveProgressState);
      if (match) {
        const event = {
          timestamp: String(match[1] || new Date().toISOString()),
          level: normalizedLine.level,
          scope: "fetcher",
          sourceId: "",
          message: normalizedLine.message
        };
        appendLogRow(refs.adminFetcherLogEl, event);
        return;
      }
      appendFetcherLog(normalizedLine.message, normalizedLine.level);
    });
  }

  function setFetcherLogPlaceholder(message) {
    resetLiveTaskPlaceholder({
      logEl: refs.adminFetcherLogEl,
      clearOffset: () => {
        state.fetcherLogRemoteOffset = 0;
      },
      setProgress: view => setFetcherProgress(view),
      appendLog: appendFetcherLog,
      message
    });
  }

  async function loadFetcherLogChunk(options = {}) {
    const payload = await loadLiveTaskLogChunk({
      getBridge,
      path: "/fetcher/log",
      state,
      offsetKey: "fetcherLogRemoteOffset",
      reset: Boolean(options?.reset),
      onText: appendFetcherServerLogText
    });
    const text = String(payload?.text || "").trim();
    const hasMore = Boolean(payload?.hasMore);
    const hasVisibleLog = Boolean(String(refs.adminFetcherLogEl?.textContent || "").trim());
    if (options?.showEmptyState && options?.reset && !text && !hasMore && !hasVisibleLog) {
      appendFetcherLog("No fetch log entries yet.", "muted");
    }
    return payload;
  }

  function stopFetcherLogPolling() {
    if (!state.fetcherLogPollTimer) return;
    clearTimeout(state.fetcherLogPollTimer);
    state.fetcherLogPollTimer = null;
  }

  function scheduleFetcherLogPoll(delayMs) {
    stopFetcherLogPolling();
    scheduleAsyncWatchTimer({
      state,
      timerKey: "fetcherLogPollTimer",
      delayMs: Math.max(250, Number(delayMs) || 900),
      task: () => loadFetcherLogChunk().catch(() => null).finally(() => {
        if (state.adminBusyState.fetcherWatch) {
          scheduleFetcherLogPoll(delayMs);
        }
      })
    });
  }

  return {
    appendFetcherLogEvent,
    appendFetcherLog,
    appendFetcherServerLogText,
    setFetcherLogPlaceholder,
    loadFetcherLogChunk,
    scheduleFetcherLogPoll,
    stopFetcherLogPolling
  };
}
