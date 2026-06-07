import {
  loadLiveTaskLogChunk,
  markLiveTaskActivity,
  runGuardedLiveTaskPoll,
  resetLiveTaskPlaceholder
} from "../live-task.js";

export function createAdminDiscoveryLogController({
  state,
  refs,
  getBridge,
  createLogEvent,
  appendLogRow,
  setDiscoveryProgress,
  updateDiscoveryProgressFromReport
}) {
  function appendDiscoveryLog(message, level = "info") {
    if (!refs.adminDiscoveryLogEl) return;
    const event = createLogEvent("discovery", message, level);
    appendLogRow(refs.adminDiscoveryLogEl, event);
  }

  function appendDiscoveryLogEvent(eventLike, fallbackLevel = "muted") {
    if (!refs.adminDiscoveryLogEl) return;
    const event = (eventLike && typeof eventLike === "object" && !Array.isArray(eventLike))
      ? {
          timestamp: String(eventLike.timestamp || new Date().toISOString()),
          level: String(eventLike.level || fallbackLevel || "muted"),
          scope: String(eventLike.scope || "discovery"),
          sourceId: String(eventLike.sourceId || ""),
          message: String(eventLike.message || "")
        }
      : createLogEvent("discovery", eventLike, fallbackLevel);
    appendLogRow(refs.adminDiscoveryLogEl, event);
  }

  function normalizeDiscoveryServerLine(rawLine) {
    const trimmed = String(rawLine || "").trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(/\s+/g, " ").trim();
    if (/launching source discovery task/i.test(normalized)) return null;
    if (/discovery report written/i.test(normalized)) return null;
    if (/watching discovery report/i.test(normalized)) return null;
    const level = /\b(error|failed|timeout|dns|ssl|forbidden)\b/i.test(normalized) ? "warn" : "muted";
    return {
      message: normalized,
      level
    };
  }

  function inferDiscoveryPhaseLabelFromServerMessage(message) {
    const normalized = String(message || "").trim().toLowerCase();
    if (!normalized) return "";
    if (/generating curated seed candidates/.test(normalized)) return "Generating seed candidates";
    if (/scanning game studios sheet directory/.test(normalized)) return "Scanning game studios sheet directory";
    if (/generating provider-pattern candidates/.test(normalized)) return "Generating provider-pattern candidates";
    if (/scanning known careers pages/.test(normalized)) return "Scanning known careers pages";
    if (/scanning gamesmap directory/.test(normalized)) return "Scanning Gamesmap directory";
    if (/running web-search discovery queries/.test(normalized)) return "Running web-search discovery queries";
    if (/starting probe phase for \d+ candidate/.test(normalized)) {
      const match = String(message || "").match(/Starting probe phase for (\d+ candidate(?:s)?)/i);
      return match ? `Probing ${match[1]}` : "Starting probe phase";
    }
    return "";
  }

  function updateDiscoveryProgressFromLivePhase(phaseLabel) {
    const nextLabel = String(phaseLabel || "").trim();
    if (!nextLabel) return;
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    if (liveState.serverPhaseLabel === nextLabel) return;
    liveState.serverPhaseLabel = nextLabel;
    markLiveTaskActivity(liveState);
    updateDiscoveryProgressFromReport(null, { running: true });
  }

  function appendDiscoveryServerLogText(text) {
    const payload = String(text || "");
    if (!payload) return;
    payload.split(/\r?\n/).forEach(line => {
      const trimmed = String(line || "").trim();
      if (!trimmed) return;
      const match = trimmed.match(/^\[([^\]]+)\]\s*(.*)$/);
      const normalizedLine = normalizeDiscoveryServerLine(match ? match[2] : trimmed);
      if (!normalizedLine) return;
      if (state.discoveryLiveProgressState?.serverLogSignatures?.has(normalizedLine.message)) return;
      state.discoveryLiveProgressState?.serverLogSignatures?.add(normalizedLine.message);
      markLiveTaskActivity(state.discoveryLiveProgressState);
      updateDiscoveryProgressFromLivePhase(inferDiscoveryPhaseLabelFromServerMessage(normalizedLine.message));
      if (match) {
        appendDiscoveryLogEvent({
          timestamp: match[1],
          level: normalizedLine.level,
          scope: "discovery",
          message: normalizedLine.message
        }, normalizedLine.level);
        return;
      }
      appendDiscoveryLog(normalizedLine.message, normalizedLine.level);
    });
  }

  function setDiscoveryLogPlaceholder(message) {
    resetLiveTaskPlaceholder({
      logEl: refs.adminDiscoveryLogEl,
      clearOffset: () => {
        state.discoveryLogRemoteOffset = 0;
      },
      setProgress: view => setDiscoveryProgress(view),
      appendLog: appendDiscoveryLog,
      message
    });
  }

  async function loadDiscoveryLogChunk(options = {}) {
    const loadChunk = () => loadLiveTaskLogChunk({
      getBridge,
      path: "/discovery/log",
      state,
      offsetKey: "discoveryLogRemoteOffset",
      reset: Boolean(options?.reset),
      view: options?.view || "",
      limitChars: Number(options?.limitChars || 0),
      requestOptions: options?.requestOptions || {},
      onText: appendDiscoveryServerLogText
    });
    const guard = options?.guarded === false
      ? null
      : state.discoveryLiveProgressState?.logPollGuard;
    if (!guard) {
      return loadChunk();
    }
    const result = await runGuardedLiveTaskPoll(guard, loadChunk);
    if (result.skipped) return null;
    if (!result.ok) throw result.error;
    return result.value;
  }

  return {
    appendDiscoveryLog,
    appendDiscoveryLogEvent,
    appendDiscoveryServerLogText,
    setDiscoveryLogPlaceholder,
    loadDiscoveryLogChunk
  };
}
