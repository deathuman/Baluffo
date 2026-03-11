export function isDiscoveryMobileViewport(width = window.innerWidth) {
  return Number(width) < 900;
}

export function setDiscoveryLogOpen(detailsEl, nextOpen, {
  onSyncStart,
  onSyncEnd,
  schedule = callback => window.setTimeout(callback, 0)
} = {}) {
  if (!detailsEl) return;
  const desired = Boolean(nextOpen);
  if (detailsEl.open === desired) return;
  onSyncStart?.();
  detailsEl.open = desired;
  schedule(() => {
    onSyncEnd?.();
  });
}

export function syncDiscoveryLogDisclosure(detailsEl, {
  isMobileViewport,
  hasLiveDiscovery,
  discoveryLogUserToggled,
  discoveryLogPreferredOpen,
  setDiscoveryLogOpen
}) {
  if (!detailsEl) return;
  if (hasLiveDiscovery) {
    setDiscoveryLogOpen(true);
    return;
  }
  if (discoveryLogUserToggled) {
    setDiscoveryLogOpen(discoveryLogPreferredOpen);
    return;
  }
  setDiscoveryLogOpen(!isMobileViewport());
}

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
  loadDiscoveryData
}) {
  function appendDiscoveryLog(message, level = "info") {
    if (!refs.adminDiscoveryLogEl) return;
    const event = createLogEvent("discovery", message, level);
    appendLogRow(refs.adminDiscoveryLogEl, event);
  }

  function appendDiscoveryLogEvent(eventLike, fallbackLevel = "muted") {
    if (!refs.adminDiscoveryLogEl) return;
    const event = createLogEvent("discovery", eventLike, fallbackLevel);
    appendLogRow(refs.adminDiscoveryLogEl, event);
  }

  function appendDiscoveryServerLogText(text) {
    const payload = String(text || "");
    if (!payload) return;
    payload.split(/\r?\n/).forEach(line => {
      const trimmed = String(line || "").trim();
      if (!trimmed) return;
      const match = trimmed.match(/^\[([^\]]+)\]\s*(.*)$/);
      if (match) {
        appendDiscoveryLogEvent({
          timestamp: match[1],
          level: "muted",
          scope: "discovery",
          message: match[2] || ""
        }, "muted");
        return;
      }
      appendDiscoveryLog(trimmed, "muted");
    });
  }

  function setDiscoveryLogPlaceholder(message) {
    if (!refs.adminDiscoveryLogEl) return;
    refs.adminDiscoveryLogEl.innerHTML = "";
    state.discoveryLogRemoteOffset = 0;
    appendDiscoveryLog(message, "muted");
  }

  async function loadDiscoveryLogChunk(options = {}) {
    if (!state.adminPin) return null;
    const reset = Boolean(options?.reset);
    const offset = reset ? 0 : Math.max(0, Number(state.discoveryLogRemoteOffset) || 0);
    const payload = await getBridge(`/discovery/log?offset=${offset}`);
    if (reset) state.discoveryLogRemoteOffset = 0;
    appendDiscoveryServerLogText(String(payload?.text || ""));
    state.discoveryLogRemoteOffset = Math.max(0, Number(payload?.nextOffset) || 0);
    return payload || null;
  }

  function runProgressAppend(report, nowMs) {
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    const summary = report?.summary || {};
    const foundCount = Number(summary.foundEndpointCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const invalidCount = Number(summary.skippedInvalidCount || 0);

    const summarySignature = [foundCount, probedCount, queuedCount, failedCount, skippedCount, invalidCount].join("|");
    if (summarySignature !== liveState.summarySignature) {
      liveState.summarySignature = summarySignature;
      appendDiscoveryLog(
        `Progress: found ${foundCount}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}, skipped dupes ${skippedCount}, skipped invalid ${invalidCount}.`,
        failedCount > 0 ? "warn" : "info"
      );
    }

    const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
    if (candidates.length > liveState.candidateCount) {
      candidates.slice(liveState.candidateCount, candidates.length).slice(-3).forEach(row => {
        appendDiscoveryLog(
          `Queued candidate: ${String(row?.name || "unknown")} [${String(row?.adapter || "unknown")}] jobs ${Number(row?.jobsFound || 0)}.`,
          "muted"
        );
      });
      liveState.candidateCount = candidates.length;
    } else {
      liveState.candidateCount = candidates.length;
    }

    const failures = Array.isArray(report?.failures) ? report.failures : [];
    if (failures.length > liveState.failureCount) {
      failures.slice(liveState.failureCount, failures.length).slice(-3).forEach(item => {
        const stage = String(item?.stage || "probe");
        const name = String(item?.name || item?.domain || "unknown");
        appendDiscoveryLog(`Probe issue: ${name} [${stage}] ${String(item?.error || "unknown error")}`, "warn");
      });
      liveState.failureCount = failures.length;
    } else {
      liveState.failureCount = failures.length;
    }

    if ((nowMs - Number(liveState.lastHeartbeatAtMs || 0)) >= 20000) {
      liveState.lastHeartbeatAtMs = nowMs;
      appendDiscoveryLog("Discovery still running. Waiting for more probe updates...", "muted");
    }
  }

  function parseReportTimestampMs(value) {
    if (!value) return 0;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function startDiscoveryCompletionWatch() {
    stopDiscoveryCompletionWatch();
    setBusyFlag("discoveryWatch", true);
    state.discoveryLaunchAtMs = Date.now();
    state.discoveryCompletionPollDeadline = state.discoveryLaunchAtMs + state.discoveryReportPollTimeoutMs;
    state.discoveryLogRemoteOffset = 0;
    state.discoveryLiveProgressState = {
      summarySignature: "",
      candidateCount: 0,
      failureCount: 0,
      lastHeartbeatAtMs: 0
    };
    appendDiscoveryLog("Watching discovery report for live progress...");
    loadDiscoveryLogChunk({ reset: true }).catch(() => {});
    scheduleDiscoveryCompletionPoll(250);
  }

  function stopDiscoveryCompletionWatch() {
    if (state.discoveryCompletionPollTimer) {
      clearTimeout(state.discoveryCompletionPollTimer);
      state.discoveryCompletionPollTimer = null;
    }
    state.discoveryLiveProgressState = null;
    setBusyFlag("discoveryWatch", false);
  }

  function scheduleDiscoveryCompletionPoll(delayMs) {
    state.discoveryCompletionPollTimer = setTimeout(() => {
      pollDiscoveryCompletion().catch(err => {
        logAdminError("Discovery completion poll failed", err);
        scheduleDiscoveryCompletionPoll(state.discoveryReportPollIntervalMs);
      });
    }, delayMs);
  }

  async function pollDiscoveryCompletion() {
    const now = Date.now();
    if (now >= state.discoveryCompletionPollDeadline) {
      appendDiscoveryLog("Could not confirm discovery completion from report within timeout window.", "warn");
      stopDiscoveryCompletionWatch();
      return;
    }

    const [report] = await Promise.all([
      getBridge("/discovery/report"),
      loadDiscoveryLogChunk().catch(() => null)
    ]);
    const startedMs = parseReportTimestampMs(report?.startedAt);
    if (startedMs >= (state.discoveryLaunchAtMs - 1000)) {
      runProgressAppend(report, now);
    }
    const finishedMs = parseReportTimestampMs(report?.finishedAt);
    if (finishedMs >= (state.discoveryLaunchAtMs - 1000)) {
      const summary = report?.summary || {};
      const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
      const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
      const failedCount = Number(summary.failedProbeCount || 0);
      appendDiscoveryLog(
        `Discovery run completed: found ${Number(summary.foundEndpointCount ?? 0)}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}.`,
        failedCount > 0 ? "warn" : "success"
      );
      await Promise.allSettled([loadDiscoveryData(), loadOpsHealthData()]);
      stopDiscoveryCompletionWatch();
      return;
    }

    scheduleDiscoveryCompletionPoll(state.discoveryReportPollIntervalMs);
  }

  async function runDiscoveryTask() {
    if (!state.adminPin) {
      showToast("Unlock admin before running discovery.", "error");
      return;
    }
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Discovery operation already in progress.", "info");
      return;
    }
    setBusyFlag("discoveryRun", true);
    setBusyFlag("liveDiscoveryRunning", true);
    state.discoveryLogRemoteOffset = 0;
    appendDiscoveryLog("Triggering source discovery task...");
    try {
      await postBridge("/tasks/run-discovery", {});
      appendDiscoveryLog("Source discovery task started.", "success");
      showToast("Source discovery started.", "success");
      startDiscoveryCompletionWatch();
      loadOpsHealthData().catch(() => {});
      scheduleOpsHealthPolling(250);
    } catch (err) {
      appendDiscoveryLog(`Could not trigger discovery task: ${getErrorMessage(err)}`, "error");
      showToast("Could not trigger source discovery task.", "error");
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
    appendDiscoveryLog,
    appendDiscoveryLogEvent,
    appendDiscoveryServerLogText,
    loadDiscoveryLogChunk,
    setDiscoveryLogPlaceholder,
    startDiscoveryCompletionWatch,
    stopDiscoveryCompletionWatch,
    runDiscoveryTask,
    formatManualCheckFailureMessage
  };
}
