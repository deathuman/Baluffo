import { deriveDiscoveryProgressModel, deriveDiscoveryQueuedCount } from "../domain.js";
import { applyAdminTaskProgress } from "./progress-ui.js";

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
  function setDiscoveryProgress(view) {
    applyAdminTaskProgress(
      refs.adminDiscoveryProgressEl,
      refs.adminDiscoveryProgressBarEl,
      refs.adminDiscoveryProgressLabelEl,
      view
    );
  }

  function updateDiscoveryProgressFromReport(report, { running = false } = {}) {
    setDiscoveryProgress(deriveDiscoveryProgressModel(report, { running }));
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

  function setOptimisticDiscoveryRun(runMeta) {
    const startedAt = String(runMeta?.startedAt || "").trim();
    if (!startedAt) {
      state.discoveryOptimisticRun = null;
      return;
    }
    state.discoveryOptimisticRun = {
      runId: String(runMeta?.runId || ""),
      startedAt
    };
  }

  function clearOptimisticDiscoveryRun() {
    state.discoveryOptimisticRun = null;
  }

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
    if (!refs.adminDiscoveryLogEl) return;
    refs.adminDiscoveryLogEl.innerHTML = "";
    state.discoveryLogRemoteOffset = 0;
    setDiscoveryProgress({ active: false });
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
    updateDiscoveryProgressFromReport(report, { running: true });
    const summary = report?.summary || {};
    const phaseLabel = String(summary.phaseLabel || summary.phase || "").trim();
    const foundCount = Number(summary.foundEndpointCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = deriveDiscoveryQueuedCount(report);
    const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const invalidCount = Number(summary.skippedInvalidCount || 0);

    const summarySignature = [foundCount, probedCount, queuedCount, deferredCount, failedCount, skippedCount, invalidCount].join("|");
    if (phaseLabel && phaseLabel !== liveState.phaseLabel) {
      liveState.phaseLabel = phaseLabel;
      appendDiscoveryLog(`Discovery phase: ${phaseLabel}.`, "muted");
    }
    if (summarySignature !== liveState.summarySignature) {
      liveState.summarySignature = summarySignature;
      appendDiscoveryLog(
        `Discovery: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}, failed ${failedCount}, skipped dupes ${skippedCount}, invalid ${invalidCount}.`,
        failedCount > 0 ? "warn" : "info"
      );
    }

    const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
    if (candidates.length > liveState.candidateCount) {
      const nextRows = candidates.slice(liveState.candidateCount, candidates.length);
      const adapterCounts = new Map();
      nextRows.forEach(row => {
        const adapter = String(row?.adapter || "unknown");
        adapterCounts.set(adapter, Number(adapterCounts.get(adapter) || 0) + 1);
      });
      const burstSummary = Array.from(adapterCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 2)
        .map(([adapter, count]) => `${adapter} ${count}`)
        .join(" | ");
      appendDiscoveryLog(
        `New queue burst: +${nextRows.length} candidate${nextRows.length === 1 ? "" : "s"}${burstSummary ? ` (${burstSummary})` : ""}.`,
        "muted"
      );
      liveState.candidateCount = candidates.length;
    } else {
      liveState.candidateCount = candidates.length;
    }

    const failures = Array.isArray(report?.failures) ? report.failures : [];
    if (failures.length > liveState.failureCount) {
      const nextFailures = failures.slice(liveState.failureCount, failures.length);
      const grouped = new Map();
      nextFailures.forEach(item => {
        const key = String(item?.stage || item?.errorCode || item?.error || "unknown").trim() || "unknown";
        grouped.set(key, Number(grouped.get(key) || 0) + 1);
      });
      const cluster = Array.from(grouped.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([label, count]) => `${label} x${count}`)
        .join(" | ");
      appendDiscoveryLog(`Failure cluster: ${cluster}`, "warn");
      liveState.failureCount = failures.length;
    } else {
      liveState.failureCount = failures.length;
    }

    if ((nowMs - Number(liveState.lastHeartbeatAtMs || 0)) >= 12000) {
      liveState.lastHeartbeatAtMs = nowMs;
      appendDiscoveryLog(
        `Discovery active${phaseLabel ? ` (${phaseLabel})` : ""}: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}.`,
        "muted"
      );
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
    const optimisticStartedAtMs = parseReportTimestampMs(state.discoveryOptimisticRun?.startedAt);
    if (optimisticStartedAtMs > 0) {
      state.discoveryLaunchAtMs = optimisticStartedAtMs;
    }
    state.discoveryCompletionPollDeadline = state.discoveryLaunchAtMs + state.discoveryReportPollTimeoutMs;
    state.discoveryLogRemoteOffset = 0;
    state.discoveryLiveProgressState = {
      phaseLabel: "",
      summarySignature: "",
      candidateCount: 0,
      failureCount: 0,
      serverLogSignatures: new Set(),
      lastHeartbeatAtMs: 0
    };
    updateDiscoveryProgressFromReport(null, { running: true });
    appendDiscoveryLog("Discovery started. Watching live progress...", "info");
    loadDiscoveryLogChunk({ reset: true }).catch(() => {});
    scheduleDiscoveryCompletionPoll(250);
  }

  function stopDiscoveryCompletionWatch() {
    if (state.discoveryCompletionPollTimer) {
      clearTimeout(state.discoveryCompletionPollTimer);
      state.discoveryCompletionPollTimer = null;
    }
    state.discoveryLiveProgressState = null;
    setDiscoveryProgress({ active: false });
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
      clearOptimisticDiscoveryRun();
      setBusyFlag("liveDiscoveryRunning", false);
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
      const queuedCount = deriveDiscoveryQueuedCount(report);
      const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
      const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
      const failedCount = Number(summary.failedProbeCount || 0);
      updateDiscoveryProgressFromReport(report, { running: true });
      appendDiscoveryLog(
        `Discovery completed: endpoints ${Number(summary.foundEndpointCount ?? 0)}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}, failed ${failedCount}.`,
        failedCount > 0 ? "warn" : "success"
      );
      clearOptimisticDiscoveryRun();
      setBusyFlag("liveDiscoveryRunning", false);
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
    updateDiscoveryProgressFromReport(null, { running: true });
    appendDiscoveryLog("Triggering source discovery task...");
    try {
      const result = await postBridge("/tasks/run-discovery", {});
      setOptimisticDiscoveryRun(result || {});
      appendDiscoveryLog("Source discovery task started.", "success");
      showToast("Source discovery started.", "success");
      startDiscoveryCompletionWatch();
      loadOpsHealthData().catch(() => {});
      scheduleOpsHealthPolling(250);
    } catch (err) {
      appendDiscoveryLog(`Could not trigger discovery task: ${getErrorMessage(err)}`, "error");
      showToast("Could not trigger source discovery task.", "error");
      clearOptimisticDiscoveryRun();
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
    clearOptimisticDiscoveryRun,
    startDiscoveryCompletionWatch,
    stopDiscoveryCompletionWatch,
    runDiscoveryTask,
    formatManualCheckFailureMessage
  };
}
