import { deriveDiscoveryProgressModel, deriveDiscoveryQueuedCount, deriveDiscoveryTaskProgress } from "../domain.js";
import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature,
  attachToActiveRun,
  clearOptimisticRun,
  createBoundedSignatureSet,
  loadLiveTaskLogChunk,
  loadTaskLivePayload,
  markLiveTaskActivity,
  parseReportTimestampMs,
  pickMeaningfulTaskLivePayload,
  pickTaskLivePayload,
  resetLiveTaskPlaceholder,
  restartCompletionWatch,
  scheduleAsyncWatchTimer,
  setOptimisticRun,
  shouldApplyTimestampGate,
  startLiveTaskWatch,
  stopLiveTaskWatch
} from "./live-task.js";
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
  activeProgressPollIntervalMs = 500,
  syncSourceTablesAfterTaskCompletion,
  loadDiscoveryData
}) {
  function populateDiscoveryConfigForm(savedConfig = {}, { force = false } = {}) {
    if (!refs.adminDiscoveryAutoApproveToggleEl) return;
    if (state.discoveryConfigDirty && !force) return;
    refs.adminDiscoveryAutoApproveToggleEl.checked = savedConfig.autoApproveHealthyPendingOnComplete !== false;
  }

  function collectDiscoveryConfigPayload() {
    return {
      autoApproveHealthyPendingOnComplete: Boolean(refs.adminDiscoveryAutoApproveToggleEl?.checked)
    };
  }

  async function loadDiscoveryConfig(options = {}) {
    const silent = Boolean(options?.silent);
    const forceForm = Boolean(options?.forceForm);
    try {
      const payload = await getBridge("/discovery/config");
      state.latestDiscoveryConfigCache = payload || null;
      populateDiscoveryConfigForm((payload || {}).savedConfig || {}, { force: forceForm });
      return payload || null;
    } catch (err) {
      if (!silent) showToast(`Could not load discovery settings: ${getErrorMessage(err)}`, "error");
      throw err;
    }
  }

  async function loadLatestDiscoveryReport(options = {}) {
    const silent = Boolean(options.silent);
    try {
      const report = await getBridge("/discovery/report");
      if (report && typeof report === "object" && !Array.isArray(report)) {
        state.latestDiscoveryReportCache = report;
      }
      return report || null;
    } catch (err) {
      if (!silent) {
        logAdminError("Failed to load discovery report", err);
      }
      return null;
    }
  }

  async function saveDiscoveryConfig() {
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/discovery/config", collectDiscoveryConfigPayload());
      state.latestDiscoveryConfigCache = result || null;
      state.discoveryConfigDirty = false;
      populateDiscoveryConfigForm((result || {}).savedConfig || {}, { force: true });
      showToast("Discovery auto-approve preference updated.", "success");
      return result || null;
    } catch (err) {
      showToast(`Could not save discovery settings: ${getErrorMessage(err)}`, "error");
      throw err;
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  function setDiscoveryProgress(view) {
    if (!refs.adminDiscoveryProgressEl || !refs.adminDiscoveryProgressBarEl || !refs.adminDiscoveryProgressLabelEl) {
      return;
    }

    applyAdminTaskProgress(
      refs.adminDiscoveryProgressEl,
      refs.adminDiscoveryProgressBarEl,
      refs.adminDiscoveryProgressLabelEl,
      view
    );
  }

  function getDiscoveryProgressPhaseHint() {
    return String(state.discoveryLiveProgressState?.serverPhaseLabel || "").trim();
  }

  function updateDiscoveryProgressFromReport(report, { running = false } = {}) {
    setDiscoveryProgress(deriveDiscoveryProgressModel(report, {
      running,
      phaseHint: getDiscoveryProgressPhaseHint()
    }));
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
    setOptimisticRun(state, "discoveryOptimisticRun", runMeta);
  }

  async function loadDiscoveryLivePayload() {
    return loadTaskLivePayload({
      getBridge,
      taskType: "discovery"
    });
  }

  function clearOptimisticDiscoveryRun() {
    clearOptimisticRun(state, "discoveryOptimisticRun");
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
    return loadLiveTaskLogChunk({
      getBridge,
      path: "/discovery/log",
      state,
      offsetKey: "discoveryLogRemoteOffset",
      reset: Boolean(options?.reset),
      onText: appendDiscoveryServerLogText
    });
  }

  function runProgressAppend(report, nowMs) {
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    updateDiscoveryProgressFromReport(report, { running: true });
    const summary = report?.summary || {};
    const progress = deriveDiscoveryTaskProgress(report, {
      running: true,
      phaseHint: getDiscoveryProgressPhaseHint()
    });
    const phaseLabel = String(progress?.phaseLabel || summary.phaseLabel || summary.phase || "").trim();
    const counts = progress?.counts && typeof progress.counts === "object" ? progress.counts : {};
    const foundCount = Number(counts.foundEndpoints ?? summary.foundEndpointCount ?? 0);
    const probedCount = Number(counts.probedCandidates ?? summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = Number(counts.queuedCandidates ?? deriveDiscoveryQueuedCount(report));
    const deferredCount = Number(counts.deferredCandidates ?? summary.discoverableButDeferredCount ?? 0);
    const failedCount = Number(counts.failedProbes ?? summary.failedProbeCount ?? 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const invalidCount = Number(summary.skippedInvalidCount || 0);
    let sawLocalActivity = false;

    const summarySignature = [foundCount, probedCount, queuedCount, deferredCount, failedCount, skippedCount, invalidCount].join("|");
    if (phaseLabel && phaseLabel !== liveState.phaseLabel) {
      liveState.phaseLabel = phaseLabel;
      sawLocalActivity = true;
      appendDiscoveryLog(`Discovery phase: ${phaseLabel}.`, "muted");
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
      sawLocalActivity = true;
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
      sawLocalActivity = true;
    } else {
      liveState.failureCount = failures.length;
    }

    if (sawLocalActivity) {
      markLiveTaskActivity(liveState, nowMs);
    }

    appendLiveTaskActivity({
      payload: report,
      liveState,
      nowMs,
      appendEvent: event => appendDiscoveryLogEvent(event, "muted"),
      scope: "discovery",
      summarySignature,
      workItemSignature: buildTaskWorkItemActivitySignature(report),
      onSummaryChange: () => {
        appendDiscoveryLog(
          `Discovery: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}, failed ${failedCount}, skipped dupes ${skippedCount}, invalid ${invalidCount}.`,
          failedCount > 0 ? "warn" : "info"
        );
      },
      onHeartbeat: () => {
        appendDiscoveryLog(
          `Discovery active${phaseLabel ? ` (${phaseLabel})` : ""}: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}.`,
          "muted"
        );
      }
    });
  }

  /** Avoid dropping live progress when report.startedAt lags discoveryLaunchAtMs (slow POST, skew, etc.). */
  function shouldApplyDiscoveryLiveProgressGate(report) {
    return shouldApplyTimestampGate(report, {
      optimisticRun: state.discoveryOptimisticRun,
      launchAtMs: state.discoveryLaunchAtMs,
      timestampField: "startedAt",
      skewMs: 60000
    });
  }

  /** Same idea for completion: match runId or allow modest clock skew vs launch anchor. */
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

  function refreshDiscoveryDataIfNeeded(report) {
    if (typeof loadDiscoveryData !== "function") return;
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    const summary = report?.summary || {};
    const signature = [
      String(report?.runId || ""),
      String(report?.startedAt || ""),
      String(report?.finishedAt || ""),
      Number(summary.foundEndpointCount || 0),
      Number(summary.probedCandidateCount ?? summary.probedCount ?? 0),
      Number(summary.queuedCandidateCount ?? 0),
      Number(summary.failedProbeCount || 0),
      Number(summary.skippedDuplicateCount || 0)
    ].join("|");
    if (signature === liveState.registryRefreshSignature) return;
    liveState.registryRefreshSignature = signature;
    loadDiscoveryData().catch(() => {});
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
      onError: err => {
        logAdminError("Discovery completion poll failed", err);
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

  async function runDiscoveryTask(runOptions = {}) {
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Discovery operation already in progress.", "info");
      return;
    }
    setBusyFlag("discoveryRun", true);
    state.discoveryLogRemoteOffset = 0;
    updateDiscoveryProgressFromReport(null, { running: true });
    appendDiscoveryLog("Triggering source discovery task...");
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
        attachToActiveDiscoveryRun(result, { announceStart: false });
        appendDiscoveryLog("Discovery already running; attached to the active bridge-managed run.", "info");
        showToast("Source discovery already running. Attached to active run.", "info");
        loadOpsHealthData().catch(() => {});
        loadLatestDiscoveryReport({ silent: true }).catch(() => {});
        scheduleOpsHealthPolling(250);
        return;
      }
      setOptimisticDiscoveryRun(result || {});
      const preset = String(result?.preset || payload?.preset || "default").trim().toLowerCase();
      const isUncapped = preset === "uncapped";
      appendDiscoveryLog(isUncapped ? "Source discovery uncapped task started." : "Source discovery task started.", "success");
      showToast(isUncapped ? "Source discovery uncapped run started." : "Source discovery started.", "success");
      startDiscoveryCompletionWatch();
      loadOpsHealthData().catch(() => {});
      scheduleOpsHealthPolling(250);
    } catch (err) {
      let recovered = false;
      const message = getErrorMessage(err);
      if (/network|empty response|bridge unreachable|fetch/i.test(String(message || ""))) {
        try {
          recovered = await recoverDiscoveryLaunchAfterTransportError(launchAttemptAtMs);
        } catch (recoveryErr) {
          logAdminError("Discovery launch recovery probe failed", recoveryErr);
        }
      }
      if (recovered) return;
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
    populateDiscoveryConfigForm,
    collectDiscoveryConfigPayload,
    loadDiscoveryConfig,
    saveDiscoveryConfig,
    appendDiscoveryLog,
    appendDiscoveryLogEvent,
    appendDiscoveryServerLogText,
    loadDiscoveryLogChunk,
    loadDiscoveryLivePayload,
    loadLatestDiscoveryReport,
    setDiscoveryLogPlaceholder,
    clearOptimisticDiscoveryRun,
    attachToActiveDiscoveryRun,
    restartDiscoveryCompletionWatch,
    startDiscoveryCompletionWatch,
    stopDiscoveryCompletionWatch,
    runDiscoveryTask,
    formatManualCheckFailureMessage
  };
}
