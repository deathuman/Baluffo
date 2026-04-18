import { deriveFetcherFailureSummary, deriveFetcherProgressModel, deriveFetcherTaskProgress } from "../domain.js";
import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature,
  attachToActiveRun,
  clearOptimisticRun,
  createBoundedSignatureSet,
  getRestorableRunMeta,
  loadLiveTaskLogChunk,
  loadTaskLivePayload,
  markLiveTaskActivity,
  parseReportTimestampMs,
  pickTaskLivePayload,
  resetLiveTaskPlaceholder,
  restartCompletionWatch,
  scheduleAsyncWatchTimer,
  setOptimisticRun,
  startLiveTaskWatch,
  stopLiveTaskWatch
} from "./live-task.js";
import { applyAdminTaskProgress } from "./progress-ui.js";

const FETCHER_FALLBACK_MESSAGES = {
  bridgeUnavailable: "Bridge is offline; using VS Code task fallback for this run.",
  presetNeedsBridge: "VS Code task fallback supports default fetcher runs only. Start admin bridge and retry.",
  launchPrimary: taskLabel => `Triggered VS Code task URI (primary): ${taskLabel}`,
  launchSecondary: "Triggered VS Code task URI fallback (quoted task label).",
  manualHint: "If VS Code did not open, run the manual command fallback shown below.",
  copiedManualCommand: command => `Copied manual command fallback: ${command}`,
  manualCommand: command => `Manual command fallback: ${command}`
};

export const FETCHER_PRESET_META = {
  default: {
    preset: "default",
    buttonKey: "default",
    busyLabel: "Fetcher Running...",
    title: "Run the standard fetcher flow with current defaults (parallel workers, domain limits, circuit breaker).",
    ariaLabel: "Run jobs fetcher with default options"
  },
  incremental: {
    preset: "incremental",
    buttonKey: "incremental",
    busyLabel: "Incremental Running...",
    title: "Run incremental mode: skip recently successful sources based on TTL and reuse existing output.",
    ariaLabel: "Run incremental fetcher"
  },
  uncapped: {
    preset: "uncapped",
    buttonKey: "uncapped",
    busyLabel: "Uncapped Running...",
    title: "Run the fetcher aggressively: bypass freshness skips, circuit-breaker quarantine, and admin-imposed fetch caps.",
    ariaLabel: "Run fetcher uncapped"
  },
  force_full: {
    preset: "force_full",
    buttonKey: "force",
    busyLabel: "Force Running...",
    title: "Run full fetch while ignoring circuit breaker quarantine for temporarily blocked sources.",
    ariaLabel: "Run fetcher ignoring circuit breaker"
  },
  retry_failed: {
    preset: "retry_failed",
    buttonKey: "retry",
    busyLabel: "Retry Running...",
    title: "Run fetcher only for sources that failed in the latest report, bypassing circuit breaker.",
    ariaLabel: "Retry failed sources only",
    requestedLog: "Retry failed sources requested."
  }
};

export function createAdminFetcherController({
  state,
  refs,
  getBridge,
  postBridge,
  fetchJobsFetchReportJson,
  writeJobsAutoRefreshSignal,
  showToast,
  getErrorMessage,
  logAdminError,
  setBusyFlag,
  getSourceStatusSetter,
  loadOpsHealthData,
  activeProgressPollIntervalMs = 500,
  jobsAutoRefreshSignalKey,
  jobsFetcherCommand,
  jobsFetcherTaskLabel,
  syncSourceTablesAfterTaskCompletion,
  createLogEvent,
  appendLogRow
}) {
  function hasLiveFetcherSummaryState() {
    const liveState = state.fetcherLiveProgressState;
    if (!liveState || typeof liveState !== "object") return false;
    if (String(liveState.summarySignature || "").trim()) return true;
    if (String(liveState.workItemSignature || "").trim()) return true;
    if (liveState.recentEventSignatures instanceof Set && liveState.recentEventSignatures.size > 0) {
      return true;
    }
    return false;
  }

  function hasVisibleFetcherProgressLabel() {
    return Boolean(String(refs.adminFetcherProgressLabelEl?.textContent || "").trim());
  }

  function formatDurationCompact(ms) {
    const value = Math.max(0, Number(ms) || 0);
    if (value < 1000) return `${value}ms`;
    if (value < 60_000) return `${Math.round(value / 1000)}s`;
    const minutes = Math.floor(value / 60_000);
    const seconds = Math.round((value % 60_000) / 1000);
    return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
  }

  function setFetcherProgress(view) {
    if (!refs.adminFetcherProgressEl || !refs.adminFetcherProgressBarEl || !refs.adminFetcherProgressLabelEl) {
      return;
    }

    applyAdminTaskProgress(
      refs.adminFetcherProgressEl,
      refs.adminFetcherProgressBarEl,
      refs.adminFetcherProgressLabelEl,
      view
    );
  }

  function getFetcherTaskProgress(report, { running = false } = {}) {
    return deriveFetcherTaskProgress(report, { running }) || {
      active: Boolean(running),
      phaseKey: "",
      phaseLabel: "",
      mode: "indeterminate",
      ratio: 0,
      counts: {}
    };
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

  function updateFetcherProgressFromReport(report, { running = false } = {}) {
    setFetcherProgress(deriveFetcherProgressModel(report, { running }));
  }

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

  async function loadFetcherLivePayload() {
    return loadTaskLivePayload({
      getBridge,
      taskType: "fetch"
    });
  }

  function getFetcherPresetMeta(preset) {
    const key = String(preset || "default").trim().toLowerCase();
    return FETCHER_PRESET_META[key] || FETCHER_PRESET_META.default;
  }

  function getFetcherPresetButtons() {
    return [
      { preset: "default", el: refs.adminRunFetcherBtnEl },
      { preset: "incremental", el: refs.adminRunFetcherIncrementalBtnEl },
      { preset: "uncapped", el: refs.adminRunFetcherUncappedBtnEl },
      { preset: "force_full", el: refs.adminRunFetcherForceBtnEl },
      { preset: "retry_failed", el: refs.adminRetryFailedBtnEl }
    ];
  }

  function applyFetcherPresetMetadata() {
    getFetcherPresetButtons().forEach(item => {
      const btn = item?.el;
      if (!btn) return;
      const meta = getFetcherPresetMeta(item.preset);
      btn.dataset.fetcherPreset = meta.preset;
      if (meta.title) btn.title = meta.title;
      if (meta.ariaLabel) btn.setAttribute("aria-label", meta.ariaLabel);
    });
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

  function setOptimisticFetchRun(runMeta) {
    setOptimisticRun(state, "fetchOptimisticRun", runMeta);
  }

  function clearOptimisticFetchRun() {
    clearOptimisticRun(state, "fetchOptimisticRun");
  }

  function attachToActiveFetchRun(runMeta = null, options = {}) {
    attachToActiveRun({
      isWatching: () => state.adminBusyState.fetcherWatch,
      setOptimisticRun: setOptimisticFetchRun,
      startWatch: () => startFetcherCompletionWatch(options)
    }, runMeta);
  }

  function restartFetcherCompletionWatch(runMeta = null, options = {}) {
    restartCompletionWatch(stopFetcherCompletionWatch, nextRunMeta => attachToActiveFetchRun(nextRunMeta, options), runMeta);
  }

  async function loadFetcherLogChunk(options = {}) {
    return loadLiveTaskLogChunk({
      getBridge,
      path: "/fetcher/log",
      state,
      offsetKey: "fetcherLogRemoteOffset",
      reset: Boolean(options?.reset),
      onText: appendFetcherServerLogText
    });
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

  function _formatFetcherRuntimeOptions(report) {
    const runtime = report?.runtime || {};
    const maxWorkers = Number(runtime.maxWorkers || 0);
    const maxPerDomain = Number(runtime.maxPerDomain || 0);
    const sourceTtlMinutes = Number(runtime.sourceTtlMinutes || 0);
    const circuitBreakerFailures = Number(runtime.circuitBreakerFailures || 0);
    const circuitBreakerCooldownMinutes = Number(runtime.circuitBreakerCooldownMinutes || 0);
    const selectedSourceCount = Number(runtime.selectedSourceCount || 0);
    const seedFromExistingOutput = Boolean(runtime.seedFromExistingOutput);
    const ignoreCircuitBreaker = Boolean(runtime.ignoreCircuitBreaker);
    if (
      maxWorkers <= 0
      && maxPerDomain <= 0
      && sourceTtlMinutes <= 0
      && circuitBreakerFailures <= 0
      && circuitBreakerCooldownMinutes <= 0
      && selectedSourceCount <= 0
      && !seedFromExistingOutput
      && !ignoreCircuitBreaker
    ) {
      return "";
    }
    return [
      `workers ${maxWorkers || "n/a"}`,
      `per-domain ${maxPerDomain || "n/a"}`,
      `ttl ${sourceTtlMinutes || 0}m`,
      `circuit ${circuitBreakerFailures || 0}/${circuitBreakerCooldownMinutes || 0}m`,
      `selected ${selectedSourceCount || 0}`,
      `seed ${seedFromExistingOutput ? "on" : "off"}`,
      `ignore-cb ${ignoreCircuitBreaker ? "on" : "off"}`
    ].join(", ");
  }

  function _formatLifecycleSummary(report) {
    const summary = report?.summary || {};
    const active = Number(summary.lifecycleActiveCount || 0);
    const likelyRemoved = Number(summary.lifecycleLikelyRemovedCount || 0);
    const archived = Number(summary.lifecycleArchivedCount || 0);
    const tracked = Number(summary.lifecycleTrackedCount || 0);
    if (active <= 0 && likelyRemoved <= 0 && archived <= 0 && tracked <= 0) {
      return "";
    }
    return `Lifecycle: active ${active.toLocaleString()}, likely removed ${likelyRemoved.toLocaleString()}, archived ${archived.toLocaleString()}, tracked ${tracked.toLocaleString()}`;
  }

  function formatStageTopSummary(report) {
    const timing = report?.runtime?.timingSummary || {};
    const stageTop = Array.isArray(timing?.stageTop) ? timing.stageTop : [];
    if (!stageTop.length) return "";
    return stageTop
      .slice(0, 3)
      .map(item => `${String(item?.stage || "unknown")} ${formatDurationCompact(item?.durationMs)}`)
      .join(" | ");
  }

  function selectSlowSources(report) {
    const runtimeSlowest = Array.isArray(report?.runtime?.slowestSources) ? report.runtime.slowestSources : [];
    if (runtimeSlowest.length) return runtimeSlowest;
    const sources = Array.isArray(report?.sources) ? report.sources : [];
    return sources
      .filter(source => Number(source?.durationMs || 0) >= 20_000)
      .sort((a, b) => Number(b?.durationMs || 0) - Number(a?.durationMs || 0))
      .slice(0, 5);
  }

  async function fetchJobsFetchReportJsonWithRetry(options = {}, maxAttempts = 3, delayMs = 850) {
    let attempt = 0;
    while (attempt < Math.max(1, Number(maxAttempts) || 1)) {
      attempt += 1;
      const report = await fetchJobsFetchReportJson(options);
      if (report) return report;
      if (attempt < maxAttempts) {
        await new Promise(resolve => {
          window.setTimeout(resolve, Math.max(100, Number(delayMs) || 850));
        });
      }
    }
    return null;
  }

  async function loadLatestFetcherReport(options = {}) {
    const silent = Boolean(options.silent);
    const hydrateActiveProgress = Boolean(options.hydrateActiveProgress);
    if (state.adminBusyState.fetcherReportLoad) {
      if (!silent) showToast("Fetch report loading already in progress.", "info");
      return null;
    }
    setBusyFlag("fetcherReportLoad", true);
    try {
      if (!silent) appendFetcherLog("Loading latest jobs fetch report...");
      const report = await fetchJobsFetchReportJsonWithRetry();
      if (!report) {
        appendFetcherLog("Fetch report is not available yet. It may still be generating.", "warn");
        updateFetcherProgressFromReport(null, { running: Boolean(state.adminBusyState.fetcherWatch || state.adminBusyState.liveFetchRunning) });
        if (!silent) showToast("Fetch report not available yet. Retry in a few seconds.", "info");
        return null;
      }
      const liveWatchActive = Boolean(state.adminBusyState.fetcherWatch || state.adminBusyState.liveFetchRunning);
      const reportFinished = Boolean(String(report?.finishedAt || "").trim());
      state.latestFetcherReportCache = report;
      if (!liveWatchActive || reportFinished) {
        updateFetcherProgressFromReport(report, { running: false });
      } else if (hydrateActiveProgress || (!hasLiveFetcherSummaryState() && !hasVisibleFetcherProgressLabel())) {
        updateFetcherProgressFromReport(report, { running: true });
      }

      if (liveWatchActive && !reportFinished) {
        return report;
      }

      const summary = report?.summary || {};
      const progress = getFetcherTaskProgress(report, { running: false });
      const counts = progress.counts && typeof progress.counts === "object" ? progress.counts : {};
      const resolvedSources = Math.max(0, Number(counts.resolvedSources ?? (Number(summary.successfulSources || 0) + Number(summary.failedSources || 0) + Number(summary.excludedSources || 0))));
      const totalSources = progress.mode === "determinate" ? Math.max(0, Number(counts.sourceCount || 0)) : 0;
      const outputCount = Math.max(0, Number(counts.outputCount ?? summary.outputCount ?? 0));
      const failedSourceCount = Math.max(0, Number(counts.failedSources ?? summary.failedSources ?? 0));
      const excludedSourceCount = Math.max(0, Number(counts.excludedSources ?? summary.excludedSources ?? 0));
      appendFetcherLog(
        `Fetcher summary: ${totalSources > 0 ? `${resolvedSources}/${totalSources} sources resolved` : `${resolvedSources} sources resolved`}, output ${outputCount.toLocaleString()}, failed ${failedSourceCount}, excluded ${excludedSourceCount}.`,
        failedSourceCount > 0 ? "warn" : "success"
      );

      const sources = Array.isArray(report?.sources) ? report.sources : [];
      if (!sources.length) {
        appendFetcherLog("No source entries found in report.", "warn");
        return;
      }

      const failedSources = sources
        .filter(source => String(source?.status || "").toLowerCase() === "error")
        .slice(0, 3)
        .map(source => `${String(source?.name || "unknown")}${source?.error ? ` [${String(source.error)}]` : ""}`);
      if (failedSources.length) {
        appendFetcherLog(`Failures: ${failedSources.join(" | ")}`, "warn");
      }
      const failureSummary = deriveFetcherFailureSummary(report);
      if (Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length) {
        const bucketLine = failureSummary.buckets
          .map(bucket => `${String(bucket.key || "").replaceAll("_", " ")} ${Number(bucket.count || 0)}`)
          .join(" | ");
        appendFetcherLog(`Failure buckets: ${bucketLine}`, "warn");
      }
      const slowSources = selectSlowSources(report)
        .slice(0, 2)
        .map(source => `${String(source?.name || "unknown")} ${formatDurationCompact(source?.durationMs)}`);
      if (slowSources.length) {
        appendFetcherLog(`Slowest sources: ${slowSources.join(" | ")}`, "muted");
      }
      const slowStages = formatStageTopSummary(report);
      if (slowStages) {
        appendFetcherLog(`Slowest stages: ${slowStages}`, "muted");
      }

      loadOpsHealthData().catch(() => {});
      return report;
    } finally {
      setBusyFlag("fetcherReportLoad", false);
    }
  }

  async function copyLatestFailureSummary() {
    const report = state.latestFetcherReportCache || await fetchJobsFetchReportJson();
    if (!report) {
      showToast("No fetch report available to copy.", "error");
      return;
    }
    state.latestFetcherReportCache = report;
    const failureSummary = deriveFetcherFailureSummary(report);
    const failures = (Array.isArray(report?.sources) ? report.sources : []).filter(
      row => String(row?.status || "").toLowerCase() === "error"
    );
    if (!failures.length && !(Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length)) {
      showToast("No failed sources in latest report.", "info");
      return;
    }
    const summaryLines = [];
    summaryLines.push(`Top-level failed sources: ${Number(failureSummary?.topLevelFailedSources || failures.length)}`);
    summaryLines.push(`Grouped detail failures: ${Number(failureSummary?.detailFailureCount || 0)}`);
    if (Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length) {
      summaryLines.push("");
      summaryLines.push("Failure buckets:");
      failureSummary.buckets.forEach(bucket => {
        const examples = Array.isArray(bucket?.examples) && bucket.examples.length ? ` (${bucket.examples.join(" | ")})` : "";
        summaryLines.push(`- ${String(bucket?.key || "uncategorized")}: ${Number(bucket?.count || 0)}${examples}`);
      });
    }
    if (failures.length) {
      summaryLines.push("");
      summaryLines.push("Top-level failures:");
      failures.forEach(row => {
        summaryLines.push(`${row?.name || "unknown"}: ${row?.error || "error"}`);
      });
    }
    const summary = summaryLines.join("\n");
    if (navigator?.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(summary);
        showToast("Failure summary copied.", "success");
        return;
      } catch {
        // Fallback to log append below.
      }
    }
    appendFetcherLog(`Failure summary:\n${summary}`, "warn");
    showToast("Could not access clipboard. Summary logged.", "warn");
  }

  function emitJobsAutoRefreshSignal(report) {
    const signal = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      finishedAt: String(report?.finishedAt || ""),
      source: "admin_fetcher"
    };
    try {
      writeJobsAutoRefreshSignal(jobsAutoRefreshSignalKey, signal);
      appendFetcherLog("Signaled jobs page to auto-refresh from unified feed.", "success");
    } catch {
      appendFetcherLog("Could not write auto-refresh signal to localStorage.", "warn");
    }
  }

  function appendFetcherProgressFromReport(report, nowMs) {
    const liveState = state.fetcherLiveProgressState;
    if (!liveState) return;
    updateFetcherProgressFromReport(report, { running: true });
    const summary = report?.summary || {};
    const progress = getFetcherTaskProgress(report, { running: true });
    const counts = progress.counts && typeof progress.counts === "object" ? progress.counts : {};
    const outputCount = Math.max(0, Number(counts.outputCount ?? summary.outputCount ?? 0));
    const selectedSourceCount = progress.mode === "determinate" ? Math.max(0, Number(counts.sourceCount || 0)) : 0;
    const runningSources = Math.max(0, Number(counts.runningTasks ?? counts.running ?? summary.running ?? 0));
    const queuedSources = Math.max(0, Number(counts.queuedTasks ?? counts.queued ?? summary.queued ?? 0));
    const failedSources = Math.max(0, Number(counts.failedSources ?? summary.failedSources ?? 0));
    const excludedSources = Math.max(0, Number(counts.excludedSources ?? summary.excludedSources ?? 0));
    const resolvedSources = Math.max(
      0,
      Number(
        counts.resolvedSources
        ?? (Number(summary.successfulSources || 0) + Number(summary.failedSources || 0) + Number(summary.excludedSources || 0))
      )
    );
    const summarySignature = [
      outputCount,
      selectedSourceCount,
      resolvedSources,
      runningSources,
      queuedSources,
      failedSources,
      excludedSources
    ].join("|");
    appendLiveTaskActivity({
      payload: report,
      liveState,
      nowMs,
      appendEvent: event => appendFetcherLogEvent(event, "muted"),
      scope: "fetch",
      summarySignature,
      workItemSignature: buildTaskWorkItemActivitySignature(report),
      onSummaryChange: () => {
        appendFetcherLog(
          `Fetcher: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} sources resolved` : `${resolvedSources} sources resolved`}, running ${runningSources}, queued ${queuedSources}, output ${outputCount.toLocaleString()}, failed ${failedSources}, excluded ${excludedSources}.`,
          failedSources > 0 ? "warn" : "info"
        );
      },
      onHeartbeat: () => {
        appendFetcherLog(
          `Fetcher active: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} sources resolved` : `${resolvedSources} sources resolved`}, running ${runningSources}, queued ${queuedSources}, output ${outputCount.toLocaleString()}.`,
          "muted"
        );
      }
    });
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
        lastActivityAtMs: Date.now()
      }),
      setProgress: () => updateFetcherProgressFromReport(null, { running: true }),
      onStart: announceStart ? () => appendFetcherLog("Fetcher started. Watching live progress...", "info") : null,
      loadInitialLogChunk: () => loadFetcherLogChunk({ reset: true }).catch(() => {}),
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
      onError: err => {
        logAdminError("Fetcher completion poll failed", err);
        scheduleFetcherCompletionPoll(activeProgressPollIntervalMs);
      }
    });
  }

  async function pollFetcherCompletion() {
    const now = Date.now();
    const livePayload = await loadFetcherLivePayload().catch(() => null);
    const normalizedLivePayload = pickTaskLivePayload(livePayload);
    const liveStartedMs = parseReportTimestampMs(normalizedLivePayload?.startedAt);
    const liveFinishedMs = parseReportTimestampMs(normalizedLivePayload?.finishedAt);

    if (normalizedLivePayload && liveStartedMs >= (state.fetcherLaunchAtMs - 1000)) {
      if (liveFinishedMs <= 0) {
        appendFetcherProgressFromReport(normalizedLivePayload, now);
        updateFetcherProgressFromReport(normalizedLivePayload, { running: true });
      }
    }

    let terminalPayload = normalizedLivePayload;
    if (!terminalPayload || liveFinishedMs > 0) {
      terminalPayload = await fetchJobsFetchReportJson({ live: true }).catch(() => null) || terminalPayload;
      if (!normalizedLivePayload && terminalPayload && !String(terminalPayload?.finishedAt || "").trim()) {
        updateFetcherProgressFromReport(terminalPayload, { running: true });
      }
    }

    const finishedMs = parseReportTimestampMs(terminalPayload?.finishedAt);
    if (finishedMs >= (state.fetcherLaunchAtMs - 1000)) {
      const finalReport = await fetchJobsFetchReportJson().catch(() => null);
      const completedPayload = finalReport || terminalPayload;
      state.latestFetcherReportCache = completedPayload || state.latestFetcherReportCache;
      const summary = completedPayload?.summary || {};
      updateFetcherProgressFromReport(completedPayload, { running: false });
      appendFetcherLog(
        `Fetcher completed: output ${Number(summary.outputCount || 0).toLocaleString()}, failed ${Number(summary.failedSources || 0)}, excluded ${Number(summary.excludedSources || 0)}.`,
        Number(summary.failedSources || 0) > 0 ? "warn" : "success"
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

    scheduleFetcherCompletionPoll(activeProgressPollIntervalMs);
  }

  function launchVsCodeUri(uri) {
    const launchLink = document.createElement("a");
    launchLink.href = uri;
    launchLink.style.display = "none";
    document.body.appendChild(launchLink);
    launchLink.click();
    launchLink.remove();
  }

  async function triggerJobsFetcherTask(runOptions = {}) {
    if (state.adminBusyState.fetcherRun || state.adminBusyState.fetcherWatch || state.adminBusyState.fetcherReportLoad || state.adminBusyState.liveFetchRunning) {
      showToast("Fetcher task is already running.", "info");
      return;
    }
    setBusyFlag("fetcherRun", true);
    const preset = String(runOptions?.preset || "default");
    const presetMeta = getFetcherPresetMeta(preset);
    const payload = { ...runOptions };
    let usedFallback = false;
    try {
      const bridgeResponse = await postBridge("/tasks/run-fetcher", payload, {
        allowStatuses: [409],
        returnMeta: true
      });
      const bridgeStatus = Number(bridgeResponse?.status || 200);
      const bridge = bridgeResponse?.data || bridgeResponse || null;
      if (bridge && bridge.started) {
        setOptimisticFetchRun(bridge);
        const presetLabel = String(bridge?.preset || presetMeta.preset || "default");
        const argsLabel = Array.isArray(bridge?.args) ? bridge.args.join(" ") : "";
        appendFetcherLog(
          `Triggered fetcher via local admin bridge (preset ${presetLabel})${argsLabel ? `, args: ${argsLabel}` : ""}.`
        );
        getSourceStatusSetter()("Triggered local fetcher task via admin bridge.");
        showToast("Fetcher started via admin bridge.", "success");
        loadOpsHealthData().catch(() => {});
        loadLatestFetcherReport({ silent: true }).catch(() => {});
        startFetcherCompletionWatch();
        return;
      }
      if (bridgeStatus === 409 && bridge?.alreadyRunning) {
        attachToActiveFetchRun(bridge, { announceStart: false });
        appendFetcherLog("Fetcher already running; attached to the active bridge-managed run.", "info");
        getSourceStatusSetter()("Attached to the active fetcher task via admin bridge.");
        showToast("Fetcher already running. Attached to active run.", "info");
        loadOpsHealthData().catch(() => {});
        loadLatestFetcherReport({ silent: true }).catch(() => {});
        return;
      }
    } catch {
      if (!usedFallback) {
        appendFetcherLog(FETCHER_FALLBACK_MESSAGES.bridgeUnavailable, "warn");
        usedFallback = true;
      }
    } finally {
      setBusyFlag("fetcherRun", false);
    }
    if (presetMeta.preset !== "default") {
      appendFetcherLog(FETCHER_FALLBACK_MESSAGES.presetNeedsBridge, "error");
      showToast("Fetcher preset requires admin bridge.", "error");
      return;
    }
    appendFetcherLog("Preparing jobs fetcher task launch from admin panel.");
    showToast("Attempting fetcher launch...", "info");
    setOptimisticFetchRun({
      runId: `fallback-fetch:${Date.now()}`,
      startedAt: new Date().toISOString()
    });
    const taskArgQuoted = encodeURIComponent(JSON.stringify(jobsFetcherTaskLabel));
    const taskArgRaw = encodeURIComponent(jobsFetcherTaskLabel);
    const taskUris = [
      `vscode://command/workbench.action.tasks.runTask?${taskArgRaw}`,
      `vscode://command/workbench.action.tasks.runTask?${taskArgQuoted}`
    ];

    try {
      launchVsCodeUri(taskUris[0]);
      appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchPrimary(jobsFetcherTaskLabel));
      getSourceStatusSetter()("Triggered VS Code task to run jobs fetcher. Check VS Code terminal for progress.");
      window.setTimeout(() => {
        launchVsCodeUri(taskUris[1]);
        appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchSecondary);
      }, 180);
      appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualHint, "warn");
      showToast("Fetcher task launch requested. Check VS Code.", "info");
    } catch (err) {
      logAdminError("Could not trigger VS Code task", err);
      appendFetcherLog(`Could not trigger VS Code task automatically: ${getErrorMessage(err)}`, "error");
      showToast(`Could not trigger VS Code task. Run ${jobsFetcherCommand}`, "error");
      getSourceStatusSetter()("Could not trigger jobs fetcher task automatically.");
      return;
    }

    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(jobsFetcherCommand)
        .then(() => {
          appendFetcherLog(FETCHER_FALLBACK_MESSAGES.copiedManualCommand(jobsFetcherCommand));
        })
        .catch(() => {
          appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(jobsFetcherCommand), "warn");
        });
    } else {
      appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(jobsFetcherCommand), "warn");
    }

    loadLatestFetcherReport({ silent: true }).catch(fetchErr => {
      logAdminError("Could not load fetch report after task trigger", fetchErr);
    });
    startFetcherCompletionWatch();
  }

  return {
    FETCHER_PRESET_META,
    FETCHER_FALLBACK_MESSAGES,
    getFetcherPresetMeta,
    applyFetcherPresetMetadata,
    setFetcherLogPlaceholder,
    clearOptimisticFetchRun,
    attachToActiveFetchRun,
    restartFetcherCompletionWatch,
    getRestorableFetcherRunMeta,
    appendFetcherLog,
    loadFetcherLivePayload,
    loadLatestFetcherReport,
    copyLatestFailureSummary,
    triggerJobsFetcherTask,
    startFetcherCompletionWatch,
    stopFetcherCompletionWatch,
    loadFetcherLogChunk,
    appendFetcherServerLogText
  };
}
