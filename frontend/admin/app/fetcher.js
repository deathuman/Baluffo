import { deriveFetcherFailureSummary, deriveFetcherProgressModel, deriveFetcherTaskProgress } from "../domain.js";
import { applyAdminTaskProgress } from "./progress-ui.js";

export const FETCHER_FALLBACK_MESSAGES = {
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
  _startOpsHealthPolling,
  fetchReportPollIntervalMs,
  fetchReportPollTimeoutMs,
  jobsAutoRefreshSignalKey,
  jobsFetcherCommand,
  jobsFetcherTaskLabel,
  _jobsFetchReportUrl,
  createLogEvent,
  appendLogRow
}) {
  function formatDurationCompact(ms) {
    const value = Math.max(0, Number(ms) || 0);
    if (value < 1000) return `${value}ms`;
    if (value < 60_000) return `${Math.round(value / 1000)}s`;
    const minutes = Math.floor(value / 60_000);
    const seconds = Math.round((value % 60_000) / 1000);
    return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
  }

  function setFetcherProgress(view) {
    // Validate DOM elements before attempting to update progress
    if (!refs.adminFetcherProgressEl) {
      console.warn("[Admin Fetcher] Progress root element not available");
      return;
    }
    if (!refs.adminFetcherProgressBarEl) {
      console.warn("[Admin Fetcher] Progress bar element not available");
      return;
    }
    if (!refs.adminFetcherProgressLabelEl) {
      console.warn("[Admin Fetcher] Progress label element not available");
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

  function updateFetcherProgressFromReport(report, { running = false } = {}) {
    setFetcherProgress(deriveFetcherProgressModel(report, { running }));
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
      if (state.fetcherLiveProgressState) {
        state.fetcherLiveProgressState.lastActivityAtMs = Date.now();
      }
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
    if (!refs.adminFetcherLogEl) return;
    refs.adminFetcherLogEl.innerHTML = "";
    state.fetcherLogRemoteOffset = 0;
    setFetcherProgress({ active: false });
    appendFetcherLog(message, "muted");
  }

  function setOptimisticFetchRun(runMeta) {
    const startedAt = String(runMeta?.startedAt || "").trim();
    if (!startedAt) {
      state.fetchOptimisticRun = null;
      return;
    }
    state.fetchOptimisticRun = {
      runId: String(runMeta?.runId || ""),
      startedAt
    };
  }

  function clearOptimisticFetchRun() {
    state.fetchOptimisticRun = null;
  }

  function attachToActiveFetchRun(runMeta = null) {
    if (state.adminBusyState.fetcherWatch) return;
    if (runMeta && typeof runMeta === "object" && !Array.isArray(runMeta)) {
      setOptimisticFetchRun(runMeta);
    }
    startFetcherCompletionWatch();
  }

  async function loadFetcherLogChunk(options = {}) {
    const reset = Boolean(options?.reset);
    const offset = reset ? 0 : Math.max(0, Number(state.fetcherLogRemoteOffset) || 0);
    const payload = await getBridge(`/fetcher/log?offset=${offset}`);
    if (reset) state.fetcherLogRemoteOffset = 0;
    appendFetcherServerLogText(String(payload?.text || ""));
    state.fetcherLogRemoteOffset = Math.max(0, Number(payload?.nextOffset) || 0);
    return payload || null;
  }

  function stopFetcherLogPolling() {
    if (!state.fetcherLogPollTimer) return;
    clearTimeout(state.fetcherLogPollTimer);
    state.fetcherLogPollTimer = null;
  }

  function scheduleFetcherLogPoll(delayMs) {
    stopFetcherLogPolling();
    state.fetcherLogPollTimer = setTimeout(() => {
      loadFetcherLogChunk().catch(() => null).finally(() => {
        if (state.adminBusyState.fetcherWatch) {
          scheduleFetcherLogPoll(delayMs);
        }
      });
    }, Math.max(250, Number(delayMs) || 900));
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

  async function fetchJobsFetchReportJsonWithRetry(maxAttempts = 3, delayMs = 850) {
    let attempt = 0;
    while (attempt < Math.max(1, Number(maxAttempts) || 1)) {
      attempt += 1;
      const report = await fetchJobsFetchReportJson();
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
    if (state.adminBusyState.fetcherReportLoad) {
      if (!silent) showToast("Fetch report loading already in progress.", "info");
      return;
    }
    setBusyFlag("fetcherReportLoad", true);
    try {
      if (!silent) appendFetcherLog("Loading latest jobs fetch report...");
      const report = await fetchJobsFetchReportJsonWithRetry();
      if (!report) {
        appendFetcherLog("Fetch report is not available yet. It may still be generating.", "warn");
        updateFetcherProgressFromReport(null, { running: Boolean(state.adminBusyState.fetcherWatch || state.adminBusyState.liveFetchRunning) });
        if (!silent) showToast("Fetch report not available yet. Retry in a few seconds.", "info");
        return;
      }
      state.latestFetcherReportCache = report;
      updateFetcherProgressFromReport(report, { running: false });

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

  function parseReportTimestampMs(value) {
    if (!value) return 0;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
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
    const failedSources = Math.max(0, Number(counts.failedSources ?? summary.failedSources ?? 0));
    const excludedSources = Math.max(0, Number(counts.excludedSources ?? summary.excludedSources ?? 0));
    const resolvedSources = Math.max(
      0,
      Number(
        counts.resolvedSources
        ?? (Number(summary.successfulSources || 0) + Number(summary.failedSources || 0) + Number(summary.excludedSources || 0))
      )
    );
    let sawActivity = false;

    const summarySignature = [
      outputCount,
      selectedSourceCount,
      resolvedSources,
      failedSources,
      excludedSources
    ].join("|");
    if (summarySignature !== liveState.summarySignature) {
      liveState.summarySignature = summarySignature;
      sawActivity = true;
      appendFetcherLog(
        `Fetcher: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} sources resolved` : `${resolvedSources} sources resolved`}, output ${outputCount.toLocaleString()}, failed ${failedSources}, excluded ${excludedSources}.`,
        failedSources > 0 ? "warn" : "info"
      );
    }

    const sources = Array.isArray(report?.sources) ? report.sources : [];
    const notableEvents = [];
    sources.forEach(source => {
      const name = String(source?.name || "unknown");
      const status = String(source?.status || "unknown").toLowerCase();
      const signature = [
        status,
        Number(source?.keptCount || 0),
        Number(source?.durationMs || 0),
        String(source?.error || "")
      ].join("|");
      const previousSignature = liveState.sourceSignatures.get(name);
      if (previousSignature === signature) return;
      liveState.sourceSignatures.set(name, signature);
      if (status === "error") {
        notableEvents.push({
          level: "error",
          message: `Failure: ${name}${source?.error ? ` [${String(source.error)}]` : ""}`
        });
        return;
      }
      if (status === "excluded") {
        notableEvents.push({
          level: "warn",
          message: `Excluded: ${name}${source?.error ? ` [${String(source.error)}]` : ""}`
        });
        return;
      }
      if (status === "running" && Number(source?.durationMs || 0) >= 20_000 && !liveState.reportedSlowSources.has(name)) {
        liveState.reportedSlowSources.add(name);
        notableEvents.push({
          level: "muted",
          message: `Slow source: ${name} still running after ${formatDurationCompact(source?.durationMs)}`
        });
      }
    });
    notableEvents.slice(0, 2).forEach(item => {
      appendFetcherLog(item.message, item.level);
    });
    if (notableEvents.length) {
      sawActivity = true;
    }

    const slowSourceLine = selectSlowSources(report)
      .slice(0, 2)
      .map(source => `${String(source?.name || "unknown")} ${formatDurationCompact(source?.durationMs)}`)
      .join(" | ");
    if (slowSourceLine && slowSourceLine !== liveState.slowSourceSummarySignature) {
      liveState.slowSourceSummarySignature = slowSourceLine;
      sawActivity = true;
      appendFetcherLog(`Slowest sources: ${slowSourceLine}`, "muted");
    }

    const slowStageLine = formatStageTopSummary(report);
    if (slowStageLine && slowStageLine !== liveState.slowStageSummarySignature) {
      liveState.slowStageSummarySignature = slowStageLine;
      sawActivity = true;
      appendFetcherLog(`Slowest stages: ${slowStageLine}`, "muted");
    }

    if (sawActivity) {
      liveState.lastActivityAtMs = nowMs;
    }

    const idleMs = nowMs - Number(liveState.lastActivityAtMs || 0);
    if (idleMs >= 60000 && (nowMs - Number(liveState.lastHeartbeatAtMs || 0)) >= 60000) {
      liveState.lastHeartbeatAtMs = nowMs;
      appendFetcherLog(
        `Fetcher active: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} resolved` : `${resolvedSources} resolved`}, output ${outputCount.toLocaleString()}.`,
        "muted"
      );
    }
  }

  function startFetcherCompletionWatch() {
    stopFetcherCompletionWatch();
    setBusyFlag("fetcherWatch", true);
    state.fetcherLaunchAtMs = Date.now();
    const optimisticStartedAtMs = parseReportTimestampMs(state.fetchOptimisticRun?.startedAt);
    if (optimisticStartedAtMs > 0) {
      state.fetcherLaunchAtMs = optimisticStartedAtMs;
    }
    state.fetcherCompletionPollDeadline = state.fetcherLaunchAtMs + fetchReportPollTimeoutMs;
    state.fetcherLogRemoteOffset = 0;
    state.fetcherLiveProgressState = {
      summarySignature: "",
      sourceSignatures: new Map(),
      reportedSlowSources: new Set(),
      serverLogSignatures: new Set(),
      slowSourceSummarySignature: "",
      slowStageSummarySignature: "",
      lastHeartbeatAtMs: 0,
      lastActivityAtMs: Date.now()
    };
    updateFetcherProgressFromReport(null, { running: true });
    appendFetcherLog("Fetcher started. Watching live progress...", "info");
    loadFetcherLogChunk({ reset: true }).catch(() => {});
    scheduleFetcherLogPoll(700);
    scheduleFetcherCompletionPoll(900);
  }

  function stopFetcherCompletionWatch() {
    if (state.fetcherCompletionPollTimer) {
      clearTimeout(state.fetcherCompletionPollTimer);
      state.fetcherCompletionPollTimer = null;
    }
    stopFetcherLogPolling();
    state.fetcherLiveProgressState = null;
    setFetcherProgress({ active: false });
    setBusyFlag("fetcherWatch", false);
  }

  function scheduleFetcherCompletionPoll(delayMs) {
    state.fetcherCompletionPollTimer = setTimeout(() => {
      pollFetcherCompletion().catch(err => {
        logAdminError("Fetcher completion poll failed", err);
        scheduleFetcherCompletionPoll(fetchReportPollIntervalMs);
      });
    }, delayMs);
  }

  async function pollFetcherCompletion() {
    const now = Date.now();
    if (now >= state.fetcherCompletionPollDeadline) {
      appendFetcherLog("Could not confirm completion from report within timeout window.", "warn");
      setBusyFlag("liveFetchRunning", false);
      clearOptimisticFetchRun();
      stopFetcherCompletionWatch();
      return;
    }

    const report = await fetchJobsFetchReportJson();

    // Always update progress during polling for real-time updates
    if (report) {
      const startedMs = parseReportTimestampMs(report?.startedAt);
      if (startedMs >= (state.fetcherLaunchAtMs - 1000)) {
        appendFetcherProgressFromReport(report, now);
      }

      // Update progress even if not started yet, for better UX
      updateFetcherProgressFromReport(report, { running: true });
    }

    const finishedMs = parseReportTimestampMs(report?.finishedAt);
    if (finishedMs >= (state.fetcherLaunchAtMs - 1000)) {
      const summary = report?.summary || {};
      updateFetcherProgressFromReport(report, { running: true });
      appendFetcherLog(
        `Fetcher completed: output ${Number(summary.outputCount || 0).toLocaleString()}, failed ${Number(summary.failedSources || 0)}, excluded ${Number(summary.excludedSources || 0)}.`,
        Number(summary.failedSources || 0) > 0 ? "warn" : "success"
      );
      const slowSources = selectSlowSources(report)
        .slice(0, 3)
        .map(source => `${String(source?.name || "unknown")} ${formatDurationCompact(source?.durationMs)}`);
      if (slowSources.length) {
        appendFetcherLog(`Slowest sources: ${slowSources.join(" | ")}`, "muted");
      }
      const slowStages = formatStageTopSummary(report);
      if (slowStages) {
        appendFetcherLog(`Slowest stages: ${slowStages}`, "muted");
      }
      emitJobsAutoRefreshSignal(report);
      setBusyFlag("liveFetchRunning", false);
      clearOptimisticFetchRun();
      stopFetcherCompletionWatch();
      return;
    }

    scheduleFetcherCompletionPoll(fetchReportPollIntervalMs);
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
      const bridge = await postBridge("/tasks/run-fetcher", payload);
      if (bridge && bridge.started) {
        setOptimisticFetchRun(bridge);
        setBusyFlag("liveFetchRunning", true);
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
    setBusyFlag("liveFetchRunning", true);
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
    appendFetcherLog,
    loadLatestFetcherReport,
    copyLatestFailureSummary,
    triggerJobsFetcherTask,
    startFetcherCompletionWatch,
    stopFetcherCompletionWatch,
    loadFetcherLogChunk,
    appendFetcherServerLogText
  };
}
