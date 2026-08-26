function cloneFilterState(filters = {}) {
  return {
    ...filters,
    countries: Array.from(filters?.countries || [])
  };
}

function filtersMatchDefault(filters = {}, defaultFilters = {}) {
  const current = cloneFilterState(filters);
  const defaults = cloneFilterState(defaultFilters);
  return JSON.stringify(current) === JSON.stringify(defaults);
}

const BOOTSTRAP_AUTO_START_KEY = "baluffo_jobs_bootstrap_auto_started";
const BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY = "baluffo_jobs_bootstrap_launch_cold_start_handled";
const LOCAL_FEED_MISSING_MESSAGE = "Local jobs feed is missing or unreadable. Retry quick refresh or run Update jobs to rebuild the full feed.";
const JOBS_FULL_FEED_SYNC_DELAY_MS = 1200;
const EMPTY_TITLE_FEED_MESSAGE = "Jobs feed contained no displayable positions. Retry quick refresh or run Update jobs to rebuild the full feed.";
const FIRST_RUN_BOOTSTRAP_STATUS = "Refreshing first-run sheet jobs. This can take several minutes...";
const FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS = "Confirming first-run sheet refresh started...";
const FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE = "Could not confirm first-run sheet refresh started. Retry quick refresh or open Admin.";
const FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS = 90 * 1000;
const FIRST_RUN_BOOTSTRAP_NOTICE = Object.freeze({
  title: "Preparing first-run jobs",
  body: "Baluffo is fetching the starter Google Sheets job feed. The first refresh can take several minutes. You can keep this window open; jobs will appear automatically when the refresh finishes.",
  primaryLabel: "Got it"
});

export function jobsFirstRunBootstrapNumberOverride(windowObject, key, fallback) {
  try {
    const search = String(windowObject?.location?.search || "");
    const params = new URLSearchParams(search);
    if (params.get("desktop") !== "1" || params.get("jobsColdStart") !== "1") {
      return fallback;
    }
    const value = params.get(key);
    const numeric = Number(value);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback;
  } catch {
    return fallback;
  }
}

function reportFinishedTimestamp(report) {
  const finishedAt = String(report?.finishedAt || "").trim();
  if (!finishedAt) return null;
  const timestamp = Date.parse(finishedAt);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function reportSummary(report) {
  return report?.summary && typeof report.summary === "object" ? report.summary : {};
}

function reportStatus(report) {
  const summary = reportSummary(report);
  return String(summary.status || report?.status || "").trim().toLowerCase();
}

function isSuccessfulJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  const status = reportStatus(report);
  if (status === "error" || status === "failed") return false;
  return Number(summary.outputCount || 0) > 0;
}

function isTerminalFailedJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  return ["error", "failed"].includes(reportStatus(report))
    || Boolean(summary.error);
}

function isNonTerminalJobsFetchReport(report) {
  return Boolean(report && typeof report === "object")
    && !isSuccessfulJobsFetchReport(report)
    && !isTerminalFailedJobsFetchReport(report);
}

function coverageScope(report) {
  const runtime = report?.runtime && typeof report.runtime === "object" ? report.runtime : {};
  const summary = reportSummary(report);
  return String(summary.coverageScope || runtime.coverageScope || "").trim();
}

function reportRunId(report) {
  const summary = reportSummary(report);
  return String(report?.runId || summary.runId || "").trim();
}

function isActiveBootstrapReport(report) {
  if (!report || typeof report !== "object") return false;
  if (reportFinishedTimestamp(report)) return false;
  const scope = coverageScope(report).toLowerCase();
  const runId = reportRunId(report);
  return scope === "bootstrap_sheets" || runId.startsWith("jobs_bootstrap_");
}

function timestampMs(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function latestBootstrapProgressTimestamp(payload) {
  if (!payload || typeof payload !== "object") return 0;
  const candidates = [
    payload.heartbeatAt,
    payload.updatedAt,
    payload.taskProgress?.updatedAt,
    payload.runtime?.lifecycle?.heartbeatAt
  ].map(timestampMs);
  for (const workItem of (Array.isArray(payload.workItems) ? payload.workItems : [])) {
    candidates.push(timestampMs(workItem?.heartbeatAt));
    candidates.push(timestampMs(workItem?.progress?.updatedAt));
  }
  return Math.max(0, ...candidates);
}

function isFreshBootstrapProgress(payload, { now = Date.now(), staleMs } = {}) {
  if (!payload || typeof payload !== "object") return false;
  const status = String(payload.status || "").trim().toLowerCase();
  const active = Boolean(
    payload.active
      || payload.taskProgress?.active
      || status === "running"
      || isActiveBootstrapReport(payload)
  );
  if (!active) return false;
  const latestProgressAt = latestBootstrapProgressTimestamp(payload);
  if (!latestProgressAt) return false;
  return now - latestProgressAt <= Math.max(1000, Number(staleMs) || FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS);
}

function bootstrapStartHasRunningEvidence(payload) {
  return Boolean(
    payload
      && typeof payload === "object"
      && (payload.started || payload.alreadyRunning || payload.alreadyCompleted)
  );
}

function isUncertainBootstrapStartError(err) {
  const message = String(err?.message || err || "").toLowerCase();
  return message.includes("timed out")
    || message.includes("bridge unreachable")
    || message.includes("network error")
    || message.includes("failed to fetch");
}

function bootstrapStartUnconfirmedError() {
  const error = new Error(FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE);
  error.bootstrapStartUnconfirmed = true;
  return error;
}

function localStorageFor(windowObject) {
  try {
    return windowObject?.localStorage || null;
  } catch {
    return null;
  }
}

function sessionStorageFor(windowObject) {
  try {
    return windowObject?.sessionStorage || null;
  } catch {
    return null;
  }
}

function bootstrapAutoStartMarker(windowObject) {
  const value = localStorageFor(windowObject)?.getItem(BOOTSTRAP_AUTO_START_KEY);
  if (!value) return { status: "none" };
  if (value === "1") return { status: "legacy" };
  try {
    const parsed = JSON.parse(value);
    const status = String(parsed?.status || "").trim().toLowerCase();
    if (status === "running" || status === "failed") {
      return {
        status,
        runId: String(parsed?.runId || "").trim(),
        error: String(parsed?.error || "").trim()
      };
    }
  } catch {
    // Fall through to legacy handling for older/corrupt markers.
  }
  return { status: "legacy" };
}

function writeBootstrapAutoStartMarker(windowObject, status, details = {}) {
  localStorageFor(windowObject)?.setItem(BOOTSTRAP_AUTO_START_KEY, JSON.stringify({
    status,
    runId: String(details.runId || "").trim(),
    error: String(details.error || "").trim(),
    updatedAt: new Date().toISOString()
  }));
}

function markBootstrapRunning(windowObject, details = {}) {
  writeBootstrapAutoStartMarker(windowObject, "running", details);
}

function markBootstrapFailed(windowObject, error) {
  writeBootstrapAutoStartMarker(windowObject, "failed", { error });
}

function clearBootstrapAutoStart(windowObject) {
  localStorageFor(windowObject)?.removeItem(BOOTSTRAP_AUTO_START_KEY);
}

function markLaunchColdStartHandled(windowObject) {
  sessionStorageFor(windowObject)?.setItem(BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY, "1");
}

function launchColdStartAlreadyHandled(windowObject) {
  return sessionStorageFor(windowObject)?.getItem(BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY) === "1";
}

function bootstrapColdStartAction(report, windowObject, { forceStart = false } = {}) {
  if (!forceStart && isSuccessfulJobsFetchReport(report)) return false;
  if (!forceStart && isTerminalFailedJobsFetchReport(report)) return "retry";
  const marker = bootstrapAutoStartMarker(windowObject);
  if (marker.status === "failed") return forceStart ? "start" : "retry";
  if (marker.status === "running" || marker.status === "legacy") {
    return forceStart || isNonTerminalJobsFetchReport(report) ? "reattach" : "retry";
  }
  return "start";
}

function bootstrapRetryMessage(report) {
  const summary = reportSummary(report);
  const error = String(summary.error || "").trim();
  return error
    ? `First-run sheet refresh failed: ${error}`
    : "No fresh local jobs feed is available yet. Retry the quick sheet refresh.";
}

function notifyFirstRunBootstrap(showFirstRunBootstrapNotice, reason = "") {
  if (typeof showFirstRunBootstrapNotice !== "function") return;
  try {
    showFirstRunBootstrapNotice({
      ...FIRST_RUN_BOOTSTRAP_NOTICE,
      reason: String(reason || "")
    });
  } catch {
    // The notice is informational only; bootstrap must continue if UI setup fails.
  }
}

function sleep(ms) {
  const delay = Math.max(0, Number(ms) || 0);
  if (delay <= 0) return Promise.resolve();
  return new Promise(resolve => setTimeout(resolve, delay));
}

export function canUseStartupPreviewFastPath(pageState = {}, defaultFilters = {}) {
  return Number(pageState?.currentPage || 1) === 1
    && filtersMatchDefault(pageState?.filters || {}, defaultFilters);
}

export async function initJobsFeed(deps) {
  const {
    hasJobsList,
    emitMetric,
    markJobsStep = () => {},
    initAuth,
    isDesktopRuntimeMode,
    isContainerRuntimeMode = () => false,
    readCachedJobs,
    normalizeRows,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    isJobsCacheStale,
    cacheTtlMs,
    setSourceStatus,
    refreshJobsNow,
    updateLastUpdatedText,
    fetchJobsReport,
    fetchJobsTaskLive,
    desktopJobsColdStart = false,
    startJobsBootstrap,
    windowObject,
    setProgress,
    setJobsStartupState,
    bootstrapStartTimeoutMs = 30000,
    bootstrapConfirmTimeoutMs = 20000,
    bootstrapConfirmIntervalMs = 1000,
    bootstrapPollIntervalMs = 1500,
    bootstrapTimeoutMs = jobsFirstRunBootstrapNumberOverride(
      windowObject,
      "jobsFirstRunBootstrapTimeoutMs",
      5 * 60 * 1000
    ),
    bootstrapProgressStaleMs = jobsFirstRunBootstrapNumberOverride(
      windowObject,
      "jobsFirstRunBootstrapProgressStaleMs",
      FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS
    ),
    setHasInitializedJobsFeed,
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs,
    setAllJobs,
    showFirstRunBootstrapNotice
  } = deps;

  if (!hasJobsList) return;
  markJobsStep("jobs_boot_start");
  emitMetric("jobs_init_start");
  let firstRunBootstrapNoticeShown = false;

  function showBootstrapNoticeOnce(reason) {
    if (firstRunBootstrapNoticeShown) return;
    firstRunBootstrapNoticeShown = true;
    notifyFirstRunBootstrap(showFirstRunBootstrapNotice, reason);
  }

  function setFirstRunStartupState(detail = "first_run_bootstrap") {
    if (typeof setJobsStartupState === "function") {
      setJobsStartupState("interactive", detail);
    }
  }

    function renderFirstRunBootstrapState() {
      if (typeof setAllJobs === "function") setAllJobs([]);
      recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    applyFiltersAndRender({
      resetPage: false,
        emptyStateReason: "first_run_bootstrap"
      });
    }

    async function loadCompletedFirstRunFeed(report = null) {
      const candidate = report || (
        typeof fetchJobsReport === "function"
          ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
          : null
      );
      if (!isSuccessfulJobsFetchReport(candidate)) return false;
      const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
      if (loaded) {
        clearBootstrapAutoStart(windowObject);
        if (typeof setProgress === "function") setProgress(false);
        return true;
      }
      return false;
    }

    try {
    initAuth();

    const desktopMode = isDesktopRuntimeMode();
    const localReport = desktopMode && typeof fetchJobsReport === "function"
      ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : null;
    const launchColdStartPending = desktopMode
      && Boolean(desktopJobsColdStart)
      && !launchColdStartAlreadyHandled(windowObject);
    const localReportSuccessful = isSuccessfulJobsFetchReport(localReport);
    const bootstrapMarker = desktopMode ? bootstrapAutoStartMarker(windowObject) : { status: "none" };
    const bootstrapMarkerRunning = bootstrapMarker.status === "running"
      || bootstrapMarker.status === "legacy";
    const activeBootstrapReport = isActiveBootstrapReport(localReport);
    const firstRunRequired = Boolean(
      desktopMode
      && !localReportSuccessful
      && (launchColdStartPending || bootstrapMarkerRunning || activeBootstrapReport)
    );
    const firstRunAction = firstRunRequired
      ? bootstrapColdStartAction(localReport, windowObject, {
        forceStart: true
      })
      : false;
    emitMetric("jobs_first_run_gate_evaluated", {
      desktopMode,
      runtimeColdStart: Boolean(desktopJobsColdStart),
      launchColdStartPending,
      reportSuccessful: localReportSuccessful,
      action: firstRunAction || "skip"
    });

    if (firstRunRequired) {
      renderFirstRunBootstrapState();
    }

    // ponytail: container boots on the bounded startup snapshot; normalizing a
    // full IndexedDB feed here blocks boot for seconds. Full feed via explicit Reload.
    const cached = (desktopMode || isContainerRuntimeMode()) ? null : await readCachedJobs();
    emitMetric("jobs_cache_checked", {
      desktopMode,
      hasCache: Boolean(cached?.jobs && cached.jobs.length > 0)
    });
    emitMetric(cached?.jobs && cached.jobs.length > 0 ? "jobs_cache_hit" : "jobs_cache_miss");

    if (cached?.jobs && cached.jobs.length > 0) {
      normalizeRows(cached.jobs);
      recalculateItemsPerPage();
      updateFilterOptions();
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: false });
      markStartupRendered("cache", getAllJobs().length);
      markJobsFirstInteractive("cache");

      if (isJobsCacheStale(cached.savedAt, cacheTtlMs)) {
        if (isContainerRuntimeMode()) {
          setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from local cache.`);
        } else {
          setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from cache. Updating stale cache...`);
          refreshJobsNow({ manual: false }).catch(() => {});
        }
      } else {
        setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from local cache.`);
      }
      updateLastUpdatedText(cached.savedAt);
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
      return;
    }

    async function startBootstrapAndLoad({ explicit = false } = {}) {
      if (explicit && await loadCompletedFirstRunFeed()) return true;
      if (explicit) clearBootstrapAutoStart(windowObject);
      markBootstrapRunning(windowObject);
      setFirstRunStartupState();
      if (typeof setProgress === "function") setProgress(true);
      setSourceStatus(FIRST_RUN_BOOTSTRAP_STATUS);
      try {
        if (typeof startJobsBootstrap !== "function") {
          throw new Error("bootstrap route unavailable");
        }
        emitMetric("jobs_first_run_bootstrap_start_requested", { explicit });
        const startedPayload = await startBootstrapWithConfirmation({ explicit });
        markBootstrapRunning(windowObject, { runId: startedPayload?.runId });
        if (startedPayload?.alreadyCompleted) {
          if (typeof setProgress === "function") setProgress(false);
          const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
          if (loaded) {
            clearBootstrapAutoStart(windowObject);
            return true;
          }
          throw new Error(LOCAL_FEED_MISSING_MESSAGE);
        }
        if (!startedPayload?.started && !startedPayload?.alreadyRunning) {
          throw new Error(String(startedPayload?.error || "bootstrap did not start"));
        }
        const pollInterval = Math.max(0, Number(bootstrapPollIntervalMs) || 0);
        const deadline = Date.now() + Math.max(1000, Number(bootstrapTimeoutMs) || 120000);
        let latestFreshProgressAt = 0;
        for (;;) {
          await new Promise(resolve => setTimeout(resolve, pollInterval));
          const now = Date.now();
          const nextReport = await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null);
          if (isSuccessfulJobsFetchReport(nextReport)) {
            if (await loadCompletedFirstRunFeed(nextReport)) return true;
            throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          }
          if (isTerminalFailedJobsFetchReport(nextReport)) {
            throw new Error(bootstrapRetryMessage(nextReport));
          }
          const taskLive = typeof fetchJobsTaskLive === "function"
            ? await fetchJobsTaskLive({ timeoutMs: 1500 }).catch(() => null)
            : null;
          if (
            isFreshBootstrapProgress(taskLive, { now, staleMs: bootstrapProgressStaleMs })
            || isFreshBootstrapProgress(nextReport, { now, staleMs: bootstrapProgressStaleMs })
          ) {
            latestFreshProgressAt = now;
          }
          if (now < deadline) {
            continue;
          }
          const finalReport = await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null);
          if (isSuccessfulJobsFetchReport(finalReport)) {
            if (await loadCompletedFirstRunFeed(finalReport)) return true;
            throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          }
          const finalTaskLive = typeof fetchJobsTaskLive === "function"
            ? await fetchJobsTaskLive({ timeoutMs: 1500 }).catch(() => null)
            : null;
          if (isFreshBootstrapProgress(finalTaskLive, { now: Date.now(), staleMs: bootstrapProgressStaleMs })) {
            latestFreshProgressAt = Date.now();
            continue;
          }
          if (
            latestFreshProgressAt
            && now - latestFreshProgressAt <= Math.max(
              1000,
              Number(bootstrapProgressStaleMs) || FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS
            )
          ) {
            continue;
          }
          break;
        }
        throw new Error("first-run sheet refresh timed out");
      } catch (err) {
        if (err?.bootstrapStartUnconfirmed) {
          clearBootstrapAutoStart(windowObject);
        } else {
          markBootstrapFailed(windowObject, String(err?.message || err || ""));
        }
        throw err;
      } finally {
        if (typeof setProgress === "function") setProgress(false);
      }
    }

    async function startBootstrapWithConfirmation({ explicit = false } = {}) {
      try {
        const payload = await startJobsBootstrap({ timeoutMs: bootstrapStartTimeoutMs });
        if (bootstrapStartHasRunningEvidence(payload)) return payload;
        return payload;
      } catch (err) {
        if (!isUncertainBootstrapStartError(err)) throw err;
        emitMetric("jobs_first_run_bootstrap_start_uncertain", {
          explicit,
          error: String(err?.message || err || "")
        });
        setSourceStatus(FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS);
        const confirmedPayload = await confirmBootstrapStart({ explicit });
        if (confirmedPayload) return confirmedPayload;
        throw bootstrapStartUnconfirmedError();
      }
    }

    async function confirmBootstrapStart({ explicit = false } = {}) {
      const deadline = Date.now() + Math.max(0, Number(bootstrapConfirmTimeoutMs) || 0);
      const interval = Math.max(0, Number(bootstrapConfirmIntervalMs) || 0);
      let retriedStart = false;
      for (;;) {
        const report = typeof fetchJobsReport === "function"
          ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
          : null;
        if (isSuccessfulJobsFetchReport(report)) {
          return { alreadyCompleted: true, runId: reportRunId(report) };
        }
        if (isTerminalFailedJobsFetchReport(report)) {
          throw new Error(bootstrapRetryMessage(report));
        }
        if (isActiveBootstrapReport(report)) {
          emitMetric("jobs_first_run_bootstrap_start_confirmed", {
            explicit,
            evidence: "report",
            runId: reportRunId(report)
          });
          return { alreadyRunning: true, runId: reportRunId(report) };
        }

        if (!retriedStart) {
          retriedStart = true;
          try {
            const retryPayload = await startJobsBootstrap({ timeoutMs: bootstrapStartTimeoutMs });
            if (bootstrapStartHasRunningEvidence(retryPayload)) {
              emitMetric("jobs_first_run_bootstrap_start_confirmed", {
                explicit,
                evidence: retryPayload?.alreadyRunning ? "already_running" : "retry_start",
                runId: String(retryPayload?.runId || "")
              });
              return retryPayload;
            }
          } catch (retryErr) {
            if (!isUncertainBootstrapStartError(retryErr)) throw retryErr;
            emitMetric("jobs_first_run_bootstrap_start_retry_uncertain", {
              explicit,
              error: String(retryErr?.message || retryErr || "")
            });
          }
        }

        if (Date.now() >= deadline) return null;
        await sleep(interval);
      }
    }

    let retryBootstrapInFlight = false;
    async function retryBootstrap(event) {
      if (retryBootstrapInFlight) return;
      retryBootstrapInFlight = true;
      const retryButton = event?.currentTarget;
      if (retryButton) {
        retryButton.disabled = true;
        retryButton.setAttribute("aria-busy", "true");
      }
      try {
        setFirstRunStartupState("first_run_bootstrap_retry");
        renderFirstRunBootstrapState();
        const ok = await startBootstrapAndLoad({ explicit: true });
        if (!ok) {
          throw new Error("unable to load promoted sheet jobs");
        }
      } catch (err) {
        showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
      } finally {
        retryBootstrapInFlight = false;
        if (retryButton?.isConnected) {
          retryButton.disabled = false;
          retryButton.removeAttribute("aria-busy");
        }
      }
    }

    if (firstRunRequired) {
      setHasInitializedJobsFeed(true);
      markStartupRendered("first_run_bootstrap", 0);
      markJobsFirstInteractive("first_run_bootstrap");
      scheduleNonCriticalStartupWork();
      if (firstRunAction === "start" || firstRunAction === "reattach") {
        try {
          if (launchColdStartPending) markLaunchColdStartHandled(windowObject);
          showBootstrapNoticeOnce(firstRunAction);
          const ok = await startBootstrapAndLoad();
          if (!ok) {
            showError("Unable to load job listings right now.", retryBootstrap);
          }
          return;
        } catch (err) {
          showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
          return;
        }
      }
      showError(bootstrapRetryMessage(localReport), retryBootstrap);
      return;
    }

    const previewLoaded = await loadStartupPreviewJobs();
    if (previewLoaded) {
      setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from startup snapshot. Syncing full feed...`);
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
      // ponytail: auto-hydrate the complete feed right after interactive so
      // the full list never requires pressing Reload; snapshot keeps boot fast.
      windowObject.setTimeout(() => {
        refreshJobsNow({ manual: false }).catch(() => {});
      }, JOBS_FULL_FEED_SYNC_DELAY_MS);
      return;
    }

    const ok = await refreshJobsNow({ manual: false, firstLoad: true });
    setHasInitializedJobsFeed(true);
    scheduleNonCriticalStartupWork();
    await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
    if (!ok) {
      if (isDesktopRuntimeMode() && isSuccessfulJobsFetchReport(localReport)) {
        try {
          const recovered = await startBootstrapAndLoad();
          if (!recovered) showError(LOCAL_FEED_MISSING_MESSAGE, retryBootstrap);
          return;
        } catch (err) {
          showError(String(err?.message || LOCAL_FEED_MISSING_MESSAGE), retryBootstrap);
          return;
        }
      }
      showError("Unable to load job listings right now.");
    }
  } catch (err) {
    setHasInitializedJobsFeed(true);
    showError("Unable to load job listings right now.");
    if (typeof deps.logError === "function") {
      deps.logError("Jobs feed init failed", err);
    } else {
      console.error("[jobs] init failed:", err);
    }
  }
}

export async function refreshJobsFeed({ manual, firstLoad = false }, deps) {
  const {
    getRefreshInFlight,
    setRefreshInFlight,
    dispatchRefreshRequested,
    setRefreshButtonDisabled,
    setProgress,
    setSourceStatus,
    firstLoadRequestTimeoutMs,
    fetchUnifiedJobs,
    dispatchRefreshFailed,
    showToast,
    logError,
    getAllJobs,
    setAllJobs,
    normalizeRows,
    setRefreshJobsNeedsAttention,
    isDesktopRuntimeMode,
    writeCachedJobs,
    fetchJobsReport,
    updateLastUpdatedText,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    markJobsStep = () => {},
    measureJobsStep = () => {},
    emitMetric,
    dispatchRefreshCompleted,
    renderDataSources
  } = deps;

  if (getRefreshInFlight()) return false;
  setRefreshInFlight(true);
  dispatchRefreshRequested();

  // Keep the page interactive while noncritical background refreshes run after
  // startup-preview/cache boot. Only blocking/manual refresh flows should lock
  // the refresh control.
  const disableRefreshButton = Boolean(manual || firstLoad);
  if (disableRefreshButton) {
    setRefreshButtonDisabled(true);
  }
  if (manual || firstLoad) setProgress(true);
  if (manual) setSourceStatus("Reloading jobs...");

  try {
    const refreshStartedAt = Date.now();
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_start");
      emitMetric("jobs_first_load_refresh_start");
    }
    const result = await fetchUnifiedJobs({
      timeoutMs: firstLoad ? firstLoadRequestTimeoutMs : 20000,
      allowSheetsFallback: !firstLoad
    });
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_done", {
        ok: Boolean(result.jobs && result.jobs.length > 0)
      });
      measureJobsStep("jobs_feed_fetch", "jobs_feed_fetch_start", "jobs_feed_fetch_done", {
        ok: Boolean(result.jobs && result.jobs.length > 0)
      });
    }
    if (!result.jobs || result.jobs.length === 0) {
      if (firstLoad) {
        setSourceStatus(result.error || "Could not fetch listings from local unified feeds.");
      }
      if (manual) showToast(result.error || "Could not reload jobs.", "error");
      dispatchRefreshFailed(result.error || "Could not reload jobs.");
      return false;
    }

    const previousLength = getAllJobs().length;
    const normalizedJobs = normalizeRows(result.jobs);
    if (!normalizedJobs.length) {
      setAllJobs([]);
      if (firstLoad) setSourceStatus(EMPTY_TITLE_FEED_MESSAGE);
      if (manual) showToast(EMPTY_TITLE_FEED_MESSAGE, "error");
      dispatchRefreshFailed(EMPTY_TITLE_FEED_MESSAGE);
      return false;
    }
    setAllJobs(normalizedJobs);
    setRefreshJobsNeedsAttention(false);
    const now = Date.now();
    const latestReport = typeof fetchJobsReport === "function"
      ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : null;
    const reportTimestamp = reportFinishedTimestamp(latestReport);
    const lastUpdated = reportTimestamp || (!isDesktopRuntimeMode() ? now : null);
    if (!isDesktopRuntimeMode()) {
      await writeCachedJobs(getAllJobs());
    }
    updateLastUpdatedText(lastUpdated);
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    if (firstLoad) {
      markJobsStep("jobs_render_start", { rowCount: getAllJobs().length });
    }
    applyFiltersAndRender({ resetPage: false });
    if (firstLoad) {
      markJobsStep("jobs_render_end", { rowCount: getAllJobs().length });
      measureJobsStep("jobs_render", "jobs_render_start", "jobs_render_end", {
        rowCount: getAllJobs().length
      });
      markStartupRendered("first_load_refresh", getAllJobs().length);
      markJobsFirstInteractive("first_load_refresh");
      emitMetric("jobs_first_load_refresh_done", {
        ok: true,
        durationMs: Math.max(0, Date.now() - refreshStartedAt),
        rowCount: getAllJobs().length
      });
    }

    if (manual) {
      showToast("Jobs reloaded.", "success");
    } else if (previousLength > 0) {
      showToast("Job cache auto-updated.", "info");
    }

    const sourceLabel = result.sourceName ? ` from ${result.sourceName}` : "";
    const limitedScope = coverageScope(latestReport) === "bootstrap_sheets";
    const coverageNote = limitedScope
      ? " Sheet-limited first-run refresh; run Update jobs for full coverage."
      : "";
    setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs${sourceLabel}.${coverageNote}`);
    renderDataSources().catch(() => {});
    dispatchRefreshCompleted();
    return true;
  } catch (err) {
    logError("Refresh failed", err);
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_done", {
        ok: false,
        error: String(err?.message || "unknown error")
      });
      measureJobsStep("jobs_feed_fetch", "jobs_feed_fetch_start", "jobs_feed_fetch_done", {
        ok: false
      });
      emitMetric("jobs_first_load_refresh_done", {
        ok: false,
        error: String(err?.message || "unknown error")
      });
    }
    if (manual) showToast("Could not reload jobs.", "error");
    dispatchRefreshFailed(err?.message || "Could not reload jobs.");
    return false;
  } finally {
    setRefreshInFlight(false);
    if (disableRefreshButton) {
      setRefreshButtonDisabled(false);
    }
    setProgress(false);
  }
}

export async function loadStartupPreviewJobsFeed(deps) {
  const {
    emitMetric,
    fetchJsonFromCandidates,
    startupPreviewJsonUrls,
    parseUnifiedJobsPayload,
    normalizeRows,
    updateLastUpdatedText,
    startupLastUpdatedTimestamp = null,
    recalculateItemsPerPage,
    pageState,
    defaultFilters,
    buildStartupPreviewFastPathPlan,
    applyFilterOptionsSnapshot,
    updateFilterOptions,
    applyStateToFilters,
    renderStartupPreviewFastPath,
    scheduleStartupPreviewMaterialization,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    markJobsStep = () => {},
    measureJobsStep = () => {},
    setSkipInitialGuestAuthRerender,
    getAllJobs
  } = deps;

  try {
    const startedAt = Date.now();
    markJobsStep("jobs_startup_preview_fetch_start");
    emitMetric("jobs_startup_preview_fetch_start");
    const payload = await fetchJsonFromCandidates(startupPreviewJsonUrls, { timeoutMs: 3000 });
    markJobsStep("jobs_startup_preview_fetch_done", {
      hasPayload: Boolean(payload)
    });
    measureJobsStep(
      "jobs_startup_preview_fetch",
      "jobs_startup_preview_fetch_start",
      "jobs_startup_preview_fetch_done",
      { hasPayload: Boolean(payload) }
    );
    emitMetric("jobs_startup_preview_fetch_complete", {
      durationMs: Math.max(0, Date.now() - startedAt),
      hasPayload: Boolean(payload)
    });
    markJobsStep("jobs_startup_preview_parse_start");
    emitMetric("jobs_startup_preview_parse_start");
    const rows = parseUnifiedJobsPayload(payload);
    markJobsStep("jobs_startup_preview_parse_done", {
      rowCount: Array.isArray(rows) ? rows.length : 0
    });
    measureJobsStep(
      "jobs_startup_preview_parse",
      "jobs_startup_preview_parse_start",
      "jobs_startup_preview_parse_done",
      { rowCount: Array.isArray(rows) ? rows.length : 0 }
    );
    emitMetric("jobs_startup_preview_parse_complete", {
      rowCount: Array.isArray(rows) ? rows.length : 0
    });
    if (!Array.isArray(rows) || rows.length === 0) return false;
    emitMetric("jobs_startup_preview_normalize_start");
    const normalizedJobs = normalizeRows(rows);
    emitMetric("jobs_startup_preview_normalize_complete", {
      rowCount: getAllJobs().length
    });
    updateLastUpdatedText(startupLastUpdatedTimestamp);
    recalculateItemsPerPage();
    markJobsStep("jobs_startup_preview_render_start", {
      rowCount: getAllJobs().length
    });
    emitMetric("jobs_startup_preview_render_start", {
      rowCount: getAllJobs().length
    });
    const useFastPath = canUseStartupPreviewFastPath(pageState, defaultFilters)
      && typeof buildStartupPreviewFastPathPlan === "function"
      && typeof renderStartupPreviewFastPath === "function";
    if (useFastPath) {
      const startupPreviewPlan = buildStartupPreviewFastPathPlan(normalizedJobs);
      if (startupPreviewPlan?.filterOptions && typeof applyFilterOptionsSnapshot === "function") {
        applyFilterOptionsSnapshot(startupPreviewPlan.filterOptions);
      } else {
        updateFilterOptions();
      }
      applyStateToFilters();
      renderStartupPreviewFastPath(startupPreviewPlan);
      if (typeof scheduleStartupPreviewMaterialization === "function") {
        scheduleStartupPreviewMaterialization(startupPreviewPlan?.materializeFilteredJobs);
      }
    } else {
      updateFilterOptions();
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: false });
    }
    emitMetric("jobs_startup_preview_render_returned", {
      rowCount: getAllJobs().length
    });
    markJobsStep("jobs_startup_preview_render_done", {
      rowCount: getAllJobs().length
    });
    measureJobsStep(
      "jobs_startup_preview_render",
      "jobs_startup_preview_render_start",
      "jobs_startup_preview_render_done",
      { rowCount: getAllJobs().length }
    );
    markStartupRendered("startup_preview", getAllJobs().length);
    markJobsFirstInteractive("startup_preview");
    markJobsStep("jobs_preview_ready", { rowCount: getAllJobs().length });
    emitMetric("jobs_startup_preview_render_complete", {
      rowCount: getAllJobs().length
    });
    if (typeof setSkipInitialGuestAuthRerender === "function") {
      setSkipInitialGuestAuthRerender(true);
    }
    emitMetric("jobs_startup_preview_loaded", {
      rowCount: getAllJobs().length,
      durationMs: Math.max(0, Date.now() - startedAt)
    });
    return true;
  } catch (error) {
    emitMetric("jobs_startup_preview_miss", {
      message: String(error?.message || error || "unknown startup preview error")
    });
    return false;
  }
}

export function handleJobsAutoRefreshSignalValue(rawValue, deps) {
  const {
    parseAutoRefreshSignal,
    getLastHandledAutoRefreshSignalId,
    getHasInitializedJobsFeed,
    setPendingAutoRefreshSignal,
    triggerAutoRefreshFromSignal,
    logError
  } = deps;

  const signal = parseAutoRefreshSignal(rawValue);
  if (!signal) return;
  if (signal.id === getLastHandledAutoRefreshSignalId()) return;

  if (!getHasInitializedJobsFeed()) {
    setPendingAutoRefreshSignal(signal);
    return;
  }

  setPendingAutoRefreshSignal(null);
  triggerAutoRefreshFromSignal(signal).catch(err => {
    logError("Auto-refresh from admin signal failed", err);
  });
}

export async function applyPendingJobsAutoRefreshSignal(deps) {
  const {
    getPendingAutoRefreshSignal,
    setPendingAutoRefreshSignal,
    readAutoRefreshSignal,
    autoRefreshSignalKey,
    handleAutoRefreshSignalValue,
    triggerAutoRefreshFromSignal
  } = deps;

  const pendingAutoRefreshSignal = getPendingAutoRefreshSignal();
  if (pendingAutoRefreshSignal) {
    setPendingAutoRefreshSignal(null);
    await triggerAutoRefreshFromSignal(pendingAutoRefreshSignal);
    return;
  }

  const latestRaw = readAutoRefreshSignal(autoRefreshSignalKey);
  handleAutoRefreshSignalValue(latestRaw);
}

export async function triggerJobsAutoRefreshFromSignal(signal, deps) {
  const {
    getLastHandledAutoRefreshSignalId,
    setSourceStatus,
    getAutoRefreshStatusText,
    refreshJobsNow,
    markAutoRefreshSignalHandled,
    showToast
  } = deps;

  if (!signal?.id) return;
  if (signal.id === getLastHandledAutoRefreshSignalId()) return;
  setSourceStatus(getAutoRefreshStatusText(signal));

  const ok = await refreshJobsNow({ manual: false });
  markAutoRefreshSignalHandled(signal.id);
  if (ok) {
    showToast("Jobs auto-refreshed from latest fetcher run.", "success");
  }
}
