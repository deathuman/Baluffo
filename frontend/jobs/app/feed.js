import { set as stateHubSet } from "../../shared/state-hub.js";

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
const LOCAL_FEED_MISSING_MESSAGE = "Local jobs feed is missing or unreadable. Retry quick refresh or run Update jobs to rebuild the full feed.";

function reportFinishedTimestamp(report) {
  const finishedAt = String(report?.finishedAt || "").trim();
  if (!finishedAt) return null;
  const timestamp = Date.parse(finishedAt);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function reportSummary(report) {
  return report?.summary && typeof report.summary === "object" ? report.summary : {};
}

export function isSuccessfulJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  const status = String(summary.status || "").trim().toLowerCase();
  if (status === "error" || status === "failed") return false;
  return Number(summary.outputCount || 0) > 0;
}

function isTerminalFailedJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  return String(summary.status || "").trim().toLowerCase() === "error"
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

function localStorageFor(windowObject) {
  try {
    return windowObject?.localStorage || null;
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

function bootstrapColdStartAction(report, windowObject) {
  if (isSuccessfulJobsFetchReport(report)) return false;
  if (isTerminalFailedJobsFetchReport(report)) return "retry";
  const marker = bootstrapAutoStartMarker(windowObject);
  if (marker.status === "failed") return "retry";
  if (marker.status === "running" || marker.status === "legacy") {
    return isNonTerminalJobsFetchReport(report) ? "reattach" : "retry";
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
    startJobsBootstrap,
    windowObject,
    setProgress,
    bootstrapPollIntervalMs = 1500,
    bootstrapTimeoutMs = 120000,
    setHasInitializedJobsFeed,
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs
  } = deps;

  if (!hasJobsList) return;
  markJobsStep("jobs_boot_start");
  emitMetric("jobs_init_start");

  try {
    initAuth();

    const cached = isDesktopRuntimeMode() ? null : await readCachedJobs();
    emitMetric("jobs_cache_checked", {
      desktopMode: isDesktopRuntimeMode(),
      hasCache: Boolean(cached?.jobs && cached.jobs.length > 0)
    });
    emitMetric(cached?.jobs && cached.jobs.length > 0 ? "jobs_cache_hit" : "jobs_cache_miss");

    if (cached?.jobs && cached.jobs.length > 0) {
      normalizeRows(cached.jobs);
      recalculateItemsPerPage();
      updateFilterOptions();
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: false });
      stateHubSet("jobsFeedCount", getAllJobs().length);
      stateHubSet("jobsLastUpdated", Date.now());
      markStartupRendered("cache", getAllJobs().length);
      markJobsFirstInteractive("cache");

      if (isJobsCacheStale(cached.savedAt, cacheTtlMs)) {
        setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from cache. Updating stale cache...`);
        refreshJobsNow({ manual: false }).catch(() => {});
      } else {
        setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from local cache.`);
      }
      updateLastUpdatedText(cached.savedAt);
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal();
      return;
    }

    async function startBootstrapAndLoad({ explicit = false } = {}) {
      if (explicit) clearBootstrapAutoStart(windowObject);
      markBootstrapRunning(windowObject);
      if (typeof setProgress === "function") setProgress(true);
      setSourceStatus("Refreshing first-run sheet jobs...");
      try {
        if (typeof startJobsBootstrap !== "function") {
          throw new Error("bootstrap route unavailable");
        }
        const startedPayload = await startJobsBootstrap();
        markBootstrapRunning(windowObject, { runId: startedPayload?.runId });
        if (startedPayload?.alreadyCompleted) {
          if (typeof setProgress === "function") setProgress(false);
          const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
          clearBootstrapAutoStart(windowObject);
          if (!loaded) throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          return true;
        }
        if (!startedPayload?.started && !startedPayload?.alreadyRunning) {
          throw new Error(String(startedPayload?.error || "bootstrap did not start"));
        }
        const pollInterval = Math.max(0, Number(bootstrapPollIntervalMs) || 0);
        const deadline = Date.now() + Math.max(1000, Number(bootstrapTimeoutMs) || 120000);
        while (Date.now() < deadline) {
          await new Promise(resolve => setTimeout(resolve, pollInterval));
          const nextReport = await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null);
          if (isSuccessfulJobsFetchReport(nextReport)) {
            if (typeof setProgress === "function") setProgress(false);
            const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
            if (loaded) {
              clearBootstrapAutoStart(windowObject);
              return true;
            }
            throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          }
          if (isTerminalFailedJobsFetchReport(nextReport)) {
            throw new Error(bootstrapRetryMessage(nextReport));
          }
        }
        throw new Error("first-run sheet refresh timed out");
      } catch (err) {
        markBootstrapFailed(windowObject, String(err?.message || err || ""));
        throw err;
      } finally {
        if (typeof setProgress === "function") setProgress(false);
      }
    }

    async function retryBootstrap() {
      try {
        const ok = await startBootstrapAndLoad({ explicit: true });
        if (!ok) {
          throw new Error("unable to load promoted sheet jobs");
        }
      } catch (err) {
        showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
      }
    }

    const localReport = isDesktopRuntimeMode() && typeof fetchJobsReport === "function"
      ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : null;
    if (isDesktopRuntimeMode() && !isSuccessfulJobsFetchReport(localReport)) {
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal();
      const bootstrapAction = bootstrapColdStartAction(localReport, windowObject);
      if (bootstrapAction === "start" || bootstrapAction === "reattach") {
        try {
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
      await applyPendingAutoRefreshSignal();
      refreshJobsNow({ manual: false }).catch(() => {});
      return;
    }

    const ok = await refreshJobsNow({ manual: false, firstLoad: true });
    setHasInitializedJobsFeed(true);
    scheduleNonCriticalStartupWork();
    await applyPendingAutoRefreshSignal();
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
    setAllJobs(normalizeRows(result.jobs));
    setRefreshJobsNeedsAttention(false);
    const now = Date.now();
    const latestReport = typeof fetchJobsReport === "function"
      ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : null;
    const reportTimestamp = reportFinishedTimestamp(latestReport);
    const lastUpdated = reportTimestamp || (!isDesktopRuntimeMode() ? now : null);
    stateHubSet("jobsFeedCount", getAllJobs().length);
    stateHubSet("jobsLastUpdated", lastUpdated || "");
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
