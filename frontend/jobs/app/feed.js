export async function initJobsFeed(deps) {
  const {
    hasJobsList,
    emitMetric,
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
    setHasInitializedJobsFeed,
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs
  } = deps;

  if (!hasJobsList) return;
  emitMetric("jobs_init_start");

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
    showError("Unable to load job listings right now.");
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
    updateLastUpdatedText,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    emitMetric,
    dispatchRefreshCompleted,
    renderDataSources
  } = deps;

  if (getRefreshInFlight()) return false;
  setRefreshInFlight(true);
  dispatchRefreshRequested();

  setRefreshButtonDisabled(true);
  if (manual || firstLoad) setProgress(true);
  if (manual) setSourceStatus("Refreshing jobs from unified feed...");

  try {
    const refreshStartedAt = Date.now();
    if (firstLoad) {
      emitMetric("jobs_first_load_refresh_start");
    }
    const result = await fetchUnifiedJobs({
      timeoutMs: firstLoad ? firstLoadRequestTimeoutMs : 20000,
      allowSheetsFallback: !firstLoad
    });
    if (!result.jobs || result.jobs.length === 0) {
      if (firstLoad) {
        setSourceStatus(result.error || "Could not fetch listings from local unified feeds.");
      }
      if (manual) showToast(result.error || "Could not refresh jobs.", "error");
      dispatchRefreshFailed(result.error || "Could not refresh jobs.");
      return false;
    }

    const previousLength = getAllJobs().length;
    setAllJobs(normalizeRows(result.jobs));
    setRefreshJobsNeedsAttention(false);
    if (!isDesktopRuntimeMode()) {
      await writeCachedJobs(getAllJobs());
    }
    updateLastUpdatedText(Date.now());
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    applyFiltersAndRender({ resetPage: false });
    if (firstLoad) {
      markStartupRendered("first_load_refresh", getAllJobs().length);
      markJobsFirstInteractive("first_load_refresh");
      emitMetric("jobs_first_load_refresh_done", {
        ok: true,
        durationMs: Math.max(0, Date.now() - refreshStartedAt),
        rowCount: getAllJobs().length
      });
    }

    if (manual) {
      showToast("Jobs refreshed.", "success");
    } else if (previousLength > 0) {
      showToast("Job cache auto-updated.", "info");
    }

    const sourceLabel = result.sourceName ? ` from ${result.sourceName}` : "";
    setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs${sourceLabel}.`);
    renderDataSources().catch(() => {});
    dispatchRefreshCompleted();
    return true;
  } catch (err) {
    logError("Refresh failed", err);
    if (firstLoad) {
      emitMetric("jobs_first_load_refresh_done", {
        ok: false,
        error: String(err?.message || "unknown error")
      });
    }
    if (manual) showToast("Could not refresh jobs.", "error");
    dispatchRefreshFailed(err?.message || "Could not refresh jobs.");
    return false;
  } finally {
    setRefreshInFlight(false);
    setRefreshButtonDisabled(false);
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
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    getAllJobs
  } = deps;

  try {
    const startedAt = Date.now();
    emitMetric("jobs_startup_preview_fetch_start");
    const payload = await fetchJsonFromCandidates(startupPreviewJsonUrls, { timeoutMs: 3000 });
    emitMetric("jobs_startup_preview_fetch_complete", {
      durationMs: Math.max(0, Date.now() - startedAt),
      hasPayload: Boolean(payload)
    });
    emitMetric("jobs_startup_preview_parse_start");
    const rows = parseUnifiedJobsPayload(payload);
    emitMetric("jobs_startup_preview_parse_complete", {
      rowCount: Array.isArray(rows) ? rows.length : 0
    });
    if (!Array.isArray(rows) || rows.length === 0) return false;
    emitMetric("jobs_startup_preview_normalize_start");
    normalizeRows(rows);
    emitMetric("jobs_startup_preview_normalize_complete", {
      rowCount: getAllJobs().length
    });
    updateLastUpdatedText(Date.now());
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    emitMetric("jobs_startup_preview_render_start", {
      rowCount: getAllJobs().length
    });
    applyFiltersAndRender({ resetPage: false });
    emitMetric("jobs_startup_preview_render_returned", {
      rowCount: getAllJobs().length
    });
    markStartupRendered("startup_preview", getAllJobs().length);
    markJobsFirstInteractive("startup_preview");
    emitMetric("jobs_startup_preview_render_complete", {
      rowCount: getAllJobs().length
    });
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
