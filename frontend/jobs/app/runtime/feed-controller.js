export function createJobsFeedController({
  dom,
  runtimeState,
  pageState,
  defaultFilters,
  professionLabels,
  sanitizeUrl,
  jobsParsing,
  startupPreviewJsonUrls,
  jobsDispatch,
  jobsActions,
  filtersController,
  showToast,
  emitDesktopStartupMetric,
  markJobsStep = () => {},
  measureJobsStep = () => {},
  markStartupRendered,
  markJobsFirstInteractive,
  applyFiltersAndRender,
  isDesktopRuntimeMode,
  logJobsError,
  logJobsInfo,
  getJobsLastUpdatedText,
  normalizeJobs,
  parseUnifiedJobsPayload,
  openJobsCacheDbFromModule,
  readJobsCache,
  writeJobsCache,
  refreshJobsFeed,
  loadStartupPreviewJobsFeed,
  fetchUnifiedJobsFromSources,
  fetchJsonFromCandidatesFromSources,
  renderDataSourcesFromSources,
  mapProfession,
  normalizeSector,
  classifyCompanyType,
  detectWorkType,
  setProgressVisibility,
  setStatusText,
  setText,
  jobsCacheDb,
  jobsCacheDbVersion,
  jobsCacheStore,
  jobsSeenStore,
  jobsCacheKey,
  jobsFirstLoadRequestTimeoutMs,
  windowObject = globalThis.window,
  now = () => Date.now(),
  nowIso = () => new Date().toISOString(),
  recalculateItemsPerPage,
  startupPreviewController
}) {
  function normalizeRows(rows) {
    runtimeState.allJobs = normalizeJobs(rows, {
      professionLabels,
      sanitizeUrl
    });
    return runtimeState.allJobs;
  }

  function openJobsCacheDb() {
    return openJobsCacheDbFromModule({
      indexedDb: windowObject.indexedDB,
      dbName: jobsCacheDb,
      dbVersion: jobsCacheDbVersion,
      cacheStore: jobsCacheStore,
      seenStore: jobsSeenStore
    });
  }

  async function readCachedJobs() {
    return readJobsCache({
      openDb: openJobsCacheDb,
      cacheStore: jobsCacheStore,
      cacheKey: jobsCacheKey
    });
  }

  function updateLastUpdatedText(timestamp) {
    if (!dom.jobsLastUpdatedEl) return;
    dom.jobsLastUpdatedEl.textContent = getJobsLastUpdatedText(timestamp);
  }

  async function writeCachedJobs(jobs) {
    return writeJobsCache(jobs, {
      openDb: openJobsCacheDb,
      cacheStore: jobsCacheStore,
      cacheKey: jobsCacheKey,
      now: now()
    });
  }

  function setRefreshJobsNeedsAttention(needsRefresh) {
    const needs = Boolean(needsRefresh);
    if (dom.refreshJobsBtn) {
      dom.refreshJobsBtn.classList.toggle("needs-refresh", needs);
      dom.refreshJobsBtn.setAttribute("aria-live", "polite");
    }
    if (dom.refreshJobsNeededBadgeEl) {
      dom.refreshJobsNeededBadgeEl.classList.toggle("hidden", !needs);
    }
  }

  function setProgress(visible) {
    setProgressVisibility(setText, dom.fetchProgress, visible);
  }

  function setSourceStatus(text) {
    setStatusText(setText, dom.sourceStatus, text);
  }

  async function fetchUnifiedJobs({ timeoutMs } = {}) {
    return fetchUnifiedJobsFromSources({
      setSourceStatus,
      jobsParsing,
      timeoutMs,
      parserDeps: {
        mapProfession,
        normalizeSector,
        classifyCompanyType,
        detectWorkType,
        logInfo: logJobsInfo,
        logError: logJobsError
      }
    });
  }

  async function fetchJsonFromCandidates(urls, options) {
    return fetchJsonFromCandidatesFromSources(urls, options);
  }

  async function renderDataSources() {
    return renderDataSourcesFromSources({
      dataSourcesListEl: dom.dataSourcesListEl,
      dataSourcesCaptionEl: dom.dataSourcesCaptionEl
    });
  }

  async function refreshJobsNow({ manual, firstLoad = false }) {
    return refreshJobsFeed({ manual, firstLoad }, {
      getRefreshInFlight: () => runtimeState.refreshInFlight,
      setRefreshInFlight: value => {
        runtimeState.refreshInFlight = Boolean(value);
      },
      dispatchRefreshRequested: () => {
        jobsDispatch.dispatch({ type: jobsActions.REFRESH_REQUESTED });
      },
      setRefreshButtonDisabled: disabled => {
        if (dom.refreshJobsBtn) dom.refreshJobsBtn.disabled = disabled;
      },
      setProgress,
      setSourceStatus,
      firstLoadRequestTimeoutMs: jobsFirstLoadRequestTimeoutMs,
      fetchUnifiedJobs,
      dispatchRefreshFailed: error => {
        jobsDispatch.dispatch({
          type: jobsActions.REFRESH_FAILED,
          payload: { error }
        });
      },
      showToast,
      logError: logJobsError,
      getAllJobs: () => runtimeState.allJobs,
      setAllJobs: jobs => {
        runtimeState.allJobs = jobs;
      },
      normalizeRows,
      setRefreshJobsNeedsAttention,
      isDesktopRuntimeMode,
      writeCachedJobs,
      updateLastUpdatedText,
      recalculateItemsPerPage,
      updateFilterOptions: () => filtersController.updateFilterOptions(runtimeState.allJobs),
      applyStateToFilters: () => filtersController.applyStateToFilters(),
      applyFiltersAndRender,
      markStartupRendered,
      markJobsFirstInteractive,
      markJobsStep,
      measureJobsStep,
      emitMetric: emitDesktopStartupMetric,
      dispatchRefreshCompleted: () => {
        jobsDispatch.dispatch({
          type: jobsActions.REFRESH_COMPLETED,
          payload: { finishedAt: nowIso() }
        });
      },
      renderDataSources
    });
  }

  async function loadStartupPreviewJobs() {
    return loadStartupPreviewJobsFeed({
      emitMetric: emitDesktopStartupMetric,
      fetchJsonFromCandidates,
      startupPreviewJsonUrls,
      parseUnifiedJobsPayload: payload => parseUnifiedJobsPayload(payload, jobsParsing),
      normalizeRows,
      updateLastUpdatedText,
      recalculateItemsPerPage,
      pageState,
      defaultFilters,
      buildStartupPreviewFastPathPlan: startupPreviewController.buildStartupPreviewFastPathPlan,
      applyFilterOptionsSnapshot: filterOptions =>
        filtersController.updateFilterOptions(runtimeState.allJobs, {
          precomputed: filterOptions
        }),
      updateFilterOptions: () => filtersController.updateFilterOptions(runtimeState.allJobs),
      applyStateToFilters: () => filtersController.applyStateToFilters(),
      renderStartupPreviewFastPath: startupPreviewController.renderStartupPreviewFastPath,
      scheduleStartupPreviewMaterialization: startupPreviewController.scheduleStartupPreviewMaterialization,
      applyFiltersAndRender,
      markJobsStep,
      measureJobsStep,
      markStartupRendered,
      markJobsFirstInteractive,
      setSkipInitialGuestAuthRerender: value => {
        runtimeState.skipInitialGuestAuthRerender = Boolean(value);
      },
      getAllJobs: () => runtimeState.allJobs
    });
  }

  return {
    openJobsCacheDb,
    readCachedJobs,
    updateLastUpdatedText,
    refreshJobsNow,
    writeCachedJobs,
    loadStartupPreviewJobs,
    setRefreshJobsNeedsAttention,
    fetchUnifiedJobs,
    fetchJsonFromCandidates,
    renderDataSources,
    setProgress,
    setSourceStatus
  };
}
