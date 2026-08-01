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
  isContainerRuntimeMode = () => false,
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
  jobsFetchReportUrls,
  availabilityHistoryUrls = [],
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
    setText(dom.sourceStatus, text);
  }

  function reportFinishedTimestamp(report) {
    const finishedAt = String(report?.finishedAt || "").trim();
    if (!finishedAt) return null;
    const timestamp = Date.parse(finishedAt);
    return Number.isFinite(timestamp) ? timestamp : null;
  }

  async function fetchJobsReport({ timeoutMs = 1500 } = {}) {
    const report = await fetchJsonFromCandidates(jobsFetchReportUrls || [], { timeoutMs });
    runtimeState.jobsFetchReport = report && typeof report === "object" ? report : null;
    return runtimeState.jobsFetchReport;
  }

  async function loadAvailabilityHistory() {
    if (runtimeState.availabilityHistoryLoaded) return runtimeState.availabilityHistoryRows || [];
    if (runtimeState.availabilityHistoryPromise) return runtimeState.availabilityHistoryPromise;
    runtimeState.availabilityHistoryPromise = fetchJsonFromCandidates(
      availabilityHistoryUrls,
      { timeoutMs: 2500 }
    ).then(payload => {
      const rawRows = Array.isArray(payload) ? payload : Array.isArray(payload?.rows) ? payload.rows : [];
      const rows = normalizeJobs(rawRows, { professionLabels, sanitizeUrl });
      const activeIds = new Set(runtimeState.allJobs.map(row => String(row?.availabilityId || "")));
      runtimeState.availabilityHistoryRows = rows.filter(row => !activeIds.has(String(row?.availabilityId || "")));
      runtimeState.allJobs = [...runtimeState.allJobs, ...runtimeState.availabilityHistoryRows];
      runtimeState.availabilityHistoryLoaded = true;
      filtersController.updateFilterOptions(runtimeState.allJobs);
      return runtimeState.availabilityHistoryRows;
    }).finally(() => {
      runtimeState.availabilityHistoryPromise = null;
    });
    return runtimeState.availabilityHistoryPromise;
  }

  function getLatestJobsReportFinishedMs() {
    return reportFinishedTimestamp(runtimeState.jobsFetchReport);
  }

  async function fetchUnifiedJobs({ timeoutMs, allowSheetsFallback = true } = {}) {
    return fetchUnifiedJobsFromSources({
      setSourceStatus,
      jobsParsing,
      timeoutMs,
      allowSheetsFallback,
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

  async function renderDataSources(options = {}) {
    const force = Boolean(options?.force);
    if (!force && runtimeState.dataSourcesLoaded) return runtimeState.dataSourcesLoadResult || null;
    if (!force && runtimeState.dataSourcesLoadPromise) return runtimeState.dataSourcesLoadPromise;
    if (dom.dataSourcesCaptionEl && !runtimeState.dataSourcesLoaded) {
      dom.dataSourcesCaptionEl.textContent = "Loading source metadata...";
    }
    runtimeState.dataSourcesLoadPromise = renderDataSourcesFromSources({
      dataSourcesListEl: dom.dataSourcesListEl,
      dataSourcesCaptionEl: dom.dataSourcesCaptionEl
    }).then(result => {
      runtimeState.dataSourcesLoaded = true;
      runtimeState.dataSourcesLoadResult = result || null;
      return runtimeState.dataSourcesLoadResult;
    }).finally(() => {
      runtimeState.dataSourcesLoadPromise = null;
    });
    return runtimeState.dataSourcesLoadPromise;
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
      isContainerRuntimeMode,
      writeCachedJobs,
      fetchJobsReport,
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
      startupLastUpdatedTimestamp: getLatestJobsReportFinishedMs(),
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
    fetchJobsReport,
    setRefreshJobsNeedsAttention,
    fetchUnifiedJobs,
    fetchJsonFromCandidates,
    renderDataSources,
    loadAvailabilityHistory,
    setProgress,
    setSourceStatus
  };
}
