export function createBaseDeps(overrides = {}) {
  let currentJobs = [];
  const calls = {
    metrics: [],
    perf: [],
    sourceStatus: [],
    showError: [],
    notices: [],
    allJobs: [],
    initialized: [],
    rendered: [],
    startupStates: [],
    interactive: []
  };
  return {
    calls,
    deps: {
      hasJobsList: true,
      emitMetric: (event, payload = {}) => calls.metrics.push({ event, payload }),
      markJobsStep: (name, payload = {}) => calls.perf.push({ type: "mark", name, payload }),
      measureJobsStep: (name, startMark, endMark, payload = {}) =>
        calls.perf.push({ type: "measure", name, startMark, endMark, payload }),
      initAuth: () => {},
      isDesktopRuntimeMode: () => false,
      readCachedJobs: async () => null,
      normalizeRows: rows => rows,
      setAllJobs: jobs => {
        currentJobs = Array.isArray(jobs) ? jobs : [];
        calls.allJobs.push(currentJobs);
      },
      recalculateItemsPerPage: () => {},
      updateFilterOptions: () => {},
      applyStateToFilters: () => {},
      applyFiltersAndRender: () => {},
      markStartupRendered: (stage, rowCount) => calls.rendered.push({ stage, rowCount }),
      markJobsFirstInteractive: reason => calls.interactive.push(reason),
      isJobsCacheStale: () => false,
      cacheTtlMs: 1000,
      setSourceStatus: text => calls.sourceStatus.push(String(text || "")),
      setProgress: () => {},
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      fetchJobsReport: async () => null,
      startJobsBootstrap: async () => ({ started: true }),
      windowObject: { localStorage: new Map(), setTimeout: () => 0 },
      setJobsStartupState: (state, detail = "") => calls.startupStates.push({
        state: String(state || ""),
        detail: String(detail || "")
      }),
      setHasInitializedJobsFeed: value => calls.initialized.push(Boolean(value)),
      scheduleNonCriticalStartupWork: () => {},
      applyPendingAutoRefreshSignal: async () => {},
      loadStartupPreviewJobs: async () => false,
      showError: message => calls.showError.push(String(message || "")),
      showFirstRunBootstrapNotice: notice => calls.notices.push(notice),
      logError: () => {},
      getAllJobs: () => currentJobs,
      ...overrides
    }
  };
}

export function createLocalStorage(initialEntries = []) {
  const storage = new Map(initialEntries);
  const session = new Map();
  const buildStorage = store => ({
    getItem: key => store.get(key) || null,
    setItem: (key, value) => store.set(key, String(value)),
    removeItem: key => store.delete(key)
  });
  return {
    storage,
    session,
    localStorage: buildStorage(storage),
    sessionStorage: buildStorage(session)
  };
}
