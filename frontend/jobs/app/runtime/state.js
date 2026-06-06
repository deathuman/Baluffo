function cloneDefaultFilters(defaultFilters = {}) {
  return {
    ...defaultFilters,
    countries: Array.from(defaultFilters?.countries || [])
  };
}

export function createJobsPageState(defaultFilters = {}) {
  return {
    currentPage: 1,
    itemsPerPage: 10,
    filters: cloneDefaultFilters(defaultFilters)
  };
}

export function createJobsPipelineUiState() {
  return {
    pollingTimer: null,
    runId: "",
    active: false,
    pendingStart: false,
    bridgeOnline: false,
    startedAt: "",
    statusPollFailureCount: 0,
    updateTooltipBridgeError: "",
    updateTooltipFirstRunBootstrapActive: false,
    updateTooltipFirstRun: false,
    updateTooltipFirstRunKnown: false,
    taskStateSummaryChecked: false,
    abortTask: null,
    abortRequestedTask: null,
    abortRequested: false,
    abortRevealActive: false,
    abortRequestError: "",
    abortRequestErrorAt: 0
  };
}

export function createJobsUserState() {
  return {
    currentUser: null,
    savedJobKeys: new Set(),
    seenJobKeys: new Set(),
    authStateListenerBound: false
  };
}

export function createJobsRuntimeState(defaultFilters = {}, { lastHandledAutoRefreshSignalId = 0 } = {}) {
  return {
    pageState: createJobsPageState(defaultFilters),
    pipelineUiState: createJobsPipelineUiState(),
    userState: createJobsUserState(),
    runtimeState: {
      allJobs: [],
      filteredJobs: [],
      refreshInFlight: false,
      hasInitializedJobsFeed: false,
      pendingAutoRefreshSignal: null,
      lastHandledAutoRefreshSignalId: Number(lastHandledAutoRefreshSignalId) || 0,
      desktopUrlStateReady: false,
      desktopPendingRememberJobsUrl: false,
      desktopPendingJobsUrl: "",
      nonCriticalStartupScheduled: false,
      desktopUpdateAutoCheckScheduled: false,
      coreEventsBound: false,
      secondaryEventsBound: false,
      adminBridgeButtonState: "checking",
      adminBridgeWatcher: null,
      desktopUpdateController: null,
      lastFilterOptionsSignature: "",
      skipInitialGuestAuthRerender: false,
      startupPreviewMaterialize: null,
      startupPreviewMaterializeTimer: null,
      startupPreviewFilteredCount: 0
    }
  };
}
