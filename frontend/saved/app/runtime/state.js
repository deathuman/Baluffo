function createSavedDomState() {
  return {
    savedJobsListEl: null,
    savedSourceStatusEl: null,
    savedAuthStatusEl: null,
    savedAuthStatusHintEl: null,
    savedAuthAvatarEl: null,
    signInBtnEl: null,
    signOutBtnEl: null,
    jobsPageBtnEl: null,
    adminPageBtnEl: null,
    addCustomJobBtnEl: null,
    customJobPanelEl: null,
    customJobFormEl: null,
    customJobTitleEl: null,
    customJobCompanyEl: null,
    customJobCityEl: null,
    customJobCountryEl: null,
    customJobWorkTypeEl: null,
    customJobContractTypeEl: null,
    customJobSectorEl: null,
    customJobProfessionEl: null,
    customJobLinkEl: null,
    customJobNotesEl: null,
    customJobReminderEl: null,
    customJobLinkWarningEl: null,
    customJobCancelBtnEl: null,
    customJobPanelTitleEl: null,
    customJobPanelHintEl: null,
    customJobSaveBtnEl: null,
    savedCustomFilterBarEl: null,
    savedCustomFilterCountEl: null,
    savedCustomFilterBtnEls: [],
    savedSortBarEl: null,
    savedSortBtnEls: [],
    savedReminderCounterEl: null,
    historyPanelToggleBtnEl: null,
    savedWorkspaceLayoutEl: null,
    savedMetricTotalEl: null,
    savedMetricRemindersEl: null,
    savedMetricActivityEl: null,
    exportBackupBtnEl: null,
    exportIncludeFilesEl: null,
    importBackupBtnEl: null,
    importBackupInputEl: null,
    globalPhaseOverrideBtnEl: null,
    activityPanelEl: null,
    activityPanelBodyEl: null,
    activityPanelStatusEl: null,
    activityRefreshBtnEl: null,
    activityScopeBtnEls: [],
    activitySelectedJobEl: null
  };
}

function createSavedViewState({
  defaultSavedFilter = "",
  defaultSavedSort = "updated",
  defaultTimelineScope = "all"
} = {}) {
  return {
    currentUser: null,
    unsubscribeSavedJobs: () => {},
    expandedJobKey: null,
    phaseOverrideArmedGlobal: false,
    activityPanelOpen: false,
    customJobPanelOpen: false,
    customJobMode: "create",
    customJobTargetKey: "",
    activeSavedSort: defaultSavedSort,
    activeSavedFilter: defaultSavedFilter,
    jobDetailTabByKey: new Map(),
    cachedActivityEntries: [],
    lastSavedJobsByKey: new Map(),
    selectedJobKey: "",
    timelineScope: defaultTimelineScope,
    lastActivityPulse: null,
    savedAuthListenerBound: false,
    savedInteractiveMetricSent: false,
    adminBridgeButtonState: "checking",
    adminBridgeWatcher: null
  };
}

export function createSavedPageState(options = {}) {
  return {
    dom: createSavedDomState(),
    viewState: createSavedViewState(options),
    noteSaveState: {
      timers: new Map(),
      inFlight: new Map(),
      pendingValues: new Map(),
      lastInteractionAt: 0
    },
    attachmentPreviewUrls: new Map()
  };
}

export function cacheSavedDomState(domState, nextDomState = {}) {
  Object.assign(domState, nextDomState);
  return domState;
}
