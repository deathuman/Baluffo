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
    savedGroupBarEl: null,
    savedGroupBtnEls: [],
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
    activityPanelEl: null,
    activityPanelBodyEl: null,
    activityPanelStatusEl: null,
    activityRefreshBtnEl: null,
    activityCloseBtnEl: null,
    activityRecentBadgeEl: null,
    activityScopeBtnEls: [],
    activitySelectedJobEl: null
  };
}

function createSavedViewState({
  defaultSavedFilter = "",
  defaultSavedSort = "updated",
  defaultSavedGroup = "none",
  defaultTimelineScope = "all"
} = {}) {
  return {
    currentUser: null,
    unsubscribeSavedJobs: () => {},
    expandedJobKey: null,
    activityPanelOpen: false,
    customJobPanelOpen: false,
    customJobMode: "create",
    customJobTargetKey: "",
    activeSavedSort: defaultSavedSort,
    activeSavedFilter: defaultSavedFilter,
    activeSavedGroup: defaultSavedGroup,
    jobDetailTabByKey: new Map(),
    cachedActivityEntries: [],
    lastSavedJobsByKey: new Map(),
    savedLifecycleOverlayByJobKey: new Map(),
    savedLifecycleOverlayRequestId: 0,
    selectedJobKey: "",
    phaseOverrideContext: null,
    trackingOverrideContext: null,
    loadedAttachmentJobKeys: new Set(),
    loadingAttachmentJobKeys: new Set(),
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
