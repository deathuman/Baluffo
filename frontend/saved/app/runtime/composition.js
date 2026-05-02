import { postJson } from "../../../shared/api-client.js";
import { createAuthReadyPoller } from "../../../shared/auth-ready-poll.js";
import { showToast } from "../../../shared/ui/index.js";
import { createSavedActivityController } from "./activity-controller.js";
import { createSavedAttachmentsController } from "./attachments-controller.js";
import { createSavedAuthController } from "./auth-controller.js";
import { createSavedCustomJobController } from "./custom-job-controller.js";
import { createSavedStartupMetrics } from "./effects.js";
import { loadSavedLifecycleOverlayByJobKey } from "./lifecycle-overlay.js";
import { createSavedRenderController } from "./render-controller.js";
import { createSavedPageState } from "./state.js";

export function composeSavedRuntime(deps) {
  const pageState = createSavedPageState({
    defaultSavedFilter: deps.defaultSavedFilter,
    defaultSavedSort: deps.defaultSavedSort,
    defaultTimelineScope: deps.defaultTimelineScope
  });
  const dom = pageState.dom;
  const viewState = pageState.viewState;
  const noteSaveState = pageState.noteSaveState;
  const attachmentPreviewUrls = pageState.attachmentPreviewUrls;
  const savedDispatch = deps.createSavedDispatcher();
  const startupMetrics = createSavedStartupMetrics({
    emitMetric: (event, payload) => {
      postJson(deps.adminBridgeBase, "/desktop-local-data/startup-metric", { event, payload: payload || {} }).catch(() => {});
    }
  });

  let savedRenderController;
  let savedAuthController;

  const savedActivityController = createSavedActivityController({
    dom,
    viewState,
    savedPageService: deps.savedPageService,
    setActivityStatus: (...args) => deps.setActivityStatus(...args),
    timelinePrefPrefix: deps.timelinePrefPrefix,
    timelineScopeAll: deps.timelineScopeAll,
    activityHighlightMs: deps.activityHighlightMs,
    renderActivityEntryHtml: deps.renderActivityEntryHtml,
    getReminderMeta: deps.getReminderMeta,
    loadSavedTimelinePreferences: deps.loadSavedTimelinePreferences,
    persistSavedTimelinePreferences: deps.persistSavedTimelinePreferences,
    activityTypeLabel: deps.activityTypeLabel,
    formatActivityDetail: deps.formatActivityDetail,
    formatPhaseTimestamp: deps.formatPhaseTimestamp
  });

  const savedCustomJobController = createSavedCustomJobController({
    dom,
    viewState,
    savedPageService: deps.savedPageService,
    normalizeCustomJobInput: deps.normalizeCustomJobInput,
    toDatetimeLocalValue: deps.toDatetimeLocalValue,
    savedDispatch,
    savedActions: deps.savedActions,
    queueActivityPulse: (...args) => savedActivityController.queueActivityPulse(...args),
    timelineScopeAll: deps.timelineScopeAll,
    refreshActivityLog: (...args) => savedActivityController.refreshActivityLog(...args)
  });

  const savedAttachmentsController = createSavedAttachmentsController({
    dom,
    viewState,
    savedPageService: deps.savedPageService,
    savedDispatch,
    savedActions: deps.savedActions,
    queueActivityPulse: (...args) => savedActivityController.queueActivityPulse(...args),
    timelineScopeAttachments: deps.timelineScopeAttachments,
    maxAttachmentsPerJob: deps.maxAttachmentsPerJob,
    maxAttachmentBytes: deps.maxAttachmentBytes,
    attachmentPreviewUrls,
    cssEscape: (...args) => deps.cssEscape(...args),
    setSelectedJobKey: (...args) => savedRenderController.setSelectedJobKey(...args)
  });

  savedRenderController = createSavedRenderController({
    dom,
    viewState,
    savedPageService: deps.savedPageService,
    timelineScopeAll: deps.timelineScopeAll,
    timelineScopeSelected: deps.timelineScopeSelected,
    phaseOptions: deps.phaseOptions,
    phaseLabels: deps.phaseLabels,
    customSourceLabel: deps.customSourceLabel,
    reminderSoonHours: deps.reminderSoonHours,
    maxAttachmentsPerJob: deps.maxAttachmentsPerJob,
    maxAttachmentBytes: deps.maxAttachmentBytes,
    computeAnchorScrollDelta: deps.computeAnchorScrollDelta,
    cssEscape: (...args) => deps.cssEscape(...args),
    renderTimeline: (...args) => savedActivityController.renderTimeline(...args),
    renderWorkspaceStats: (...args) => savedActivityController.renderWorkspaceStats(...args),
    renderSelectedJobHint: (...args) => savedActivityController.renderSelectedJobHint(...args),
    updateTimelineScopeButtons: (...args) => savedActivityController.updateTimelineScopeButtons(...args),
    setSavedFilterBarVisible: (...args) => deps.setSavedFilterBarVisible(...args),
    setSavedSortBarVisible: (...args) => deps.setSavedSortBarVisible(...args),
    renderSavedFilterMeta: (...args) => deps.renderSavedFilterMeta(...args),
    renderReminderCounter: (...args) => deps.renderReminderCounter(...args),
    hydrateAttachmentLists: (...args) => savedAttachmentsController.hydrateAttachmentLists(...args),
    bindAttachmentActionButtons: (...args) => savedAttachmentsController.bindAttachmentActionButtons(...args),
    renderSavedJobBlockHtml: deps.renderSavedJobBlockHtml,
    parseIsoDate: deps.parseIsoDate,
    getReminderMeta: deps.getReminderMeta,
    formatRelativeTime: deps.formatRelativeTime,
    getJobHistoryEntries: deps.getJobHistoryEntries,
    renderPhaseBar: deps.renderPhaseBar,
    renderWebIcon: deps.renderWebIcon,
    formatPhaseTimestamp: deps.formatPhaseTimestamp,
    renderDetailsSummary: deps.renderDetailsSummary,
    activityTypeLabel: deps.activityTypeLabel,
    formatActivityDetail: deps.formatActivityDetail
  });

  const savedAuthReadyPoller = createAuthReadyPoller({
    isReady: () => deps.savedPageService.isAvailable() && deps.isSavedApiReady(),
    onReady: () => savedAuthController.initSavedJobsPage()
  });

  savedAuthController = createSavedAuthController({
    refs: dom,
    viewState,
    savedPageService: deps.savedPageService,
    savedAuthService: deps.savedAuthService,
    savedAuthReadyPoller,
    isSavedApiReady: deps.isSavedApiReady,
    savedDispatch,
    SAVED_ACTIONS: deps.savedActions,
    clearNoteSaveQueues: (...args) => deps.clearNoteSaveQueues(...args),
    setActivityPanelOpen: (...args) => savedActivityController.setActivityPanelOpen(...args),
    setCustomJobPanelOpen: (...args) => savedCustomJobController.setCustomJobPanelOpen(...args),
    setCustomJobAvailability: (...args) => savedCustomJobController.setCustomJobAvailability(...args),
    updateTimelineScopeButtons: (...args) => savedActivityController.updateTimelineScopeButtons(...args),
    renderWorkspaceStats: (...args) => savedActivityController.renderWorkspaceStats(...args),
    emitSavedStartupMetric: (...args) => deps.emitSavedStartupMetric(...args),
    setSourceStatus: (...args) => deps.setSourceStatus(...args),
    setActivityStatus: (...args) => deps.setActivityStatus(...args),
    renderAuthRequired: message => savedRenderController.renderAuthRequired(message),
    renderTimeline: (...args) => savedActivityController.renderTimeline(...args),
    markSavedFirstInteractive: (...args) => deps.markSavedFirstInteractive(...args),
    setSavedFilter: (...args) => deps.setSavedFilter(...args),
    defaultSavedFilter: deps.defaultSavedFilter,
    setSavedSort: (...args) => deps.setSavedSort(...args),
    defaultSavedSort: deps.defaultSavedSort,
    renderSelectedJobHint: (...args) => savedActivityController.renderSelectedJobHint(...args),
    setBackupButtonsEnabled: (...args) => deps.setBackupButtonsEnabled(...args),
    setSavedFilterBarVisible: (...args) => deps.setSavedFilterBarVisible(...args),
    setSavedSortBarVisible: (...args) => deps.setSavedSortBarVisible(...args),
    loadTimelinePreferences: uid => savedActivityController.loadTimelinePreferences(uid),
    subscribeToSavedJobs: (...args) => deps.subscribeToSavedJobs(...args),
    refreshActivityLog: (...args) => savedActivityController.refreshActivityLog(...args),
    timelineScopeAll: deps.timelineScopeAll,
    showToast
  });

  function applySavedAdminBridgeState(params) {
    return deps.applySavedAdminBridgeStateFromModule({ ...params, viewState });
  }

  function loadSavedLifecycleOverlay() {
    return loadSavedLifecycleOverlayByJobKey();
  }

  return {
    startupMetrics,
    dom,
    viewState,
    noteSaveState,
    attachmentPreviewUrls,
    savedDispatch,
    savedAuthController,
    applySavedAdminBridgeState,
    loadSavedLifecycleOverlay,
    renderAuthRequired: message => savedRenderController.renderAuthRequired(message),
    renderSavedJobs: jobs => savedRenderController.renderSavedJobs(jobs),
    getJobDetailsTab: jobKey => savedRenderController.getJobDetailsTab(jobKey),
    setJobDetailsTab: (jobKey, tab) => savedRenderController.setJobDetailsTab(jobKey, tab),
    normalizePhase: phase => savedRenderController.normalizePhase(phase),
    canTransition: (currentPhase, nextPhase) => savedRenderController.canTransition(currentPhase, nextPhase),
    setSelectedJobKey: (jobKey, options = {}) => savedRenderController.setSelectedJobKey(jobKey, options),
    toggleDetailsForJob: jobKey => savedRenderController.toggleDetailsForJob(jobKey),
    applyDetailsAccordion: () => savedRenderController.applyDetailsAccordion(),
    setNoteSaveState: (jobKey, state) => savedRenderController.setNoteSaveState(jobKey, state),
    hydrateAttachmentLists: jobs => savedAttachmentsController.hydrateAttachmentLists(jobs),
    uploadAttachments: (jobKey, files) => savedAttachmentsController.uploadAttachments(jobKey, files),
    renderAttachmentList: (jobKey, attachments) => savedAttachmentsController.renderAttachmentList(jobKey, attachments),
    bindAttachmentActionButtons: () => savedAttachmentsController.bindAttachmentActionButtons(),
    applyJobDetailsTab: (jobKey, tab) => savedRenderController.applyJobDetailsTab(jobKey, tab),
    updateCustomJobWarning: () => savedCustomJobController.updateCustomJobWarning(),
    setCustomJobAvailability: enabled => savedCustomJobController.setCustomJobAvailability(enabled),
    setCustomJobPanelOpen: open => savedCustomJobController.setCustomJobPanelOpen(open),
    openCustomJobEditor: (jobKey, duplicate) => savedCustomJobController.openCustomJobEditor(jobKey, duplicate),
    createCustomJob: () => savedCustomJobController.createCustomJob(),
    setActivityPanelOpen: (open, options = {}) => savedActivityController.setActivityPanelOpen(open, options),
    buildTimelinePrefsKey: uid => savedActivityController.buildTimelinePrefsKey(uid),
    loadTimelinePreferences: uid => savedActivityController.loadTimelinePreferences(uid),
    setTimelineScope: nextScope => savedActivityController.setTimelineScope(nextScope),
    updateTimelineScopeButtons: () => savedActivityController.updateTimelineScopeButtons(),
    queueActivityPulse: (jobKey, category) => savedActivityController.queueActivityPulse(jobKey, category),
    clearExpiredPulse: () => savedActivityController.clearExpiredPulse(),
    renderSelectedJobHint: () => savedActivityController.renderSelectedJobHint(),
    renderWorkspaceStats: (jobs = null) => savedActivityController.renderWorkspaceStats(jobs),
    refreshActivityLog: () => savedActivityController.refreshActivityLog(),
    renderTimeline: () => savedActivityController.renderTimeline()
  };
}
