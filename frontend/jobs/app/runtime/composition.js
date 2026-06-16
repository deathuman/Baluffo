import { emitStartupMetric, logError, logInfo, markFirstInteractive } from "../../../shared/app-boot.js";
import { createAuthReadyPoller } from "../../../shared/auth-ready-poll.js";
import { createPerfMarks } from "../../../shared/perf-marks.js";
import { navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { bindAsyncClick, bindHandlersMap, bindUi, escapeHtml, setText, showToast } from "../../../shared/ui/index.js";
import { UI_TOKENS, ui } from "../../../shared/ui/selectors.js";
import { normalizeToken } from "../../../shared/text-utils.js";
import { debounce } from "../runtime-utils.js";
import { createJobsBridgeRequest } from "./actions.js";
import { createJobsAuthController } from "./auth-controller.js";
import { createJobsEventsController } from "./events.js";
import { createJobsFeedController } from "./feed-controller.js";
import { createJobsFiltersController } from "./filters-ui.js?v=8";
import { setupJobsListDelegation as setupJobsListDelegationFromEvents } from "./jobs-list-events.js";
import { createJobsPipelineController } from "./pipeline-controller.js?v=12";
import { createJobsStartupPreviewController } from "./startup-preview.js";
import { createJobsStartupMetrics } from "./effects.js";
import { createJobsRuntimeState } from "./state.js?v=2";
import { createJobsUrlPersistence } from "./url-persistence.js?v=5";
import { sortJobs as sortJobsFromQuery } from "./query.js?v=6";

const JOBS_LOG_SCOPE = "jobs";

export function composeJobsRuntime(deps) {
  const jobsRuntime = createJobsRuntimeState(deps.defaultFilters, {
    lastHandledAutoRefreshSignalId: deps.lastHandledAutoRefreshSignalId
  });
  const state = jobsRuntime.pageState;
  const userState = jobsRuntime.userState;
  const jobsPipelineUiState = jobsRuntime.pipelineUiState;
  const runtimeState = jobsRuntime.runtimeState;
  const dom = {};
  const jobsControllerRefs = dom;
  const jobsDispatch = deps.createJobsDispatcher();

  function isDesktopRuntimeMode() {
    return deps.isDesktopRuntimeModeFromStartup(deps.windowObject.location.href);
  }

  const startupMetrics = createJobsStartupMetrics({
    emitMetric: (event, payload) => {
      if (!isDesktopRuntimeMode()) return;
      deps.postStartupMetricToBridge(String(event || "").trim() || "unknown", payload);
    }
  });
  const jobsPerfMarks = createPerfMarks(startupMetrics);

  function emitDesktopStartupMetric(event, payload = {}) {
    emitStartupMetric(startupMetrics, event, payload);
  }

  function markStartupRendered(stage, rowCount) {
    startupMetrics.markRendered(stage, rowCount);
  }

  function logJobsInfo(message, ...args) {
    logInfo(JOBS_LOG_SCOPE, message, ...args);
  }

  function logJobsError(message, err) {
    logError(JOBS_LOG_SCOPE, message, err);
  }

  function markJobsFirstInteractive(reason) {
    markFirstInteractive(startupMetrics, reason);
    deps.setJobsStartupState("interactive", String(reason || "interactive"));
    runtimeState.desktopUrlStateReady = true;
    jobsUrlPersistence.flushDesktopPendingJobsUrlState();
  }

  function applyJobsAdminBridgeState({ buttonEl, state: nextState, label, title }) {
    deps.applyJobsAdminBridgeStateFromModule({
      buttonEl,
      state: nextState,
      label,
      title,
      runtimeState
    });
  }

  function getJobKeyForJobWithService(job) {
    return deps.getJobKeyForJob(job, {
      generateJobKey: row => deps.jobsPageService.generateJobKey(row)
    });
  }

  let authController;
  let feedController;

  const authReadyPoller = createAuthReadyPoller({
    isReady: () => deps.isJobsApiReady() && deps.jobsPageService.isAvailable(),
    onReady: () => authController.initAuth()
  });

  const filtersController = createJobsFiltersController({
    refs: jobsControllerRefs,
    state,
    defaultFilters: deps.defaultFilters,
    quickFilters: deps.quickFilters,
    professionLabels: deps.professionLabels,
    jobsDispatch,
    JOBS_ACTIONS: deps.jobsActions,
    applyFiltersAndRender: (...args) => deps.applyFiltersAndRender(...args),
    buildFilterOptions: deps.buildFilterOptions,
    getJobLocationCities: deps.getJobLocationCities,
    getJobLocationCountries: deps.getJobLocationCountries,
    isValidCountry: deps.isValidCountry,
    isSemanticallyValidLocationValue: deps.isSemanticallyValidLocationValue,
    isCityFilterEligible: deps.isCityFilterEligible,
    readQuickFilterPreferences: deps.readQuickFilterPreferences,
    writeQuickFilterPreferences: deps.writeQuickFilterPreferences,
    QUICK_FILTER_PREFS_KEY: deps.quickFilterPrefsKey,
    escapeHtml,
    normalizeLifecycleStatus: deps.normalizeLifecycleStatus
  });

  const callJobsBridge = createJobsBridgeRequest({
    baseUrl: deps.adminBridgeBase,
    timeoutMs: deps.bridgeTimeoutMs,
    request: deps.callJobsBridgeFromModule
  });

  authController = createJobsAuthController({
    refs: jobsControllerRefs,
    userState,
    authReadyPoller,
    jobsAuthService: deps.jobsAuthService,
    jobsSavedJobsService: deps.jobsSavedJobsService,
    jobsPageService: deps.jobsPageService,
    jobsDispatch,
    JOBS_ACTIONS: deps.jobsActions,
    isJobsApiReady: deps.isJobsApiReady,
    emitDesktopStartupMetric,
    showToast,
    logJobsError,
    getAllJobs: () => runtimeState.allJobs,
    applyFiltersAndRender: (...args) => deps.applyFiltersAndRender(...args),
    getSkipInitialGuestAuthRerender: () => runtimeState.skipInitialGuestAuthRerender,
    setSkipInitialGuestAuthRerender: value => {
      runtimeState.skipInitialGuestAuthRerender = Boolean(value);
    },
    loadSeenJobKeys: deps.loadSeenJobKeys,
    markSeenJob: deps.markSeenJob,
    buildSeenRowKey: deps.buildSeenRowKey,
    getJobKeyForJob: getJobKeyForJobWithService,
    openJobsCacheDb: () => feedController.openJobsCacheDb(),
    JOBS_SEEN_STORE: deps.jobsSeenStore,
    toJobSnapshot: deps.toJobSnapshot,
    sanitizeUrl: deps.sanitizeUrl
  });

  const pipelineController = createJobsPipelineController({
    refs: jobsControllerRefs,
    jobsPipelineUiState,
    callJobsBridge,
    getAllJobs: () => runtimeState.allJobs,
    showToast,
    setRefreshJobsNeedsAttention: needsRefresh => feedController.setRefreshJobsNeedsAttention(needsRefresh),
    refreshJobsAfterPipelineCompletion: () => feedController.refreshJobsNow({ manual: false }),
    isErrorStage: payload => Boolean(payload?.error) || normalizeToken(payload?.stage) === "error",
    pollDelayMs: deps.pipelineStatusPollMs,
    idlePollDelayMs: deps.pipelineStatusIdlePollMs,
    isContainerRuntimeMode: () => Boolean(deps.isContainerRuntimeMode?.())
  });

  const startupPreviewController = createJobsStartupPreviewController({
    runtimeState,
    pageState: state,
    displayJobs: (...args) => deps.displayJobs(...args),
    createFilterOptionsAccumulator: deps.createFilterOptionsAccumulator,
    addJobToFilterOptions: deps.addJobToFilterOptions,
    finalizeFilterOptions: deps.finalizeFilterOptions,
    compareJobsForSort: deps.compareJobsForSort,
    sortJobs: sortJobsFromQuery,
    getJobLocationCities: deps.getJobLocationCities,
    getJobLocationCountries: deps.getJobLocationCountries,
    isSemanticallyValidLocationValue: deps.isSemanticallyValidLocationValue,
    isValidCountry: deps.isValidCountry,
    getAvailableRegionOptions: deps.getAvailableRegionOptions,
    fullCountryName: deps.fullCountryName
  });

  const eventsController = createJobsEventsController({
    dom,
    pageState: state,
    runtimeState,
    filtersController,
    authController,
    rememberCurrentJobsUrl: (...args) => deps.rememberCurrentJobsUrl(...args),
    navigateDesktopPage,
    openAdminPageFromJobs: (...args) => deps.openAdminPageFromJobs(...args),
    refreshJobsNow: (...args) => feedController.refreshJobsNow(...args),
    triggerJobsPipelineRun: (...args) => deps.triggerJobsPipelineRun(...args),
    handleAutoRefreshSignalValue: (...args) => deps.handleAutoRefreshSignalValue(...args),
    renderDataSources: (...args) => feedController.renderDataSources(...args),
    applyFiltersAndRender: (...args) => deps.applyFiltersAndRender(...args),
    bindUi,
    bindAsyncClick,
    bindHandlersMap,
    debounce,
    jobsAutoRefreshSignalKey: deps.jobsAutoRefreshSignalKey,
    jobsListDelegation: () => setupJobsListDelegationFromEvents({
      jobsList: dom.jobsList,
      jobRowSelector: `${ui(UI_TOKENS.jobs.jobRow)}[data-job-link]`,
      saveJobBtnSelector: ui(UI_TOKENS.jobs.saveJobBtn),
      sanitizeUrl: deps.sanitizeUrl,
      getJobById: jobId => runtimeState.allJobs.find(job => String(job.id) === String(jobId || "")),
      onToggleSaveJob: job => authController.toggleSaveJob(job),
      onOpenJobLink: deps.openJobLinkInDefaultBrowser,
      onMarkJobSeen: jobKey => authController.markJobSeenFromInteraction(jobKey)
    }),
    goToPage: (...args) => deps.goToPage(...args),
    windowObject: deps.windowObject,
    documentObject: deps.documentObject
  });

  feedController = createJobsFeedController({
    dom,
    runtimeState,
    pageState: state,
    defaultFilters: deps.defaultFilters,
    professionLabels: deps.professionLabels,
    sanitizeUrl: deps.sanitizeUrl,
    jobsParsing: deps.jobsParsing,
    startupPreviewJsonUrls: deps.startupPreviewJsonUrls,
    jobsDispatch,
    jobsActions: deps.jobsActions,
    filtersController,
    showToast,
    emitDesktopStartupMetric,
    markJobsStep: jobsPerfMarks.markStep,
    measureJobsStep: jobsPerfMarks.measureStep,
    markStartupRendered,
    markJobsFirstInteractive,
    applyFiltersAndRender: (...args) => deps.applyFiltersAndRender(...args),
    isDesktopRuntimeMode,
    isContainerRuntimeMode: () => Boolean(deps.isContainerRuntimeMode?.()),
    logJobsError,
    logJobsInfo,
    getJobsLastUpdatedText: deps.getJobsLastUpdatedText,
    normalizeJobs: deps.normalizeJobs,
    parseUnifiedJobsPayload: deps.parseUnifiedJobsPayload,
    openJobsCacheDbFromModule: deps.openJobsCacheDbFromModule,
    readJobsCache: deps.readJobsCache,
    writeJobsCache: deps.writeJobsCache,
    refreshJobsFeed: deps.refreshJobsFeed,
    loadStartupPreviewJobsFeed: deps.loadStartupPreviewJobsFeed,
    fetchUnifiedJobsFromSources: deps.fetchUnifiedJobsFromSources,
    fetchJsonFromCandidatesFromSources: deps.fetchJsonFromCandidatesFromSources,
    renderDataSourcesFromSources: deps.renderDataSourcesFromSources,
    jobsFetchReportUrls: deps.jobsFetchReportUrls,
    mapProfession: deps.mapProfession,
    normalizeSector: deps.normalizeSector,
    classifyCompanyType: deps.classifyCompanyType,
    detectWorkType: deps.detectWorkType,
    setProgressVisibility: deps.setProgressVisibility,
    setStatusText: deps.setStatusText,
    setText,
    jobsCacheDb: deps.jobsCacheDb,
    jobsCacheDbVersion: deps.jobsCacheDbVersion,
    jobsCacheStore: deps.jobsCacheStore,
    jobsSeenStore: deps.jobsSeenStore,
    jobsCacheKey: deps.jobsCacheKey,
    jobsFirstLoadRequestTimeoutMs: deps.jobsFirstLoadRequestTimeoutMs,
    recalculateItemsPerPage: (...args) => eventsController.recalculateItemsPerPage(...args),
    startupPreviewController
  });

  const jobsUrlPersistence = createJobsUrlPersistence({
    windowObject: deps.windowObject,
    buildJobsPageUrl: deps.buildJobsPageUrl,
    resolveStartupProbeEnabled: deps.resolveStartupProbeEnabled,
    isDesktopRuntimeMode: () => isDesktopRuntimeMode(),
    rememberJobsUrl: deps.rememberJobsUrl,
    emitMetric: (event, payload = {}) => emitDesktopStartupMetric(event, payload),
    getDesktopUrlStateReady: () => runtimeState.desktopUrlStateReady,
    getDesktopPendingRememberJobsUrl: () => runtimeState.desktopPendingRememberJobsUrl,
    setDesktopPendingRememberJobsUrl: value => {
      runtimeState.desktopPendingRememberJobsUrl = Boolean(value);
    },
    getDesktopPendingJobsUrl: () => runtimeState.desktopPendingJobsUrl,
    setDesktopPendingJobsUrl: value => {
      runtimeState.desktopPendingJobsUrl = String(value || "");
    },
    lastUrlKey: deps.jobsLastUrlKey
  });

  return {
    state,
    userState,
    runtimeState,
    dom,
    filtersController,
    authController,
    pipelineController,
    startupPreviewController,
    eventsController,
    feedController,
    jobsUrlPersistence,
    callJobsBridge,
    isDesktopRuntimeMode,
    emitDesktopStartupMetric,
    markStartupRendered,
    markJobsFirstInteractive,
    logJobsError,
    applyJobsAdminBridgeState,
    getJobKeyForJobWithService
  };
}
