import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js?v=2";
import { JobsStateModule as jobsStateModule } from "../state.js?v=4";
import { postStartupMetricToBridge, resolveStartupProbeEnabled } from "../../../probes/startup-probe.js";
import {
  BaluffoJobsParsing as jobsParsing,
  parseUnifiedJobsPayload
} from "../parsing-utils.js";
import {
  detectWorkType,
  normalizeSector,
  classifyCompanyType,
  mapProfession,
  isInternshipJob,
  isValidCountry,
  isSemanticallyValidLocationValue,
  getJobLocationCities,
  getJobLocationCountries,
  normalizeJobs,
  getJobKeyForJob,
  toJobSnapshot
} from "../domain.js";
import { isJobsApiReady, jobsAuthService, jobsSavedJobsService, jobsPageService } from "../services.js";
import { createJobsDispatcher, JOBS_ACTIONS } from "../actions.js";
import { renderJobRowHtml, showJobsError } from "../render.js?v=5";
import { sanitizeUrl } from "./runtime-utils.js";
import {
  readAutoRefreshAppliedId,
  readAutoRefreshSignal,
  writeAutoRefreshAppliedId,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  rememberJobsUrl
} from "../state-sync/index.js";
import { requestConfirmationDialog } from "../../local-data/profile-name-dialog.js";
import { callJobsBridge as callJobsBridgeFromModule } from "./pipeline.js?v=8";
import { openFirstRunJobsNotice } from "./first-run-notice.js?v=1";
import { applyJobsAdminBridgeState as applyJobsAdminBridgeStateFromModule } from "./admin-bridge-state.js?v=5";
import {
  buildSeenRowKey,
  openJobsCacheDb as openJobsCacheDbFromModule,
  readJobsCache,
  writeJobsCache,
  loadSeenJobKeys,
  markSeenJob,
  isJobsCacheStale
} from "./cache.js";
import { normalizeLifecycleStatus } from "./filters.js?v=6";
import {
  isDesktopRuntimeMode as isDesktopRuntimeModeFromStartup,
  buildJobsPageUrl,
  getJobsLastUpdatedText
} from "./startup.js";
import {
  addJobToFilterOptions,
  buildFilterOptions,
  compareJobsForSort,
  createFilterOptionsAccumulator,
  finalizeFilterOptions
} from "./runtime/query.js?v=4";
import { refreshJobsFeed, loadStartupPreviewJobsFeed } from "./feed.js?v=9";
import { setProgressVisibility, setStatusText } from "./runtime/view.js";
import {
  fullCountryName as fullCountryNameForJobs,
  getAvailableRegionOptions as getAvailableRegionOptionsForJobs
} from "./countries.js";
import {
  STARTUP_PREVIEW_JSON_URLS,
  JOBS_FETCH_REPORT_URLS,
  fetchUnifiedJobs as fetchUnifiedJobsFromSources,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromSources,
  renderDataSources as renderDataSourcesFromSources
} from "./sources.js";
import { composeJobsRuntime } from "./runtime/composition.js?v=9";
import { createJobsBoot } from "./runtime/boot.js?v=2";
import { createJobsPageFlow } from "./runtime/page-flow.js?v=5";

const defaultFilters = jobsStateModule.DEFAULT_FILTERS || {
  workType: "",
  lifecycleStatus: "active",
  countries: [],
  city: "",
  sector: "",
  profession: "",
  newOnly: false,
  excludeInternship: false,
  search: "",
  sort: "relevance"
};

const JOBS_CACHE_DB = "baluffo_jobs_cache";
const JOBS_CACHE_DB_VERSION = 2;
const JOBS_CACHE_STORE = "jobs_feed";
const JOBS_SEEN_STORE = "jobs_seen";
const JOBS_CACHE_KEY = "latest";
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const JOBS_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const JOBS_AUTO_REFRESH_SIGNAL_KEY = "baluffo_jobs_auto_refresh_signal";
const JOBS_AUTO_REFRESH_APPLIED_KEY = "baluffo_jobs_auto_refresh_applied";
const QUICK_FILTER_PREFS_KEY = "baluffo_quick_filter_prefs";
const JOBS_PIPELINE_STATUS_POLL_MS = 1500;
const JOBS_PIPELINE_STATUS_IDLE_POLL_MS = 5000;
const JOBS_BRIDGE_REQUEST_TIMEOUT_MS = 1800;
const JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS = 4500;

const PROFESSION_LABELS = jobsStateModule.PROFESSION_LABELS || {};
const QUICK_FILTERS = Array.isArray(jobsStateModule.QUICK_FILTERS) ? jobsStateModule.QUICK_FILTERS : [];
const windowObject = typeof window === "undefined"
  ? (globalThis.window || { location: { href: "" } })
  : window;
const documentObject = typeof document === "undefined"
  ? (globalThis.document || null)
  : document;

let jobsPageFlow;
let jobsBoot;

const jobsRuntime = composeJobsRuntime({
  defaultFilters,
  lastHandledAutoRefreshSignalId: readAutoRefreshAppliedId(JOBS_AUTO_REFRESH_APPLIED_KEY),
  quickFilters: QUICK_FILTERS,
  professionLabels: PROFESSION_LABELS,
  createJobsDispatcher,
  jobsActions: JOBS_ACTIONS,
  adminBridgeBase: adminConfig.ADMIN_BRIDGE_BASE,
  bridgeTimeoutMs: JOBS_BRIDGE_REQUEST_TIMEOUT_MS,
  isDesktopRuntimeModeFromStartup,
  postStartupMetricToBridge,
  callJobsBridgeFromModule,
  jobsAuthService,
  jobsSavedJobsService,
  jobsPageService,
  isJobsApiReady,
  buildFilterOptions,
  getJobLocationCities,
  getJobLocationCountries,
  isValidCountry,
  isSemanticallyValidLocationValue,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  quickFilterPrefsKey: QUICK_FILTER_PREFS_KEY,
  loadSeenJobKeys,
  markSeenJob,
  buildSeenRowKey,
  jobsSeenStore: JOBS_SEEN_STORE,
  toJobSnapshot,
  sanitizeUrl,
  pipelineStatusPollMs: JOBS_PIPELINE_STATUS_POLL_MS,
  pipelineStatusIdlePollMs: JOBS_PIPELINE_STATUS_IDLE_POLL_MS,
  createFilterOptionsAccumulator,
  addJobToFilterOptions,
  finalizeFilterOptions,
  compareJobsForSort,
  getAvailableRegionOptions: getAvailableRegionOptionsForJobs,
  fullCountryName: fullCountryNameForJobs,
  jobsAutoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
  openJobLinkInDefaultBrowser: (...args) => openJobLinkInDefaultBrowser(...args),
  windowObject,
  documentObject,
  setJobsStartupState: (...args) => jobsPageFlow.setJobsStartupState(...args),
  applyFiltersAndRender: (...args) => jobsPageFlow.applyFiltersAndRender(...args),
  rememberCurrentJobsUrl: (...args) => jobsPageFlow.rememberCurrentJobsUrl(...args),
  openAdminPageFromJobs: (...args) => jobsBoot.openAdminPageFromJobs(...args),
  triggerJobsPipelineRun: (...args) => jobsPageFlow.triggerJobsPipelineRun(...args),
  handleAutoRefreshSignalValue: (...args) => jobsPageFlow.handleAutoRefreshSignalValue(...args),
  goToPage: (...args) => jobsPageFlow.goToPage(...args),
  displayJobs: (...args) => jobsPageFlow.displayJobs(...args),
  jobsParsing,
  startupPreviewJsonUrls: STARTUP_PREVIEW_JSON_URLS,
  jobsFetchReportUrls: JOBS_FETCH_REPORT_URLS,
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
  getJobsLastUpdatedText,
  normalizeJobs,
  jobsCacheDb: JOBS_CACHE_DB,
  jobsCacheDbVersion: JOBS_CACHE_DB_VERSION,
  jobsCacheStore: JOBS_CACHE_STORE,
  jobsCacheKey: JOBS_CACHE_KEY,
  jobsFirstLoadRequestTimeoutMs: JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS,
  buildJobsPageUrl,
  resolveStartupProbeEnabled,
  rememberJobsUrl,
  jobsLastUrlKey: JOBS_LAST_URL_KEY,
  applyJobsAdminBridgeStateFromModule,
  getJobKeyForJob,
  normalizeLifecycleStatus
});

jobsPageFlow = createJobsPageFlow({
  defaultFilters,
  jobsAutoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
  jobsAutoRefreshAppliedKey: JOBS_AUTO_REFRESH_APPLIED_KEY,
  state: jobsRuntime.state,
  userState: jobsRuntime.userState,
  runtimeState: jobsRuntime.runtimeState,
  dom: jobsRuntime.dom,
  filtersController: jobsRuntime.filtersController,
  startupPreviewController: jobsRuntime.startupPreviewController,
  feedController: jobsRuntime.feedController,
  pipelineController: jobsRuntime.pipelineController,
  jobsUrlPersistence: jobsRuntime.jobsUrlPersistence,
  getJobKeyForJob: jobsRuntime.getJobKeyForJobWithService,
  isJobsApiReady,
  renderJobRowHtml,
  showJobsError,
  emitDesktopStartupMetric: jobsRuntime.emitDesktopStartupMetric,
  logJobsError: jobsRuntime.logJobsError,
  readAutoRefreshSignal,
  writeAutoRefreshAppliedId,
  normalizeLifecycleStatus,
  getJobLocationCities,
  getJobLocationCountries,
  isInternshipJob,
  fullCountryName: fullCountryNameForJobs,
  retryInit: (...args) => jobsBoot.init(...args),
  windowObject,
  documentObject
});

jobsBoot = createJobsBoot({
  adminBridgeBase: adminConfig.ADMIN_BRIDGE_BASE,
  dom: jobsRuntime.dom,
  runtimeState: jobsRuntime.runtimeState,
  feedController: jobsRuntime.feedController,
  eventsController: jobsRuntime.eventsController,
  filtersController: jobsRuntime.filtersController,
  authController: jobsRuntime.authController,
  normalizeJobs,
  professionLabels: PROFESSION_LABELS,
  sanitizeUrl,
  emitDesktopStartupMetric: jobsRuntime.emitDesktopStartupMetric,
  markStartupRendered: jobsRuntime.markStartupRendered,
  markJobsFirstInteractive: jobsRuntime.markJobsFirstInteractive,
  isDesktopRuntimeMode: jobsRuntime.isDesktopRuntimeMode,
  desktopJobsColdStart: Boolean(adminConfig.DESKTOP_JOBS_COLD_START),
  isJobsCacheStale,
  jobsCacheTtlMs: JOBS_CACHE_TTL_MS,
  applyPendingAutoRefreshSignal: (...args) => jobsPageFlow.applyPendingAutoRefreshSignal(...args),
  applyFiltersAndRender: (...args) => jobsPageFlow.applyFiltersAndRender(...args),
  showError: (...args) => jobsPageFlow.showError(...args),
  handleJobsStartupFailure: (...args) => jobsPageFlow.handleJobsStartupFailure(...args),
  setJobsStartupState: (...args) => jobsPageFlow.setJobsStartupState(...args),
  readStateFromUrl: (...args) => jobsPageFlow.readStateFromUrl(...args),
  rememberCurrentJobsUrl: (...args) => jobsPageFlow.rememberCurrentJobsUrl(...args),
  ensureJobsPipelineStatusWatch: (...args) => jobsPageFlow.ensureJobsPipelineStatusWatch(...args),
  callJobsBridge: (...args) => jobsRuntime.callJobsBridge(...args),
  applyJobsAdminBridgeState: jobsRuntime.applyJobsAdminBridgeState,
  logJobsError: jobsRuntime.logJobsError,
  openJobLinkInDefaultBrowser: (...args) => openJobLinkInDefaultBrowser(...args),
  requestConfirmationDialog,
  showFirstRunBootstrapNotice: options => openFirstRunJobsNotice({
    ...options,
    documentTarget: documentObject,
    windowTarget: windowObject
  }),
  windowObject,
  documentObject
});

export async function openJobLinkInDefaultBrowser(url, deps = {}) {
  if (!url) return;
  const isDesktop = typeof deps.isDesktopRuntimeMode === "function"
    ? deps.isDesktopRuntimeMode
    : jobsRuntime.isDesktopRuntimeMode;
  const openWindow = typeof deps.openWindow === "function"
    ? deps.openWindow
    : target => windowObject?.open?.(target, "_blank", "noopener,noreferrer");
  const callBridge = typeof deps.callJobsBridge === "function"
    ? deps.callJobsBridge
    : jobsRuntime.callJobsBridge;
  const logJobsError = typeof deps.logJobsError === "function"
    ? deps.logJobsError
    : jobsRuntime.logJobsError;
  if (!isDesktop()) {
    openWindow(url);
    return;
  }
  try {
    await callBridge("/desktop-local-data/open-url", {
      method: "POST",
      body: { url }
    });
  } catch (err) {
    logJobsError("Failed to open job link in the default browser", err);
  }
}

globalThis.__baluffoBootJobsPage = () => jobsBoot.bootJobsPage();
