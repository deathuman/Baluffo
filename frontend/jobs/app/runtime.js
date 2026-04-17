import { JobsStateModule as jobsStateModule } from "../state.js";
import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import {
  postStartupMetricToBridge,
  resolveStartupProbeEnabled
} from "../../../probes/startup-probe.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick,
  bindHandlersMap
} from "../../shared/ui/index.js";
import { emitStartupMetric, logError, logInfo, markFirstInteractive } from "../../shared/app-boot.js";
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
import { renderJobRowHtml, showJobsLoading, showJobsError } from "../render.js";
import { debounce, sanitizeUrl } from "./runtime-utils.js";
import {
  readAutoRefreshAppliedId,
  readAutoRefreshSignal,
  writeAutoRefreshAppliedId,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  rememberJobsUrl
} from "../state-sync/index.js";
import { requestConfirmationDialog } from "../../local-data/profile-name-dialog.js";
import { navigateDesktopPage } from "../../shared/local-data/desktop-client.js";
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { fetchJson, postJson } from "../../shared/api-client.js";
import { createAdminBridgeButtonWatcher } from "../../shared/admin-bridge-button.js";
import { createAuthReadyPoller } from "../../shared/auth-ready-poll.js";
import { normalizeToken } from "../../shared/text-utils.js";
import { cacheJobsDom } from "./dom.js";
import { createJobsDesktopUpdateController } from "./desktop-update.js";
import { openReleaseNotesDialog } from "../../shared/ui/release-notes-dialog.js";
import { callJobsBridge as callJobsBridgeFromModule } from "./pipeline.js";
import { applyJobsAdminBridgeState as applyJobsAdminBridgeStateFromModule } from "./admin-bridge-state.js";
import {
  buildSeenRowKey,
  openJobsCacheDb as openJobsCacheDbFromModule,
  readJobsCache,
  writeJobsCache,
  loadSeenJobKeys,
  markSeenJob,
  isJobsCacheStale
} from "./cache.js";
import {
  normalizeLifecycleStatus,
} from "./filters.js";
import {
  isDesktopRuntimeMode as isDesktopRuntimeModeFromStartup,
  scheduleNonCriticalStartup,
  parseJobsPageUrlState,
  buildJobsPageUrl,
  getJobsLastUpdatedText,
  parseAutoRefreshSignal as parseAutoRefreshSignalFromStartup,
  getAutoRefreshStatusText
} from "./startup.js";
import {
  addJobToFilterOptions,
  buildFilterOptions,
  compareJobsForSort,
  createFilterOptionsAccumulator,
  finalizeFilterOptions,
  filterJobs,
  sortJobs as sortJobsFromQuery
} from "./runtime/query.js";
import {
  displayJobs as displayJobsFromView,
  goToPage as goToPageFromView,
  updateResultsSummary as updateResultsSummaryFromView
} from "./runtime/list-view.js";
import { setupJobsListDelegation as setupJobsListDelegationFromEvents } from "./runtime/jobs-list-events.js";
import { createJobsUrlPersistence } from "./runtime/url-persistence.js";
import {
  initJobsFeed,
  refreshJobsFeed,
  loadStartupPreviewJobsFeed,
  handleJobsAutoRefreshSignalValue,
  applyPendingJobsAutoRefreshSignal,
  triggerJobsAutoRefreshFromSignal
} from "./feed.js";
import { createJobsRuntimeState } from "./runtime/state.js";
import { createJobsStartupMetrics } from "./runtime/effects.js";
import { createJobsBridgeRequest } from "./runtime/actions.js";
import { setProgressVisibility, setStatusText } from "./runtime/view.js";
import { bindWindowResize } from "./runtime/events.js";
import { createJobsAuthController } from "./runtime/auth-controller.js";
import { createJobsPipelineController } from "./runtime/pipeline-controller.js";
import { createJobsFiltersController } from "./runtime/filters-ui.js";
import {
  fullCountryName as fullCountryNameForJobs,
  getAvailableRegionOptions as getAvailableRegionOptionsForJobs,
  matchesCountrySelection as matchesCountrySelectionForJobs
} from "./countries.js";
import {
  STARTUP_PREVIEW_JSON_URLS,
  fetchUnifiedJobs as fetchUnifiedJobsFromSources,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromSources,
  renderDataSources as renderDataSourcesFromSources
} from "./sources.js";
const JOBS_LOG_SCOPE = "jobs";
/**
 * @typedef {Object} JobRow
 * @property {string} title
 * @property {string} company
 * @property {string} city
 * @property {string} country
 * @property {string} workType
 * @property {string} contractType
 * @property {string} jobLink
 * @property {string} sector
 * @property {string} profession
 */
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

/**
 * @typedef {Object} JobsFilterState
 * @property {string} workType
 * @property {string} lifecycleStatus
 * @property {string[]} countries
 * @property {string} city
 * @property {string} sector
 * @property {string} profession
 * @property {boolean} newOnly
 * @property {boolean} excludeInternship
 * @property {string} search
 * @property {string} sort
 */

/**
 * @typedef {Object} JobsPageState
 * @property {number} currentPage
 * @property {number} itemsPerPage
 * @property {JobsFilterState} filters
 */

/**
 * @typedef {Object} JobsAuthViewModel
 * @property {string} label
 * @property {string} hint
 */

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
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const JOBS_PIPELINE_STATUS_POLL_MS = 1500;
const JOBS_PIPELINE_STATUS_IDLE_POLL_MS = 5000;
const JOBS_BRIDGE_REQUEST_TIMEOUT_MS = 1800;
const JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS = 4500;

const jobsRuntime = createJobsRuntimeState(defaultFilters, {
  lastHandledAutoRefreshSignalId: readAppliedAutoRefreshId()
});
const state = jobsRuntime.pageState;
const userState = jobsRuntime.userState;
const jobsPipelineUiState = jobsRuntime.pipelineUiState;
const runtimeState = jobsRuntime.runtimeState;
const dom = {};
const jobsControllerRefs = dom;
const jobsDispatch = createJobsDispatcher();
const PROFESSION_LABELS = jobsStateModule.PROFESSION_LABELS || {};
const QUICK_FILTERS = Array.isArray(jobsStateModule.QUICK_FILTERS) ? jobsStateModule.QUICK_FILTERS : [];
const authReadyPoller = createAuthReadyPoller({
  isReady: () => isJobsApiReady() && jobsPageService.isAvailable(),
  onReady: () => initAuth()
});
const filtersController = createJobsFiltersController({
  refs: jobsControllerRefs,
  state,
  defaultFilters,
  quickFilters: QUICK_FILTERS,
  professionLabels: PROFESSION_LABELS,
  jobsDispatch,
  JOBS_ACTIONS,
  applyFiltersAndRender,
  buildFilterOptions,
  getJobLocationCities,
  getJobLocationCountries,
  isValidCountry,
  isSemanticallyValidLocationValue,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  QUICK_FILTER_PREFS_KEY,
  escapeHtml,
  normalizeLifecycleStatus
});
const startupMetrics = createJobsStartupMetrics({
  emitMetric: (event, payload) => {
    if (!isDesktopRuntimeMode()) return;
    postStartupMetricToBridge(String(event || "").trim() || "unknown", payload);
  }
});
const callJobsBridge = createJobsBridgeRequest({
  baseUrl: ADMIN_BRIDGE_BASE,
  timeoutMs: JOBS_BRIDGE_REQUEST_TIMEOUT_MS,
  request: callJobsBridgeFromModule
});

const authController = createJobsAuthController({
  refs: jobsControllerRefs,
  userState,
  authReadyPoller,
  jobsAuthService,
  jobsSavedJobsService,
  jobsPageService,
  jobsDispatch,
  JOBS_ACTIONS,
  isJobsApiReady,
  emitDesktopStartupMetric,
  showToast,
  logJobsError,
  getAllJobs: () => runtimeState.allJobs,
  applyFiltersAndRender,
  getSkipInitialGuestAuthRerender: () => runtimeState.skipInitialGuestAuthRerender,
  setSkipInitialGuestAuthRerender: value => {
    runtimeState.skipInitialGuestAuthRerender = Boolean(value);
  },
  loadSeenJobKeys,
  markSeenJob,
  buildSeenRowKey,
  getJobKeyForJob: getJobKeyForJobWithService,
  openJobsCacheDb,
  JOBS_SEEN_STORE,
  toJobSnapshot,
  sanitizeUrl
});

const pipelineController = createJobsPipelineController({
  refs: jobsControllerRefs,
  jobsPipelineUiState,
  callJobsBridge,
  getAllJobs: () => runtimeState.allJobs,
  showToast,
  setRefreshJobsNeedsAttention,
  isErrorStage: payload => Boolean(payload?.error) || normalizeToken(payload?.stage) === "error",
  pollDelayMs: JOBS_PIPELINE_STATUS_POLL_MS,
  idlePollDelayMs: JOBS_PIPELINE_STATUS_IDLE_POLL_MS
});

const jobsUrlPersistence = createJobsUrlPersistence({
  windowObject: window,
  buildJobsPageUrl,
  resolveStartupProbeEnabled,
  isDesktopRuntimeMode: () => isDesktopRuntimeMode(),
  rememberJobsUrl,
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
  lastUrlKey: JOBS_LAST_URL_KEY
});

/**
 * Entry map (Jobs runtime):
 * - boot initializes DOM, URL state, event bindings, auth/feed startup.
 * - state concern: ./runtime/state.js
 * - events concern: ./runtime/events.js
 * - actions concern: ./runtime/actions.js
 * - view concern: ./runtime/view.js
 * - effects concern: ./runtime/effects.js
 */

function logJobsInfo(message, ...args) {
  logInfo(JOBS_LOG_SCOPE, message, ...args);
}

function logJobsError(message, err) {
  logError(JOBS_LOG_SCOPE, message, err);
}

/**
 * Applies page-specific presentation for admin bridge button state.
 * @param {Object} params
 * @param {HTMLElement} params.buttonEl
 * @param {string} params.state - "online", "offline", or "checking"
 * @param {string} params.label
 * @param {string} params.title
 * @param {number} params.activeAlerts
 */
function applyJobsAdminBridgeState({ buttonEl, state, label, title }) {
  applyJobsAdminBridgeStateFromModule({
    buttonEl,
    state,
    label,
    title,
    runtimeState
  });
}

export async function openJobLinkInDefaultBrowser(url, deps = {}) {
  if (!url) return;
  const isDesktop = typeof deps.isDesktopRuntimeMode === "function"
    ? deps.isDesktopRuntimeMode
    : isDesktopRuntimeMode;
  const openWindow = typeof deps.openWindow === "function"
    ? deps.openWindow
    : target => window.open(target, "_blank", "noopener,noreferrer");
  const callBridge = typeof deps.callJobsBridge === "function"
    ? deps.callJobsBridge
    : callJobsBridge;
  const logError = typeof deps.logJobsError === "function"
    ? deps.logJobsError
    : logJobsError;
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
    logError("Failed to open job link in the default browser", err);
  }
}

/**
 * Sets up event delegation on the jobs list container.
 * Called once during boot to avoid reattaching listeners after each render.
 */
function setupJobsListDelegation() {
  setupJobsListDelegationFromEvents({
    jobsList: dom.jobsList,
    jobRowSelector: `${ui(UI_TOKENS.jobs.jobRow)}[data-job-link]`,
    saveJobBtnSelector: ui(UI_TOKENS.jobs.saveJobBtn),
    sanitizeUrl,
    getJobById: jobId => runtimeState.allJobs.find(job => String(job.id) === String(jobId || "")),
    onToggleSaveJob: toggleSaveJob,
    onOpenJobLink: openJobLinkInDefaultBrowser,
    onMarkJobSeen: jobKey => authController.markJobSeenFromInteraction(jobKey)
  });
}

function bootJobsPage() {
  cacheDom();
  runtimeState.adminBridgeWatcher = createAdminBridgeButtonWatcher({
    buttonEl: dom.adminPageBtn,
    baseUrl: ADMIN_BRIDGE_BASE,
    fetchJson,
    applyState: applyJobsAdminBridgeState
  });
  runtimeState.desktopUpdateController = createJobsDesktopUpdateController({
    refs: dom,
    baseUrl: ADMIN_BRIDGE_BASE,
    fetchJson,
    postJson,
    bindAsyncClick,
    showToast,
    requestConfirmationDialog,
    isDesktopRuntimeMode,
    showReleaseNotesDialog: options => openReleaseNotesDialog(options),
    openExternalUrl: url => openJobLinkInDefaultBrowser(url),
  });
  startAdminBridgeButtonWatch();
  (async () => {
    try {
      await runtimeState.desktopUpdateController.mount();
    } catch (err) {
      logJobsError("Failed to initialize desktop update UI", err);
      return;
    }
    try {
      await runtimeState.desktopUpdateController.startAutoCheck();
    } catch (err) {
      logJobsError("Failed to auto-check desktop updates", err);
    }
  })();
  setupJobsListDelegation();
  setJobsStartupState("loading", "booting");
  bindCoreEvents();
  try {
    filtersController.initializeQuickFilters();
    bindEvents();
    readStateFromUrl();
    filtersController.applyStateToStaticFilters();
  } catch (err) {
    handleJobsStartupFailure("Jobs page boot failed", err, { allowRetryReload: true });
    return;
  }
  init().catch(err => handleJobsStartupFailure("Error initializing jobs", err));
}

function scheduleNonCriticalStartupWork() {
  if (runtimeState.nonCriticalStartupScheduled) return;
  runtimeState.nonCriticalStartupScheduled = true;
  scheduleNonCriticalStartup(window, () => {
    renderDataSources().catch(() => {});
    ensureJobsPipelineStatusWatch();
  });
}


function cacheDom() {
  Object.assign(dom, cacheJobsDom(document));
}

function isDesktopRuntimeMode() {
  return isDesktopRuntimeModeFromStartup(window.location.href);
}

function emitDesktopStartupMetric(event, payload = {}) {
  emitStartupMetric(startupMetrics, event, payload);
}

function markStartupRendered(stage, rowCount) {
  startupMetrics.markRendered(stage, rowCount);
}

function markJobsFirstInteractive(reason) {
  markFirstInteractive(startupMetrics, reason);
  setJobsStartupState("interactive", String(reason || "interactive"));
  runtimeState.desktopUrlStateReady = true;
  jobsUrlPersistence.flushDesktopPendingJobsUrlState();
}

function startAdminBridgeButtonWatch() {
  if (!runtimeState.adminBridgeWatcher) return;
  runtimeState.adminBridgeWatcher.startAdminBridgeButtonWatch();
}

async function openAdminPageFromJobs() {
  if (runtimeState.adminBridgeButtonState !== "online") {
    showToast("Admin bridge is offline.", "info");
    return;
  }
  rememberCurrentJobsUrl();
  navigateDesktopPage("admin.html");
}

function bindCoreEvents() {
  if (runtimeState.coreEventsBound) return;
  runtimeState.coreEventsBound = true;
  const clickHandlers = new Map([
    [dom.savedJobsBtn, () => {
      rememberCurrentJobsUrl();
      navigateDesktopPage("saved.html");
    }],
    [dom.countryPickerClearBtn, () => {
      state.filters.countries = [];
      filtersController.applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    }],
    [dom.quickFiltersResetBtn, () => {
      filtersController.resetQuickFilterPreferences();
    }]
  ]);
  bindHandlersMap(clickHandlers);

  bindAsyncClick(dom.authSignInBtn, signInUser);
  bindAsyncClick(dom.authSignOutBtn, signOutUser);
  bindAsyncClick(dom.adminPageBtn, openAdminPageFromJobs);
  bindAsyncClick(dom.refreshJobsBtn, () => refreshJobsNow({ manual: true }));
  bindAsyncClick(dom.jobsPipelineRunBtn, triggerJobsPipelineRun);
}

function bindEvents() {
  if (runtimeState.secondaryEventsBound) return;
  runtimeState.secondaryEventsBound = true;
  [
    dom.workTypeFilter,
    dom.lifecycleStatusFilter,
    dom.countryFilter,
    dom.cityFilter,
    dom.sectorFilter,
    dom.professionFilter,
    dom.sortFilter
  ].forEach(el => bindUi(el, "change", () => filtersController.onFilterChange()));

  if (dom.professionSearchFilter) {
    dom.professionSearchFilter.addEventListener("input", () => {
      filtersController.renderProfessionOptions(dom.professionSearchFilter.value);
    });
  }

  if (dom.countryPickerBtn) {
    dom.countryPickerBtn.addEventListener("click", e => {
      e.stopPropagation();
      filtersController.toggleCountryPickerPanel();
    });
  }
  if (dom.countryPickerSearch) {
    dom.countryPickerSearch.addEventListener("input", () => {
      filtersController.renderCountryPickerOptions(dom.countryPickerSearch.value);
    });
  }
  if (dom.countryPickerOptions) {
    dom.countryPickerOptions.addEventListener("change", event => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
      const current = new Set(state.filters.countries || []);
      if (target.checked) current.add(target.value);
      else current.delete(target.value);
      state.filters.countries = Array.from(current);
      filtersController.applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  }

  const handleDocumentPointerDown = event => {
    if (dom.countryPickerPanel && !dom.countryPickerPanel.classList.contains("hidden")) {
      const clickedInsidePanel = dom.countryPickerPanel.contains(event.target);
      const clickedTrigger = dom.countryPickerBtn && dom.countryPickerBtn.contains(event.target);
      if (!clickedInsidePanel && !clickedTrigger) {
        filtersController.closeCountryPickerPanel();
      }
    }

    if (dom.quickFiltersPanel && !dom.quickFiltersPanel.classList.contains("hidden")) {
      const clickedInsideQuickPanel = dom.quickFiltersPanel.contains(event.target);
      const clickedQuickTrigger = dom.customizeQuickFiltersBtn && dom.customizeQuickFiltersBtn.contains(event.target);
      if (!clickedInsideQuickPanel && !clickedQuickTrigger) {
        filtersController.closeQuickFiltersPanel();
      }
    }
  };
  document.addEventListener("pointerdown", handleDocumentPointerDown, true);
  document.addEventListener("mousedown", handleDocumentPointerDown, true);
  document.addEventListener("keydown", event => {
    if (event.key !== "Escape") return;
    event.preventDefault();
    filtersController.closeCountryPickerPanel();
    filtersController.closeQuickFiltersPanel();
  }, true);

  if (dom.searchFilter) {
    bindUi(dom.searchFilter, "input", debounce(() => {
      filtersController.onFilterChange();
    }, 180));
  }

  bindWindowResize(debounce(() => {
    if (!runtimeState.allJobs.length) return;
    const changed = recalculateItemsPerPage();
    if (changed) {
      applyFiltersAndRender({ resetPage: false });
    }
  }, 150));

  if (dom.quickActionsEl) {
    dom.quickActionsEl.addEventListener("click", event => {
      const btn = event.target.closest(".quick-btn");
      if (!btn) return;
      const quick = btn.dataset.quick;
      if (!quick) return;
      filtersController.applyQuickFilter(quick);
      filtersController.applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  }

  if (dom.customizeQuickFiltersBtn) {
    dom.customizeQuickFiltersBtn.addEventListener("click", event => {
      event.stopPropagation();
      filtersController.toggleQuickFiltersPanel();
    });
  }

  if (dom.quickFiltersOptionsEl) {
    dom.quickFiltersOptionsEl.addEventListener("change", event => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
      const { quick } = target.dataset;
      if (!quick) return;
      filtersController.setQuickFilterVisibility(quick, target.checked);
    });
  }

  window.addEventListener("storage", event => {
    if (event.key !== JOBS_AUTO_REFRESH_SIGNAL_KEY) return;
    if (!event.newValue) return;
    handleAutoRefreshSignalValue(event.newValue);
  });

  enableKeyboardNav();
}

async function init() {
  return initJobsFeed({
    hasJobsList: Boolean(dom.jobsList),
    emitMetric: emitDesktopStartupMetric,
    initAuth,
    isDesktopRuntimeMode,
    readCachedJobs,
    normalizeRows: rows => {
      runtimeState.allJobs = normalizeJobs(rows, {
        professionLabels: PROFESSION_LABELS,
        sanitizeUrl
      });
      return runtimeState.allJobs;
    },
    recalculateItemsPerPage,
    updateFilterOptions: () => filtersController.updateFilterOptions(runtimeState.allJobs),
    applyStateToFilters: () => filtersController.applyStateToFilters(),
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    isJobsCacheStale,
    cacheTtlMs: JOBS_CACHE_TTL_MS,
    setSourceStatus,
    refreshJobsNow,
    updateLastUpdatedText,
    setHasInitializedJobsFeed: value => {
      runtimeState.hasInitializedJobsFeed = Boolean(value);
    },
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs: () => runtimeState.allJobs
  });
}

function ensureJobsPipelineStatusWatch() {
  return pipelineController.ensureJobsPipelineStatusWatch();
}

async function triggerJobsPipelineRun() {
  return pipelineController.triggerJobsPipelineRun();
}

function readAppliedAutoRefreshId() {
  return readAutoRefreshAppliedId(JOBS_AUTO_REFRESH_APPLIED_KEY);
}

function markAutoRefreshSignalHandled(signalId) {
  if (!signalId) return;
  runtimeState.lastHandledAutoRefreshSignalId = signalId;
  writeAutoRefreshAppliedId(JOBS_AUTO_REFRESH_APPLIED_KEY, signalId);
}

function handleAutoRefreshSignalValue(rawValue) {
  return handleJobsAutoRefreshSignalValue(rawValue, {
    parseAutoRefreshSignal: parseAutoRefreshSignalFromStartup,
    getLastHandledAutoRefreshSignalId: () => runtimeState.lastHandledAutoRefreshSignalId,
    getHasInitializedJobsFeed: () => runtimeState.hasInitializedJobsFeed,
    setPendingAutoRefreshSignal: value => {
      runtimeState.pendingAutoRefreshSignal = value;
    },
    triggerAutoRefreshFromSignal,
    logError: logJobsError
  });
}

async function applyPendingAutoRefreshSignal() {
  return applyPendingJobsAutoRefreshSignal({
    getPendingAutoRefreshSignal: () => runtimeState.pendingAutoRefreshSignal,
    setPendingAutoRefreshSignal: value => {
      runtimeState.pendingAutoRefreshSignal = value;
    },
    readAutoRefreshSignal,
    autoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
    handleAutoRefreshSignalValue,
    triggerAutoRefreshFromSignal
  });
}

async function triggerAutoRefreshFromSignal(signal) {
  return triggerJobsAutoRefreshFromSignal(signal, {
    getLastHandledAutoRefreshSignalId: () => runtimeState.lastHandledAutoRefreshSignalId,
    setSourceStatus,
    getAutoRefreshStatusText,
    refreshJobsNow,
    markAutoRefreshSignalHandled,
    showToast
  });
}

function initAuth() {
  return authController.initAuth();
}

async function signInUser() {
  return authController.signInUser();
}

async function signOutUser() {
  return authController.signOutUser();
}

function readStateFromUrl() {
  const nextState = parseJobsPageUrlState(window.location.search, {
    defaultFilters,
    normalizeLifecycleStatus
  });
  state.currentPage = nextState.currentPage;
  state.filters = {
    ...state.filters,
    ...nextState.filters,
    countries: Array.from(nextState.filters.countries || [])
  };
}

function writeStateToUrl() {
  jobsUrlPersistence.writeStateToUrl(state);
}

function rememberCurrentJobsUrl() {
  jobsUrlPersistence.rememberCurrentJobsUrl();
}

function openJobsCacheDb() {
  return openJobsCacheDbFromModule({
    indexedDb: window.indexedDB,
    dbName: JOBS_CACHE_DB,
    dbVersion: JOBS_CACHE_DB_VERSION,
    cacheStore: JOBS_CACHE_STORE,
    seenStore: JOBS_SEEN_STORE
  });
}

async function readCachedJobs() {
  return readJobsCache({
    openDb: openJobsCacheDb,
    cacheStore: JOBS_CACHE_STORE,
    cacheKey: JOBS_CACHE_KEY
  });
}

function updateLastUpdatedText(timestamp) {
  if (!dom.jobsLastUpdatedEl) return;
  dom.jobsLastUpdatedEl.textContent = getJobsLastUpdatedText(timestamp);
}

async function refreshJobsNow({ manual, firstLoad = false }) {
  return refreshJobsFeed({ manual, firstLoad }, {
    getRefreshInFlight: () => runtimeState.refreshInFlight,
    setRefreshInFlight: value => {
      runtimeState.refreshInFlight = Boolean(value);
    },
    dispatchRefreshRequested: () => {
      jobsDispatch.dispatch({ type: JOBS_ACTIONS.REFRESH_REQUESTED });
    },
    setRefreshButtonDisabled: disabled => {
      if (dom.refreshJobsBtn) dom.refreshJobsBtn.disabled = disabled;
    },
    setProgress,
    setSourceStatus,
    firstLoadRequestTimeoutMs: JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS,
    fetchUnifiedJobs,
    dispatchRefreshFailed: error => {
      jobsDispatch.dispatch({
        type: JOBS_ACTIONS.REFRESH_FAILED,
        payload: { error }
      });
    },
    showToast,
    logError: logJobsError,
    getAllJobs: () => runtimeState.allJobs,
    setAllJobs: jobs => {
      runtimeState.allJobs = jobs;
    },
    normalizeRows: rows => normalizeJobs(rows, {
      professionLabels: PROFESSION_LABELS,
      sanitizeUrl
    }),
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
    emitMetric: emitDesktopStartupMetric,
    dispatchRefreshCompleted: () => {
      jobsDispatch.dispatch({
        type: JOBS_ACTIONS.REFRESH_COMPLETED,
        payload: { finishedAt: new Date().toISOString() }
      });
    },
    renderDataSources
  });
}

async function writeCachedJobs(jobs) {
  return writeJobsCache(jobs, {
    openDb: openJobsCacheDb,
    cacheStore: JOBS_CACHE_STORE,
    cacheKey: JOBS_CACHE_KEY,
    now: Date.now()
  });
}

function clearPendingStartupPreviewMaterialization() {
  if (runtimeState.startupPreviewMaterializeTimer) {
    window.clearTimeout(runtimeState.startupPreviewMaterializeTimer);
  }
  runtimeState.startupPreviewMaterialize = null;
  runtimeState.startupPreviewMaterializeTimer = null;
  runtimeState.startupPreviewFilteredCount = 0;
}

function materializePendingStartupPreview({ render = false } = {}) {
  if (typeof runtimeState.startupPreviewMaterialize !== "function") return runtimeState.filteredJobs;
  const materialize = runtimeState.startupPreviewMaterialize;
  clearPendingStartupPreviewMaterialization();
  runtimeState.filteredJobs = materialize();
  if (render) {
    displayJobs(runtimeState.filteredJobs);
  }
  return runtimeState.filteredJobs;
}

function scheduleStartupPreviewMaterialization(materializeFilteredJobs) {
  if (typeof materializeFilteredJobs !== "function") return;
  clearPendingStartupPreviewMaterialization();
  runtimeState.startupPreviewMaterialize = materializeFilteredJobs;
  runtimeState.startupPreviewMaterializeTimer = window.setTimeout(() => {
    materializePendingStartupPreview();
  }, 0);
}

function insertTopStartupPreviewJob(topJobs, job, limit) {
  if (!Number.isFinite(limit) || limit <= 0) return;
  let insertIndex = topJobs.findIndex(existing =>
    compareJobsForSort(job, existing, "relevance", {
      fullCountryName: fullCountryNameForJobs
    }) < 0
  );
  if (insertIndex < 0) insertIndex = topJobs.length;
  if (insertIndex >= limit && topJobs.length >= limit) return;
  topJobs.splice(insertIndex, 0, job);
  if (topJobs.length > limit) {
    topJobs.pop();
  }
}

function buildStartupPreviewFastPathPlan(allJobs) {
  const filterOptionsAccumulator = createFilterOptionsAccumulator();
  const activeJobs = [];
  const firstPageJobs = [];
  const firstPageLimit = Math.max(1, Number(state.itemsPerPage) || 1);

  (allJobs || []).forEach(job => {
    addJobToFilterOptions(filterOptionsAccumulator, job, {
      getJobLocationCities,
      getJobLocationCountries,
      isSemanticallyValidLocationValue,
      isValidCountry
    });
    if (String(job?.status || "active").toLowerCase() !== "active") return;
    activeJobs.push(job);
    insertTopStartupPreviewJob(firstPageJobs, job, firstPageLimit);
  });

  return {
    filterOptions: finalizeFilterOptions(filterOptionsAccumulator, {
      getAvailableRegionOptions: getAvailableRegionOptionsForJobs,
      fullCountryName: fullCountryNameForJobs
    }),
    filteredCount: activeJobs.length,
    pageJobs: firstPageJobs,
    materializeFilteredJobs: () => sortJobsFromQuery(activeJobs, "relevance", {
      fullCountryName: fullCountryNameForJobs
    })
  };
}

function renderStartupPreviewFastPath(plan = {}) {
  const pageJobs = Array.isArray(plan?.pageJobs) ? plan.pageJobs : [];
  const filteredCount = Number.isFinite(Number(plan?.filteredCount))
    ? Number(plan.filteredCount)
    : pageJobs.length;
  runtimeState.filteredJobs = pageJobs;
  runtimeState.startupPreviewFilteredCount = filteredCount;
  displayJobs(runtimeState.filteredJobs, {
    pageJobsOverride: pageJobs,
    totalCountOverride: filteredCount
  });
}

async function loadStartupPreviewJobs() {
  return loadStartupPreviewJobsFeed({
    emitMetric: emitDesktopStartupMetric,
    fetchJsonFromCandidates,
    startupPreviewJsonUrls: STARTUP_PREVIEW_JSON_URLS,
    parseUnifiedJobsPayload: payload => parseUnifiedJobsPayload(payload, jobsParsing),
    normalizeRows: rows => {
      runtimeState.allJobs = normalizeJobs(rows, {
        professionLabels: PROFESSION_LABELS,
        sanitizeUrl
      });
      return runtimeState.allJobs;
    },
    updateLastUpdatedText,
    recalculateItemsPerPage,
    pageState: state,
    defaultFilters,
    buildStartupPreviewFastPathPlan,
    applyFilterOptionsSnapshot: filterOptions =>
      filtersController.updateFilterOptions(runtimeState.allJobs, {
        precomputed: filterOptions
      }),
    updateFilterOptions: () => filtersController.updateFilterOptions(runtimeState.allJobs),
    applyStateToFilters: () => filtersController.applyStateToFilters(),
    renderStartupPreviewFastPath,
    scheduleStartupPreviewMaterialization,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    setSkipInitialGuestAuthRerender: value => {
      runtimeState.skipInitialGuestAuthRerender = Boolean(value);
    },
    getAllJobs: () => runtimeState.allJobs
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

function applyFiltersAndRender({ resetPage }) {
  clearPendingStartupPreviewMaterialization();
  if (resetPage) {
    state.currentPage = 1;
  }

  emitDesktopStartupMetric("jobs_apply_filters_start", {
    resetPage: Boolean(resetPage),
    totalJobs: runtimeState.allJobs.length
  });
  filtersController.syncStateFromFilters();
  runtimeState.filteredJobs = filterJobs(runtimeState.allJobs, state.filters, {
    currentUser: userState.currentUser,
    seenJobKeys: userState.seenJobKeys,
    getJobKeyForJob: getJobKeyForJobWithService,
    getJobLocationCities,
    getJobLocationCountries,
    isInternshipJob,
    matchesCountrySelection: matchesCountrySelectionForJobs
  });

  emitDesktopStartupMetric("jobs_apply_filters_complete", {
    filteredCount: runtimeState.filteredJobs.length
  });
  runtimeState.filteredJobs = sortJobsFromQuery(runtimeState.filteredJobs, state.filters.sort, {
    fullCountryName: fullCountryNameForJobs
  });
  emitDesktopStartupMetric("jobs_sort_complete", {
    filteredCount: runtimeState.filteredJobs.length,
    sortMode: String(state.filters.sort || "relevance")
  });
  displayJobs(runtimeState.filteredJobs);
  emitDesktopStartupMetric("jobs_write_state_start");
  writeStateToUrl();
  emitDesktopStartupMetric("jobs_write_state_complete");
}

function displayJobs(jobs, options = {}) {
  return displayJobsFromView(jobs, {
    jobsList: dom.jobsList,
    pagination: dom.pagination,
    resultsSummary: dom.resultsSummary,
    state,
    allJobs: runtimeState.allJobs,
    currentUser: userState.currentUser,
    seenJobKeys: userState.seenJobKeys,
    savedJobKeys: userState.savedJobKeys,
    isJobsApiReady,
    getJobKeyForJob: getJobKeyForJobWithService,
    fullCountryName: fullCountryNameForJobs,
    goToPage,
    emitDesktopStartupMetric,
    renderJobRowHtml
  }, options);
}

function goToPage(page) {
  if (page !== state.currentPage) {
    materializePendingStartupPreview();
  }
  return goToPageFromView(page, {
    filteredJobs: runtimeState.filteredJobs,
    state,
    displayJobs,
    writeStateToUrl
  });
}


function recalculateItemsPerPage() {
  if (!dom.jobsList) return false;

  const top = dom.jobsList.getBoundingClientRect().top;
  const viewportHeight = window.innerHeight;
  const reservedSpace = 140;
  const availableHeight = Math.max(260, viewportHeight - top - reservedSpace);
  const rowHeight = window.innerWidth <= 900 ? 136 : 52;
  const next = Math.max(4, Math.min(25, Math.floor(availableHeight / rowHeight)));

  if (next !== state.itemsPerPage) {
    state.itemsPerPage = next;
    return true;
  }
  return false;
}
function enableKeyboardNav() {
  document.addEventListener("keydown", e => {
    const isField = ["INPUT", "SELECT", "TEXTAREA"].includes(e.target.tagName) || e.target.isContentEditable;
    if (isField) return;

    if (e.key === "ArrowLeft" && state.currentPage > 1) {
      goToPage(state.currentPage - 1);
    } else if (e.key === "ArrowRight") {
      const totalPages = Math.ceil(runtimeState.filteredJobs.length / state.itemsPerPage);
      if (state.currentPage < totalPages) {
        goToPage(state.currentPage + 1);
      }
    }
  });
}

function updateResultsSummary(total, from, to, loadedTotal = total) {
  return updateResultsSummaryFromView(dom.resultsSummary, total, from, to, loadedTotal);
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

function getJobKeyForJobWithService(job) {
  return getJobKeyForJob(job, {
    generateJobKey: row => jobsPageService.generateJobKey(row)
  });
}

async function toggleSaveJob(job) {
  return authController.toggleSaveJob(job);
}

function setProgress(visible) {
  setProgressVisibility(setText, dom.fetchProgress, visible);
}

function setSourceStatus(text) {
  setStatusText(setText, dom.sourceStatus, text);
}

function setJobsStartupState(state, detail = "") {
  if (!document?.body) return;
  const normalized = normalizeToken(state) || "loading";
  document.body.setAttribute("data-jobs-startup-state", normalized);
  if (detail) {
    document.body.setAttribute("data-jobs-startup-detail", String(detail));
  } else {
    document.body.removeAttribute("data-jobs-startup-detail");
  }
}

function _showLoading(text) {
  showJobsLoading(dom.jobsList, text);
}

function showError(message, onRetry = null) {
  setJobsStartupState("error", "load_error");
  showJobsError(dom.jobsList, dom.pagination, message, () => {
    const retry = typeof onRetry === "function"
      ? onRetry
      : () => init().catch(err => handleJobsStartupFailure("Retry failed", err));
    return retry();
  });
  updateResultsSummary(0, 0, 0, runtimeState.allJobs.length);
}

function handleJobsStartupFailure(context, err, options = {}) {
  logJobsError(context, err);
  setProgress(false);
  setSourceStatus("Jobs page failed to start.");
  const retry = options.allowRetryReload
    ? () => window.location.reload()
    : () => init().catch(nextErr => handleJobsStartupFailure("Retry failed", nextErr));
  showError("Unable to load job listings right now.", retry);
}

globalThis.__baluffoBootJobsPage = bootJobsPage;
