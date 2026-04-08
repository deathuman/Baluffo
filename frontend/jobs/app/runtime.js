import { JobsStateModule as jobsStateModule } from "../../../jobs-state.js";
import { AdminConfig as adminConfig } from "../../../admin-config.js";
import { resolveStartupProbeEnabled } from "../../../probes/startup-probe.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick,
  bindHandlersMap
} from "../../shared/ui/index.js";
import { emitStartupMetric, logError, logInfo, markFirstInteractive } from "../../shared/app-boot.js";
import { BaluffoJobsParsing as jobsParsing, parseUnifiedJobsPayload } from "../../../jobs-parsing-utils.js";
import {
  detectWorkType,
  detectContractType,
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
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { fetchJson, postJson } from "../../shared/api-client.js";
import { createAdminBridgeButtonWatcher } from "../../shared/admin-bridge-button.js";
import { createAuthReadyPoller } from "../../shared/auth-ready-poll.js";
import { normalizeToken } from "../../shared/text-utils.js";
import { cacheJobsDom } from "./dom.js";
import { callJobsBridge as callJobsBridgeFromModule } from "./pipeline.js";
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
  optionExists,
  normalizeSelectedCountries,
  getCountrySelectionBadgeText,
  getDefaultQuickFilterKeys,
  sanitizeQuickFilterKeys,
  renderQuickFiltersHtml,
  renderQuickFilterOptionsHtml,
  getNextQuickFilterKeys,
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
  buildFilterOptions,
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
import { createJobsPageState, createJobsPipelineUiState } from "./runtime/state.js";
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
  getCountryFilterOptionLabel as getCountryFilterOptionLabelForJobs,
  matchesCountrySelection as matchesCountrySelectionForJobs
} from "./countries.js";
import {
  STARTUP_PREVIEW_JSON_URLS,
  fetchUnifiedJobs as fetchUnifiedJobsFromSources,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromSources,
  renderDataSources as renderDataSourcesFromSources
} from "./sources.js";
let allJobs = [];
let filteredJobs = [];
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

/** @type {JobsPageState} */
const state = createJobsPageState(defaultFilters);
const jobsDispatch = createJobsDispatcher();

const PROFESSION_LABELS = jobsStateModule.PROFESSION_LABELS || {};

let jobsList;
let workTypeFilter;
let lifecycleStatusFilter;
let countryFilter;
let countryPickerBtn;
let countryPickerPanel;
let countryPickerSearch;
let countryPickerOptions;
let countryPickerClearBtn;
let cityFilter;
let sectorFilter;
let professionFilter;
let professionSearchFilter;
let searchFilter;
let sortFilter;
let resultsSummary;
let countrySelectionBadge;
let sourceStatus;
let fetchProgress;
let pagination;
let refreshJobsBtn;
let refreshJobsNeededBadgeEl;
let jobsLastUpdatedEl;
let authStatus;
let authStatusHint;
let authAvatar;
let authSignInBtn;
let authSignOutBtn;
let adminPageBtn;
let savedJobsBtn;
let activeFiltersSummaryEl;
let quickActionsEl;
let customizeQuickFiltersBtn;
let quickFiltersPanel;
let quickFiltersOptionsEl;
let desktopUrlStateReady = false;
let desktopPendingRememberJobsUrl = false;
let desktopPendingJobsUrl = "";
let quickFiltersResetBtn;
let dataSourcesListEl;
let dataSourcesCaptionEl;
let jobsPipelineRunBtn;

const userState = {
  currentUser: null,
  savedJobKeys: new Set(),
  seenJobKeys: new Set(),
  authStateListenerBound: false
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
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const JOBS_PIPELINE_STATUS_POLL_MS = 1500;
const JOBS_PIPELINE_STATUS_IDLE_POLL_MS = 5000;
const JOBS_BRIDGE_REQUEST_TIMEOUT_MS = 1800;
const JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS = 4500;

const QUICK_FILTERS = Array.isArray(jobsStateModule.QUICK_FILTERS) ? jobsStateModule.QUICK_FILTERS : [];
let refreshInFlight = false;
let hasInitializedJobsFeed = false;
let pendingAutoRefreshSignal = null;
let lastHandledAutoRefreshSignalId = readAppliedAutoRefreshId();
let _lastFilterOptionsSignature = "";
const authReadyPoller = createAuthReadyPoller({
  isReady: () => isJobsApiReady() && jobsPageService.isAvailable(),
  onReady: () => initAuth()
});
let nonCriticalStartupScheduled = false;
let coreEventsBound = false;
let secondaryEventsBound = false;
let adminBridgeButtonState = "checking";
let adminBridgeWatcher = null;
const jobsPipelineUiState = createJobsPipelineUiState();
const jobsControllerRefs = {
  get authStatus() {
    return authStatus;
  },
  get authStatusHint() {
    return authStatusHint;
  },
  get authAvatar() {
    return authAvatar;
  },
  get authSignInBtn() {
    return authSignInBtn;
  },
  get authSignOutBtn() {
    return authSignOutBtn;
  },
  get savedJobsBtn() {
    return savedJobsBtn;
  },
  get jobsPipelineRunBtn() {
    return jobsPipelineRunBtn;
  },
  get workTypeFilter() {
    return workTypeFilter;
  },
  get lifecycleStatusFilter() {
    return lifecycleStatusFilter;
  },
  get countryFilter() {
    return countryFilter;
  },
  get countryPickerBtn() {
    return countryPickerBtn;
  },
  get countryPickerPanel() {
    return countryPickerPanel;
  },
  get countryPickerSearch() {
    return countryPickerSearch;
  },
  get countryPickerOptions() {
    return countryPickerOptions;
  },
  get countryPickerClearBtn() {
    return countryPickerClearBtn;
  },
  get cityFilter() {
    return cityFilter;
  },
  get sectorFilter() {
    return sectorFilter;
  },
  get professionFilter() {
    return professionFilter;
  },
  get professionSearchFilter() {
    return professionSearchFilter;
  },
  get searchFilter() {
    return searchFilter;
  },
  get sortFilter() {
    return sortFilter;
  },
  get countrySelectionBadge() {
    return countrySelectionBadge;
  },
  get activeFiltersSummaryEl() {
    return activeFiltersSummaryEl;
  },
  get quickActionsEl() {
    return quickActionsEl;
  },
  get customizeQuickFiltersBtn() {
    return customizeQuickFiltersBtn;
  },
  get quickFiltersPanel() {
    return quickFiltersPanel;
  },
  get quickFiltersOptionsEl() {
    return quickFiltersOptionsEl;
  },
  get quickFiltersResetBtn() {
    return quickFiltersResetBtn;
  }
};
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
  isInternshipJob,
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
    postJson(ADMIN_BRIDGE_BASE, "/desktop-local-data/startup-metric", {
      event: String(event || "").trim() || "unknown",
      payload: payload && typeof payload === "object" ? payload : {}
    }).catch(() => {});
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
  getAllJobs: () => allJobs,
  applyFiltersAndRender,
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
  getAllJobs: () => allJobs,
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
  getDesktopUrlStateReady: () => desktopUrlStateReady,
  setDesktopUrlStateReady: value => {
    desktopUrlStateReady = Boolean(value);
  },
  getDesktopPendingRememberJobsUrl: () => desktopPendingRememberJobsUrl,
  setDesktopPendingRememberJobsUrl: value => {
    desktopPendingRememberJobsUrl = Boolean(value);
  },
  getDesktopPendingJobsUrl: () => desktopPendingJobsUrl,
  setDesktopPendingJobsUrl: value => {
    desktopPendingJobsUrl = String(value || "");
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
  if (!buttonEl) return;
  const enabled = state === "online";
  adminBridgeButtonState = state;
  buttonEl.dataset.bridgeState = state;
  buttonEl.textContent = label || "Admin Checking...";
  buttonEl.title = title || label || "Checking admin bridge status";
  buttonEl.disabled = !enabled;
  buttonEl.setAttribute("aria-disabled", enabled ? "false" : "true");
}

/**
 * Sets up event delegation on the jobs list container.
 * Called once during boot to avoid reattaching listeners after each render.
 */
function setupJobsListDelegation() {
  setupJobsListDelegationFromEvents({
    jobsList,
    jobRowSelector: `${ui(UI_TOKENS.jobs.jobRow)}[data-job-link]`,
    saveJobBtnSelector: ui(UI_TOKENS.jobs.saveJobBtn),
    sanitizeUrl,
    getJobById: jobId => allJobs.find(job => String(job.id) === String(jobId || "")),
    onToggleSaveJob: toggleSaveJob,
    onMarkJobSeen: jobKey => authController.markJobSeenFromInteraction(jobKey)
  });
}

function bootJobsPage() {
  cacheDom();
  adminBridgeWatcher = createAdminBridgeButtonWatcher({
    buttonEl: adminPageBtn,
    baseUrl: ADMIN_BRIDGE_BASE,
    fetchJson,
    applyState: applyJobsAdminBridgeState
  });
  adminBridgeWatcher.setAdminPageButtonState("checking", "Admin Checking...", "Checking admin bridge status");
  setupJobsListDelegation();  setJobsStartupState("loading", "booting");
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
  if (nonCriticalStartupScheduled) return;
  nonCriticalStartupScheduled = true;
  scheduleNonCriticalStartup(window, () => {
    renderDataSources().catch(() => {});
    ensureJobsPipelineStatusWatch();
    startAdminBridgeButtonWatch();
  });
}


function cacheDom() {
  ({
    jobsList,
    workTypeFilter,
    lifecycleStatusFilter,
    countryFilter,
    countryPickerBtn,
    countryPickerPanel,
    countryPickerSearch,
    countryPickerOptions,
    countryPickerClearBtn,
    cityFilter,
    sectorFilter,
    professionFilter,
    professionSearchFilter,
    searchFilter,
    sortFilter,
    resultsSummary,
    countrySelectionBadge,
    sourceStatus,
    fetchProgress,
    pagination,
    refreshJobsBtn,
    refreshJobsNeededBadgeEl,
    jobsLastUpdatedEl,
    authStatus,
    authStatusHint,
    authAvatar,
    authSignInBtn,
    authSignOutBtn,
    adminPageBtn,
    savedJobsBtn,
    activeFiltersSummaryEl,
    quickActionsEl,
    customizeQuickFiltersBtn,
    quickFiltersPanel,
    quickFiltersOptionsEl,
    quickFiltersResetBtn,
    dataSourcesListEl,
    dataSourcesCaptionEl,
    jobsPipelineRunBtn
  } = cacheJobsDom(document));
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
  desktopUrlStateReady = true;
  if (desktopPendingRememberJobsUrl) {
    desktopPendingRememberJobsUrl = false;
    const pendingUrl = desktopPendingJobsUrl || `${window.location.pathname}${window.location.search}`;
    desktopPendingJobsUrl = "";
    window.setTimeout(() => {
      persistDesktopJobsUrlState(pendingUrl);
    }, 0);
  }
}

function startAdminBridgeButtonWatch() {
  if (!adminBridgeWatcher) return;
  adminBridgeWatcher.startAdminBridgeButtonWatch();
}

async function openAdminPageFromJobs() {
  if (adminBridgeButtonState !== "online") {
    showToast("Admin bridge is offline.", "info");
    return;
  }
  rememberCurrentJobsUrl();
  window.location.href = "admin.html";
}

function bindCoreEvents() {
  if (coreEventsBound) return;
  coreEventsBound = true;
  const clickHandlers = new Map([
    [savedJobsBtn, () => {
      rememberCurrentJobsUrl();
      window.location.href = "saved.html";
    }],
    [countryPickerClearBtn, () => {
      state.filters.countries = [];
      filtersController.applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    }],
    [quickFiltersResetBtn, () => {
      filtersController.resetQuickFilterPreferences();
    }]
  ]);
  bindHandlersMap(clickHandlers);

  bindAsyncClick(authSignInBtn, signInUser);
  bindAsyncClick(authSignOutBtn, signOutUser);
  bindAsyncClick(adminPageBtn, openAdminPageFromJobs);
  bindAsyncClick(refreshJobsBtn, () => refreshJobsNow({ manual: true }));
  bindAsyncClick(jobsPipelineRunBtn, triggerJobsPipelineRun);
}

function bindEvents() {
  if (secondaryEventsBound) return;
  secondaryEventsBound = true;
  [
    workTypeFilter,
    lifecycleStatusFilter,
    countryFilter,
    cityFilter,
    sectorFilter,
    professionFilter,
    sortFilter
  ].forEach(el => bindUi(el, "change", () => filtersController.onFilterChange()));

  if (professionSearchFilter) {
    professionSearchFilter.addEventListener("input", () => {
      filtersController.renderProfessionOptions(professionSearchFilter.value);
    });
  }

  if (countryPickerBtn) {
    countryPickerBtn.addEventListener("click", e => {
      e.stopPropagation();
      filtersController.toggleCountryPickerPanel();
    });
  }
  if (countryPickerSearch) {
    countryPickerSearch.addEventListener("input", () => {
      filtersController.renderCountryPickerOptions(countryPickerSearch.value);
    });
  }
  if (countryPickerOptions) {
    countryPickerOptions.addEventListener("change", event => {
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

  document.addEventListener("click", event => {
    if (countryPickerPanel && !countryPickerPanel.classList.contains("hidden")) {
      const clickedInsidePanel = countryPickerPanel.contains(event.target);
      const clickedTrigger = countryPickerBtn && countryPickerBtn.contains(event.target);
      if (!clickedInsidePanel && !clickedTrigger) {
        closeCountryPickerPanel();
      }
    }

    if (quickFiltersPanel && !quickFiltersPanel.classList.contains("hidden")) {
      const clickedInsideQuickPanel = quickFiltersPanel.contains(event.target);
      const clickedQuickTrigger = customizeQuickFiltersBtn && customizeQuickFiltersBtn.contains(event.target);
      if (!clickedInsideQuickPanel && !clickedQuickTrigger) {
        closeQuickFiltersPanel();
      }
    }
  });
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      closeCountryPickerPanel();
      closeQuickFiltersPanel();
    }
  });

  if (searchFilter) {
    bindUi(searchFilter, "input", debounce(() => {
      filtersController.onFilterChange();
    }, 180));
  }

  bindWindowResize(debounce(() => {
    if (!allJobs.length) return;
    const changed = recalculateItemsPerPage();
    if (changed) {
      applyFiltersAndRender({ resetPage: false });
    }
  }, 150));

  if (quickActionsEl) {
    quickActionsEl.addEventListener("click", event => {
      const btn = event.target.closest(".quick-btn");
      if (!btn) return;
      const quick = btn.dataset.quick;
      if (!quick) return;
      filtersController.applyQuickFilter(quick);
      filtersController.applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  }

  if (customizeQuickFiltersBtn) {
    customizeQuickFiltersBtn.addEventListener("click", event => {
      event.stopPropagation();
      filtersController.toggleQuickFiltersPanel();
    });
  }

  if (quickFiltersOptionsEl) {
    quickFiltersOptionsEl.addEventListener("change", event => {
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
    hasJobsList: Boolean(jobsList),
    emitMetric: emitDesktopStartupMetric,
    initAuth,
    isDesktopRuntimeMode,
    readCachedJobs,
    normalizeRows: rows => {
      allJobs = normalizeJobs(rows, {
        professionLabels: PROFESSION_LABELS,
        sanitizeUrl
      });
      return allJobs;
    },
    recalculateItemsPerPage,
    updateFilterOptions: () => filtersController.updateFilterOptions(allJobs),
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
      hasInitializedJobsFeed = Boolean(value);
    },
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs: () => allJobs
  });
}

function updateJobsPipelineUi(options = {}) {
  return pipelineController.updateJobsPipelineUi(options);
}

function _clearJobsPipelinePolling() {
  return pipelineController.clearJobsPipelinePolling();
}

function scheduleJobsPipelineStatusPoll(delayMs) {
  return pipelineController.scheduleJobsPipelineStatusPoll(delayMs);
}

function handlePipelineCompletionStatus(payload) {
  return pipelineController.handlePipelineCompletionStatus(payload);
}

async function pollJobsPipelineStatus() {
  return pipelineController.pollJobsPipelineStatus();
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
  lastHandledAutoRefreshSignalId = signalId;
  writeAutoRefreshAppliedId(JOBS_AUTO_REFRESH_APPLIED_KEY, signalId);
}

function handleAutoRefreshSignalValue(rawValue) {
  return handleJobsAutoRefreshSignalValue(rawValue, {
    parseAutoRefreshSignal: parseAutoRefreshSignalFromStartup,
    getLastHandledAutoRefreshSignalId: () => lastHandledAutoRefreshSignalId,
    getHasInitializedJobsFeed: () => hasInitializedJobsFeed,
    setPendingAutoRefreshSignal: value => {
      pendingAutoRefreshSignal = value;
    },
    triggerAutoRefreshFromSignal,
    logError: logJobsError
  });
}

async function applyPendingAutoRefreshSignal() {
  return applyPendingJobsAutoRefreshSignal({
    getPendingAutoRefreshSignal: () => pendingAutoRefreshSignal,
    setPendingAutoRefreshSignal: value => {
      pendingAutoRefreshSignal = value;
    },
    readAutoRefreshSignal,
    autoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
    handleAutoRefreshSignalValue,
    triggerAutoRefreshFromSignal
  });
}

async function triggerAutoRefreshFromSignal(signal) {
  return triggerJobsAutoRefreshFromSignal(signal, {
    getLastHandledAutoRefreshSignalId: () => lastHandledAutoRefreshSignalId,
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

function persistDesktopJobsUrlState(url) {
  jobsUrlPersistence.persistDesktopJobsUrlState(url);
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
  if (!jobsLastUpdatedEl) return;
  jobsLastUpdatedEl.textContent = getJobsLastUpdatedText(timestamp);
}

async function refreshJobsNow({ manual, firstLoad = false }) {
  return refreshJobsFeed({ manual, firstLoad }, {
    getRefreshInFlight: () => refreshInFlight,
    setRefreshInFlight: value => {
      refreshInFlight = Boolean(value);
    },
    dispatchRefreshRequested: () => {
      jobsDispatch.dispatch({ type: JOBS_ACTIONS.REFRESH_REQUESTED });
    },
    setRefreshButtonDisabled: disabled => {
      if (refreshJobsBtn) refreshJobsBtn.disabled = disabled;
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
    getAllJobs: () => allJobs,
    setAllJobs: jobs => {
      allJobs = jobs;
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
    updateFilterOptions: () => filtersController.updateFilterOptions(allJobs),
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

async function loadStartupPreviewJobs() {
  return loadStartupPreviewJobsFeed({
    emitMetric: emitDesktopStartupMetric,
    fetchJsonFromCandidates,
    startupPreviewJsonUrls: STARTUP_PREVIEW_JSON_URLS,
    parseUnifiedJobsPayload: payload => parseUnifiedJobsPayload(payload, jobsParsing),
    normalizeRows: rows => {
      allJobs = normalizeJobs(rows, {
        professionLabels: PROFESSION_LABELS,
        sanitizeUrl
      });
      return allJobs;
    },
    updateLastUpdatedText,
    recalculateItemsPerPage,
    updateFilterOptions: () => filtersController.updateFilterOptions(allJobs),
    applyStateToFilters: () => filtersController.applyStateToFilters(),
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    getAllJobs: () => allJobs
  });
}

function setRefreshJobsNeedsAttention(needsRefresh) {
  const needs = Boolean(needsRefresh);
  if (refreshJobsBtn) {
    refreshJobsBtn.classList.toggle("needs-refresh", needs);
    refreshJobsBtn.setAttribute("aria-live", "polite");
  }
  if (refreshJobsNeededBadgeEl) {
    refreshJobsNeededBadgeEl.classList.toggle("hidden", !needs);
  }
}

function applyFiltersAndRender({ resetPage }) {
  if (resetPage) {
    state.currentPage = 1;
  }

  emitDesktopStartupMetric("jobs_apply_filters_start", {
    resetPage: Boolean(resetPage),
    totalJobs: allJobs.length
  });
  filtersController.syncStateFromFilters();
  filteredJobs = filterJobs(allJobs, state.filters, {
    currentUser: userState.currentUser,
    seenJobKeys: userState.seenJobKeys,
    getJobKeyForJob: getJobKeyForJobWithService,
    getJobLocationCities,
    getJobLocationCountries,
    isInternshipJob,
    matchesCountrySelection: matchesCountrySelectionForJobs
  });

  emitDesktopStartupMetric("jobs_apply_filters_complete", {
    filteredCount: filteredJobs.length
  });
  filteredJobs = sortJobsFromQuery(filteredJobs, state.filters.sort, {
    fullCountryName: fullCountryNameForJobs
  });
  emitDesktopStartupMetric("jobs_sort_complete", {
    filteredCount: filteredJobs.length,
    sortMode: String(state.filters.sort || "relevance")
  });
  displayJobs(filteredJobs);
  emitDesktopStartupMetric("jobs_write_state_start");
  writeStateToUrl();
  emitDesktopStartupMetric("jobs_write_state_complete");
}

function displayJobs(jobs) {
  return displayJobsFromView(jobs, {
    jobsList,
    pagination,
    resultsSummary,
    state,
    allJobs,
    currentUser: userState.currentUser,
    seenJobKeys: userState.seenJobKeys,
    savedJobKeys: userState.savedJobKeys,
    isJobsApiReady,
    getJobKeyForJob: getJobKeyForJobWithService,
    fullCountryName: fullCountryNameForJobs,
    goToPage,
    emitDesktopStartupMetric,
    renderJobRowHtml
  });
}

function goToPage(page) {
  return goToPageFromView(page, {
    filteredJobs,
    state,
    displayJobs,
    writeStateToUrl
  });
}


function recalculateItemsPerPage() {
  if (!jobsList) return false;

  const top = jobsList.getBoundingClientRect().top;
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
      const totalPages = Math.ceil(filteredJobs.length / state.itemsPerPage);
      if (state.currentPage < totalPages) {
        goToPage(state.currentPage + 1);
      }
    }
  });
}

function updateResultsSummary(total, from, to, loadedTotal = total) {
  return updateResultsSummaryFromView(resultsSummary, total, from, to, loadedTotal);
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
    dataSourcesListEl,
    dataSourcesCaptionEl
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
  setProgressVisibility(setText, fetchProgress, visible);
}

function setSourceStatus(text) {
  setStatusText(setText, sourceStatus, text);
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
  showJobsLoading(jobsList, text);
}

function showError(message, onRetry = null) {
  setJobsStartupState("error", "load_error");
  showJobsError(jobsList, pagination, message, () => {
    const retry = typeof onRetry === "function"
      ? onRetry
      : () => init().catch(err => handleJobsStartupFailure("Retry failed", err));
    return retry();
  });
  updateResultsSummary(0, 0, 0, allJobs.length);
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
