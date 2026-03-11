import { JobsStateModule as jobsStateModule } from "../../../jobs-state.js";
import { AdminConfig as adminConfig } from "../../../admin-config.js";
import { resolveStartupProbeEnabled } from "../../../startup-probe.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick,
  bindHandlersMap
} from "../../shared/ui/index.js";
import { sanitizeUrl as sanitizeUrlValue, fullCountryName as fullCountryNameFromData } from "../../shared/data/index.js";
import { BaluffoJobsParsing as jobsParsing } from "../../../jobs-parsing-utils.js";
import {
  detectWorkType,
  detectContractType,
  normalizeSector,
  classifyCompanyType,
  mapProfession,
  isInternshipJob,
  normalizeCountryToken,
  canonicalizeCountryName,
  fullCountryName as fullCountryNameFromDomainLayer,
  isValidCountry,
  normalizeJobs,
  getJobKeyForJob,
  toJobSnapshot
} from "../domain.js";
import {
  fetchUnifiedJobs as fetchUnifiedJobsFromData,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromData,
  parseUnifiedJobsPayload,
  parseCSVLarge as parseCSVLargeFromData
} from "../data-source.js";
import { isJobsApiReady, jobsAuthService, jobsSavedJobsService, jobsPageService } from "../services.js";
import { createJobsDispatcher, JOBS_ACTIONS } from "../actions.js";
import { renderDataSourcesPanel, renderJobRowHtml, showJobsLoading, showJobsError } from "../render.js";
import {
  readAutoRefreshAppliedId,
  readAutoRefreshSignal,
  writeAutoRefreshAppliedId,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  writeAutoRefreshSignal,
  rememberJobsUrl
} from "../state-sync/index.js";
import { cacheJobsDom } from "./dom.js";
import {
  toggleJobsAuthButtons,
  setJobsAuthControlsReady,
  setJobsAuthStatus
} from "./auth.js";
import {
  getPipelineProgressLabel,
  updateJobsPipelineUi as updateJobsPipelineUiFromModule,
  clearJobsPipelinePolling as clearJobsPipelinePollingFromModule,
  scheduleJobsPipelineStatusPoll as scheduleJobsPipelineStatusPollFromModule,
  callJobsBridge as callJobsBridgeFromModule
} from "./pipeline.js";
import {
  buildSeenRowKey,
  openJobsCacheDb as openJobsCacheDbFromModule,
  readJobsCache,
  writeJobsCache,
  loadSeenJobKeys,
  markSeenJob,
  markSeenJobsBulk,
  isJobsCacheStale
} from "./cache.js";
import {
  normalizeLifecycleStatus,
  optionExists,
  getAvailableRegionOptions as getAvailableRegionOptionsFromModule,
  getCountryFilterOptionLabel as getCountryFilterOptionLabelFromModule,
  matchesCountrySelection as matchesCountrySelectionFromModule,
  resolveRegionSelection as resolveRegionSelectionFromModule,
  countryMatchesRegion as countryMatchesRegionFromModule,
  resolveCountryCode as resolveCountryCodeFromModule,
  normalizeSelectedCountries,
  getCountrySelectionBadgeText,
  renderCountryPickerOptionsHtml,
  getCountryPickerTriggerText,
  getDefaultQuickFilterKeys,
  sanitizeQuickFilterKeys,
  renderQuickFiltersHtml,
  renderQuickFilterOptionsHtml,
  getNextQuickFilterKeys,
  applyQuickFilterToState,
  isQuickFilterActive,
  getActiveFilterSummaryItems
} from "./filters.js";
import { getVisiblePages } from "./pagination.js";
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
let jobsPipelineProgressEl;

let currentUser = null;
let savedJobKeys = new Set();
let seenJobKeys = new Set();

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
const UNIFIED_JSON_SOURCES = [
  { name: "Unified JSON light (local data)", url: "data/jobs-unified-light.json" },
  { name: "Unified JSON (local data)", url: "data/jobs-unified.json" },
  { name: "Unified JSON (root)", url: "jobs-unified.json" }
];
const STARTUP_PREVIEW_JSON_URLS = [
  "data/jobs-unified-startup.json",
  "jobs-unified-startup.json"
];
const UNIFIED_CSV_SOURCES = [
  { name: "Unified CSV (local data)", url: "data/jobs-unified.csv" },
  { name: "Unified CSV (root)", url: "jobs-unified.csv" }
];
const SHEETS_FALLBACK_SOURCE = {
  sheetId: "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE",
  gid: "1560329579"
};
const SOURCE_REGISTRY_ACTIVE_URLS = [
  "data/source-registry-active.json",
  "source-registry-active.json"
];
const JOBS_FETCH_REPORT_URLS = [
  "data/jobs-fetch-report.json",
  "jobs-fetch-report.json"
];
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const JOBS_PIPELINE_STATUS_POLL_MS = 1500;
const JOBS_PIPELINE_STATUS_IDLE_POLL_MS = 5000;
const JOBS_BRIDGE_REQUEST_TIMEOUT_MS = 1800;
const JOBS_FIRST_LOAD_REQUEST_TIMEOUT_MS = 4500;

const QUICK_FILTERS = Array.isArray(jobsStateModule.QUICK_FILTERS) ? jobsStateModule.QUICK_FILTERS : [];
const COUNTRY_DISPLAY_NAMES = (typeof Intl !== "undefined" && typeof Intl.DisplayNames === "function")
  ? new Intl.DisplayNames(["en"], { type: "region" })
  : null;
const COUNTRY_NAME_BY_CODE = {
  US: "United States",
  CA: "Canada",
  GB: "United Kingdom",
  UK: "United Kingdom",
  DE: "Germany",
  FI: "Finland",
  JP: "Japan",
  AU: "Australia",
  SG: "Singapore",
  FR: "France",
  NL: "Netherlands",
  SE: "Sweden",
  NO: "Norway",
  DK: "Denmark",
  ES: "Spain",
  IT: "Italy",
  BR: "Brazil",
  IN: "India",
  MX: "Mexico",
  AR: "Argentina",
  CL: "Chile",
  PL: "Poland",
  PT: "Portugal",
  IE: "Ireland",
  CH: "Switzerland",
  AT: "Austria",
  BE: "Belgium",
  CZ: "Czechia",
  CN: "China",
  KR: "South Korea",
  NZ: "New Zealand"
};
const COUNTRY_ALIAS_TO_CANONICAL = {
  usa: "United States",
  unitedstatesofamerica: "United States",
  america: "United States",
  uk: "United Kingdom",
  greatbritain: "United Kingdom",
  england: "United Kingdom",
  uae: "United Arab Emirates",
  czechrepublic: "Czechia",
  korea: "South Korea",
  republicofkorea: "South Korea",
  russianfederation: "Russia"
};
const COUNTRY_NAME_OPTIONS = {
  fullCountryNameFromData,
  countryNamesByCode: COUNTRY_NAME_BY_CODE,
  countryAliasToCanonical: COUNTRY_ALIAS_TO_CANONICAL,
  countryDisplayNames: COUNTRY_DISPLAY_NAMES
};
const REGION_DEFINITIONS = [
  {
    value: "region:europe",
    label: "Europe",
    countries: [
      "Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria",
      "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia", "Finland", "France", "Germany",
      "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein",
      "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands",
      "North Macedonia", "Norway", "Poland", "Portugal", "Romania", "San Marino", "Serbia",
      "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Ukraine", "United Kingdom",
      "Vatican City"
    ]
  },
  {
    value: "region:north-america",
    label: "North America",
    countries: [
      "Antigua and Barbuda", "Bahamas", "Barbados", "Belize", "Canada", "Costa Rica", "Cuba",
      "Dominica", "Dominican Republic", "El Salvador", "Grenada", "Guatemala", "Haiti", "Honduras",
      "Jamaica", "Mexico", "Nicaragua", "Panama", "Saint Kitts and Nevis", "Saint Lucia",
      "Saint Vincent and the Grenadines", "Trinidad and Tobago", "United States"
    ]
  },
  {
    value: "region:south-america",
    label: "South America",
    countries: [
      "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador", "Guyana", "Paraguay",
      "Peru", "Suriname", "Uruguay", "Venezuela"
    ]
  },
  {
    value: "region:asia",
    label: "Asia",
    countries: [
      "Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Bhutan", "Brunei", "Cambodia",
      "China", "Georgia", "India", "Indonesia", "Iran", "Iraq", "Israel", "Japan", "Jordan",
      "Kazakhstan", "Kuwait", "Kyrgyzstan", "Laos", "Lebanon", "Malaysia", "Maldives", "Mongolia",
      "Myanmar", "Nepal", "North Korea", "Oman", "Pakistan", "Palestine", "Philippines", "Qatar",
      "Russia", "Saudi Arabia", "Singapore", "South Korea", "Sri Lanka", "Syria", "Taiwan",
      "Tajikistan", "Thailand", "Timor-Leste", "Turkey", "Turkmenistan", "United Arab Emirates",
      "Uzbekistan", "Vietnam", "Yemen"
    ]
  },
  {
    value: "region:africa",
    label: "Africa",
    countries: [
      "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde", "Cameroon",
      "Central African Republic", "Chad", "Comoros", "Congo", "Democratic Republic of the Congo",
      "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
      "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Kenya", "Lesotho", "Liberia", "Libya",
      "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia",
      "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone",
      "Somalia", "South Africa", "South Sudan", "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda",
      "Zambia", "Zimbabwe"
    ]
  },
  {
    value: "region:oceania",
    label: "Oceania",
    countries: [
      "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Zealand",
      "Palau", "Papua New Guinea", "Samoa", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu"
    ]
  },
  {
    value: "region:remote-worldwide",
    label: "Remote / Worldwide",
    countries: ["Remote", "Worldwide", "Global"]
  }
];
const REMOTE_WORLDWIDE_TOKENS = new Set(
  ["remote", "worldwide", "global", "anywhere"].map(item => normalizeCountryToken(item))
);
const REGION_COUNTRY_TOKEN_LOOKUP = Object.fromEntries(
  REGION_DEFINITIONS.map(region => [
    region.value,
    new Set(region.countries.map(item => normalizeCountryToken(canonicalizeCountryName(item, COUNTRY_NAME_OPTIONS))).filter(Boolean))
  ])
);

let refreshInFlight = false;
let availableProfessions = [];
let availableCountries = [];
let availableCountryFilterValues = [];
let visibleQuickFilterKeys = [];
let hasInitializedJobsFeed = false;
let pendingAutoRefreshSignal = null;
let lastHandledAutoRefreshSignalId = readAppliedAutoRefreshId();
let lastFilterOptionsSignature = "";
let authReadyPollTimer = null;
let authStateListenerBound = false;
let nonCriticalStartupScheduled = false;
const jobsPipelineUiState = createJobsPipelineUiState();
const startupMetrics = createJobsStartupMetrics({
  emitMetric: (event, payload) => {
    if (!isDesktopRuntimeMode()) return;
    fetch(`${ADMIN_BRIDGE_BASE}/desktop-local-data/startup-metric?t=${Date.now()}`, {
      method: "POST",
      cache: "no-store",
      keepalive: true,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event: String(event || "").trim() || "unknown",
        payload: payload && typeof payload === "object" ? payload : {}
      })
    }).catch(() => {});
  }
});
const callJobsBridge = createJobsBridgeRequest({
  baseUrl: ADMIN_BRIDGE_BASE,
  timeoutMs: JOBS_BRIDGE_REQUEST_TIMEOUT_MS,
  request: callJobsBridgeFromModule
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
  console.info(`[${JOBS_LOG_SCOPE}] ${message}`, ...args);
}

function logJobsError(message, err) {
  console.error(`[${JOBS_LOG_SCOPE}] ${message}:`, err);
}

function bootJobsPage() {
  cacheDom();
  initializeQuickFilters();
  bindEvents();
  readStateFromUrl();
  applyStateToStaticFilters();
  init().catch(err => logJobsError("Error initializing jobs", err));
}

function scheduleNonCriticalStartupWork() {
  if (nonCriticalStartupScheduled) return;
  nonCriticalStartupScheduled = true;
  scheduleNonCriticalStartup(window, () => {
    renderDataSources().catch(() => {});
    ensureJobsPipelineStatusWatch();
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
    savedJobsBtn,
    activeFiltersSummaryEl,
    quickActionsEl,
    customizeQuickFiltersBtn,
    quickFiltersPanel,
    quickFiltersOptionsEl,
    quickFiltersResetBtn,
    dataSourcesListEl,
    dataSourcesCaptionEl,
    jobsPipelineRunBtn,
    jobsPipelineProgressEl
  } = cacheJobsDom(document));
}

function isDesktopRuntimeMode() {
  return isDesktopRuntimeModeFromStartup(window.location.href);
}

function emitDesktopStartupMetric(event, payload = {}) {
  startupMetrics.emit(event, payload);
}

function markStartupRendered(stage, rowCount) {
  startupMetrics.markRendered(stage, rowCount);
}

function markJobsFirstInteractive(reason) {
  startupMetrics.markInteractive(reason);
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

function bindEvents() {

  const clickHandlers = new Map([
    [savedJobsBtn, () => {
      rememberCurrentJobsUrl();
      window.location.href = "saved.html";
    }],
    [countryPickerClearBtn, () => {
      state.filters.countries = [];
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    }],
    [quickFiltersResetBtn, () => {
      resetQuickFilterPreferences();
    }]
  ]);
  bindHandlersMap(clickHandlers);

  bindAsyncClick(authSignInBtn, signInUser);
  bindAsyncClick(authSignOutBtn, signOutUser);
  bindAsyncClick(refreshJobsBtn, () => refreshJobsNow({ manual: true }));
  bindAsyncClick(jobsPipelineRunBtn, triggerJobsPipelineRun);

  [
    workTypeFilter,
    lifecycleStatusFilter,
    countryFilter,
    cityFilter,
    sectorFilter,
    professionFilter,
    sortFilter
  ].forEach(el => bindUi(el, "change", () => onFilterChange()));

  if (professionSearchFilter) {
    professionSearchFilter.addEventListener("input", () => {
      renderProfessionOptions(professionSearchFilter.value);
    });
  }

  if (countryPickerBtn) {
    countryPickerBtn.addEventListener("click", e => {
      e.stopPropagation();
      toggleCountryPickerPanel();
    });
  }
  if (countryPickerSearch) {
    countryPickerSearch.addEventListener("input", () => {
      renderCountryPickerOptions(countryPickerSearch.value);
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
      applyStateToFilters();
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
      onFilterChange();
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
      applyQuickFilter(quick);
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  }

  if (customizeQuickFiltersBtn) {
    customizeQuickFiltersBtn.addEventListener("click", event => {
      event.stopPropagation();
      toggleQuickFiltersPanel();
    });
  }

  if (quickFiltersOptionsEl) {
    quickFiltersOptionsEl.addEventListener("change", event => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
      const { quick } = target.dataset;
      if (!quick) return;
      setQuickFilterVisibility(quick, target.checked);
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
    updateFilterOptions,
    applyStateToFilters,
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

function updateJobsPipelineUi({ running = false, disabled = false, buttonLabel = "", progressLabel = "", isError = false } = {}) {
  updateJobsPipelineUiFromModule(
    { jobsPipelineRunBtn, jobsPipelineProgressEl },
    { running, disabled, buttonLabel, progressLabel, isError }
  );
}

function clearJobsPipelinePolling() {
  clearJobsPipelinePollingFromModule(jobsPipelineUiState);
}

function scheduleJobsPipelineStatusPoll(delayMs) {
  scheduleJobsPipelineStatusPollFromModule(
    jobsPipelineUiState,
    delayMs,
    pollJobsPipelineStatus,
    JOBS_PIPELINE_STATUS_POLL_MS
  );
}

function handlePipelineCompletionStatus(payload) {
  const updatesFound = Boolean(payload?.updatesFound || payload?.refreshRecommended);
  setRefreshJobsNeedsAttention(updatesFound);
  jobsPipelineUiState.active = false;
  jobsPipelineUiState.runId = "";
  updateJobsPipelineUi({
    running: false,
    disabled: !jobsPipelineUiState.bridgeOnline,
    progressLabel: updatesFound ? "Pipeline complete. Updates found." : "Pipeline complete."
  });
  if (updatesFound) {
    showToast("Pipeline completed. Refresh jobs to load new updates.", "success");
  } else if (payload?.error) {
    showToast(`Pipeline failed: ${String(payload.error)}`, "error");
  }
}

async function pollJobsPipelineStatus() {
  try {
    const payload = await callJobsBridge("/tasks/run-jobs-pipeline-status");
    jobsPipelineUiState.bridgeOnline = true;

    const active = Boolean(payload?.active);
    const runId = String(payload?.runId || "");
    if (active) {
      jobsPipelineUiState.active = true;
      jobsPipelineUiState.runId = runId || jobsPipelineUiState.runId;
      updateJobsPipelineUi({
        running: true,
        disabled: true,
        buttonLabel: "Pipeline Running...",
        progressLabel: getPipelineProgressLabel(payload)
      });
      scheduleJobsPipelineStatusPoll(JOBS_PIPELINE_STATUS_POLL_MS);
      return;
    }

    const trackedRunId = String(jobsPipelineUiState.runId || "");
    if ((trackedRunId && trackedRunId === runId) || jobsPipelineUiState.active) {
      handlePipelineCompletionStatus(payload);
    } else {
      updateJobsPipelineUi({
        running: false,
        disabled: false,
        progressLabel: "Ready"
      });
    }
    scheduleJobsPipelineStatusPoll(JOBS_PIPELINE_STATUS_IDLE_POLL_MS);
  } catch {
    jobsPipelineUiState.bridgeOnline = false;
    jobsPipelineUiState.active = false;
    jobsPipelineUiState.runId = "";
    updateJobsPipelineUi({
      running: false,
      disabled: true,
      progressLabel: "Bridge offline (desktop runtime required)",
      isError: true
    });
    scheduleJobsPipelineStatusPoll(JOBS_PIPELINE_STATUS_IDLE_POLL_MS);
  }
}

function ensureJobsPipelineStatusWatch() {
  updateJobsPipelineUi({
    running: false,
    disabled: true,
    progressLabel: "Checking bridge..."
  });
  pollJobsPipelineStatus().catch(() => {});
}

async function triggerJobsPipelineRun() {
  if (!jobsPipelineRunBtn || jobsPipelineRunBtn.disabled || jobsPipelineUiState.active) return;

  updateJobsPipelineUi({
    running: true,
    disabled: true,
    buttonLabel: "Starting Pipeline...",
    progressLabel: "Requesting pipeline start..."
  });
  try {
    const payload = await callJobsBridge("/tasks/run-jobs-pipeline", {
      method: "POST",
      body: {
        jobsPageLoadedCount: Array.isArray(allJobs) ? allJobs.length : 0
      }
    });
    const started = Boolean(payload?.started);
    if (!started) {
      throw new Error(String(payload?.error || "pipeline did not start"));
    }
    jobsPipelineUiState.bridgeOnline = true;
    jobsPipelineUiState.active = true;
    jobsPipelineUiState.runId = String(payload?.runId || "");
    updateJobsPipelineUi({
      running: true,
      disabled: true,
      buttonLabel: "Pipeline Running...",
      progressLabel: getPipelineProgressLabel(payload)
    });
    showToast("Jobs pipeline started.", "success");
    scheduleJobsPipelineStatusPoll(JOBS_PIPELINE_STATUS_POLL_MS);
  } catch (err) {
    const message = String(err?.message || "Could not start jobs pipeline.");
    jobsPipelineUiState.active = false;
    jobsPipelineUiState.runId = "";
    updateJobsPipelineUi({
      running: false,
      disabled: true,
      progressLabel: "Bridge offline (desktop runtime required)",
      isError: true
    });
    showToast(message.toLowerCase().includes("409") ? "Pipeline already running." : "Could not start jobs pipeline.", "error");
    scheduleJobsPipelineStatusPoll(JOBS_PIPELINE_STATUS_IDLE_POLL_MS);
  }
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
  if (!isJobsApiReady() || !jobsPageService.isAvailable()) {
    emitDesktopStartupMetric("jobs_auth_waiting");
    setAuthStatus("Local auth starting...");
    toggleAuthButtons(false);
    setAuthControlsReady(false);
    scheduleAuthReadyPoll();
    return;
  }
  stopAuthReadyPoll();
  emitDesktopStartupMetric("jobs_auth_ready");
  setAuthControlsReady(true);
  if (authStateListenerBound) return;
  authStateListenerBound = true;

  jobsAuthService.onAuthStateChanged(async user => {
    currentUser = user || null;
    jobsDispatch.dispatch({
      type: JOBS_ACTIONS.AUTH_CHANGED,
      payload: { uid: currentUser?.uid || "" }
    });
    if (!currentUser) {
      savedJobKeys = new Set();
      seenJobKeys = new Set();
      setAuthStatus("Browsing as guest");
      toggleAuthButtons(false);
      if (allJobs.length) applyFiltersAndRender({ resetPage: false });
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    toggleAuthButtons(true);

    try {
      const [savedKeysResult, loadedSeenJobKeys] = await Promise.all([
        jobsSavedJobsService.getSavedJobKeys(currentUser.uid),
        loadSeenJobKeys(currentUser.uid)
      ]);
      savedJobKeys = new Set(savedKeysResult.data || []);
      seenJobKeys = loadedSeenJobKeys;
    } catch (err) {
      logJobsError("Failed to load saved jobs", err);
      showToast("Could not load profile job state.", "error");
      savedJobKeys = new Set();
      seenJobKeys = new Set();
    }

    if (allJobs.length) applyFiltersAndRender({ resetPage: false });
  });
}

function stopAuthReadyPoll() {
  if (!authReadyPollTimer) return;
  clearTimeout(authReadyPollTimer);
  authReadyPollTimer = null;
}

function scheduleAuthReadyPoll(delayMs = 600) {
  stopAuthReadyPoll();
  authReadyPollTimer = setTimeout(() => {
    authReadyPollTimer = null;
    if (isJobsApiReady() && jobsPageService.isAvailable()) {
      initAuth();
      return;
    }
    scheduleAuthReadyPoll(delayMs);
  }, Math.max(250, Number(delayMs) || 600));
}

function setAuthControlsReady(ready) {
  setJobsAuthControlsReady({ authSignInBtn, authSignOutBtn }, ready);
}

function setAuthStatus(text) {
  setJobsAuthStatus({ authStatus, authStatusHint, authAvatar }, text);
}

function toggleAuthButtons(isSignedIn) {
  toggleJobsAuthButtons({ authSignInBtn, authSignOutBtn, savedJobsBtn }, isSignedIn);
}

async function signInUser() {
  if (!isJobsApiReady() || !jobsPageService.isAvailable()) {
    setAuthControlsReady(false);
    scheduleAuthReadyPoll();
    showToast("Local auth provider is starting. Try again in a moment.", "info");
    return;
  }
  if (!authStateListenerBound) {
    initAuth();
  }
  setAuthControlsReady(true);
  const result = await jobsAuthService.signIn();
  if (!result.ok) {
    if (String(result.error || "").toLowerCase().includes("cancel")) return;
    logJobsError("Sign-in failed", new Error(result.error));
    showToast("Sign-in failed. Please try again.", "error");
  }
}

async function signOutUser() {
  if (!isJobsApiReady() || !jobsPageService.isAvailable()) {
    setAuthControlsReady(false);
    scheduleAuthReadyPoll();
    return;
  }
  if (!authStateListenerBound) {
    initAuth();
  }
  setAuthControlsReady(true);
  const result = await jobsAuthService.signOut();
  if (!result.ok) {
    logJobsError("Sign-out failed", new Error(result.error));
    showToast("Sign-out failed. Please try again.", "error");
  }
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
  emitDesktopStartupMetric("jobs_write_state_params_start");
  const url = buildJobsPageUrl(window.location.pathname, state);
  emitDesktopStartupMetric("jobs_write_state_params_complete");
  if (resolveStartupProbeEnabled()) {
    emitDesktopStartupMetric("jobs_write_state_probe_skip", { url });
    return;
  }
  if (isDesktopRuntimeMode()) {
    if (!desktopUrlStateReady) {
      desktopPendingRememberJobsUrl = true;
      desktopPendingJobsUrl = url;
      emitDesktopStartupMetric("jobs_write_state_desktop_deferred", { url });
      return;
    }
    emitDesktopStartupMetric("jobs_write_state_desktop_flush", { url });
    window.setTimeout(() => {
      persistDesktopJobsUrlState(url);
    }, 0);
    return;
  }
  emitDesktopStartupMetric("jobs_write_state_replace_state_start", { url });
  window.history.replaceState({}, "", url);
  emitDesktopStartupMetric("jobs_write_state_replace_state_complete");
  emitDesktopStartupMetric("jobs_write_state_remember_url_start");
  rememberCurrentJobsUrl();
  emitDesktopStartupMetric("jobs_write_state_remember_url_complete");
}

function persistDesktopJobsUrlState(url) {
  try {
    emitDesktopStartupMetric("jobs_write_state_remember_url_start");
    rememberJobsUrl(JOBS_LAST_URL_KEY, String(url || ""));
    emitDesktopStartupMetric("jobs_write_state_remember_url_complete");
  } catch {
    emitDesktopStartupMetric("jobs_write_state_remember_url_failed");
  }
}

function rememberCurrentJobsUrl() {
  const url = `${window.location.pathname}${window.location.search}`;
  if (isDesktopRuntimeMode()) {
    if (!desktopUrlStateReady) {
      desktopPendingRememberJobsUrl = true;
      desktopPendingJobsUrl = url;
      return;
    }
    window.setTimeout(() => {
      persistDesktopJobsUrlState(url);
    }, 0);
    return;
  }
  rememberJobsUrl(JOBS_LAST_URL_KEY, url);
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
    updateFilterOptions,
    applyStateToFilters,
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
    updateFilterOptions,
    applyStateToFilters,
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

async function markJobSeenFromInteraction(jobKey) {
  const safeJobKey = String(jobKey || "").trim();
  if (!currentUser?.uid || !safeJobKey) return;
  if (seenJobKeys.has(safeJobKey)) return;

  seenJobKeys.add(safeJobKey);
  await markSeenJob(currentUser.uid, safeJobKey, {
    openDb: openJobsCacheDb,
    seenStore: JOBS_SEEN_STORE,
    seenAt: Date.now(),
    buildKey: buildSeenRowKey
  });
  if (allJobs.length) applyFiltersAndRender({ resetPage: false });
}

function applyStateToStaticFilters() {
  if (workTypeFilter) workTypeFilter.value = state.filters.workType;
  if (lifecycleStatusFilter) lifecycleStatusFilter.value = state.filters.lifecycleStatus || "active";
  if (searchFilter) searchFilter.value = state.filters.search;
  if (sortFilter) sortFilter.value = state.filters.sort;
}

function applyStateToFilters() {
  applyStateToStaticFilters();
  state.filters.countries = normalizeSelectedCountries(state.filters.countries, {
    resolveCountryCode,
    availableCountryFilterValues
  });

  if (countryFilter) {
    const selected = new Set(state.filters.countries || []);
    Array.from(countryFilter.options).forEach(option => {
      option.selected = selected.has(option.value);
    });
  }
  syncCountryPickerChecks();

  if (cityFilter && optionExists(cityFilter, state.filters.city)) {
    cityFilter.value = state.filters.city;
  } else {
    state.filters.city = "";
  }

  if (sectorFilter && optionExists(sectorFilter, state.filters.sector)) {
    sectorFilter.value = state.filters.sector;
  } else {
    state.filters.sector = "";
  }

  if (professionFilter && optionExists(professionFilter, state.filters.profession)) {
    professionFilter.value = state.filters.profession;
  } else if (state.filters.profession && state.filters.profession !== "") {
    state.filters.profession = "";
  }

  updateCountrySelectionBadge();
  updateCountryPickerTrigger();
  updateQuickChipStates();
  updateActiveFiltersSummary();
}

function onFilterChange() {
  jobsDispatch.dispatch({
    type: JOBS_ACTIONS.FILTERS_CHANGED,
    payload: { signature: JSON.stringify(state.filters || {}) }
  });
  syncStateFromFilters();
  applyFiltersAndRender({ resetPage: true });
}

function syncStateFromFilters() {
  state.filters.workType = workTypeFilter ? workTypeFilter.value : "";
  state.filters.lifecycleStatus = normalizeLifecycleStatus(lifecycleStatusFilter ? lifecycleStatusFilter.value : "active", "active");
  state.filters.countries = countryFilter
    ? Array.from(countryFilter.selectedOptions).map(option => option.value)
    : [];
  state.filters.city = cityFilter ? cityFilter.value : "";
  state.filters.sector = sectorFilter ? sectorFilter.value : "";
  state.filters.profession = professionFilter ? professionFilter.value : "";
  state.filters.newOnly = Boolean(state.filters.newOnly);
  state.filters.excludeInternship = Boolean(state.filters.excludeInternship);
  state.filters.search = searchFilter ? searchFilter.value.trim() : "";
  state.filters.sort = sortFilter ? sortFilter.value : "relevance";
  updateCountrySelectionBadge();
  updateCountryPickerTrigger();
  updateQuickChipStates();
  updateActiveFiltersSummary();
}

function resetFilters() {
  state.filters = { ...defaultFilters, countries: Array.from(defaultFilters.countries || []) };
  if (professionSearchFilter) professionSearchFilter.value = "";
  renderProfessionOptions("");
  applyStateToFilters();
}

function applyFiltersAndRender({ resetPage }) {
  if (resetPage) {
    state.currentPage = 1;
  }

  emitDesktopStartupMetric("jobs_apply_filters_start", {
    resetPage: Boolean(resetPage),
    totalJobs: allJobs.length
  });
  syncStateFromFilters();

  const searchTerm = state.filters.search.toLowerCase();

  filteredJobs = allJobs.filter(job => {
    const matchesWorkType = !state.filters.workType || job.workType === state.filters.workType;
    const lifecycleStatus = String(job.status || "active").toLowerCase() || "active";
    const matchesLifecycle = !state.filters.lifecycleStatus || lifecycleStatus === state.filters.lifecycleStatus;
    const matchesCountry = state.filters.countries.length === 0
      || matchesCountrySelection(job.country, state.filters.countries);
    const matchesCity = !state.filters.city || job.city === state.filters.city;
    const matchesSector = !state.filters.sector || job.sector === state.filters.sector;
    const matchesProfession = !state.filters.profession || job.profession === state.filters.profession;
    const jobKey = getJobKeyForJobWithService(job);
    const matchesNewOnly = !state.filters.newOnly || !currentUser || !seenJobKeys.has(jobKey);
    const matchesInternship = !state.filters.excludeInternship || !isInternshipJob(job);
    const matchesSearch =
      !searchTerm ||
      job.title.toLowerCase().includes(searchTerm) ||
      job.company.toLowerCase().includes(searchTerm) ||
      (job.city || "").toLowerCase().includes(searchTerm) ||
      (job.sector || "").toLowerCase().includes(searchTerm);

    return matchesWorkType
      && matchesLifecycle
      && matchesCountry
      && matchesCity
      && matchesSector
      && matchesProfession
      && matchesNewOnly
      && matchesInternship
      && matchesSearch;
  });

  emitDesktopStartupMetric("jobs_apply_filters_complete", {
    filteredCount: filteredJobs.length
  });
  sortJobs(filteredJobs, state.filters.sort);
  emitDesktopStartupMetric("jobs_sort_complete", {
    filteredCount: filteredJobs.length,
    sortMode: String(state.filters.sort || "relevance")
  });
  displayJobs(filteredJobs);
  emitDesktopStartupMetric("jobs_write_state_start");
  writeStateToUrl();
  emitDesktopStartupMetric("jobs_write_state_complete");
}

function sortJobs(jobs, sortMode) {
  if (sortMode === "title-asc") {
    jobs.sort((a, b) => a.title.localeCompare(b.title));
    return;
  }
  if (sortMode === "company-asc") {
    jobs.sort((a, b) => a.company.localeCompare(b.company));
    return;
  }
  if (sortMode === "country-asc") {
    jobs.sort((a, b) =>
      fullCountryNameFromDomainLayer(a.country, COUNTRY_NAME_OPTIONS)
        .localeCompare(fullCountryNameFromDomainLayer(b.country, COUNTRY_NAME_OPTIONS))
    );
    return;
  }
  if (sortMode === "remote-first") {
    const order = { Remote: 0, Hybrid: 1, Onsite: 2 };
    jobs.sort((a, b) => {
      const diff = (order[a.workType] ?? 99) - (order[b.workType] ?? 99);
      if (diff !== 0) return diff;
      return a.title.localeCompare(b.title);
    });
  }
}

function displayJobs(jobs) {
  if (!jobsList || !pagination) return;
  emitDesktopStartupMetric("jobs_display_start", {
    totalCount: jobs.length,
    currentPage: state.currentPage
  });

  if (jobs.length === 0) {
    jobsList.innerHTML = '<div class="no-results">No jobs found matching your filters.</div>';
    pagination.innerHTML = "";
    updateResultsSummary(0, 0, 0);
    emitDesktopStartupMetric("jobs_display_empty");
    return;
  }

  const totalPages = Math.ceil(jobs.length / state.itemsPerPage);
  if (state.currentPage > totalPages) state.currentPage = totalPages;

  const startIndex = (state.currentPage - 1) * state.itemsPerPage;
  const pageJobs = jobs.slice(startIndex, startIndex + state.itemsPerPage);
  emitDesktopStartupMetric("jobs_display_markup_start", {
    pageJobs: pageJobs.length,
    totalPages
  });

  jobsList.innerHTML = `
    <div class="jobs-table-header">
      <div class="job-row-header">
        <div class="col-freshness" title="Freshness (posted/fetched recency)" aria-hidden="true"></div>
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-sector">Sector</div>
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-contract">Contract</div>
        <div class="col-type">Type</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${pageJobs.map(renderJobRow).join("")}
    </div>
  `;
  emitDesktopStartupMetric("jobs_display_dom_committed", {
    pageJobs: pageJobs.length
  });

  renderPagination(totalPages);
  emitDesktopStartupMetric("jobs_display_pagination_complete", {
    totalPages
  });
  bindRenderedJobEvents(pageJobs);
  emitDesktopStartupMetric("jobs_display_bind_complete", {
    pageJobs: pageJobs.length
  });
  updateResultsSummary(jobs.length, startIndex + 1, startIndex + pageJobs.length);
  emitDesktopStartupMetric("jobs_display_complete", {
    startIndex: startIndex + 1,
    endIndex: startIndex + pageJobs.length,
    totalCount: jobs.length
  });
  window.requestAnimationFrame(() => {
    emitDesktopStartupMetric("jobs_display_frame_presented", {
      pageJobs: pageJobs.length
    });
  });
}

function renderJobRow(job) {
  const jobKey = getJobKeyForJobWithService(job);
  const isSeen = Boolean(currentUser && seenJobKeys.has(jobKey));
  return renderJobRowHtml(job, {
    fullCountryName: value => fullCountryNameFromDomainLayer(value, COUNTRY_NAME_OPTIONS),
    sanitizeUrl,
    getJobKeyForJob: getJobKeyForJobWithService,
    savedJobKeys,
    isSeen,
    isNew: Boolean(currentUser && !isSeen),
    isJobsApiReady,
    toContractClass,
    capitalizeFirst
  });
}

function bindRenderedJobEvents(pageJobs) {
  if (!jobsList) return;
  const pageById = new Map(pageJobs.map(job => [String(job.id), job]));

  jobsList.querySelectorAll(".job-row[data-job-link]").forEach(row => {
    const link = row.dataset.jobLink;
    if (!link) return;
    const jobKey = String(row.dataset.jobKey || "").trim();

    row.tabIndex = 0;
    row.setAttribute("role", "link");
    row.addEventListener("click", e => {
      if (e.target.closest(".save-job-btn")) return;
      window.open(link, "_blank", "noopener,noreferrer");
      markJobSeenFromInteraction(jobKey).catch(() => {});
    });
    row.addEventListener("keydown", e => {
      if (e.key !== "Enter") return;
      if (e.target.closest(".save-job-btn")) return;
      window.open(link, "_blank", "noopener,noreferrer");
      markJobSeenFromInteraction(jobKey).catch(() => {});
    });
  });

  jobsList.querySelectorAll(".save-job-btn").forEach(btn => {
    btn.addEventListener("click", async e => {
      e.preventDefault();
      e.stopPropagation();

      const job = pageById.get(btn.dataset.jobId || "");
      if (!job) return;
      await toggleSaveJob(job);
    });
  });
}

function renderPagination(totalPages) {
  let html = "";

  if (totalPages > 1) {
    if (state.currentPage > 1) {
      html += `<button class="page-btn" data-page="${state.currentPage - 1}" aria-label="Previous page">Prev</button>`;
    }

    const visiblePages = getVisiblePages(totalPages, state.currentPage);
    visiblePages.forEach(item => {
      if (item === "...") {
        html += '<span class="page-ellipsis">...</span>';
      } else {
        html += `<button class="page-btn ${item === state.currentPage ? "active" : ""}" data-page="${item}">${item}</button>`;
      }
    });

    if (state.currentPage < totalPages) {
      html += `<button class="page-btn" data-page="${state.currentPage + 1}" aria-label="Next page">Next</button>`;
    }
  }

  pagination.innerHTML = html;

  pagination.querySelectorAll(".page-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const page = parseInt(btn.dataset.page, 10);
      if (!isNaN(page)) {
        goToPage(page);
      }
    });
  });
}

function goToPage(page) {
  const totalPages = Math.max(1, Math.ceil(filteredJobs.length / state.itemsPerPage));
  const nextPage = Math.min(Math.max(page, 1), totalPages);
  if (nextPage === state.currentPage) return;

  state.currentPage = nextPage;
  displayJobs(filteredJobs);
  writeStateToUrl();
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

function updateResultsSummary(total, from, to) {
  if (!resultsSummary) return;
  if (total === 0) {
    resultsSummary.textContent = "0 jobs";
    return;
  }
  const pageText = `Showing ${from}-${to} of ${total.toLocaleString()} jobs`;
  resultsSummary.textContent = pageText;
}

function updateFilterOptions() {
  if (!workTypeFilter || !countryFilter || !professionFilter || !cityFilter || !sectorFilter) return;

  const countries = new Set();
  const professions = new Set();
  const cities = new Set();
  const sectors = new Set();

  allJobs.forEach(job => {
    if (isValidCountry(job.country)) countries.add(job.country);
    if (job.profession) professions.add(job.profession);
    if (job.city) cities.add(job.city);
    if (job.sector) sectors.add(job.sector);
  });

  availableCountries = Array.from(countries).sort((a, b) =>
    fullCountryNameFromDomainLayer(a, COUNTRY_NAME_OPTIONS)
      .localeCompare(fullCountryNameFromDomainLayer(b, COUNTRY_NAME_OPTIONS))
  );
  const availableRegions = getAvailableRegionOptions(availableCountries);
  availableCountryFilterValues = [
    ...availableRegions.map(region => region.value),
    ...availableCountries
  ];

  countryFilter.innerHTML = "";
  availableCountryFilterValues.forEach(country => {
    const opt = document.createElement("option");
    opt.value = country;
    opt.textContent = getCountryFilterOptionLabel(country);
    countryFilter.appendChild(opt);
  });
  renderCountryPickerOptions(countryPickerSearch ? countryPickerSearch.value : "");

  cityFilter.innerHTML = '<option value="">All Cities</option>';
  Array.from(cities).sort().forEach(city => {
    const opt = document.createElement("option");
    opt.value = city;
    opt.textContent = city;
    cityFilter.appendChild(opt);
  });

  sectorFilter.innerHTML = '<option value="">All Sectors</option>';
  Array.from(sectors).sort((a, b) => a.localeCompare(b)).forEach(sector => {
    const opt = document.createElement("option");
    opt.value = sector;
    opt.textContent = sector;
    sectorFilter.appendChild(opt);
  });

  availableProfessions = Array.from(professions).sort();
  renderProfessionOptions(professionSearchFilter ? professionSearchFilter.value : "");
  updateCountrySelectionBadge();
  updateCountryPickerTrigger();
}

function renderProfessionOptions(query = "") {
  if (!professionFilter) return;
  const normalized = String(query || "").trim().toLowerCase();
  const current = state.filters.profession;

  professionFilter.innerHTML = '<option value="">All Roles</option>';
  availableProfessions.forEach(profession => {
    const label = PROFESSION_LABELS[profession] || capitalizeFirst(profession);
    if (normalized && !label.toLowerCase().includes(normalized) && profession !== current) {
      return;
    }
    const opt = document.createElement("option");
    opt.value = profession;
    opt.textContent = label;
    professionFilter.appendChild(opt);
  });

  if (optionExists(professionFilter, current)) {
    professionFilter.value = current;
  } else if (current) {
    state.filters.profession = "";
    professionFilter.value = "";
  }
}

function updateCountrySelectionBadge() {
  if (!countrySelectionBadge) return;
  countrySelectionBadge.textContent = getCountrySelectionBadgeText(state.filters.countries);
}

function getCountryFilterOptionLabel(value) {
  return getCountryFilterOptionLabelFromModule(value, {
    regionDefinitions: REGION_DEFINITIONS,
    fullCountryName: fullCountryNameFromDomainLayer,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}

function getAvailableRegionOptions(countries) {
  return getAvailableRegionOptionsFromModule(countries, {
    canonicalizeCountryName,
    normalizeCountryToken,
    regionDefinitions: REGION_DEFINITIONS,
    regionCountryTokenLookup: REGION_COUNTRY_TOKEN_LOOKUP,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}

function matchesCountrySelection(jobCountry, selections) {
  return matchesCountrySelectionFromModule(jobCountry, selections, {
    canonicalizeCountryName,
    normalizeCountryToken,
    countryNameOptions: COUNTRY_NAME_OPTIONS,
    regionCountryMatcher: countryMatchesRegion
  });
}

function countryMatchesRegion(countryToken, regionValue) {
  return countryMatchesRegionFromModule(countryToken, regionValue, {
    regionCountryTokenLookup: REGION_COUNTRY_TOKEN_LOOKUP,
    remoteWorldwideTokens: REMOTE_WORLDWIDE_TOKENS
  });
}

function toggleCountrySelection(countryCode) {
  const mapped = resolveCountryCode(countryCode);
  if (!mapped) return;
  const selected = new Set(state.filters.countries || []);
  if (selected.has(mapped)) {
    selected.delete(mapped);
  } else {
    selected.add(mapped);
  }
  state.filters.countries = Array.from(selected);
}

function resolveCountryCode(countryCode) {
  return resolveCountryCodeFromModule(countryCode, {
    availableCountries,
    availableCountryFilterValues,
    resolveRegionValue: value => resolveRegionSelectionFromModule(value, {
      normalizeCountryToken,
      regionDefinitions: REGION_DEFINITIONS
    }),
    canonicalizeCountryName,
    normalizeCountryToken,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}

function syncCountryPickerChecks() {
  if (!countryPickerOptions) return;
  const selected = new Set(state.filters.countries || []);
  countryPickerOptions.querySelectorAll('input[type="checkbox"]').forEach(input => {
    input.checked = selected.has(input.value);
  });
}

function renderCountryPickerOptions(query = "") {
  if (!countryPickerOptions) return;
  countryPickerOptions.innerHTML = renderCountryPickerOptionsHtml({
    availableCountryFilterValues,
    selectedCountries: state.filters.countries,
    query,
    getCountryFilterOptionLabel,
    escapeHtml
  });
}

function toggleCountryPickerPanel() {
  if (!countryPickerPanel) return;
  const isHidden = countryPickerPanel.classList.contains("hidden");
  if (isHidden) {
    countryPickerPanel.classList.remove("hidden");
    if (countryPickerBtn) countryPickerBtn.setAttribute("aria-expanded", "true");
    if (countryPickerSearch) countryPickerSearch.focus();
    return;
  }
  closeCountryPickerPanel();
}

function closeCountryPickerPanel() {
  if (!countryPickerPanel) return;
  countryPickerPanel.classList.add("hidden");
  if (countryPickerBtn) countryPickerBtn.setAttribute("aria-expanded", "false");
}

function initializeQuickFilters() {
  visibleQuickFilterKeys = loadQuickFilterPreferences();
  renderQuickFilters();
  renderQuickFilterOptions();
}

function loadQuickFilterPreferences() {
  const defaults = getDefaultQuickFilterKeys(QUICK_FILTERS);
  const parsed = readQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, defaults);
  return sanitizeQuickFilterKeys(parsed, QUICK_FILTERS);
}

function saveQuickFilterPreferences() {
  writeQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, visibleQuickFilterKeys);
}

function renderQuickFilters() {
  if (!quickActionsEl) return;
  quickActionsEl.innerHTML = renderQuickFiltersHtml(visibleQuickFilterKeys, QUICK_FILTERS);
  updateQuickChipStates();
}

function renderQuickFilterOptions() {
  if (!quickFiltersOptionsEl) return;
  quickFiltersOptionsEl.innerHTML = renderQuickFilterOptionsHtml(visibleQuickFilterKeys, QUICK_FILTERS);
}

function setQuickFilterVisibility(key, visible) {
  visibleQuickFilterKeys = getNextQuickFilterKeys(visibleQuickFilterKeys, key, visible, QUICK_FILTERS);
  saveQuickFilterPreferences();
  renderQuickFilters();
  renderQuickFilterOptions();
}

function resetQuickFilterPreferences() {
  visibleQuickFilterKeys = getDefaultQuickFilterKeys(QUICK_FILTERS);
  saveQuickFilterPreferences();
  renderQuickFilters();
  renderQuickFilterOptions();
}

function toggleQuickFiltersPanel() {
  if (!quickFiltersPanel) return;
  const hidden = quickFiltersPanel.classList.contains("hidden");
  if (hidden) {
    renderQuickFilterOptions();
    quickFiltersPanel.classList.remove("hidden");
    if (customizeQuickFiltersBtn) customizeQuickFiltersBtn.setAttribute("aria-expanded", "true");
    return;
  }
  closeQuickFiltersPanel();
}

function closeQuickFiltersPanel() {
  if (!quickFiltersPanel) return;
  quickFiltersPanel.classList.add("hidden");
  if (customizeQuickFiltersBtn) customizeQuickFiltersBtn.setAttribute("aria-expanded", "false");
}

function applyQuickFilter(quick) {
  const item = QUICK_FILTERS.find(filter => filter.key === quick);
  if (!item) return;

  if (item.type === "clear") {
    resetFilters();
    return;
  }
  applyQuickFilterToState(quick, state.filters, QUICK_FILTERS, { toggleCountrySelection });
}

function updateCountryPickerTrigger() {
  if (!countryPickerBtn) return;
  countryPickerBtn.textContent = getCountryPickerTriggerText(state.filters.countries);
}

function updateQuickChipStates() {
  if (!quickActionsEl) return;
  quickActionsEl.querySelectorAll(".quick-chip").forEach(chip => {
    const key = chip.dataset.quick;
    const item = QUICK_FILTERS.find(filter => filter.key === key);
    if (!item) return;
    const active = isQuickFilterActive(item, state.filters, { resolveCountryCode });
    chip.classList.toggle("active", active);
    chip.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function updateActiveFiltersSummary() {
  if (!activeFiltersSummaryEl) return;
  const active = getActiveFilterSummaryItems(state.filters, {
    professionLabels: PROFESSION_LABELS
  });
  activeFiltersSummaryEl.textContent = active.length ? `Active filters: ${active.join(" • ")}` : "No active filters";
}

async function fetchUnifiedJobs() {
  return fetchUnifiedJobsFromData({
    unifiedJsonSources: UNIFIED_JSON_SOURCES,
    unifiedCsvSources: UNIFIED_CSV_SOURCES,
    sheetsFallbackSource: SHEETS_FALLBACK_SOURCE,
    setSourceStatus,
    parseUnifiedPayload: payload => parseUnifiedJobsPayload(payload, jobsParsing),
    parseCSV: parseJobsCsv
  });
}

async function fetchJsonFromCandidates(urls) {
  return fetchJsonFromCandidatesFromData(urls);
}

async function renderDataSources() {
  return renderDataSourcesPanel({
    dataSourcesListEl,
    dataSourcesCaptionEl,
    sourceRegistryActiveUrls: SOURCE_REGISTRY_ACTIVE_URLS,
    jobsFetchReportUrls: JOBS_FETCH_REPORT_URLS,
    sheetsFallbackSource: SHEETS_FALLBACK_SOURCE,
    fetchJsonFromCandidates
  });
}

function parseJobsCsv(csv) {
  return parseCSVLargeFromData(csv, {
    jobsParsing,
    parserDeps: {
      mapProfession,
      normalizeSector,
      classifyCompanyType,
      detectWorkType,
      detectContractType,
      logInfo: logJobsInfo,
      logError: logJobsError
    }
  });
}

function toContractClass(contractType) {
  const normalized = (contractType || "").toLowerCase();
  if (normalized === "full-time") return "full-time";
  if (normalized === "internship") return "internship";
  if (normalized === "temporary") return "temporary";
  return "unknown";
}

function getJobKeyForJobWithService(job) {
  return getJobKeyForJob(job, {
    generateJobKey: row => jobsPageService.generateJobKey(row)
  });
}

async function toggleSaveJob(job) {
  if (!isJobsApiReady()) {
    showToast("Local storage provider unavailable.", "error");
    return;
  }

  if (!currentUser) {
    showToast("Sign in to save jobs.", "info");
    await signInUser();
    return;
  }

  const jobKey = getJobKeyForJobWithService(job);
  const isSaved = savedJobKeys.has(jobKey);

  try {
    if (isSaved) {
      const removeResult = await jobsSavedJobsService.removeSavedJobForUser(currentUser.uid, jobKey);
      if (!removeResult.ok) throw new Error(removeResult.error);
      savedJobKeys.delete(jobKey);
      showToast("Removed from saved jobs.", "success");
    } else {
      const saveResult = await jobsSavedJobsService.saveJobForUser(currentUser.uid, toJobSnapshot(job, { sanitizeUrl }));
      if (!saveResult.ok) throw new Error(saveResult.error);
      savedJobKeys.add(jobKey);
      showToast("Saved job to your profile.", "success");
    }
    jobsDispatch.dispatch({ type: JOBS_ACTIONS.SAVE_TOGGLED, payload: { jobKey } });
    applyFiltersAndRender({ resetPage: false });
  } catch (err) {
    logJobsError("Could not toggle saved job", err);
    showToast("Could not update saved jobs right now.", "error");
  }
}

function sanitizeUrl(url) {
  return sanitizeUrlValue(url);
}

function capitalizeFirst(str) {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1);
}

function debounce(fn, waitMs) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), waitMs);
  };
}

function setProgress(visible) {
  setProgressVisibility(setText, fetchProgress, visible);
}

function setSourceStatus(text) {
  setStatusText(setText, sourceStatus, text);
}

function showLoading(text) {
  showJobsLoading(jobsList, text);
}

function showError(message) {
  showJobsError(jobsList, pagination, message, () => {
    init().catch(err => logJobsError("Retry failed", err));
  });
  updateResultsSummary(0, 0, 0);
}

export { bootJobsPage as boot };


