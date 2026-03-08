import { JobsStateModule as jobsStateModule } from "../../jobs-state.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick,
  bindHandlersMap
} from "../shared/ui/index.js";
import { sanitizeUrl as sanitizeUrlValue, fullCountryName as fullCountryNameFromData } from "../shared/data/index.js";
import { BaluffoJobsParsing as jobsParsing } from "../../jobs-parsing-utils.js";
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
} from "./domain.js";
import {
  fetchUnifiedJobs as fetchUnifiedJobsFromData,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromData,
  parseUnifiedJobsPayload,
  parseCSVLarge as parseCSVLargeFromData
} from "./data-source.js";
import { isJobsApiReady, jobsAuthService, jobsSavedJobsService, jobsPageService } from "./services.js";
import { createJobsDispatcher, JOBS_ACTIONS } from "./actions.js";
import { renderDataSourcesPanel } from "./source-metadata.js";
import { renderJobRowHtml, showJobsLoading, showJobsError } from "./render.js";
import {
  readAutoRefreshAppliedId,
  readAutoRefreshSignal,
  writeAutoRefreshAppliedId,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  writeAutoRefreshSignal,
  rememberJobsUrl
} from "./state-sync/index.js";
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

/** @type {JobsPageState} */
const state = {
  currentPage: 1,
  itemsPerPage: 10,
  filters: { ...defaultFilters, countries: Array.from(defaultFilters.countries || []) }
};
const jobsDispatch = createJobsDispatcher();

const PROFESSION_LABELS = jobsStateModule.PROFESSION_LABELS || {};

let jobsList;
let backBtn;
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
let quickFiltersResetBtn;
let dataSourcesListEl;
let dataSourcesCaptionEl;

let currentUser = null;
let savedJobKeys = new Set();

const JOBS_CACHE_DB = "baluffo_jobs_cache";
const JOBS_CACHE_STORE = "jobs_feed";
const JOBS_CACHE_KEY = "latest";
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const JOBS_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const JOBS_AUTO_REFRESH_SIGNAL_KEY = "baluffo_jobs_auto_refresh_signal";
const JOBS_AUTO_REFRESH_APPLIED_KEY = "baluffo_jobs_auto_refresh_applied";
const QUICK_FILTER_PREFS_KEY = "baluffo_quick_filter_prefs";
const UNIFIED_JSON_SOURCES = [
  { name: "Unified JSON (local data)", url: "data/jobs-unified.json" },
  { name: "Unified JSON (root)", url: "jobs-unified.json" }
];
const UNIFIED_CSV_SOURCES = [
  { name: "Unified CSV (local data)", url: "data/jobs-unified.csv" },
  { name: "Unified CSV (root)", url: "jobs-unified.csv" }
];
const LEGACY_SHEETS_SOURCE = {
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


function cacheDom() {
  jobsList = document.getElementById("jobs-list");
  backBtn = document.getElementById("back-btn");
  workTypeFilter = document.getElementById("work-type-filter");
  lifecycleStatusFilter = document.getElementById("lifecycle-status-filter");
  countryFilter = document.getElementById("country-filter");
  countryPickerBtn = document.getElementById("country-picker-btn");
  countryPickerPanel = document.getElementById("country-picker-panel");
  countryPickerSearch = document.getElementById("country-picker-search");
  countryPickerOptions = document.getElementById("country-picker-options");
  countryPickerClearBtn = document.getElementById("country-picker-clear-btn");
  cityFilter = document.getElementById("city-filter");
  sectorFilter = document.getElementById("sector-filter");
  professionFilter = document.getElementById("profession-filter");
  professionSearchFilter = document.getElementById("profession-search-filter");
  searchFilter = document.getElementById("search-filter");
  sortFilter = document.getElementById("sort-filter");
  resultsSummary = document.getElementById("results-summary");
  countrySelectionBadge = document.getElementById("country-selection-badge");
  sourceStatus = document.getElementById("source-status");
  fetchProgress = document.getElementById("fetch-progress");
  pagination = document.getElementById("pagination");
  refreshJobsBtn = document.getElementById("refresh-jobs-btn");
  jobsLastUpdatedEl = document.getElementById("jobs-last-updated");
  authStatus = document.getElementById("auth-status");
  authStatusHint = document.getElementById("auth-status-hint");
  authAvatar = document.getElementById("auth-avatar");
  authSignInBtn = document.getElementById("auth-sign-in-btn");
  authSignOutBtn = document.getElementById("auth-sign-out-btn");
  savedJobsBtn = document.getElementById("saved-jobs-btn");
  activeFiltersSummaryEl = document.getElementById("active-filters-summary");
  quickActionsEl = document.getElementById("quick-actions");
  customizeQuickFiltersBtn = document.getElementById("customize-quick-filters-btn");
  quickFiltersPanel = document.getElementById("quick-filters-panel");
  quickFiltersOptionsEl = document.getElementById("quick-filters-options");
  quickFiltersResetBtn = document.getElementById("quick-filters-reset-btn");
  dataSourcesListEl = document.getElementById("data-sources-list");
  dataSourcesCaptionEl = document.getElementById("data-sources-caption");
}

function bindEvents() {

  const clickHandlers = new Map([
    [backBtn, () => { window.location.href = "index.html"; }],
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

  window.addEventListener("resize", debounce(() => {
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
  if (!jobsList) return;
  renderDataSources().catch(() => {});

  initAuth();

  const cached = await readCachedJobs();
  if (cached?.jobs && cached.jobs.length > 0) {
    allJobs = normalizeJobs(cached.jobs, {
      professionLabels: PROFESSION_LABELS,
      sanitizeUrl
    });
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    applyFiltersAndRender({ resetPage: false });

    if (isCacheStale(cached.savedAt)) {
      setSourceStatus(`Loaded ${allJobs.length.toLocaleString()} jobs from cache. Updating stale cache...`);
      refreshJobsNow({ manual: false }).catch(() => {
        // Silent background refresh failure; cache remains usable.
      });
    } else {
      setSourceStatus(`Loaded ${allJobs.length.toLocaleString()} jobs from local cache.`);
    }
    updateLastUpdatedText(cached.savedAt);
    hasInitializedJobsFeed = true;
    await applyPendingAutoRefreshSignal();
    return;
  }

  const ok = await refreshJobsNow({ manual: false, firstLoad: true });
  hasInitializedJobsFeed = true;
  await applyPendingAutoRefreshSignal();
  if (!ok) {
    showError("Unable to load job listings right now.");
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

function parseAutoRefreshSignal(rawValue) {
  if (!rawValue) return null;
  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object") return null;
    const signalId = String(parsed.id || "").trim();
    if (!signalId) return null;
    if (String(parsed.source || "").trim() !== "admin_fetcher") return null;
    return {
      id: signalId,
      finishedAt: String(parsed.finishedAt || "")
    };
  } catch {
    return null;
  }
}

function handleAutoRefreshSignalValue(rawValue) {
  const signal = parseAutoRefreshSignal(rawValue);
  if (!signal) return;
  if (signal.id === lastHandledAutoRefreshSignalId) return;

  if (!hasInitializedJobsFeed) {
    pendingAutoRefreshSignal = signal;
    return;
  }

  pendingAutoRefreshSignal = null;
  triggerAutoRefreshFromSignal(signal).catch(err => {
    logJobsError("Auto-refresh from admin signal failed", err);
  });
}

async function applyPendingAutoRefreshSignal() {
  if (pendingAutoRefreshSignal) {
    const signal = pendingAutoRefreshSignal;
    pendingAutoRefreshSignal = null;
    await triggerAutoRefreshFromSignal(signal);
    return;
  }

  const latestRaw = readAutoRefreshSignal(JOBS_AUTO_REFRESH_SIGNAL_KEY);
  handleAutoRefreshSignalValue(latestRaw);
}

async function triggerAutoRefreshFromSignal(signal) {
  if (!signal?.id) return;
  if (signal.id === lastHandledAutoRefreshSignalId) return;

  const completedAt = signal.finishedAt ? new Date(signal.finishedAt) : null;
  const completedLabel = completedAt && !Number.isNaN(completedAt.getTime())
    ? completedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : "";
  const statusTail = completedLabel ? ` (${completedLabel})` : "";
  setSourceStatus(`New feed available from admin fetcher${statusTail}. Refreshing jobs...`);

  const ok = await refreshJobsNow({ manual: false });
  markAutoRefreshSignalHandled(signal.id);
  if (ok) {
    showToast("Jobs auto-refreshed from latest fetcher run.", "success");
  }
}

function initAuth() {
  if (!isJobsApiReady() || !jobsPageService.isAvailable()) {
    setAuthStatus("Browsing as guest");
    toggleAuthButtons(false);
    return;
  }

  jobsAuthService.onAuthStateChanged(async user => {
    currentUser = user || null;
    jobsDispatch.dispatch({
      type: JOBS_ACTIONS.AUTH_CHANGED,
      payload: { uid: currentUser?.uid || "" }
    });
    if (!currentUser) {
      savedJobKeys = new Set();
      setAuthStatus("Browsing as guest");
      toggleAuthButtons(false);
      if (allJobs.length) applyFiltersAndRender({ resetPage: false });
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    toggleAuthButtons(true);

    try {
      const keysResult = await jobsSavedJobsService.getSavedJobKeys(currentUser.uid);
      savedJobKeys = new Set(keysResult.data || []);
    } catch (err) {
      logJobsError("Failed to load saved jobs", err);
      showToast("Could not load saved jobs.", "error");
      savedJobKeys = new Set();
    }

    if (allJobs.length) applyFiltersAndRender({ resetPage: false });
  });
}

function setAuthStatus(text) {
  if (!authStatus) return;
  const raw = String(text || "").trim();
  let label = raw || "Guest";
  let hint = "";
  const signedInMatch = raw.match(/^signed\s+in\s+as\s+(.+)$/i);

  if (!raw || /^browsing\s+as\s+guest$/i.test(raw) || /^guest$/i.test(raw)) {
    label = "Guest";
    hint = "Browsing as guest";
  } else if (signedInMatch) {
    label = String(signedInMatch[1] || "").trim() || "User";
    hint = "Signed in";
  }

  authStatus.textContent = label;
  if (authStatusHint) {
    authStatusHint.textContent = hint;
  }
  if (authAvatar) {
    const initial = label.charAt(0).toUpperCase();
    authAvatar.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}

function toggleAuthButtons(isSignedIn) {
  if (authSignInBtn) authSignInBtn.classList.toggle("hidden", isSignedIn);
  if (authSignOutBtn) authSignOutBtn.classList.toggle("hidden", !isSignedIn);
  if (savedJobsBtn) savedJobsBtn.classList.toggle("hidden", !isSignedIn);
}

async function signInUser() {
  if (!isJobsApiReady()) {
    showToast("Local auth provider unavailable.", "error");
    return;
  }
  const result = await jobsAuthService.signIn();
  if (!result.ok) {
    if (String(result.error || "").toLowerCase().includes("cancel")) return;
    logJobsError("Sign-in failed", new Error(result.error));
    showToast("Sign-in failed. Please try again.", "error");
  }
}

async function signOutUser() {
  if (!isJobsApiReady()) return;
  const result = await jobsAuthService.signOut();
  if (!result.ok) {
    logJobsError("Sign-out failed", new Error(result.error));
    showToast("Sign-out failed. Please try again.", "error");
  }
}

function readStateFromUrl() {
  const params = new URLSearchParams(window.location.search);

  const page = parseInt(params.get("page"), 10);
  if (!isNaN(page) && page > 0) {
    state.currentPage = page;
  }

  state.filters.workType = params.get("workType") || "";
  state.filters.lifecycleStatus = normalizeLifecycleStatus(params.get("lifecycleStatus"), "active");
  state.filters.countries = Array.from(new Set(params.getAll("country").filter(Boolean)));
  state.filters.city = params.get("city") || "";
  state.filters.sector = params.get("sector") || "";
  state.filters.profession = params.get("profession") || "";
  state.filters.excludeInternship = params.get("excludeInternship") === "1";
  state.filters.search = params.get("search") || "";
  state.filters.sort = params.get("sort") || "relevance";
}

function writeStateToUrl() {
  const params = new URLSearchParams();

  if (state.currentPage > 1) params.set("page", String(state.currentPage));
  if (state.filters.workType) params.set("workType", state.filters.workType);
  if (state.filters.lifecycleStatus && state.filters.lifecycleStatus !== "active") {
    params.set("lifecycleStatus", state.filters.lifecycleStatus);
  }
  state.filters.countries.forEach(country => params.append("country", country));
  if (state.filters.city) params.set("city", state.filters.city);
  if (state.filters.sector) params.set("sector", state.filters.sector);
  if (state.filters.profession) params.set("profession", state.filters.profession);
  if (state.filters.excludeInternship) params.set("excludeInternship", "1");
  if (state.filters.search) params.set("search", state.filters.search);
  if (state.filters.sort && state.filters.sort !== "relevance") params.set("sort", state.filters.sort);

  const query = params.toString();
  const url = query ? `${window.location.pathname}?${query}` : window.location.pathname;
  window.history.replaceState({}, "", url);
  rememberCurrentJobsUrl();
}

function rememberCurrentJobsUrl() {
  const url = `${window.location.pathname}${window.location.search}`;
  rememberJobsUrl(JOBS_LAST_URL_KEY, url);
}

function openJobsCacheDb() {
  return new Promise((resolve, reject) => {
    if (!window.indexedDB) {
      resolve(null);
      return;
    }

    const request = indexedDB.open(JOBS_CACHE_DB, 1);

    request.onupgradeneeded = event => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(JOBS_CACHE_STORE)) {
        db.createObjectStore(JOBS_CACHE_STORE, { keyPath: "id" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open jobs cache database."));
  });
}

async function readCachedJobs() {
  try {
    const db = await openJobsCacheDb();
    if (!db) return null;

    return await new Promise((resolve, reject) => {
      const tx = db.transaction(JOBS_CACHE_STORE, "readonly");
      const store = tx.objectStore(JOBS_CACHE_STORE);
      const request = store.get(JOBS_CACHE_KEY);

      request.onsuccess = () => {
        const row = request.result;
        resolve({
          jobs: Array.isArray(row?.jobs) ? row.jobs : null,
          savedAt: Number(row?.savedAt) || 0
        });
      };
      request.onerror = () => reject(request.error || new Error("Could not read jobs cache."));
    });
  } catch {
    return null;
  }
}

function isCacheStale(savedAt) {
  if (!savedAt) return true;
  return (Date.now() - savedAt) > JOBS_CACHE_TTL_MS;
}

function updateLastUpdatedText(timestamp) {
  if (!jobsLastUpdatedEl) return;
  if (!timestamp || !Number.isFinite(Number(timestamp))) {
    jobsLastUpdatedEl.textContent = "";
    return;
  }

  const dt = new Date(Number(timestamp));
  if (Number.isNaN(dt.getTime())) {
    jobsLastUpdatedEl.textContent = "";
    return;
  }

  const mins = Math.max(0, Math.floor((Date.now() - dt.getTime()) / 60000));
  const relative = mins < 1 ? "just now" : mins === 1 ? "1 min ago" : `${mins} mins ago`;
  jobsLastUpdatedEl.textContent = `Last updated: ${relative}`;
}

async function refreshJobsNow({ manual, firstLoad = false }) {
  if (refreshInFlight) return false;
  refreshInFlight = true;
  jobsDispatch.dispatch({ type: JOBS_ACTIONS.REFRESH_REQUESTED });

  if (refreshJobsBtn) refreshJobsBtn.disabled = true;
  if (manual || firstLoad) setProgress(true);
  if (manual) setSourceStatus("Refreshing jobs from unified feed...");

  try {
    const result = await fetchUnifiedJobs();
    if (!result.jobs || result.jobs.length === 0) {
      if (manual) showToast(result.error || "Could not refresh jobs.", "error");
      jobsDispatch.dispatch({
        type: JOBS_ACTIONS.REFRESH_FAILED,
        payload: { error: result.error || "Could not refresh jobs." }
      });
      return false;
    }

    const previousLength = allJobs.length;
    allJobs = normalizeJobs(result.jobs, {
      professionLabels: PROFESSION_LABELS,
      sanitizeUrl
    });
    await writeCachedJobs(allJobs);
    updateLastUpdatedText(Date.now());
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    applyFiltersAndRender({ resetPage: false });

    if (manual) {
      showToast("Jobs refreshed.", "success");
    } else if (previousLength > 0) {
      showToast("Job cache auto-updated.", "info");
    }

    const sourceLabel = result.sourceName ? ` from ${result.sourceName}` : "";
    setSourceStatus(`Loaded ${allJobs.length.toLocaleString()} jobs${sourceLabel}.`);
    renderDataSources().catch(() => {});
    jobsDispatch.dispatch({
      type: JOBS_ACTIONS.REFRESH_COMPLETED,
      payload: { finishedAt: new Date().toISOString() }
    });
    return true;
  } catch (err) {
    logJobsError("Refresh failed", err);
    if (manual) showToast("Could not refresh jobs.", "error");
    jobsDispatch.dispatch({
      type: JOBS_ACTIONS.REFRESH_FAILED,
      payload: { error: err?.message || "Could not refresh jobs." }
    });
    return false;
  } finally {
    refreshInFlight = false;
    if (refreshJobsBtn) refreshJobsBtn.disabled = false;
    setProgress(false);
  }
}

async function writeCachedJobs(jobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) return;

  try {
    const db = await openJobsCacheDb();
    if (!db) return;

    await new Promise((resolve, reject) => {
      const tx = db.transaction(JOBS_CACHE_STORE, "readwrite");
      const store = tx.objectStore(JOBS_CACHE_STORE);
      const request = store.put({
        id: JOBS_CACHE_KEY,
        savedAt: Date.now(),
        jobs
      });

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error || new Error("Could not write jobs cache."));
    });
  } catch {
    // Ignore cache write failures.
  }
}

function applyStateToStaticFilters() {
  if (workTypeFilter) workTypeFilter.value = state.filters.workType;
  if (lifecycleStatusFilter) lifecycleStatusFilter.value = state.filters.lifecycleStatus || "active";
  if (searchFilter) searchFilter.value = state.filters.search;
  if (sortFilter) sortFilter.value = state.filters.sort;
}

function applyStateToFilters() {
  applyStateToStaticFilters();
  state.filters.countries = Array.from(
    new Set(
      (state.filters.countries || [])
        .map(resolveCountryCode)
        .filter(code => availableCountryFilterValues.includes(code))
    )
  );

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

function optionExists(select, value) {
  if (!value) return true;
  return Array.from(select.options).some(option => option.value === value);
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
  state.filters.excludeInternship = Boolean(state.filters.excludeInternship);
  state.filters.search = searchFilter ? searchFilter.value.trim() : "";
  state.filters.sort = sortFilter ? sortFilter.value : "relevance";
  updateCountrySelectionBadge();
}

function normalizeLifecycleStatus(value, fallback = "active") {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (normalized === "active" || normalized === "likely_removed" || normalized === "archived") return normalized;
  return fallback;
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
    const matchesInternship = !state.filters.excludeInternship || !isInternshipJob(job);
    const matchesSearch =
      !searchTerm ||
      job.title.toLowerCase().includes(searchTerm) ||
      job.company.toLowerCase().includes(searchTerm) ||
      (job.city || "").toLowerCase().includes(searchTerm) ||
      (job.sector || "").toLowerCase().includes(searchTerm);

    return matchesWorkType && matchesLifecycle && matchesCountry && matchesCity && matchesSector && matchesProfession && matchesInternship && matchesSearch;
  });

  sortJobs(filteredJobs, state.filters.sort);
  displayJobs(filteredJobs);
  writeStateToUrl();
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

  if (jobs.length === 0) {
    jobsList.innerHTML = '<div class="no-results">No jobs found matching your filters.</div>';
    pagination.innerHTML = "";
    updateResultsSummary(0, 0, 0);
    return;
  }

  const totalPages = Math.ceil(jobs.length / state.itemsPerPage);
  if (state.currentPage > totalPages) state.currentPage = totalPages;

  const startIndex = (state.currentPage - 1) * state.itemsPerPage;
  const pageJobs = jobs.slice(startIndex, startIndex + state.itemsPerPage);

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

  renderPagination(totalPages);
  bindRenderedJobEvents(pageJobs);
  updateResultsSummary(jobs.length, startIndex + 1, startIndex + pageJobs.length);
}

function renderJobRow(job) {
  return renderJobRowHtml(job, {
    fullCountryName: value => fullCountryNameFromDomainLayer(value, COUNTRY_NAME_OPTIONS),
    sanitizeUrl,
    getJobKeyForJob: getJobKeyForJobWithService,
    savedJobKeys,
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

    row.tabIndex = 0;
    row.setAttribute("role", "link");
    row.addEventListener("click", e => {
      if (e.target.closest(".save-job-btn")) return;
      window.open(link, "_blank", "noopener,noreferrer");
    });
    row.addEventListener("keydown", e => {
      if (e.key !== "Enter") return;
      if (e.target.closest(".save-job-btn")) return;
      window.open(link, "_blank", "noopener,noreferrer");
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

function getVisiblePages(totalPages, currentPage) {
  if (totalPages <= 9) {
    return Array.from({ length: totalPages }, (_, idx) => idx + 1);
  }

  const pages = [1];
  let left = currentPage - 2;
  let right = currentPage + 2;

  if (left <= 2) {
    left = 2;
    right = 5;
  }

  if (right >= totalPages - 1) {
    right = totalPages - 1;
    left = totalPages - 4;
  }

  if (left > 2) pages.push("...");
  for (let p = left; p <= right; p++) pages.push(p);
  if (right < totalPages - 1) pages.push("...");
  pages.push(totalPages);

  return pages;
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
  const count = state.filters.countries.length;
  if (count === 0) {
    countrySelectionBadge.textContent = "All countries";
    return;
  }
  countrySelectionBadge.textContent = count === 1
    ? `1 country selected`
    : `${count} countries selected`;
}

function isRegionSelection(value) {
  return String(value || "").startsWith("region:");
}

function getCountryFilterOptionLabel(value) {
  const region = REGION_DEFINITIONS.find(item => item.value === value);
  if (region) return region.label;
  return fullCountryNameFromDomainLayer(value, COUNTRY_NAME_OPTIONS);
}

function resolveRegionSelection(value) {
  const normalized = normalizeCountryToken(value);
  if (!normalized) return "";
  const match = REGION_DEFINITIONS.find(region =>
    normalizeCountryToken(region.value) === normalized || normalizeCountryToken(region.label) === normalized
  );
  return match ? match.value : "";
}

function getAvailableRegionOptions(countries) {
  const countryTokens = new Set(
    (countries || [])
      .map(item => normalizeCountryToken(canonicalizeCountryName(item, COUNTRY_NAME_OPTIONS)))
      .filter(Boolean)
  );

  return REGION_DEFINITIONS.filter(region => {
    const regionTokens = REGION_COUNTRY_TOKEN_LOOKUP[region.value];
    if (!regionTokens || regionTokens.size === 0) return false;

    for (const token of regionTokens) {
      if (countryTokens.has(token)) return true;
    }
    return false;
  });
}

function matchesCountrySelection(jobCountry, selections) {
  const countryToken = normalizeCountryToken(canonicalizeCountryName(jobCountry, COUNTRY_NAME_OPTIONS));
  if (!countryToken) return false;

  for (const selection of selections || []) {
    if (isRegionSelection(selection)) {
      if (countryMatchesRegion(countryToken, selection)) return true;
      continue;
    }

    const selectionToken = normalizeCountryToken(canonicalizeCountryName(selection, COUNTRY_NAME_OPTIONS));
    if (selectionToken && selectionToken === countryToken) return true;
  }
  return false;
}

function countryMatchesRegion(countryToken, regionValue) {
  const regionCountries = REGION_COUNTRY_TOKEN_LOOKUP[regionValue];
  if (!regionCountries || regionCountries.size === 0) return false;
  if (regionValue === "region:remote-worldwide") {
    return REMOTE_WORLDWIDE_TOKENS.has(countryToken);
  }
  return regionCountries.has(countryToken);
}

function appendCountrySelection(countryCode) {
  let target = countryCode;
  const mapped = resolveCountryCode(countryCode);
  if (mapped) target = mapped;
  if (!availableCountryFilterValues.includes(target)) return;
  const selected = new Set(state.filters.countries || []);
  selected.add(target);
  state.filters.countries = Array.from(selected);
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
  const raw = String(countryCode || "").trim();
  if (!raw) return "";

  const regionValue = resolveRegionSelection(raw);
  if (regionValue && availableCountryFilterValues.includes(regionValue)) {
    return regionValue;
  }

  if (availableCountryFilterValues.includes(raw)) return raw;

  const normalized = normalizeCountryToken(canonicalizeCountryName(raw, COUNTRY_NAME_OPTIONS));
  const byName = availableCountries.find(
    code => normalizeCountryToken(canonicalizeCountryName(code, COUNTRY_NAME_OPTIONS)) === normalized
  );
  return byName || "";
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
  const normalized = String(query || "").trim().toLowerCase();
  const selected = new Set(state.filters.countries || []);
  const rows = availableCountryFilterValues.filter(code => {
    if (!normalized) return true;
    const label = getCountryFilterOptionLabel(code).toLowerCase();
    return label.includes(normalized);
  });

  if (rows.length === 0) {
    countryPickerOptions.innerHTML = '<div class="country-empty">No matches.</div>';
    return;
  }

  countryPickerOptions.innerHTML = rows.map(code => `
    <label class="country-option">
      <input type="checkbox" value="${escapeHtml(code)}" ${selected.has(code) ? "checked" : ""}>
      <span>${escapeHtml(getCountryFilterOptionLabel(code))}</span>
    </label>
  `).join("");

  countryPickerOptions.querySelectorAll('input[type="checkbox"]').forEach(input => {
    input.addEventListener("change", () => {
      const current = new Set(state.filters.countries || []);
      if (input.checked) current.add(input.value);
      else current.delete(input.value);
      state.filters.countries = Array.from(current);
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
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
  const defaults = QUICK_FILTERS.filter(item => item.defaultVisible).map(item => item.key);
  const parsed = readQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, defaults);
  if (!Array.isArray(parsed)) return defaults;
  const valid = parsed.filter(key => QUICK_FILTERS.some(item => item.key === key));
  const keepClear = valid.includes("clear") ? valid : [...valid, "clear"];
  return orderQuickFilterKeys(keepClear);
}

function saveQuickFilterPreferences() {
  writeQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, visibleQuickFilterKeys);
}

function orderQuickFilterKeys(keys) {
  const set = new Set(keys);
  return QUICK_FILTERS.filter(item => set.has(item.key)).map(item => item.key);
}

function renderQuickFilters() {
  if (!quickActionsEl) return;
  quickActionsEl.innerHTML = visibleQuickFilterKeys
    .map(key => {
      const item = QUICK_FILTERS.find(filter => filter.key === key);
      if (!item) return "";
      const isClear = item.type === "clear";
      const classes = isClear ? "btn quick-btn quick-clear" : "btn quick-btn quick-chip";
      const ariaPressed = isClear ? "" : ' aria-pressed="false"';
      return `<button class="${classes}" data-quick="${item.key}"${ariaPressed}>${item.label}</button>`;
    })
    .join("");
  updateQuickChipStates();
}

function renderQuickFilterOptions() {
  if (!quickFiltersOptionsEl) return;
  const current = new Set(visibleQuickFilterKeys);
  quickFiltersOptionsEl.innerHTML = QUICK_FILTERS
    .filter(item => item.type !== "clear")
    .map(item => `
      <label class="quick-filter-option">
        <input type="checkbox" data-quick="${item.key}" ${current.has(item.key) ? "checked" : ""}>
        <span>${item.label}</span>
      </label>
    `)
    .join("");
}

function setQuickFilterVisibility(key, visible) {
  const item = QUICK_FILTERS.find(filter => filter.key === key && filter.type !== "clear");
  if (!item) return;
  const next = new Set(visibleQuickFilterKeys);
  if (visible) next.add(key);
  else next.delete(key);
  next.add("clear");
  visibleQuickFilterKeys = orderQuickFilterKeys(Array.from(next));
  saveQuickFilterPreferences();
  renderQuickFilters();
  renderQuickFilterOptions();
}

function resetQuickFilterPreferences() {
  visibleQuickFilterKeys = QUICK_FILTERS.filter(item => item.defaultVisible).map(item => item.key);
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
  if (item.type === "workType") {
    state.filters.workType = state.filters.workType === item.value ? "" : item.value;
    return;
  }
  if (item.type === "profession") {
    state.filters.profession = state.filters.profession === item.value ? "" : item.value;
    return;
  }
  if (item.type === "sector") {
    state.filters.sector = state.filters.sector === item.value ? "" : item.value;
    return;
  }
  if (item.type === "country") {
    toggleCountrySelection(item.value);
    return;
  }
  if (item.type === "flag" && item.value === "excludeInternship") {
    state.filters.excludeInternship = !state.filters.excludeInternship;
  }
}

function updateCountryPickerTrigger() {
  if (!countryPickerBtn) return;
  const count = state.filters.countries.length;
  countryPickerBtn.textContent = count > 0 ? `Country: ${count} selected` : "Country: All";
}

function updateQuickChipStates() {
  if (!quickActionsEl) return;
  quickActionsEl.querySelectorAll(".quick-chip").forEach(chip => {
    const key = chip.dataset.quick;
    const item = QUICK_FILTERS.find(filter => filter.key === key);
    if (!item) return;
    let active = false;
    if (item.type === "workType") active = state.filters.workType === item.value;
    else if (item.type === "profession") active = state.filters.profession === item.value;
    else if (item.type === "sector") active = state.filters.sector === item.value;
    else if (item.type === "flag" && item.value === "excludeInternship") active = Boolean(state.filters.excludeInternship);
    else if (item.type === "country") {
      const mapped = resolveCountryCode(item.value);
      active = Boolean(mapped) && state.filters.countries.includes(mapped);
    }
    chip.classList.toggle("active", active);
    chip.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function updateActiveFiltersSummary() {
  if (!activeFiltersSummaryEl) return;
  const active = [];
  if (state.filters.workType) active.push(state.filters.workType);
  if (state.filters.lifecycleStatus && state.filters.lifecycleStatus !== "active") {
    active.push(`Status: ${state.filters.lifecycleStatus.replace("_", " ")}`);
  }
  if (state.filters.countries.length > 0) active.push(`Countries: ${state.filters.countries.length}`);
  if (state.filters.city) active.push(`City: ${state.filters.city}`);
  if (state.filters.sector) active.push(`Sector: ${state.filters.sector}`);
  if (state.filters.profession) active.push(PROFESSION_LABELS[state.filters.profession] || state.filters.profession);
  if (state.filters.excludeInternship) active.push("Exclude Internship");
  if (state.filters.search) active.push(`Search: "${state.filters.search}"`);
  activeFiltersSummaryEl.textContent = active.length ? `Active filters: ${active.join(" • ")}` : "No active filters";
}

async function fetchUnifiedJobs() {
  return fetchUnifiedJobsFromData({
    unifiedJsonSources: UNIFIED_JSON_SOURCES,
    unifiedCsvSources: UNIFIED_CSV_SOURCES,
    legacySheetsSource: LEGACY_SHEETS_SOURCE,
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
    legacySheetsSource: LEGACY_SHEETS_SOURCE,
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
  if (!fetchProgress) return;
  fetchProgress.classList.toggle("hidden", !visible);
}

function setSourceStatus(text) {
  setText(sourceStatus, text);
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


