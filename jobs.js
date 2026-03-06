let allJobs = [];
let filteredJobs = [];

const state = {
  currentPage: 1,
  itemsPerPage: 10,
  filters: {
    workType: "",
    country: "",
    city: "",
    profession: "",
    search: "",
    sort: "relevance"
  }
};

const PROFESSION_LABELS = {
  gameplay: "Gameplay Programmer",
  graphics: "Graphics Programmer",
  engine: "Engine Programmer",
  ai: "AI Programmer",
  tools: "Tools Programmer",
  "technical-artist": "Technical Artist",
  designer: "Game Designer",
  artist: "Artist",
  animator: "Animator",
  other: "Other"
};

let jobsList;
let backBtn;
let workTypeFilter;
let countryFilter;
let cityFilter;
let professionFilter;
let searchFilter;
let sortFilter;
let resultsSummary;
let sourceStatus;
let fetchProgress;
let pagination;
let clearFiltersBtn;
let refreshJobsBtn;
let jobsLastUpdatedEl;
let authStatus;
let authSignInBtn;
let authSignOutBtn;
let savedJobsBtn;
let adminPageBtn;

let currentUser = null;
let savedJobKeys = new Set();

const JOBS_CACHE_DB = "baluffo_jobs_cache";
const JOBS_CACHE_STORE = "jobs_feed";
const JOBS_CACHE_KEY = "latest";
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const JOBS_CACHE_TTL_MS = 2 * 60 * 60 * 1000;

let refreshInFlight = false;

document.addEventListener("DOMContentLoaded", () => {
  cacheDom();
  bindEvents();
  readStateFromUrl();
  applyStateToStaticFilters();
  init().catch(err => console.error("Error initializing jobs:", err));
});

function cacheDom() {
  jobsList = document.getElementById("jobs-list");
  backBtn = document.getElementById("back-btn");
  workTypeFilter = document.getElementById("work-type-filter");
  countryFilter = document.getElementById("country-filter");
  cityFilter = document.getElementById("city-filter");
  professionFilter = document.getElementById("profession-filter");
  searchFilter = document.getElementById("search-filter");
  sortFilter = document.getElementById("sort-filter");
  resultsSummary = document.getElementById("results-summary");
  sourceStatus = document.getElementById("source-status");
  fetchProgress = document.getElementById("fetch-progress");
  pagination = document.getElementById("pagination");
  clearFiltersBtn = document.getElementById("clear-filters-btn");
  refreshJobsBtn = document.getElementById("refresh-jobs-btn");
  jobsLastUpdatedEl = document.getElementById("jobs-last-updated");
  authStatus = document.getElementById("auth-status");
  authSignInBtn = document.getElementById("auth-sign-in-btn");
  authSignOutBtn = document.getElementById("auth-sign-out-btn");
  savedJobsBtn = document.getElementById("saved-jobs-btn");
  adminPageBtn = document.getElementById("admin-page-btn");
}

function bindEvents() {
  if (backBtn) {
    backBtn.addEventListener("click", () => {
      window.location.href = "index.html";
    });
  }

  if (savedJobsBtn) {
    savedJobsBtn.addEventListener("click", () => {
      rememberCurrentJobsUrl();
      window.location.href = "saved.html";
    });
  }

  if (adminPageBtn) {
    adminPageBtn.addEventListener("click", () => {
      rememberCurrentJobsUrl();
      window.location.href = "admin.html";
    });
  }

  if (authSignInBtn) {
    authSignInBtn.addEventListener("click", async () => {
      await signInUser();
    });
  }

  if (authSignOutBtn) {
    authSignOutBtn.addEventListener("click", async () => {
      await signOutUser();
    });
  }

  if (workTypeFilter) workTypeFilter.addEventListener("change", () => onFilterChange());
  if (countryFilter) countryFilter.addEventListener("change", () => onFilterChange());
  if (cityFilter) cityFilter.addEventListener("change", () => onFilterChange());
  if (professionFilter) professionFilter.addEventListener("change", () => onFilterChange());
  if (sortFilter) sortFilter.addEventListener("change", () => onFilterChange());

  if (searchFilter) {
    searchFilter.addEventListener("input", debounce(() => {
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

  if (clearFiltersBtn) {
    clearFiltersBtn.addEventListener("click", () => {
      resetFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  }

  if (refreshJobsBtn) {
    refreshJobsBtn.addEventListener("click", async () => {
      await refreshJobsNow({ manual: true });
    });
  }

  document.querySelectorAll(".quick-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const quick = btn.dataset.quick;
      if (quick === "remote") {
        state.filters.workType = "Remote";
      } else if (quick === "technical-artist") {
        state.filters.profession = "technical-artist";
      } else if (quick === "netherlands") {
        if (countryFilter && optionExists(countryFilter, "NL")) {
          state.filters.country = "NL";
        } else {
          state.filters.country = "Netherlands";
        }
      } else {
        resetFilters();
      }
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: true });
    });
  });

  enableKeyboardNav();
}

async function init() {
  if (!jobsList) return;

  initAuth();

  const cached = await readCachedJobs();
  if (cached?.jobs && cached.jobs.length > 0) {
    allJobs = cached.jobs;
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
    return;
  }

  const ok = await refreshJobsNow({ manual: false, firstLoad: true });
  if (!ok) {
    showError("Unable to load job listings right now.");
  }
}

function initAuth() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    setAuthStatus("Browsing as guest");
    toggleAuthButtons(false);
    return;
  }

  api.onAuthStateChanged(async user => {
    currentUser = user || null;
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
      savedJobKeys = await api.getSavedJobKeys(currentUser.uid);
    } catch (err) {
      console.error("Failed to load saved jobs:", err);
      showToast("Could not load saved jobs.", "error");
      savedJobKeys = new Set();
    }

    if (allJobs.length) applyFiltersAndRender({ resetPage: false });
  });
}

function setAuthStatus(text) {
  if (!authStatus) return;
  authStatus.textContent = text;
}

function toggleAuthButtons(isSignedIn) {
  if (authSignInBtn) authSignInBtn.classList.toggle("hidden", isSignedIn);
  if (authSignOutBtn) authSignOutBtn.classList.toggle("hidden", !isSignedIn);
  if (savedJobsBtn) savedJobsBtn.classList.toggle("hidden", !isSignedIn);
}

async function signInUser() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    showToast("Local auth provider unavailable.", "error");
    return;
  }
  try {
    await api.signIn();
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("cancel")) return;
    console.error("Sign-in failed:", err);
    showToast("Sign-in failed. Please try again.", "error");
  }
}

async function signOutUser() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) return;
  try {
    await api.signOut();
  } catch (err) {
    console.error("Sign-out failed:", err);
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
  state.filters.country = params.get("country") || "";
  state.filters.city = params.get("city") || "";
  state.filters.profession = params.get("profession") || "";
  state.filters.search = params.get("search") || "";
  state.filters.sort = params.get("sort") || "relevance";
}

function writeStateToUrl() {
  const params = new URLSearchParams();

  if (state.currentPage > 1) params.set("page", String(state.currentPage));
  if (state.filters.workType) params.set("workType", state.filters.workType);
  if (state.filters.country) params.set("country", state.filters.country);
  if (state.filters.city) params.set("city", state.filters.city);
  if (state.filters.profession) params.set("profession", state.filters.profession);
  if (state.filters.search) params.set("search", state.filters.search);
  if (state.filters.sort && state.filters.sort !== "relevance") params.set("sort", state.filters.sort);

  const query = params.toString();
  const url = query ? `${window.location.pathname}?${query}` : window.location.pathname;
  window.history.replaceState({}, "", url);
  rememberCurrentJobsUrl();
}

function rememberCurrentJobsUrl() {
  try {
    const url = `${window.location.pathname}${window.location.search}`;
    sessionStorage.setItem(JOBS_LAST_URL_KEY, url);
  } catch {
    // Ignore storage errors.
  }
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

  if (refreshJobsBtn) refreshJobsBtn.disabled = true;
  if (manual || firstLoad) setProgress(true);
  if (manual) setSourceStatus("Refreshing jobs from Google Sheets...");

  try {
    const result = await fetchFromGoogleSheets();
    if (!result.jobs || result.jobs.length === 0) {
      if (manual) showToast(result.error || "Could not refresh jobs.", "error");
      return false;
    }

    const previousLength = allJobs.length;
    allJobs = result.jobs;
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

    setSourceStatus(`Loaded ${allJobs.length.toLocaleString()} jobs.`);
    return true;
  } catch (err) {
    console.error("Refresh failed:", err);
    if (manual) showToast("Could not refresh jobs.", "error");
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
  if (searchFilter) searchFilter.value = state.filters.search;
  if (sortFilter) sortFilter.value = state.filters.sort;
}

function applyStateToFilters() {
  applyStateToStaticFilters();

  if (countryFilter && optionExists(countryFilter, state.filters.country)) {
    countryFilter.value = state.filters.country;
  } else {
    state.filters.country = "";
  }

  if (cityFilter && optionExists(cityFilter, state.filters.city)) {
    cityFilter.value = state.filters.city;
  } else {
    state.filters.city = "";
  }

  if (professionFilter && optionExists(professionFilter, state.filters.profession)) {
    professionFilter.value = state.filters.profession;
  } else if (state.filters.profession && state.filters.profession !== "") {
    state.filters.profession = "";
  }
}

function optionExists(select, value) {
  if (!value) return true;
  return Array.from(select.options).some(option => option.value === value);
}

function onFilterChange() {
  syncStateFromFilters();
  applyFiltersAndRender({ resetPage: true });
}

function syncStateFromFilters() {
  state.filters.workType = workTypeFilter ? workTypeFilter.value : "";
  state.filters.country = countryFilter ? countryFilter.value : "";
  state.filters.city = cityFilter ? cityFilter.value : "";
  state.filters.profession = professionFilter ? professionFilter.value : "";
  state.filters.search = searchFilter ? searchFilter.value.trim() : "";
  state.filters.sort = sortFilter ? sortFilter.value : "relevance";
}

function resetFilters() {
  state.filters = {
    workType: "",
    country: "",
    city: "",
    profession: "",
    search: "",
    sort: "relevance"
  };
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
    const matchesCountry = !state.filters.country || job.country === state.filters.country;
    const matchesCity = !state.filters.city || job.city === state.filters.city;
    const matchesProfession = !state.filters.profession || job.profession === state.filters.profession;
    const matchesSearch =
      !searchTerm ||
      job.title.toLowerCase().includes(searchTerm) ||
      job.company.toLowerCase().includes(searchTerm) ||
      (job.city || "").toLowerCase().includes(searchTerm);

    return matchesWorkType && matchesCountry && matchesCity && matchesProfession && matchesSearch;
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
    jobs.sort((a, b) => fullCountryName(a.country).localeCompare(fullCountryName(b.country)));
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
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
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
  const safeTitle = escapeHtml(job.title);
  const safeCompany = escapeHtml(job.company);
  const companyType = job.companyType || classifyCompanyType(job.company, job.title);
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country));
  const safeJobLink = sanitizeUrl(job.jobLink);
  const jobKey = getJobKeyForJob(job);
  const isSaved = savedJobKeys.has(jobKey);

  const content = `
    <button
      class="save-job-btn job-inline-save-btn ${isSaved ? "saved" : ""}"
      data-job-id="${job.id}"
      data-job-key="${jobKey}"
      ${!window.JobAppLocalData?.isReady() ? "disabled" : ""}
      aria-label="${isSaved ? "Remove saved job" : "Save job"}"
    >
      ${isSaved ? "x" : "+"}
    </button>
    <div class="col-title job-cell" data-label="Position">
      <div class="job-title-wrap">
        <div class="job-title-compact">${safeTitle}</div>
      </div>
    </div>
    <div class="col-company job-cell" data-label="Company">
      <span class="job-company-compact" title="${safeCompany}">${escapeHtml(companyType)}</span>
    </div>
    <div class="col-city job-cell" data-label="City">
      <span class="job-location">${safeCity}</span>
    </div>
    <div class="col-country job-cell" data-label="Country">
      <span class="job-location">${safeCountry}</span>
    </div>
    <div class="col-contract job-cell" data-label="Contract">
      <span class="job-contract ${toContractClass(job.contractType)}">${escapeHtml(job.contractType || "Unknown")}</span>
    </div>
    <div class="col-type job-cell" data-label="Type">
      <span class="job-tag ${job.workType.toLowerCase()}">${capitalizeFirst(job.workType)}</span>
    </div>
  `;

  return `<div class="job-row ${safeJobLink ? "job-row-link" : ""}" data-job-link="${safeJobLink}">${content}</div>`;
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
  const active = [];
  if (state.filters.workType) active.push(state.filters.workType);
  if (state.filters.country) active.push(fullCountryName(state.filters.country));
  if (state.filters.city) active.push(state.filters.city);
  if (state.filters.profession) active.push(PROFESSION_LABELS[state.filters.profession] || state.filters.profession);
  if (state.filters.search) active.push(`"${state.filters.search}"`);

  resultsSummary.textContent = active.length > 0 ? `${pageText} | Filters: ${active.join(", ")}` : pageText;
}

function updateFilterOptions() {
  if (!workTypeFilter || !countryFilter || !professionFilter || !cityFilter) return;

  const countries = new Set();
  const professions = new Set();
  const cities = new Set();

  allJobs.forEach(job => {
    if (isValidCountry(job.country)) countries.add(job.country);
    if (job.profession) professions.add(job.profession);
    if (job.city) cities.add(job.city);
  });

  countryFilter.innerHTML = '<option value="">All Countries</option>';
  Array.from(countries).sort().forEach(country => {
    const opt = document.createElement("option");
    opt.value = country;
    opt.textContent = fullCountryName(country);
    countryFilter.appendChild(opt);
  });

  cityFilter.innerHTML = '<option value="">All Cities</option>';
  Array.from(cities).sort().forEach(city => {
    const opt = document.createElement("option");
    opt.value = city;
    opt.textContent = city;
    cityFilter.appendChild(opt);
  });

  professionFilter.innerHTML = '<option value="">All Roles</option>';
  Array.from(professions).sort().forEach(profession => {
    const opt = document.createElement("option");
    opt.value = profession;
    opt.textContent = PROFESSION_LABELS[profession] || capitalizeFirst(profession);
    professionFilter.appendChild(opt);
  });
}

async function fetchFromGoogleSheets() {
  const sheetId = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE";
  const gid = "1560329579";
  const csvUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv&gid=${gid}`;

  const sources = [
    { name: "Google Sheets", url: csvUrl },
    { name: "AllOrigins mirror", url: `https://api.allorigins.win/raw?url=${encodeURIComponent(csvUrl)}` }
  ];

  for (const source of sources) {
    try {
      setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetchWithTimeout(source.url, 20000);
      if (!response.ok) continue;

      const csv = await response.text();
      if (!csv || csv.length < 100) continue;

      const jobs = parseCSVLarge(csv);
      if (jobs.length > 0) {
        return { jobs, error: "" };
      }
    } catch (_) {
      // Try next source.
    }
  }

  return {
    jobs: null,
    error: "Could not fetch listings from the source feed. Check your connection and retry."
  };
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      signal: controller.signal,
      mode: "cors",
      credentials: "omit"
    });
  } finally {
    clearTimeout(timeoutId);
  }
}

function parseCSVLarge(csv) {
  try {
    const startTime = performance.now();
    const rows = parseCSVRecords(csv);
    if (rows.length < 2) return [];

    let headerIdx = -1;
    for (let i = 0; i < Math.min(250, rows.length); i++) {
      const normalizedRow = rows[i]
        .map(cell => cell.toLowerCase().trim())
        .filter(Boolean);

      // Require actual header cells to avoid matching intro/instruction text rows.
      const hasTitleHeader = normalizedRow.includes("title");
      const hasCompanyHeader = normalizedRow.includes("company name") || normalizedRow.includes("company");
      const hasLocationHeader = normalizedRow.includes("city") || normalizedRow.includes("country");

      if (hasTitleHeader && hasCompanyHeader && hasLocationHeader) {
        headerIdx = i;
        break;
      }
    }

    if (headerIdx === -1) return [];

    const headers = rows[headerIdx].map(h => h.toLowerCase().trim());

    const companyIdx = findColumnIndex(headers, ["company name", "company"]);
    const titleIdx = findColumnIndex(headers, ["title", "role"]);
    const cityIdx = findColumnIndex(headers, ["city"]);
    const countryIdx = findColumnIndex(headers, ["country"]);
    const locationTypeIdx = findColumnIndex(headers, ["location type", "work type"]);
    const contractTypeIdx = findColumnIndex(headers, ["employment type", "contract type", "employment", "contract", "position type"]);
    const jobLinkIdx = findColumnIndex(headers, ["job link", "url", "apply"]);

    if (titleIdx === -1 || companyIdx === -1) return [];

    const jobs = [];
    for (let i = headerIdx + 1; i < rows.length; i++) {
      const fields = rows[i];
      if (!fields || fields.length === 0) continue;

      const title = (fields[titleIdx] || "").trim();
      const company = (fields[companyIdx] || "").trim();
      const city = (fields[cityIdx] || "").trim();
      const country = (fields[countryIdx] || "Unknown").trim();
      const locationType = (fields[locationTypeIdx] || "On-site").trim();
      const contractTypeText = contractTypeIdx !== -1 ? (fields[contractTypeIdx] || "").trim() : "";
      const jobLink = jobLinkIdx !== -1 ? (fields[jobLinkIdx] || "").trim() : "";

      if (!title || !company) continue;

      jobs.push({
        id: 1000 + i,
        title,
        company,
        companyType: classifyCompanyType(company, title),
        city,
        country,
        workType: detectWorkType(locationType),
        contractType: detectContractType(contractTypeText, title),
        profession: mapProfession(title),
        description: `${title} at ${company}`,
        jobLink
      });
    }

    const endTime = performance.now();
    console.log(`Loaded ${jobs.length} jobs in ${((endTime - startTime) / 1000).toFixed(2)}s`);
    return jobs;
  } catch (err) {
    console.error("Error parsing CSV:", err.message);
    return [];
  }
}

function parseCSVRecords(csv) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < csv.length; i++) {
    const ch = csv[i];

    if (ch === '"') {
      if (inQuotes && csv[i + 1] === '"') {
        field += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }

    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && csv[i + 1] === "\n") i++;
      row.push(field);
      field = "";
      if (row.some(cell => cell.trim() !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    field += ch;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    if (row.some(cell => cell.trim() !== "")) {
      rows.push(row);
    }
  }

  return rows;
}

function findColumnIndex(headers, possibleNames) {
  for (let i = 0; i < headers.length; i++) {
    for (const name of possibleNames) {
      if (headers[i].includes(name)) return i;
    }
  }
  return -1;
}

function detectWorkType(text) {
  if (!text) return "Onsite";
  const lower = text.toLowerCase();
  if (lower.includes("remote")) return "Remote";
  if (lower.includes("hybrid") || lower.includes("mixed")) return "Hybrid";
  return "Onsite";
}

function detectContractType(text, title = "") {
  const lower = `${text} ${title}`.toLowerCase();

  if (
    lower.includes("full-time") ||
    lower.includes("full time") ||
    lower.includes("permanent")
  ) {
    return "Full-time";
  }

  if (
    lower.includes("temporary") ||
    lower.includes("temp ") ||
    lower.includes("contract") ||
    lower.includes("fixed-term") ||
    lower.includes("fixed term") ||
    lower.includes("freelance") ||
    lower.includes("part-time") ||
    lower.includes("part time") ||
    lower.includes("intern")
  ) {
    return "Temporary";
  }

  return "Unknown";
}

function toContractClass(contractType) {
  const normalized = (contractType || "").toLowerCase();
  if (normalized === "full-time") return "full-time";
  if (normalized === "temporary") return "temporary";
  return "unknown";
}

function classifyCompanyType(company, title = "") {
  const text = `${company} ${title}`.toLowerCase();

  const isGame =
    /\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b/.test(text) ||
    /\b(gameplay|level design|character artist|environment artist|technical artist|animator)\b/.test(text);

  return isGame ? "Game" : "Tech";
}

function getJobKeyForJob(job) {
  const api = window.JobAppLocalData;
  if (api && typeof api.generateJobKey === "function") {
    return api.generateJobKey(job);
  }

  const canonical = `${job.title || ""}|${job.company || ""}|${job.city || ""}|${job.country || ""}`.toLowerCase();
  return `job_${simpleHash(canonical)}`;
}

function simpleHash(input) {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = ((hash << 5) - hash) + input.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash).toString(16);
}

function toJobSnapshot(job) {
  return {
    title: job.title || "",
    company: job.company || "",
    companyType: job.companyType || classifyCompanyType(job.company, job.title),
    city: job.city || "",
    country: job.country || "",
    workType: job.workType || "Onsite",
    contractType: job.contractType || "Unknown",
    jobLink: sanitizeUrl(job.jobLink || "")
  };
}

async function toggleSaveJob(job) {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    showToast("Local storage provider unavailable.", "error");
    return;
  }

  if (!currentUser) {
    showToast("Sign in to save jobs.", "info");
    await signInUser();
    return;
  }

  const jobKey = getJobKeyForJob(job);
  const isSaved = savedJobKeys.has(jobKey);

  try {
    if (isSaved) {
      await api.removeSavedJobForUser(currentUser.uid, jobKey);
      savedJobKeys.delete(jobKey);
      showToast("Removed from saved jobs.", "success");
    } else {
      await api.saveJobForUser(currentUser.uid, toJobSnapshot(job));
      savedJobKeys.add(jobKey);
      showToast("Saved job to your profile.", "success");
    }
    applyFiltersAndRender({ resetPage: false });
  } catch (err) {
    console.error("Could not toggle saved job:", err);
    showToast("Could not update saved jobs right now.", "error");
  }
}

function mapProfession(title) {
  const lower = title.toLowerCase();

  if (lower.includes("technical artist")) return "technical-artist";
  if (lower.includes("gameplay") || lower.includes("game mechanics")) return "gameplay";
  if (lower.includes("graphics") || lower.includes("rendering") || lower.includes("shader")) return "graphics";
  if (lower.includes("engine") || lower.includes("architecture") || lower.includes("systems")) return "engine";
  if (lower.includes("ai") || lower.includes("artificial intelligence") || lower.includes("behavior")) return "ai";
  if (lower.includes("animator") || lower.includes("motion")) return "animator";
  if (lower.includes("tool") || lower.includes("pipeline") || lower.includes("editor") || (lower.includes("technical") && !lower.includes("artist"))) return "tools";
  if (lower.includes("designer") || lower.includes("level") || lower.includes("game design")) return "designer";
  if (lower.includes("artist") || lower.includes("animation") || lower.includes("visual")) return "artist";

  return "other";
}



function fullCountryName(code) {
  const map = {
    US: "United States",
    CA: "Canada",
    GB: "United Kingdom",
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
    Remote: "Remote"
  };
  return map[code] || code;
}

function isValidCountry(country) {
  if (!country || typeof country !== "string") return false;
  const trimmed = country.trim();
  if (!trimmed || trimmed.length < 2) return false;
  if (trimmed.includes(",")) return false;
  return true;
}

function sanitizeUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.href;
    }
    return "";
  } catch {
    return "";
  }
}

function escapeHtml(text) {
  if (!text) return "";
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
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
  if (!sourceStatus) return;
  sourceStatus.textContent = text;
}

function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  document.body.appendChild(toast);

  requestAnimationFrame(() => toast.classList.add("visible"));

  setTimeout(() => {
    toast.classList.remove("visible");
    setTimeout(() => toast.remove(), 220);
  }, 2600);
}

function showLoading(text) {
  if (!jobsList) return;
  jobsList.innerHTML = `<div class="loading">${escapeHtml(text)}</div>`;
}

function showError(message) {
  if (!jobsList) return;
  jobsList.innerHTML = `
    <div class="error">
      <p>${escapeHtml(message)}</p>
      <button id="retry-fetch-btn" class="btn retry-btn">Retry</button>
    </div>
  `;
  if (pagination) pagination.innerHTML = "";
  updateResultsSummary(0, 0, 0);

  const retryBtn = document.getElementById("retry-fetch-btn");
  if (retryBtn) {
    retryBtn.addEventListener("click", () => {
      init().catch(err => console.error("Retry failed:", err));
    });
  }
}
