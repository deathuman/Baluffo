import test from "node:test";
import assert from "node:assert/strict";

import {
  canUseStartupPreviewFastPath,
  initJobsFeed,
  loadStartupPreviewJobsFeed
} from "../../../frontend/jobs/app/feed.js";
import { STARTUP_PREVIEW_JSON_URLS } from "../../../frontend/jobs/app/sources.js";
import { createJobsAuthController } from "../../../frontend/jobs/app/runtime/auth-controller.js";

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === true) {
        values.add(token);
        return true;
      }
      if (force === false) {
        values.delete(token);
        return false;
      }
      if (values.has(token)) {
        values.delete(token);
        return false;
      }
      values.add(token);
      return true;
    },
    contains(token) {
      return values.has(token);
    }
  };
}

function createElement(overrides = {}) {
  return {
    textContent: "",
    disabled: false,
    title: "",
    classList: createClassList(),
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    ...overrides
  };
}

function createBaseDeps(overrides = {}) {
  const calls = {
    metrics: [],
    sourceStatus: [],
    showError: [],
    initialized: [],
    rendered: [],
    interactive: []
  };
  return {
    calls,
    deps: {
      hasJobsList: true,
      emitMetric: (event, payload = {}) => calls.metrics.push({ event, payload }),
      initAuth: () => {},
      isDesktopRuntimeMode: () => false,
      readCachedJobs: async () => null,
      normalizeRows: rows => rows,
      recalculateItemsPerPage: () => {},
      updateFilterOptions: () => {},
      applyStateToFilters: () => {},
      applyFiltersAndRender: () => {},
      markStartupRendered: (stage, rowCount) => calls.rendered.push({ stage, rowCount }),
      markJobsFirstInteractive: reason => calls.interactive.push(reason),
      isJobsCacheStale: () => false,
      cacheTtlMs: 1000,
      setSourceStatus: text => calls.sourceStatus.push(String(text || "")),
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      setHasInitializedJobsFeed: value => calls.initialized.push(Boolean(value)),
      scheduleNonCriticalStartupWork: () => {},
      applyPendingAutoRefreshSignal: async () => {},
      loadStartupPreviewJobs: async () => false,
      showError: message => calls.showError.push(String(message || "")),
      logError: () => {},
      getAllJobs: () => [],
      ...overrides
    }
  };
}

test("initJobsFeed marks startup initialized and interactive on successful first-load refresh", async () => {
  const { calls, deps } = createBaseDeps({
    refreshJobsNow: async ({ firstLoad = false } = {}) => Boolean(firstLoad),
  });

  await initJobsFeed(deps);

  assert.deepEqual(calls.showError, []);
  assert.equal(calls.initialized.at(-1), true);
});

test("initJobsFeed renders explicit error path when startup throws before first load completes", async () => {
  const { calls, deps } = createBaseDeps({
    initAuth: () => {
      throw new Error("startup exploded");
    },
  });

  await initJobsFeed(deps);

  assert.equal(calls.initialized.at(-1), true);
  assert.deepEqual(calls.showError, ["Unable to load job listings right now."]);
});

test("canUseStartupPreviewFastPath only accepts the default first-page startup state", () => {
  const defaultFilters = {
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

  assert.equal(
    canUseStartupPreviewFastPath(
      { currentPage: 1, filters: { ...defaultFilters, countries: [] } },
      defaultFilters
    ),
    true
  );
  assert.equal(
    canUseStartupPreviewFastPath(
      { currentPage: 2, filters: { ...defaultFilters, countries: [] } },
      defaultFilters
    ),
    false
  );
  assert.equal(
    canUseStartupPreviewFastPath(
      { currentPage: 1, filters: { ...defaultFilters, search: "animation" } },
      defaultFilters
    ),
    false
  );
});

test("startup preview sources prefer the packaged startup snapshot first", () => {
  assert.deepEqual(STARTUP_PREVIEW_JSON_URLS.slice(0, 4), [
    "data/jobs-unified-startup.json",
    "data/jobs-unified-light.json",
    "data/jobs-unified.json",
    "jobs-unified-startup.json"
  ]);
});

test("loadStartupPreviewJobsFeed uses the startup fast path for the default launch state", async () => {
  let allJobs = [];
  const calls = {
    applyStateToFilters: 0,
    updateFilterOptions: 0,
    applyFilterOptionsSnapshot: [],
    renderStartupPreviewFastPath: [],
    applyFiltersAndRender: 0,
    scheduleStartupPreviewMaterialization: 0
  };
  const defaultFilters = {
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
  const plan = {
    filterOptions: { availableCountries: ["France"] },
    filteredCount: 2,
    pageJobs: [{ id: "job-1" }],
    materializeFilteredJobs: () => [{ id: "job-1" }, { id: "job-2" }]
  };

  const loaded = await loadStartupPreviewJobsFeed({
    emitMetric: () => {},
    fetchJsonFromCandidates: async () => ({ rows: [{ id: "job-1" }, { id: "job-2" }] }),
    startupPreviewJsonUrls: ["http://example.test/preview.json"],
    parseUnifiedJobsPayload: payload => payload.rows,
    normalizeRows: rows => {
      allJobs = rows.map(row => ({ ...row, status: "active" }));
      return allJobs;
    },
    updateLastUpdatedText: () => {},
    recalculateItemsPerPage: () => {},
    pageState: { currentPage: 1, filters: { ...defaultFilters, countries: [] } },
    defaultFilters,
    buildStartupPreviewFastPathPlan: jobs => {
      assert.equal(jobs, allJobs);
      return plan;
    },
    applyFilterOptionsSnapshot: snapshot => {
      calls.applyFilterOptionsSnapshot.push(snapshot);
    },
    updateFilterOptions: () => {
      calls.updateFilterOptions += 1;
    },
    applyStateToFilters: () => {
      calls.applyStateToFilters += 1;
    },
    renderStartupPreviewFastPath: receivedPlan => {
      calls.renderStartupPreviewFastPath.push(receivedPlan);
    },
    scheduleStartupPreviewMaterialization: materialize => {
      calls.scheduleStartupPreviewMaterialization += 1;
      assert.equal(materialize, plan.materializeFilteredJobs);
    },
    applyFiltersAndRender: () => {
      calls.applyFiltersAndRender += 1;
    },
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    getAllJobs: () => allJobs
  });

  assert.equal(loaded, true);
  assert.equal(calls.applyStateToFilters, 1);
  assert.equal(calls.updateFilterOptions, 0);
  assert.equal(calls.applyFiltersAndRender, 0);
  assert.equal(calls.scheduleStartupPreviewMaterialization, 1);
  assert.deepEqual(calls.applyFilterOptionsSnapshot, [plan.filterOptions]);
  assert.deepEqual(calls.renderStartupPreviewFastPath, [plan]);
});

test("loadStartupPreviewJobsFeed still accepts the legacy array startup snapshot", async () => {
  let allJobs = [];
  const calls = {
    updateFilterOptions: 0,
    applyStateToFilters: 0,
    applyFiltersAndRender: 0
  };
  const defaultFilters = {
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

  const loaded = await loadStartupPreviewJobsFeed({
    emitMetric: () => {},
    fetchJsonFromCandidates: async () => ([{ id: "job-1" }, { id: "job-2" }]),
    startupPreviewJsonUrls: ["http://example.test/preview.json"],
    parseUnifiedJobsPayload: payload => Array.isArray(payload) ? payload : [],
    normalizeRows: rows => {
      allJobs = rows.map(row => ({ ...row, status: "active" }));
      return allJobs;
    },
    updateLastUpdatedText: () => {},
    recalculateItemsPerPage: () => {},
    pageState: { currentPage: 1, filters: { ...defaultFilters, search: "rigging" } },
    defaultFilters,
    buildStartupPreviewFastPathPlan: () => {
      throw new Error("legacy array snapshot should take the full render path here");
    },
    applyFilterOptionsSnapshot: () => {
      throw new Error("legacy array snapshot should not use precomputed filter options here");
    },
    updateFilterOptions: () => {
      calls.updateFilterOptions += 1;
    },
    applyStateToFilters: () => {
      calls.applyStateToFilters += 1;
    },
    renderStartupPreviewFastPath: () => {
      throw new Error("legacy array snapshot should not use the fast path here");
    },
    scheduleStartupPreviewMaterialization: () => {
      throw new Error("legacy array snapshot should not schedule startup materialization here");
    },
    applyFiltersAndRender: () => {
      calls.applyFiltersAndRender += 1;
    },
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    getAllJobs: () => allJobs
  });

  assert.equal(loaded, true);
  assert.equal(calls.updateFilterOptions, 1);
  assert.equal(calls.applyStateToFilters, 1);
  assert.equal(calls.applyFiltersAndRender, 1);
});

test("loadStartupPreviewJobsFeed falls back to the full render path for non-default startup state", async () => {
  let allJobs = [];
  const calls = {
    applyStateToFilters: 0,
    updateFilterOptions: 0,
    renderStartupPreviewFastPath: 0,
    applyFiltersAndRender: 0
  };
  const defaultFilters = {
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

  const loaded = await loadStartupPreviewJobsFeed({
    emitMetric: () => {},
    fetchJsonFromCandidates: async () => ({ rows: [{ id: "job-1" }] }),
    startupPreviewJsonUrls: ["http://example.test/preview.json"],
    parseUnifiedJobsPayload: payload => payload.rows,
    normalizeRows: rows => {
      allJobs = rows.map(row => ({ ...row, status: "active" }));
      return allJobs;
    },
    updateLastUpdatedText: () => {},
    recalculateItemsPerPage: () => {},
    pageState: { currentPage: 1, filters: { ...defaultFilters, search: "rigging" } },
    defaultFilters,
    buildStartupPreviewFastPathPlan: () => {
      throw new Error("fast path should not be used");
    },
    applyFilterOptionsSnapshot: () => {
      throw new Error("precomputed options should not be used");
    },
    updateFilterOptions: () => {
      calls.updateFilterOptions += 1;
    },
    applyStateToFilters: () => {
      calls.applyStateToFilters += 1;
    },
    renderStartupPreviewFastPath: () => {
      calls.renderStartupPreviewFastPath += 1;
    },
    scheduleStartupPreviewMaterialization: () => {
      throw new Error("startup materialization should not be scheduled");
    },
    applyFiltersAndRender: () => {
      calls.applyFiltersAndRender += 1;
    },
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    getAllJobs: () => allJobs
  });

  assert.equal(loaded, true);
  assert.equal(calls.applyStateToFilters, 1);
  assert.equal(calls.updateFilterOptions, 1);
  assert.equal(calls.renderStartupPreviewFastPath, 0);
  assert.equal(calls.applyFiltersAndRender, 1);
});

test("jobs auth controller skips the initial guest rerender after startup preview but still rerenders on later auth changes", async () => {
  let authStateChanged = null;
  let renderCount = 0;
  const skipDecisions = [];
  const userState = {
    currentUser: null,
    savedJobKeys: new Set(),
    seenJobKeys: new Set(),
    authStateListenerBound: false
  };
  const refs = {
    authSignInBtn: createElement(),
    authSignOutBtn: createElement(),
    savedJobsBtn: createElement(),
    authStatus: createElement(),
    authStatusHint: createElement(),
    authAvatar: createElement(),
    guestNoticeEl: createElement({ hidden: true })
  };
  const controller = createJobsAuthController({
    refs,
    userState,
    authReadyPoller: {
      stopPoll() {},
      schedulePoll() {}
    },
    jobsAuthService: {
      onAuthStateChanged(callback) {
        authStateChanged = callback;
      },
      async signIn() {
        return { ok: true };
      },
      async signOut() {
        return { ok: true };
      }
    },
    jobsSavedJobsService: {
      async getSavedJobKeys() {
        return { data: [] };
      },
      async removeSavedJobForUser() {
        return { ok: true };
      },
      async saveJobForUser() {
        return { ok: true };
      }
    },
    jobsPageService: {
      isAvailable() {
        return true;
      }
    },
    jobsDispatch: {
      dispatch() {}
    },
    JOBS_ACTIONS: {
      AUTH_CHANGED: "auth_changed",
      SAVE_TOGGLED: "save_toggled"
    },
    isJobsApiReady: () => true,
    emitDesktopStartupMetric: () => {},
    showToast: () => {},
    logJobsError: () => {},
    getAllJobs: () => [{ id: "job-1" }],
    applyFiltersAndRender: () => {
      renderCount += 1;
    },
    getSkipInitialGuestAuthRerender: () =>
      skipDecisions.length === 0 || skipDecisions.at(-1) === true,
    setSkipInitialGuestAuthRerender: value => {
      skipDecisions.push(Boolean(value));
    },
    openJobsCacheDb: async () => null,
    JOBS_SEEN_STORE: "jobs_seen",
    loadSeenJobKeys: async () => new Set(),
    markSeenJob: async () => {},
    buildSeenRowKey: value => String(value || ""),
    getJobKeyForJob: job => String(job?.id || ""),
    toJobSnapshot: job => job,
    sanitizeUrl: value => String(value || "")
  });

  controller.initAuth();
  assert.equal(typeof authStateChanged, "function");

  await authStateChanged(null);
  assert.equal(renderCount, 0);
  assert.equal(skipDecisions.at(-1), false);
  assert.equal(refs.guestNoticeEl.hidden, false);

  await authStateChanged({ uid: "user-1", displayName: "Warm User" });
  assert.equal(renderCount, 1);
  assert.equal(refs.guestNoticeEl.hidden, true);

  await authStateChanged(null);
  assert.equal(renderCount, 2);
  assert.equal(refs.guestNoticeEl.hidden, false);
});
