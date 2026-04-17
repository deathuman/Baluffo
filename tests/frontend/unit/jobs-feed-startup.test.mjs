import test from "node:test";
import assert from "node:assert/strict";

import {
  canUseStartupPreviewFastPath,
  initJobsFeed,
  loadStartupPreviewJobsFeed
} from "../../../frontend/jobs/app/feed.js";

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
