import assert from "node:assert/strict";
import test from "node:test";
import { composeJobsRuntime } from "../../../frontend/jobs/app/runtime/composition.js";
import { createLazyJobsPipelineController } from "../../../frontend/jobs/app/runtime/lazy-pipeline.js";
import { createJobsDesktopUpdateSurface } from "../../../frontend/jobs/app/desktop-update-surface.js";

function deferred() {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return { promise, resolve };
}

function sleep(ms = 0) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const noop = () => {};
const asyncNoop = async () => {};

// The runtime composition is the seam that decides when the deferred surfaces
// start, so it is exercised with the same dependency shape `runtime.js` builds.
function composeStubRuntime(overrides = {}) {
  return composeJobsRuntime({
    defaultFilters: {
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
    },
    lastHandledAutoRefreshSignalId: "",
    quickFilters: [],
    professionLabels: {},
    createJobsDispatcher: () => noop,
    jobsActions: {},
    adminBridgeBase: "http://127.0.0.1:1",
    bridgeTimeoutMs: 100,
    isDesktopRuntimeModeFromStartup: () => true,
    postStartupMetricToBridge: noop,
    callJobsBridgeFromModule: async () => ({}),
    isContainerRuntimeMode: () => false,
    jobsAuthService: {},
    jobsSavedJobsService: {},
    jobsPageService: { isAvailable: () => false, generateJobKey: () => "job-key" },
    isJobsApiReady: () => false,
    canManageAvailability: () => false,
    buildFilterOptions: () => [],
    getJobLocationCities: () => [],
    getJobLocationCountries: () => [],
    isValidCountry: () => true,
    isSemanticallyValidLocationValue: () => true,
    isCityFilterEligible: () => true,
    readQuickFilterPreferences: () => ({}),
    writeQuickFilterPreferences: noop,
    quickFilterPrefsKey: "prefs",
    loadSeenJobKeys: async () => [],
    markSeenJob: asyncNoop,
    buildSeenRowKey: () => "row-key",
    jobsSeenStore: "seen",
    toJobSnapshot: value => value,
    sanitizeUrl: value => value,
    pipelineStatusPollMs: 1000,
    pipelineStatusIdlePollMs: 5000,
    createFilterOptionsAccumulator: () => ({}),
    addJobToFilterOptions: noop,
    finalizeFilterOptions: () => [],
    compareJobsForSort: () => 0,
    getAvailableRegionOptions: () => [],
    fullCountryName: value => value,
    jobsAutoRefreshSignalKey: "signal",
    openJobLinkInDefaultBrowser: noop,
    windowObject: {
      location: { href: "http://127.0.0.1/jobs.html?desktop=1" },
      setTimeout: () => 0
    },
    documentObject: { body: null },
    setJobsStartupState: noop,
    applyFiltersAndRender: noop,
    rememberCurrentJobsUrl: noop,
    openAdminPageFromJobs: noop,
    triggerJobsPipelineRun: noop,
    handleAutoRefreshSignalValue: noop,
    goToPage: noop,
    displayJobs: noop,
    jobsParsing: {},
    startupPreviewJsonUrls: [],
    jobsFetchReportUrls: [],
    availabilityHistoryUrls: [],
    parseUnifiedJobsPayload: () => ({}),
    openJobsCacheDbFromModule: asyncNoop,
    readJobsCache: async () => null,
    writeJobsCache: asyncNoop,
    refreshJobsFeed: asyncNoop,
    loadStartupPreviewJobsFeed: asyncNoop,
    fetchUnifiedJobsFromSources: async () => ({}),
    fetchJsonFromCandidatesFromSources: async () => ({}),
    renderDataSourcesFromSources: noop,
    mapProfession: value => value,
    normalizeSector: value => value,
    classifyCompanyType: value => value,
    detectWorkType: value => value,
    setProgressVisibility: noop,
    setStatusText: noop,
    getJobsLastUpdatedText: () => "",
    normalizeJobs: value => value,
    jobsCacheDb: "db",
    jobsCacheDbVersion: 1,
    jobsCacheStore: "store",
    jobsCacheKey: "key",
    jobsFirstLoadRequestTimeoutMs: 1,
    buildJobsPageUrl: () => "",
    resolveStartupProbeEnabled: () => false,
    rememberJobsUrl: noop,
    jobsLastUrlKey: "last-url",
    applyJobsAdminBridgeStateFromModule: noop,
    getJobKeyForJob: () => "job-key",
    normalizeLifecycleStatus: value => value,
    ...overrides
  });
}

test("the deferred update surface starts only once rows exist", () => {
  const starts = [];
  const runtime = composeStubRuntime({ startDesktopUpdateSurface: () => starts.push("start") });

  runtime.markStartupRendered("startup_preview", 3);
  assert.deepEqual(starts, ["start"]);

  // An empty render (first-run bootstrap) has no rows, so the panel's chunk must
  // not be pulled in ahead of the first-run modal it would compete with.
  starts.length = 0;
  runtime.markStartupRendered("first_run_bootstrap", 0);
  assert.deepEqual(starts, []);
});

test("the deferred half of boot starts only once rows exist", () => {
  const starts = [];
  const runtime = composeStubRuntime({ startBootFollowUp: () => starts.push("boot") });

  runtime.markStartupRendered("startup_preview", 3);
  assert.deepEqual(starts, ["boot"]);

  // An empty render has no rows to protect, but it is also not the first
  // content: the fallback armed by boot owns that case.
  starts.length = 0;
  runtime.markStartupRendered("first_run_bootstrap", 0);
  assert.deepEqual(starts, []);
});

test("the pipeline facade keeps its promise contract while the chunk is pending", async () => {
  const calls = [];
  const gate = deferred();
  const facade = createLazyJobsPipelineController({
    createController: () => gate.promise.then(() => ({
      ensureJobsPipelineStatusWatch: () => calls.push("watch"),
      triggerJobsPipelineRun: async () => calls.push("run")
    }))
  });

  const watching = facade.ensureJobsPipelineStatusWatch();
  assert.equal(typeof watching.then, "function");
  assert.deepEqual(calls, []);
  assert.equal(facade.isLoaded(), false);

  gate.resolve();
  await watching;
  assert.deepEqual(calls, ["watch"]);
  assert.equal(facade.isLoaded(), true);
});

test("a click that lands before the chunk runs the pipeline instead of being dropped", async () => {
  const calls = [];
  const gate = deferred();
  const facade = createLazyJobsPipelineController({
    createController: () => gate.promise.then(() => ({
      ensureJobsPipelineStatusWatch: () => calls.push("watch"),
      triggerJobsPipelineRun: async () => calls.push("run")
    }))
  });

  const first = facade.triggerJobsPipelineRun();
  gate.resolve();
  await first;
  assert.deepEqual(calls, ["run"]);
});

test("status watch and run trigger share one chunk load", async () => {
  let created = 0;
  const facade = createLazyJobsPipelineController({
    createController: async () => {
      created += 1;
      return { ensureJobsPipelineStatusWatch: noop, triggerJobsPipelineRun: asyncNoop };
    }
  });

  await Promise.all([
    facade.ensureJobsPipelineStatusWatch(),
    facade.triggerJobsPipelineRun(),
    facade.triggerJobsPipelineRun()
  ]);
  assert.equal(created, 1);
});

test("construction alone does not fetch the chunk; the first call does", async () => {
  let created = 0;
  const facade = createLazyJobsPipelineController({
    createController: async () => {
      created += 1;
      return {};
    }
  });

  await sleep(10);
  assert.equal(created, 0);
  await facade.ensureJobsPipelineStatusWatch();
  assert.equal(created, 1);
});

test("a chunk that fails to load leaves an inert controller that does not retry", async () => {
  let attempts = 0;
  const facade = createLazyJobsPipelineController({
    createController: async () => {
      attempts += 1;
      throw new Error("chunk 404");
    }
  });

  const watch = facade.ensureJobsPipelineStatusWatch();
  const run = facade.triggerJobsPipelineRun();

  assert.equal(await watch, undefined);
  assert.equal(await run, undefined);
  assert.equal(facade.isLoaded(), true);
  assert.equal(attempts, 1);
});

test("the update surface factory builds a controller without touching the DOM", () => {
  const refs = {};
  const controller = createJobsDesktopUpdateSurface({
    refs,
    documentObject: { addEventListener: noop },
    emitDiagnostic: noop,
    baseUrl: "http://127.0.0.1:1",
    isDesktopRuntimeMode: () => false,
    openExternalUrl: noop
  });

  for (const method of ["mount", "startAutoCheck", "checkForUpdates", "handlePrimaryAction"]) {
    assert.equal(typeof controller[method], "function", `missing update controller method ${method}`);
  }
});
