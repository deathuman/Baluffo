import test from "node:test";
import assert from "node:assert/strict";

import {
  initJobsFeed,
  jobsFirstRunBootstrapNumberOverride
} from "../../../frontend/jobs/app/feed.js";
import { createJobsBoot } from "../../../frontend/jobs/app/runtime/boot.js";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

const EMPTY_REPORT = { summary: { outputCount: 0 } };
const SUCCESS_BOOTSTRAP_REPORT = {
  runId: "jobs_bootstrap_matrix",
  finishedAt: "2026-05-17T10:00:00+00:00",
  summary: { status: "ok", outputCount: 1, coverageScope: "bootstrap_sheets" }
};

test("createJobsBoot first-run bootstrap request does not force a duplicate refresh", async () => {
  const { localStorage } = createLocalStorage();
  const bridgeCalls = [];
  const errors = [];
  let reportCalls = 0;
  const deps = {
    dom: { jobsList: {} },
    runtimeState: { allJobs: [] },
    emitDesktopStartupMetric: () => {},
    authController: { initAuth: () => {} },
    isDesktopRuntimeMode: () => true,
    feedController: {
      readCachedJobs: async () => null,
      setSourceStatus: () => {},
      setProgress: () => {},
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      fetchJobsReport: async () => {
        reportCalls += 1;
        return reportCalls === 1 ? EMPTY_REPORT : SUCCESS_BOOTSTRAP_REPORT;
      },
      loadStartupPreviewJobs: async () => false,
      renderDataSources: async () => {}
    },
    normalizeJobs: rows => rows,
    professionLabels: {},
    sanitizeUrl: url => url,
    eventsController: { recalculateItemsPerPage: () => {} },
    filtersController: { updateFilterOptions: () => {}, applyStateToFilters: () => {} },
    applyFiltersAndRender: () => {},
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    isJobsCacheStale: () => false,
    jobsCacheTtlMs: 0,
    callJobsBridge: async (path, options = {}) => {
      bridgeCalls.push({ path, options });
      if (path === "/tasks/run-jobs-bootstrap") {
        return { started: true, runId: "jobs_bootstrap_matrix" };
      }
      if (path === "/ops/task-live/fetch") return null;
      return {};
    },
    desktopJobsColdStart: true,
    windowObject: {
      localStorage,
      setTimeout: fn => {
        fn();
        return 1;
      },
      clearTimeout: () => {}
    },
    setJobsStartupState: () => {},
    bootstrapStartTimeoutMs: 30000,
    bootstrapConfirmTimeoutMs: 0,
    bootstrapConfirmIntervalMs: 0,
    applyPendingAutoRefreshSignal: async () => {},
    ensureJobsPipelineStatusWatch: () => {},
    showError: message => errors.push(String(message || "")),
    showFirstRunBootstrapNotice: () => {}
  };

  await createJobsBoot(deps).init();

  const startCall = bridgeCalls.find(call => call.path === "/tasks/run-jobs-bootstrap");
  assert.deepEqual(errors, []);
  assert.deepEqual(startCall.options.body, { source: "jobs_first_run" });
});

test("createJobsBoot skips automatic Data Sources load in container mode", async () => {
  let renderDataSourcesCalls = 0;
  let pipelineWatchCalls = 0;
  const deps = {
    dom: { jobsList: {} },
    runtimeState: { allJobs: [] },
    emitDesktopStartupMetric: () => {},
    authController: { initAuth: () => {} },
    isDesktopRuntimeMode: () => false,
    isContainerRuntimeMode: () => true,
    feedController: {
      readCachedJobs: async () => ({ jobs: [{ id: "cached-job" }], savedAt: Date.now() }),
      setSourceStatus: () => {},
      setProgress: () => {},
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      fetchJobsReport: async () => EMPTY_REPORT,
      loadStartupPreviewJobs: async () => false,
      renderDataSources: async () => {
        renderDataSourcesCalls += 1;
      }
    },
    normalizeJobs: rows => rows,
    professionLabels: {},
    sanitizeUrl: url => url,
    eventsController: { recalculateItemsPerPage: () => {} },
    filtersController: { updateFilterOptions: () => {}, applyStateToFilters: () => {} },
    applyFiltersAndRender: () => {},
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    isJobsCacheStale: () => false,
    jobsCacheTtlMs: 0,
    callJobsBridge: async () => ({}),
    desktopJobsColdStart: false,
    windowObject: {
      setTimeout: fn => {
        fn();
        return 1;
      },
      clearTimeout: () => {}
    },
    setJobsStartupState: () => {},
    bootstrapStartTimeoutMs: 30000,
    bootstrapConfirmTimeoutMs: 0,
    bootstrapConfirmIntervalMs: 0,
    applyPendingAutoRefreshSignal: async () => {},
    ensureJobsPipelineStatusWatch: () => {
      pipelineWatchCalls += 1;
    },
    showError: () => {},
    showFirstRunBootstrapNotice: () => {}
  };

  await createJobsBoot(deps).init();

  assert.equal(renderDataSourcesCalls, 0);
  assert.equal(pipelineWatchCalls, 1);
});

test("createJobsBoot keeps automatic Data Sources load outside container mode", async () => {
  let renderDataSourcesCalls = 0;
  const deps = {
    dom: { jobsList: {} },
    runtimeState: { allJobs: [] },
    emitDesktopStartupMetric: () => {},
    authController: { initAuth: () => {} },
    isDesktopRuntimeMode: () => false,
    isContainerRuntimeMode: () => false,
    feedController: {
      readCachedJobs: async () => ({ jobs: [{ id: "cached-job" }], savedAt: Date.now() }),
      setSourceStatus: () => {},
      setProgress: () => {},
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      fetchJobsReport: async () => EMPTY_REPORT,
      loadStartupPreviewJobs: async () => false,
      renderDataSources: async () => {
        renderDataSourcesCalls += 1;
      }
    },
    normalizeJobs: rows => rows,
    professionLabels: {},
    sanitizeUrl: url => url,
    eventsController: { recalculateItemsPerPage: () => {} },
    filtersController: { updateFilterOptions: () => {}, applyStateToFilters: () => {} },
    applyFiltersAndRender: () => {},
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    isJobsCacheStale: () => false,
    jobsCacheTtlMs: 0,
    callJobsBridge: async () => ({}),
    desktopJobsColdStart: false,
    windowObject: {
      setTimeout: fn => {
        fn();
        return 1;
      },
      clearTimeout: () => {}
    },
    setJobsStartupState: () => {},
    bootstrapStartTimeoutMs: 30000,
    bootstrapConfirmTimeoutMs: 0,
    bootstrapConfirmIntervalMs: 0,
    applyPendingAutoRefreshSignal: async () => {},
    ensureJobsPipelineStatusWatch: () => {},
    showError: () => {},
    showFirstRunBootstrapNotice: () => {}
  };

  await createJobsBoot(deps).init();

  assert.equal(renderDataSourcesCalls, 1);
});

test("jobsFirstRunBootstrapNumberOverride accepts positive URL values and falls back otherwise", () => {
  assert.equal(
    jobsFirstRunBootstrapNumberOverride(
      { location: { search: "?desktop=1&jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=3000" } },
      "jobsFirstRunBootstrapTimeoutMs",
      123
    ),
    3000
  );
  assert.equal(
    jobsFirstRunBootstrapNumberOverride(
      { location: { search: "?desktop=1&jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=0" } },
      "jobsFirstRunBootstrapTimeoutMs",
      123
    ),
    123
  );
  assert.equal(
    jobsFirstRunBootstrapNumberOverride(
      { location: { search: "?desktop=1&jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=bad" } },
      "jobsFirstRunBootstrapTimeoutMs",
      123
    ),
    123
  );
  assert.equal(
    jobsFirstRunBootstrapNumberOverride(
      { location: { search: "?jobsFirstRunBootstrapTimeoutMs=3000" } },
      "jobsFirstRunBootstrapTimeoutMs",
      123
    ),
    123
  );
});

test("initJobsFeed applies smoke URL timeout override for first-run bootstrap", async () => {
  const { localStorage } = createLocalStorage();
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: {
      localStorage,
      location: {
        search: "?desktop=1&jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=1&jobsFirstRunBootstrapProgressStaleMs=1"
      }
    },
    bootstrapPollIntervalMs: 0,
    fetchJobsReport: async () => ({ summary: { outputCount: 0 } }),
    fetchJobsTaskLive: async () => ({
      active: true,
      heartbeatAt: "2026-01-01T00:00:00.000Z",
      taskProgress: { active: true, updatedAt: "2026-01-01T00:00:00.000Z" }
    }),
    startJobsBootstrap: async () => ({ started: true, runId: "jobs_bootstrap_url_override" })
  });

  await initJobsFeed(deps);

  assert.equal(calls.showError.length, 1);
  assert.match(calls.showError[0], /first-run sheet refresh timed out/);
});

test("initJobsFeed waits past timeout while task-live heartbeat stays fresh", async () => {
  const { localStorage } = createLocalStorage();
  const reports = [EMPTY_REPORT, EMPTY_REPORT, EMPTY_REPORT, SUCCESS_BOOTSTRAP_REPORT];
  let reportIndex = 0;
  let taskLiveCalls = 0;
  let refreshes = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1,
    bootstrapProgressStaleMs: 60000,
    fetchJobsReport: async () => reports[Math.min(reportIndex++, reports.length - 1)],
    fetchJobsTaskLive: async () => {
      taskLiveCalls += 1;
      return {
        active: true,
        status: "running",
        heartbeatAt: new Date().toISOString(),
        taskProgress: { active: true, updatedAt: new Date().toISOString() }
      };
    },
    startJobsBootstrap: async () => ({ started: true, runId: "jobs_bootstrap_matrix" }),
    refreshJobsNow: async () => {
      refreshes += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshes, 1);
  assert.equal(taskLiveCalls >= 1, true);
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed checks the final report once more before showing timeout", async () => {
  const { localStorage } = createLocalStorage();
  const reports = [EMPTY_REPORT, EMPTY_REPORT, SUCCESS_BOOTSTRAP_REPORT];
  let reportIndex = 0;
  let refreshes = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1,
    fetchJobsReport: async () => reports[Math.min(reportIndex++, reports.length - 1)],
    fetchJobsTaskLive: async () => ({ active: false, heartbeatAt: "2026-05-17T09:00:00+00:00" }),
    startJobsBootstrap: async () => ({ started: true, runId: "jobs_bootstrap_matrix" }),
    refreshJobsNow: async () => {
      refreshes += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshes, 1);
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed retry loads an already completed feed before starting another bootstrap", async () => {
  const { localStorage } = createLocalStorage();
  let reportCalls = 0;
  let bootstrapStarts = 0;
  let retryCallback = null;
  let refreshes = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapConfirmTimeoutMs: 0,
    fetchJobsReport: async () => {
      reportCalls += 1;
      return reportCalls <= 2 ? EMPTY_REPORT : SUCCESS_BOOTSTRAP_REPORT;
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      throw new Error("Bridge request timed out");
    },
    refreshJobsNow: async () => {
      refreshes += 1;
      return true;
    },
    showError: (message, onRetry) => {
      calls.showError.push(String(message || ""));
      retryCallback = onRetry;
    }
  });

  await initJobsFeed(deps);
  assert.equal(typeof retryCallback, "function");

  await retryCallback({ currentTarget: null });

  assert.equal(bootstrapStarts, 2);
  assert.equal(refreshes, 1);
  assert.match(calls.showError[0], /Could not confirm first-run sheet refresh started/);
});
