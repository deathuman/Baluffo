import test from "node:test";
import assert from "node:assert/strict";

import {
  canUseStartupPreviewFastPath,
  initJobsFeed,
  refreshJobsFeed,
  loadStartupPreviewJobsFeed
} from "../../../frontend/jobs/app/feed.js";
import { STARTUP_PREVIEW_JSON_URLS } from "../../../frontend/jobs/app/sources.js";
import { createJobsAuthController } from "../../../frontend/jobs/app/runtime/auth-controller.js";
import { createElement } from "./helpers/jobs-runtime-helpers.mjs";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

test("initJobsFeed marks startup initialized and interactive on successful first-load refresh", async () => {
  const { calls, deps } = createBaseDeps({
    refreshJobsNow: async ({ firstLoad = false } = {}) => Boolean(firstLoad),
  });

  await initJobsFeed(deps);

  assert.deepEqual(calls.showError, []);
  assert.equal(calls.initialized.at(-1), true);
  assert.equal(calls.perf[0].name, "jobs_boot_start");
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
  assert.equal(calls.perf[0].name, "jobs_boot_start");
});

test("refreshJobsFeed marks first-load fetch and render milestones", async () => {
  let refreshInFlight = false;
  let allJobs = [];
  let fetchUnifiedOptions = null;
  let lastUpdated = null;
  const perf = [];
  const ok = await refreshJobsFeed({ manual: false, firstLoad: true }, {
    getRefreshInFlight: () => refreshInFlight,
    setRefreshInFlight: value => {
      refreshInFlight = Boolean(value);
    },
    dispatchRefreshRequested: () => {},
    setRefreshButtonDisabled: () => {},
    setProgress: () => {},
    setSourceStatus: () => {},
    firstLoadRequestTimeoutMs: 4500,
    fetchUnifiedJobs: async options => {
      fetchUnifiedOptions = options;
      return { jobs: [{ id: "job-1" }], sourceName: "test" };
    },
    dispatchRefreshFailed: () => {},
    showToast: () => {},
    logError: () => {},
    getAllJobs: () => allJobs,
    setAllJobs: jobs => {
      allJobs = jobs;
    },
    normalizeRows: rows => rows.map(row => ({ ...row, normalized: true })),
    setRefreshJobsNeedsAttention: () => {},
    isDesktopRuntimeMode: () => true,
    writeCachedJobs: async () => {},
    fetchJobsReport: async () => ({
      finishedAt: "2026-05-17T10:00:00+00:00",
      summary: { outputCount: 1, coverageScope: "bootstrap_sheets" }
    }),
    updateLastUpdatedText: value => {
      lastUpdated = value;
    },
    recalculateItemsPerPage: () => {},
    updateFilterOptions: () => {},
    applyStateToFilters: () => {},
    applyFiltersAndRender: () => {},
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    markJobsStep: (name, payload = {}) => perf.push({ type: "mark", name, payload }),
    measureJobsStep: (name, startMark, endMark, payload = {}) =>
      perf.push({ type: "measure", name, startMark, endMark, payload }),
    emitMetric: () => {},
    dispatchRefreshCompleted: () => {},
    renderDataSources: async () => {}
  });

  assert.equal(ok, true);
  assert.equal(fetchUnifiedOptions.allowSheetsFallback, false);
  assert.equal(lastUpdated, Date.parse("2026-05-17T10:00:00+00:00"));
  assert.deepEqual(
    perf.map(item => `${item.type}:${item.name}`),
    [
      "mark:jobs_feed_fetch_start",
      "mark:jobs_feed_fetch_done",
      "measure:jobs_feed_fetch",
      "mark:jobs_render_start",
      "mark:jobs_render_end",
      "measure:jobs_render"
    ]
  );
  assert.deepEqual(perf.find(item => item.name === "jobs_render")?.payload, {
    rowCount: 1
  });
});

test("refreshJobsFeed treats all-filtered missing-title rows as no data", async () => {
  let refreshInFlight = false;
  let allJobs = [{ id: "old-job" }];
  let renderCalls = 0;
  let completedCalls = 0;
  const sourceStatus = [];
  const failures = [];

  const ok = await refreshJobsFeed({ manual: false, firstLoad: true }, {
    getRefreshInFlight: () => refreshInFlight,
    setRefreshInFlight: value => {
      refreshInFlight = Boolean(value);
    },
    dispatchRefreshRequested: () => {},
    setRefreshButtonDisabled: () => {},
    setProgress: () => {},
    setSourceStatus: text => sourceStatus.push(String(text || "")),
    firstLoadRequestTimeoutMs: 4500,
    fetchUnifiedJobs: async () => ({
      jobs: [{ id: "bad-row", title: "", company: "Studio" }],
      sourceName: "test"
    }),
    dispatchRefreshFailed: error => failures.push(String(error || "")),
    showToast: () => {},
    logError: () => {},
    getAllJobs: () => allJobs,
    setAllJobs: jobs => {
      allJobs = jobs;
    },
    normalizeRows: () => [],
    setRefreshJobsNeedsAttention: () => {},
    isDesktopRuntimeMode: () => true,
    writeCachedJobs: async () => {},
    fetchJobsReport: async () => null,
    updateLastUpdatedText: () => {},
    recalculateItemsPerPage: () => {},
    updateFilterOptions: () => {},
    applyStateToFilters: () => {},
    applyFiltersAndRender: () => {
      renderCalls += 1;
    },
    markStartupRendered: () => {},
    markJobsFirstInteractive: () => {},
    emitMetric: () => {},
    dispatchRefreshCompleted: () => {
      completedCalls += 1;
    },
    renderDataSources: async () => {}
  });

  assert.equal(ok, false);
  assert.deepEqual(allJobs, []);
  assert.equal(renderCalls, 0);
  assert.equal(completedCalls, 0);
  assert.match(sourceStatus.at(-1), /no displayable positions/);
  assert.match(failures.at(-1), /no displayable positions/);
});

test("initJobsFeed skips packaged feeds and auto-starts bootstrap on desktop cold start", async () => {
  const { storage, localStorage } = createLocalStorage();
  let reportCalls = 0;
  let bootstrapStarts = 0;
  let refreshOptions = null;
  let startupPreviewCalled = false;
  let pendingAutoRefreshCalled = false;
  const eventOrder = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) {
        return { summary: { outputCount: 0 } };
      }
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      eventOrder.push("bootstrap");
      return { started: true, task: "jobs_bootstrap" };
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      eventOrder.push("refresh");
      return true;
    },
    loadStartupPreviewJobs: async () => {
      startupPreviewCalled = true;
      return true;
    },
    applyPendingAutoRefreshSignal: async () => {
      pendingAutoRefreshCalled = true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.equal(startupPreviewCalled, false);
  assert.equal(pendingAutoRefreshCalled, false);
  assert.deepEqual(eventOrder, ["bootstrap", "refresh"]);
  assert.deepEqual(calls.allJobs[0], []);
  assert.deepEqual(calls.rendered[0], { stage: "first_run_bootstrap", rowCount: 0 });
  assert.equal(calls.interactive[0], "first_run_bootstrap");
  assert.deepEqual(calls.startupStates.at(-1), {
    state: "interactive",
    detail: "first_run_bootstrap"
  });
  assert.notEqual(calls.startupStates.at(-1)?.state, "loading");
  assert.equal(calls.metrics.find(metric => metric.event === "jobs_first_run_gate_evaluated")?.payload.action, "start");
  assert.equal(calls.notices.length, 1);
  assert.equal(calls.notices[0].title, "Preparing first-run jobs");
  assert.match(calls.notices[0].body, /several minutes/);
  assert.equal(calls.notices[0].primaryLabel, "Got it");
  assert.equal(calls.notices[0].reason, "start");
  assert.equal(
    calls.sourceStatus.at(-1),
    "Refreshing first-run sheet jobs. This can take several minutes..."
  );
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});

test("initJobsFeed keeps waiting past first-run timeout while task-live heartbeat is fresh", async () => {
  const { localStorage } = createLocalStorage();
  let reportCalls = 0;
  let taskLiveCalls = 0;
  let refreshOptions = null;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1,
    bootstrapProgressStaleMs: 90_000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls <= 2) return { summary: { outputCount: 0 } };
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    fetchJobsTaskLive: async () => {
      taskLiveCalls += 1;
      return {
        active: true,
        heartbeatAt: new Date().toISOString(),
        taskProgress: { active: true, updatedAt: new Date().toISOString() }
      };
    },
    startJobsBootstrap: async () => ({ started: true, runId: "jobs_bootstrap_live" }),
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(taskLiveCalls >= 1, true);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed times out when first-run task-live heartbeat is stale", async () => {
  const { localStorage } = createLocalStorage();
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1,
    bootstrapProgressStaleMs: 1,
    fetchJobsReport: async () => ({ summary: { outputCount: 0 } }),
    fetchJobsTaskLive: async () => ({
      active: true,
      heartbeatAt: "2026-01-01T00:00:00.000Z",
      taskProgress: { active: true, updatedAt: "2026-01-01T00:00:00.000Z" }
    }),
    startJobsBootstrap: async () => ({ started: true, runId: "jobs_bootstrap_stale" })
  });

  await initJobsFeed(deps);

  assert.equal(calls.showError.length, 1);
  assert.match(calls.showError[0], /first-run sheet refresh timed out/);
});

test("initJobsFeed does not await the first-run notice before bootstrap polling", async () => {
  const { localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  let refreshCompleted = false;
  let reportCalls = 0;
  const { deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return { summary: { outputCount: 0 } };
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 1, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    refreshJobsNow: async () => {
      refreshCompleted = true;
      return true;
    },
    showFirstRunBootstrapNotice: () => new Promise(() => {})
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.equal(refreshCompleted, true);
});

test("initJobsFeed shows retryable no-data UI when first-run feed has no displayable rows", async () => {
  const { localStorage } = createLocalStorage();
  let reportCalls = 0;
  let errorMessage = "";
  let retryable = false;
  const { deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return { summary: { outputCount: 0 } };
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 1, coverageScope: "bootstrap_sheets" }
      };
    },
    refreshJobsNow: async () => false,
    showError: (message, onRetry) => {
      errorMessage = String(message || "");
      retryable = typeof onRetry === "function";
    }
  });

  await initJobsFeed(deps);

  assert.match(errorMessage, /Local jobs feed is missing|unreadable/);
  assert.equal(retryable, true);
});

test("initJobsFeed skips first-run notice and bootstrap for returning desktop user with successful feed", async () => {
  const { localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  let refreshCalls = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    fetchJobsReport: async () => ({
      finishedAt: "2026-05-17T10:00:00+00:00",
      summary: { status: "ok", outputCount: 5 }
    }),
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    refreshJobsNow: async ({ firstLoad = false } = {}) => {
      refreshCalls += 1;
      return Boolean(firstLoad);
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshCalls, 1);
  assert.equal(bootstrapStarts, 0);
  assert.deepEqual(calls.notices, []);
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed does not infer first-run from an unavailable report without launch flag", async () => {
  const { localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  let startupPreviewCalled = false;
  let pendingAutoRefreshCalled = false;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    fetchJobsReport: async () => {
      throw new Error("bridge unavailable");
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    loadStartupPreviewJobs: async () => {
      startupPreviewCalled = true;
      return true;
    },
    applyPendingAutoRefreshSignal: async () => {
      pendingAutoRefreshCalled = true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(startupPreviewCalled, true);
  assert.equal(pendingAutoRefreshCalled, true);
  assert.equal(bootstrapStarts, 0);
  assert.deepEqual(calls.notices, []);
  assert.equal(calls.metrics.find(metric => metric.event === "jobs_first_run_gate_evaluated")?.payload.action, "skip");
});

test("initJobsFeed skips launch-time desktop cold-start marker when report is successful", async () => {
  const { localStorage, sessionStorage, session } = createLocalStorage();
  let bootstrapStarts = 0;
  let refreshOptions = null;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    desktopJobsColdStart: true,
    windowObject: { localStorage, sessionStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => ({
      finishedAt: "2026-05-17T10:00:00+00:00",
      summary: { status: "ok", outputCount: 30967 }
    }),
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 0);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.deepEqual(calls.notices, []);
  assert.equal(session.get("baluffo_jobs_bootstrap_launch_cold_start_handled"), undefined);
});

test("initJobsFeed does not repeat launch-time cold-start bootstrap after session handling", async () => {
  const { localStorage, sessionStorage, session } = createLocalStorage();
  session.set("baluffo_jobs_bootstrap_launch_cold_start_handled", "1");
  let bootstrapStarts = 0;
  let refreshCalls = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    desktopJobsColdStart: true,
    windowObject: { localStorage, sessionStorage },
    fetchJobsReport: async () => ({
      finishedAt: "2026-05-17T10:00:00+00:00",
      summary: { status: "ok", outputCount: 30967 }
    }),
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    refreshJobsNow: async ({ firstLoad = false } = {}) => {
      refreshCalls += 1;
      return Boolean(firstLoad);
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshCalls, 1);
  assert.equal(bootstrapStarts, 0);
  assert.deepEqual(calls.notices, []);
});

test("initJobsFeed ignores failed bootstrap marker when first-run is still required", async () => {
  const { localStorage } = createLocalStorage([
    ["baluffo_jobs_bootstrap_auto_started", JSON.stringify({ status: "failed" })]
  ]);
  let bootstrapStarts = 0;
  let reportCalls = 0;
  let refreshOptions = null;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) {
        return {
          finishedAt: "2026-05-17T10:00:00+00:00",
          summary: { status: "error", outputCount: 0, error: "sheet failed" }
        };
      }
      return {
        finishedAt: "2026-05-17T10:05:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed recovers with bootstrap when successful report has no loadable feed", async () => {
  const { storage, localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  let reportCalls = 0;
  const refreshCalls = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true, runId: "jobs_bootstrap_test" };
    },
    loadStartupPreviewJobs: async () => false,
    refreshJobsNow: async options => {
      refreshCalls.push(options);
      return refreshCalls.length > 1;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.equal(reportCalls >= 2, true);
  assert.deepEqual(refreshCalls, [
    { manual: false, firstLoad: true },
    { manual: false, firstLoad: true }
  ]);
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});

test("initJobsFeed reports missing local feed when bootstrap is already completed", async () => {
  const { localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    fetchJobsReport: async () => ({
      finishedAt: "2026-05-17T10:00:00+00:00",
      summary: { status: "ok", outputCount: 3 }
    }),
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { alreadyCompleted: true, error: "full_pipeline_already_completed" };
    },
    loadStartupPreviewJobs: async () => false,
    refreshJobsNow: async () => false
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.equal(calls.showError.length, 1);
  assert.match(calls.showError[0], /Local jobs feed is missing|Run Update jobs/);
});

test("initJobsFeed reattaches to running bootstrap after reload", async () => {
  const { storage, localStorage } = createLocalStorage([
    ["baluffo_jobs_bootstrap_auto_started", JSON.stringify({ status: "running" })]
  ]);
  let bootstrapStarts = 0;
  let reportCalls = 0;
  let refreshOptions = null;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) {
        return { summary: { outputCount: 0, coverageScope: "bootstrap_sheets" } };
      }
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { alreadyRunning: true, runId: "jobs_bootstrap_test" };
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.equal(calls.notices.length, 1);
  assert.equal(calls.notices[0].reason, "reattach");
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});

test("initJobsFeed does not let a legacy marker block first-run recovery", async () => {
  const { localStorage } = createLocalStorage([["baluffo_jobs_bootstrap_auto_started", "1"]]);
  let bootstrapStarts = 0;
  let reportCalls = 0;
  let refreshOptions = null;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) {
        return {
          finishedAt: "2026-05-17T10:00:00+00:00",
          summary: { status: "error", outputCount: 0, error: "sheet failed" }
        };
      }
      return {
        finishedAt: "2026-05-17T10:05:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { alreadyRunning: true };
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed reattaches legacy marker when report is non-terminal", async () => {
  const { storage, localStorage } = createLocalStorage([
    ["baluffo_jobs_bootstrap_auto_started", "1"]
  ]);
  let bootstrapStarts = 0;
  let reportCalls = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return { summary: { outputCount: 0 } };
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { alreadyRunning: true, runId: "jobs_bootstrap_test" };
    },
    refreshJobsNow: async () => true
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
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
    scheduleStartupPreviewMaterialization: 0,
    perf: []
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
    markJobsStep: (name, payload = {}) => calls.perf.push({ type: "mark", name, payload }),
    measureJobsStep: (name, startMark, endMark, payload = {}) =>
      calls.perf.push({ type: "measure", name, startMark, endMark, payload }),
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
  assert.deepEqual(
    calls.perf.map(item => `${item.type}:${item.name}`),
    [
      "mark:jobs_startup_preview_fetch_start",
      "mark:jobs_startup_preview_fetch_done",
      "measure:jobs_startup_preview_fetch",
      "mark:jobs_startup_preview_parse_start",
      "mark:jobs_startup_preview_parse_done",
      "measure:jobs_startup_preview_parse",
      "mark:jobs_startup_preview_render_start",
      "mark:jobs_startup_preview_render_done",
      "measure:jobs_startup_preview_render",
      "mark:jobs_preview_ready"
    ]
  );
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
