import test from "node:test";
import assert from "node:assert/strict";
import { createJobsBoot } from "../../../frontend/jobs/app/runtime/boot.js";

// Boot's acceptance contract: the startup snapshot read is the first thing the
// page waits on, and everything that only interaction needs follows the rows.
const CONTROLS = ["core-events", "quick-filters", "events", "static-filters"];

function createHarness({ paintRows = true } = {}) {
  const calls = [];
  const timers = [];
  const documentObject = {
    body: { setAttribute() {}, removeAttribute() {} },
    querySelector: () => null,
    getElementById: () => null,
    createElement: () => ({ textContent: "", innerHTML: "" })
  };
  const windowObject = {
    document: documentObject,
    setTimeout(callback, delay = 0) {
      timers.push({ callback, delay });
      return timers.length;
    }
  };
  const deps = {
    adminBridgeBase: "http://127.0.0.1:1",
    dom: {},
    runtimeState: {},
    documentObject,
    windowObject,
    emitDesktopStartupMetric: () => {},
    normalizeJobs: rows => rows,
    professionLabels: {},
    sanitizeUrl: value => value,
    markStartupRendered: () => calls.push("rendered"),
    markJobsFirstInteractive: () => calls.push("interactive"),
    isDesktopRuntimeMode: () => true,
    isContainerRuntimeMode: () => false,
    desktopJobsColdStart: false,
    bootstrapStartTimeoutMs: 1,
    bootstrapConfirmTimeoutMs: 1,
    bootstrapConfirmIntervalMs: 1,
    isJobsCacheStale: () => false,
    jobsCacheTtlMs: 1,
    readStateFromUrl: () => calls.push("url-state"),
    setJobsStartupState: () => calls.push("startup-state"),
    applyJobsAdminBridgeState: () => {},
    handleJobsStartupFailure: () => calls.push("startup-failure"),
    logJobsError: () => {},
    openJobLinkInDefaultBrowser: () => {},
    applyFiltersAndRender: () => {},
    rememberCurrentJobsUrl: () => {},
    showFirstRunBootstrapNotice: () => {},
    showError: () => {},
    applyPendingAutoRefreshSignal: async () => {},
    callJobsBridge: async () => ({}),
    feedController: {
      readCachedJobs: async () => null,
      setSourceStatus: () => {},
      setProgress: () => {},
      refreshJobsNow: async () => true,
      updateLastUpdatedText: () => {},
      fetchJobsReport: async () => null,
      loadStartupPreviewJobs: async () => false,
      renderDataSources: async () => {}
    },
    eventsController: {
      setupJobsListDelegation: () => calls.push("list-delegation"),
      bindCoreEvents: () => calls.push("core-events"),
      bindEvents: () => calls.push("events"),
      recalculateItemsPerPage: () => {}
    },
    filtersController: {
      initializeQuickFilters: () => calls.push("quick-filters"),
      applyStateToStaticFilters: () => calls.push("static-filters"),
      updateFilterOptions: () => {},
      applyStateToFilters: () => {}
    },
    authController: { initAuth: () => calls.push("auth") },
    initJobsFeed: async options => {
      calls.push("init");
      if (paintRows) options.markJobsFirstInteractive();
      return true;
    }
  };
  return { calls, timers, deps };
}

function runTimers(timers, delay) {
  for (const timer of [...timers].filter(entry => entry.delay === delay)) {
    timer.callback();
  }
}

test("jobs boot reaches the feed before binding anything interaction-only", () => {
  const { calls, deps } = createHarness();
  createJobsBoot(deps).bootJobsPage();

  assert.deepEqual(calls, [
    "list-delegation",
    "startup-state",
    "url-state",
    "init",
    "interactive"
  ]);
  for (const step of ["auth", ...CONTROLS]) {
    assert.ok(!calls.includes(step), `${step} ran before the first rows`);
  }
});

test("the first rows release filter setup, event binding and auth in a later task", () => {
  const { calls, timers, deps } = createHarness();
  createJobsBoot(deps).bootJobsPage();

  // The rows path schedules its follow-up at delay 0; the 1500 ms timer is the
  // fallback for a page that never paints, so it must not be what ran here.
  runTimers(timers, 0);

  assert.equal(calls.filter(call => call === "auth").length, 1);
  assert.deepEqual(calls.slice(calls.indexOf("interactive") + 1).filter(call => call !== "auth"), CONTROLS);

  // The fallback fires on every boot; it must not repeat the work.
  runTimers(timers, 1500);
  assert.equal(calls.filter(call => call === "auth").length, 1);
  assert.equal(calls.filter(call => call === "core-events").length, 1);
});

test("the rows hook hands the follow-up to a new task and runs it once", () => {
  const { calls, timers, deps } = createHarness({ paintRows: false });
  const boot = createJobsBoot(deps);
  boot.bootJobsPage();

  boot.startBootFollowUp();
  assert.ok(!calls.includes("core-events"), "the follow-up must not run inside the render task");

  runTimers(timers, 0);
  assert.equal(calls.filter(call => call === "core-events").length, 1);

  boot.startBootFollowUp();
  runTimers(timers, 0);
  assert.equal(calls.filter(call => call === "core-events").length, 1);
});

test("a page that never paints still gets its controls from the fallback", () => {
  const { calls, timers, deps } = createHarness({ paintRows: false });
  createJobsBoot(deps).bootJobsPage();

  assert.ok(!calls.includes("auth"));
  assert.ok(!calls.includes("core-events"));

  runTimers(timers, 1500);

  assert.deepEqual(calls.filter(call => call === "auth" || CONTROLS.includes(call)), [
    "auth",
    ...CONTROLS
  ]);
});
