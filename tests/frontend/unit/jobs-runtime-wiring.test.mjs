import test from "node:test";
import assert from "node:assert/strict";
import { createJobsBoot } from "../../../frontend/jobs/app/runtime/boot.js";

function createBootDeps({ isContainerRuntimeMode }) {
  const initJobsFeedSpy = async (options) => {
    initJobsFeedSpy.lastOptions = options;
    return true;
  };
  const noop = () => {};
  const asyncNoop = async () => {};
  const deps = {
    dom: { jobsList: null },
    emitDesktopStartupMetric: noop,
    authController: { initAuth: noop },
    isDesktopRuntimeMode: () => !isContainerRuntimeMode,
    isContainerRuntimeMode: () => Boolean(isContainerRuntimeMode),
    isJobsCacheStale: () => false,
    jobsCacheTtlMs: 1,
    bootstrapStartTimeoutMs: 1,
    bootstrapConfirmTimeoutMs: 1,
    bootstrapConfirmIntervalMs: 1,
    desktopJobsColdStart: false,
    runtimeState: {},
    windowObject: { setTimeout: () => 0 },
    applyFiltersAndRender: noop,
    applyPendingAutoRefreshSignal: noop,
    showError: noop,
    handleJobsStartupFailure: noop,
    setJobsStartupState: noop,
    showFirstRunBootstrapNotice: noop,
    markJobsFirstInteractive: noop,
    normalizeJobs: (rows) => rows,
    callJobsBridge: async () => ({}),
    feedController: {
      readCachedJobs: async () => null,
      setSourceStatus: noop,
      setProgress: noop,
      refreshJobsNow: asyncNoop,
      fetchJobsReport: asyncNoop,
      updateLastUpdatedText: noop,
      loadStartupPreviewJobs: asyncNoop
    },
    eventsController: { recalculateItemsPerPage: noop },
    filtersController: { updateFilterOptions: noop, applyStateToFilters: noop },
    initJobsFeed: initJobsFeedSpy
  };
  return { deps, initJobsFeedSpy };
}

for (const isContainer of [true, false]) {
  test(`jobs boot forwards runtime mode into initJobsFeed (container=${isContainer})`, async () => {
    const { deps, initJobsFeedSpy } = createBootDeps({ isContainerRuntimeMode: isContainer });
    await createJobsBoot(deps).init();
    const options = initJobsFeedSpy.lastOptions;
    assert.ok(options, "initJobsFeed was called with options");
    assert.ok(options, "initJobsFeed was called with options");
    assert.equal(options.isContainerRuntimeMode(), isContainer);
    assert.equal(options.isDesktopRuntimeMode(), !isContainer);
  });
}
