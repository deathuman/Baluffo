import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";

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
