import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";
import { createBaseDeps } from "./helpers/jobs-feed-test-helpers.mjs";

test("container boot loads the full feed directly instead of painting the startup snapshot", async () => {
  let refreshCalls = 0;
  let previewCalls = 0;
  const { calls, deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    loadStartupPreviewJobs: async () => {
      previewCalls += 1;
      return true;
    },
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(previewCalls, 0);
  assert.equal(refreshCalls, 1);
  assert.equal(calls.initialized.at(-1), true);
});

test("container boot schedules no delayed snapshot hydration", async () => {
  const scheduled = [];
  let refreshCalls = 0;
  const { deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    readCachedJobs: async () => null,
    loadStartupPreviewJobs: async () => true,
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    },
    windowObject: {
      localStorage: new Map(),
      setTimeout: (callback, delay) => {
        scheduled.push({ callback, delay });
        return 0;
      }
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshCalls, 1);
  assert.deepEqual(scheduled, []);
});

test("container startup does not read a populated IndexedDB cache before the full feed", async () => {
  let cacheReads = 0;
  let refreshCalls = 0;
  const { deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    readCachedJobs: async () => {
      cacheReads += 1;
      return { jobs: [{ id: "job-1" }], savedAt: Date.now() };
    },
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(cacheReads, 0);
  assert.equal(refreshCalls, 1);
});

test("container boot applies pending admin signals as acknowledge-only", async () => {
  let refreshCalls = 0;
  const appliedOptions = [];
  const { deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    readCachedJobs: async () => null,
    loadStartupPreviewJobs: async () => true,
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    },
    applyPendingAutoRefreshSignal: options => appliedOptions.push(options)
  });

  await initJobsFeed(deps);

  assert.equal(refreshCalls, 1);
  assert.deepEqual(appliedOptions, [{ acknowledgeOnly: true, feedLoaded: true }]);
});

test("container boot defers the admin refresh when the full feed did not load", async () => {
  const appliedOptions = [];
  const { deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    readCachedJobs: async () => null,
    loadStartupPreviewJobs: async () => true,
    refreshJobsNow: async () => false,
    applyPendingAutoRefreshSignal: options => appliedOptions.push(options)
  });

  await initJobsFeed(deps);

  assert.deepEqual(appliedOptions, [{ acknowledgeOnly: true, feedLoaded: false }]);
});

test("browser startup keeps applying pending signals through the normal refresh path", async () => {
  let cacheReads = 0;
  const appliedOptions = [];
  const { deps } = createBaseDeps({
    readCachedJobs: async () => {
      cacheReads += 1;
      return { jobs: [{ id: "job-1" }], savedAt: Date.now() };
    },
    applyPendingAutoRefreshSignal: options => appliedOptions.push(options)
  });

  await initJobsFeed(deps);

  assert.equal(cacheReads, 1);
  assert.deepEqual(appliedOptions, [{}]);
});
