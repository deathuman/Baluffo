import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";
import { createBaseDeps } from "./helpers/jobs-feed-test-helpers.mjs";

test("container startup preview does not immediately fetch the full jobs feed", async () => {
  let refreshCalls = 0;
  const { calls, deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    loadStartupPreviewJobs: async () => true,
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(refreshCalls, 0);
  assert.equal(calls.initialized.at(-1), true);
  assert.match(calls.sourceStatus.at(-1), /startup snapshot/i);
  assert.doesNotMatch(calls.sourceStatus.at(-1), /syncing full feed/i);
});

test("container startup does not read a populated IndexedDB cache before the startup snapshot", async () => {
  let cacheReads = 0;
  const { calls, deps } = createBaseDeps({
    isContainerRuntimeMode: () => true,
    readCachedJobs: async () => {
      cacheReads += 1;
      return { jobs: [{ id: "job-1" }], savedAt: Date.now() };
    },
    loadStartupPreviewJobs: async () => true
  });

  await initJobsFeed(deps);

  assert.equal(cacheReads, 0);
  assert.match(calls.sourceStatus.at(-1), /startup snapshot/i);
});

test("container boot applies pending admin signals as acknowledge-only without refreshing", async () => {
  let refreshCalls = 0;
  const appliedOptions = [];
  const { calls, deps } = createBaseDeps({
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

  assert.equal(refreshCalls, 0);
  assert.deepEqual(appliedOptions, [{ acknowledgeOnly: true }]);
  assert.equal(calls.initialized.at(-1), true);
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
