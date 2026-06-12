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
