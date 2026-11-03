/**
 * The Action Center's storage poll is diagnostic-only.
 *
 * /ops/storage-health can be slow while the bridge serves every other route, so
 * its failure must not be reported as the bridge going offline. The health poll
 * keeps the default attribution, so a genuinely dead bridge still shows up.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";
import {
  cleanHealthPayload,
  cleanStoragePayload,
  cleanSyncPayload
} from "./helpers/action-center-fixture.mjs";

test("action center marks the storage health poll as diagnostic-only", async () => {
  const seen = [];
  const { controller } = createFixture({
    getBridge: async (path, options) => {
      seen.push([path, options]);
      if (path === "/ops/fetch-kpis?view=summary") return cleanHealthPayload();
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      if (path === "/ops/storage-health") return cleanStoragePayload();
      return null;
    }
  });

  await controller.pollActionCenter({ includeStorage: true });

  const storageCall = seen.find(([path]) => path === "/ops/storage-health");
  assert.equal(storageCall?.[1]?.affectsReachability, false);

  // The core signal routes still own the badge, so a dead bridge cannot hide
  // behind the diagnostic-only flag.
  const healthCall = seen.find(([path]) => path === "/ops/fetch-kpis?view=summary");
  assert.equal(healthCall?.[1]?.affectsReachability, undefined);
});
