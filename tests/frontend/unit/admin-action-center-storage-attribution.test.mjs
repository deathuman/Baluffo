/**
 * The Action Center's data polls are not bridge-health probes.
 *
 * /ops/storage-health and the shared /ops/fetch-kpis?view=summary route can be
 * slow while the bridge serves every other route, so their failures must not be
 * reported as the bridge going offline. The sync poll keeps the default
 * attribution, and the bridge-status watcher owns /app/ready.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";
import {
  cleanHealthPayload,
  cleanStoragePayload,
  cleanSyncPayload
} from "./helpers/action-center-fixture.mjs";

test("action center marks its passive data polls as diagnostic-only", async () => {
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

  const healthCall = seen.find(([path]) => path === "/ops/fetch-kpis?view=summary");
  assert.equal(healthCall?.[1]?.affectsReachability, false);

  // User actions and the sync poll still report reachability failures.
  const syncCall = seen.find(([path]) => path === "/sync/status?view=summary");
  assert.equal(syncCall?.[1]?.affectsReachability, undefined);
});
