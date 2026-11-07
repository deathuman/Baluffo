import test from "node:test";
import assert from "node:assert/strict";

import { openJobsCacheDb } from "../../../frontend/jobs/app/cache.js";

function cacheOptions(indexedDb) {
  return {
    indexedDb,
    dbName: "baluffo_jobs_cache",
    dbVersion: 2,
    cacheStore: "jobs_feed",
    seenStore: "jobs_seen"
  };
}

test("jobs cache open rejects loudly when another connection blocks the upgrade", async () => {
  const request = {
    result: null,
    error: null,
    onupgradeneeded: null,
    onblocked: null,
    onsuccess: null,
    onerror: null
  };
  const closed = [];
  const fakeIndexedDb = {
    open() {
      queueMicrotask(() => request.onblocked?.());
      return request;
    }
  };

  await assert.rejects(
    openJobsCacheDb(cacheOptions(fakeIndexedDb)),
    /blocked by another connection/
  );

  // The open request can still succeed after the promise rejected; close that
  // late connection instead of leaking it.
  request.result = { close: () => closed.push(true) };
  request.onsuccess?.();
  assert.equal(closed.length, 1);
});
