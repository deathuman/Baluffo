import test from "node:test";
import assert from "node:assert/strict";

import { createStorageBreakdownController } from "../../../frontend/admin/app/storage-breakdown.js";
import {
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml
} from "../../../frontend/admin/render/storage-tables.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

const TABLE_SIZES = {
  status: "ok",
  estimated: true,
  complete: true,
  databaseBytes: 10_000,
  allocatedBytes: 8_000,
  freelistBytes: 2_000,
  tables: [{ name: "source_runs", bytes: 6_000, share: 0.75, rowCount: 1_200 }],
  otherBytes: 1_000
};

function fixture({
  payload = { storage: { tableSizes: TABLE_SIZES } },
  reject = null,
  shouldDefer = () => false
} = {}) {
  const calls = [];
  const listeners = {};
  const refs = {
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement(),
    adminStorageTablesRefreshBtnEl: createElement({
      addEventListener(type, handler) {
        listeners[type] = handler;
      }
    })
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async (path, options) => {
      calls.push({ path, options });
      if (reject) throw reject;
      return payload;
    },
    formatBytes: value => `${value} B`,
    renderStorageTablesHtml,
    renderStorageTablesMessageHtml,
    shouldDefer
  });
  return { controller, refs, calls, listeners };
}

test("storage breakdown defers without calling storage health", async () => {
  const f = fixture({ shouldDefer: () => true });

  await f.controller.load();

  assert.equal(f.calls.length, 0);
  assert.match(f.refs.adminStorageTablesBodyEl.innerHTML, /deferred while an active run/i);
  assert.equal(f.refs.adminStorageTablesStatusEl.textContent, "Deferred");
});

test("storage breakdown loads once with a bounded non-reachability read", async () => {
  const f = fixture();

  await f.controller.load();

  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].path, "/ops/storage-health");
  assert.equal(f.calls[0].options.affectsReachability, false);
  assert.equal(f.calls[0].options.timeoutMs, 5000);
  assert.match(f.refs.adminStorageTablesBodyEl.innerHTML, /source_runs/);
  assert.equal(f.refs.adminStorageTablesStatusEl.textContent, "Estimated");
});

test("storage breakdown forwards the startup-lane allowance to the defer predicate", async () => {
  const seen = [];
  const f = fixture({
    shouldDefer: options => {
      seen.push(options);
      return !options?.allowStartupBridgeLane;
    }
  });

  await f.controller.load({ allowStartupBridgeLane: true });

  assert.deepEqual(seen, [{ allowStartupBridgeLane: true }]);
  assert.equal(f.calls.length, 1);
  assert.equal(f.refs.adminStorageTablesStatusEl.textContent, "Estimated");
});

test("refresh click reloads the breakdown", async () => {
  const f = fixture();
  f.controller.init();

  await f.listeners.click();

  assert.equal(f.calls.length, 1);
});

test("storage breakdown reports read failures", async () => {
  const f = fixture({ reject: new Error("boom") });

  await f.controller.load();

  assert.match(f.refs.adminStorageTablesBodyEl.innerHTML, /Could not load storage health: boom/);
  assert.equal(f.refs.adminStorageTablesStatusEl.textContent, "Unavailable");
});

test("storage breakdown reports a missing tableSizes payload", async () => {
  const f = fixture({ payload: { storage: { healthy: true } } });

  await f.controller.load();

  assert.match(f.refs.adminStorageTablesBodyEl.innerHTML, /did not include a table-size breakdown/);
  assert.equal(f.refs.adminStorageTablesStatusEl.textContent, "Unavailable");
});
