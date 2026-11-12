import test from "node:test";
import assert from "node:assert/strict";

import { createStorageBreakdownController } from "../../../frontend/admin/app/storage-breakdown.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

function fixture({
  payload = { storage: {} },
  maintenancePayload = { ok: true, started: true, alreadyRunning: false },
  shouldDefer = () => false
} = {}) {
  const calls = [];
  const postCalls = [];
  const listeners = {};
  const refs = {
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement(),
    adminStorageMaintenanceBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.maintenance = handler;
      }
    }),
    adminStorageMaintenanceStatusEl: createElement(),
    adminStorageMaintenanceProgressEl: createElement({ hidden: true }),
    adminStorageWriteLockEl: createElement({ hidden: true })
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async (path, options) => {
      calls.push({ path, options });
      return payload;
    },
    postBridge: async (path, body, options) => {
      postCalls.push({ path, body, options });
      return maintenancePayload;
    },
    formatBytes: value => `${value} B`,
    shouldDefer
  });
  return { controller, refs, calls, postCalls, listeners };
}

const LIVE_STATUS = {
  status: "running",
  running: true,
  phase: "prune-source-runs",
  stage: "deleting",
  elapsedMs: 65_000,
  processedRunCount: 2,
  totalRunCount: 10,
  deletedRunCount: 2,
  deletedRowCount: 20
};

test("maintenance button starts retention and reports the background handoff", async () => {
  const f = fixture();
  f.controller.init();

  await f.listeners.maintenance();

  assert.equal(f.postCalls.length, 1);
  assert.equal(f.postCalls[0].path, "/ops/storage-maintenance");
  assert.deepEqual(f.postCalls[0].body, {});
  assert.equal(f.postCalls[0].options.affectsReachability, false);
  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Maintenance started/);
  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /database compacted/);
  f.controller.stopMaintenancePoll();
});

test("storage health renders live maintenance phase and progress", () => {
  const f = fixture();
  f.controller.acceptStorageHealth({
    storage: {
      runtimeRetention: LIVE_STATUS,
      compaction: { status: "not-run" }
    }
  });

  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Maintenance running/);
  assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Removing old fetch history/);
  assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /2 of 10 runs/);
  assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /2 runs \/ 20 rows/);
  assert.equal(f.refs.adminStorageMaintenanceProgressEl.hidden, false);
  f.controller.stopMaintenancePoll();
});

test("storage health renders the current write-lock holder and duration", () => {
  const f = fixture();
  f.controller.acceptStorageHealth({
    storage: {
      writeLock: {
        held: true,
        threadName: "baluffo-sqlite-runtime-retention",
        operation: "prune-job-generations",
        heldMs: 65_000
      }
    }
  });

  assert.match(f.refs.adminStorageWriteLockEl.innerHTML, /Held by baluffo-sqlite-runtime-retention/);
  assert.match(f.refs.adminStorageWriteLockEl.innerHTML, /prune-job-generations/);
  assert.match(f.refs.adminStorageWriteLockEl.innerHTML, /for 1m 5s/);
  assert.equal(f.refs.adminStorageWriteLockEl.hidden, false);
});

test("storage health renders the no-lock fallback without maintenance state", () => {
  const f = fixture();
  f.controller.acceptStorageHealth({ storage: { writeLock: { held: false } } });

  assert.match(f.refs.adminStorageWriteLockEl.innerHTML, /No write lock held/);
  assert.equal(f.refs.adminStorageWriteLockEl.hidden, false);
});

test("maintenance status reader renders completion from storage health", async () => {
  const f = fixture({
    payload: {
      storage: {
        runtimeRetention: {
          status: "ok",
          running: false,
          phase: "complete",
          elapsedMs: 90_000,
          deletedRunCount: 1,
          deletedRowCount: 2
        },
        compaction: { status: "ok", reclaimedBytes: 500 }
      }
    }
  });

  await f.controller.refreshMaintenanceStatus();

  assert.equal(f.calls.length, 1);
  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Maintenance complete/);
  assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Removed 1 runs \/ 2 rows/);
  assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /500 B reclaimed/);
});

test("maintenance live polling defers while active work holds the gate", () => {
  const f = fixture({ shouldDefer: () => true });
  f.controller.acceptStorageHealth({
    storage: {
      runtimeRetention: {
        status: "running",
        running: true,
        phase: "prune-source-runs",
        elapsedMs: 1000
      }
    }
  });

  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Live updates pause/);
  f.controller.stopMaintenancePoll();
});

test("maintenance reports an active-task hold instead of claiming it started", async () => {
  const f = fixture({
    maintenancePayload: {
      ok: true,
      started: false,
      alreadyRunning: false,
      reason: "held-active-task"
    }
  });

  await f.controller.startMaintenance();

  assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /active task/i);
  assert.equal(f.refs.adminStorageMaintenanceStatusEl.attributes["data-tone"], "warning");
});
