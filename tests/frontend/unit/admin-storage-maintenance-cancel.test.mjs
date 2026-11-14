import test from "node:test";
import assert from "node:assert/strict";

import { createStorageBreakdownController } from "../../../frontend/admin/app/storage-breakdown.js";
import { createElement, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

function fixture({ cancelPayload = null, previewPayload = null } = {}) {
  const postCalls = [];
  const bridgeCalls = [];
  const listeners = {};
  const refs = {
    adminStorageMaintenancePreviewBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.preview = handler;
      }
    }),
    adminStorageMaintenancePreviewEl: createElement({ hidden: true }),
    adminStorageMaintenanceBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.start = handler;
      }
    }),
    adminStorageMaintenanceCancelBtnEl: createElement({
      hidden: true,
      addEventListener(type, handler) {
        listeners.cancel = handler;
      }
    }),
    adminStorageMaintenanceStatusEl: createElement(),
    adminStorageMaintenanceProgressEl: createElement({ hidden: true }),
    adminStorageWriteLockEl: createElement(),
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement()
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async (path, options) => {
      bridgeCalls.push({ path, options });
      return path.endsWith("/preview")
        ? previewPayload || { ok: false, error: "no preview fixture" }
        : { storage: {} };
    },
    postBridge: async (path, payload, options) => {
      postCalls.push({ path, payload, options });
      if (path.endsWith("/cancel")) {
        return cancelPayload || {
          cancelling: true,
          runtimeRetention: {
            status: "cancelling",
            running: true,
            phase: "cancelling",
            runId: "retention_test",
            cancelRequested: true
          }
        };
      }
      return {
        started: true,
        runtimeRetention: {
          status: "running",
          running: true,
          phase: "prune-source-runs",
          runId: "retention_test",
          elapsedMs: 1_000
        }
      };
    },
    formatBytes: value => `${value} B`,
    shouldDefer: () => false
  });
  return { controller, refs, listeners, postCalls, bridgeCalls };
}

test("maintenance preview renders candidates without starting cleanup", async () => {
  const f = fixture({
    previewPayload: {
      ok: true,
      maintenancePreview: {
        status: "ok",
        generatedAt: "2026-11-13T10:00:00Z",
        currentGeneration: "gen-current",
        currentFeedProtected: true,
        candidateGenerationCount: 1,
        candidateRowCount: 4,
        estimatedBytes: 4096,
        freeDiskBytes: 10_000_000,
        freelistBytes: 1024,
        countsKnown: true,
        candidates: [
          { generation: "gen-old", jobRowCount: 1, sourceRowCount: 3, estimatedBytes: 4096 }
        ]
      }
    }
  });

  await f.controller.previewMaintenance();

  assert.equal(f.postCalls.length, 0);
  assert.equal(f.refs.adminStorageMaintenancePreviewEl.hidden, false);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /Cleanup preview/);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /gen-current/);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /gen-old/);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /free disk/);
});

test("maintenance preview waits with its bounded timeout and restores the control", async () => {
  const previewPayload = {
    ok: true,
    maintenancePreview: {
      status: "ok",
      currentGeneration: "gen-current",
      currentFeedProtected: true,
      candidateGenerationCount: 0,
      candidateRowCount: 0,
      estimatedBytes: 0,
      freeDiskBytes: 10_000_000,
      freelistBytes: 1024,
      countsKnown: true,
      candidates: []
    }
  };
  let resolvePreview;
  const pendingPreview = new Promise(resolve => {
    resolvePreview = resolve;
  });
  const f = fixture({ previewPayload: pendingPreview });

  const run = f.controller.previewMaintenance();

  assert.equal(f.bridgeCalls.at(-1).path, "/ops/storage-maintenance/preview");
  assert.equal(f.bridgeCalls.at(-1).options.timeoutMs, 45 * 1000);
  assert.equal(f.refs.adminStorageMaintenancePreviewBtnEl.disabled, true);
  assert.equal(f.refs.adminStorageMaintenancePreviewBtnEl.attributes["aria-busy"], "true");
  assert.equal(f.refs.adminStorageMaintenancePreviewEl.hidden, false);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /Preparing cleanup preview/);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /bounded payload samples/);

  resolvePreview(previewPayload);
  await run;

  assert.equal(f.refs.adminStorageMaintenancePreviewBtnEl.disabled, false);
  assert.equal(f.refs.adminStorageMaintenancePreviewBtnEl.attributes["aria-busy"], undefined);
  assert.match(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /gen-current/);
});

test("a pending preview cannot overwrite the panel after cleanup starts", async () => {
  const timers = stubScheduledTimers();
  let resolvePreview;
  const pendingPreview = new Promise(resolve => {
    resolvePreview = resolve;
  });
  const f = fixture({ previewPayload: pendingPreview });
  try {
    const run = f.controller.previewMaintenance();
    await f.controller.startMaintenance();
    assert.equal(f.refs.adminStorageMaintenancePreviewEl.hidden, true);

    resolvePreview({
      ok: true,
      maintenancePreview: {
        status: "ok",
        currentGeneration: "gen-current",
        currentFeedProtected: true,
        candidates: [{ generation: "gen-old", jobRowCount: 1, sourceRowCount: 0 }]
      }
    });
    await run;

    assert.equal(f.refs.adminStorageMaintenancePreviewEl.hidden, true);
    assert.doesNotMatch(f.refs.adminStorageMaintenancePreviewEl.innerHTML, /gen-old/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("maintenance stop control appears only while maintenance is running", async () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, true);

    await f.controller.startMaintenance();

    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, false);
    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.disabled, false);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("maintenance stop posts the cancel route and disables the control", async () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    await f.controller.startMaintenance();

    const result = await f.controller.cancelMaintenance();

    assert.equal(result.cancelling, true);
    assert.equal(f.postCalls.at(-1).path, "/ops/storage-maintenance/cancel");
    assert.deepEqual(f.postCalls.at(-1).payload, { runId: "retention_test" });
    assert.deepEqual(f.postCalls.at(-1).options.allowStatuses, [409]);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /stopping/i);
    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.disabled, true);
    assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Maintenance stopping/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("maintenance stop reports a stale run without pretending it stopped", async () => {
  const timers = stubScheduledTimers();
  const f = fixture({
    cancelPayload: {
      stale: true,
      runtimeRetention: {
        status: "running",
        running: true,
        phase: "archive-payloads",
        runId: "retention_newer"
      }
    }
  });
  try {
    f.controller.init();
    await f.controller.startMaintenance();

    const result = await f.controller.cancelMaintenance();

    assert.equal(result.stale, true);
    assert.deepEqual(f.postCalls.at(-1).payload, { runId: "retention_test" });
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /stale/i);
    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, false);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("maintenance stop hides after the cancelled status arrives", async () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    await f.controller.startMaintenance();
    await f.controller.cancelMaintenance();

    f.controller.acceptStorageHealth({
      storage: {
        runtimeRetention: {
          status: "cancelled",
          running: false,
          phase: "cancelled",
          deletedRunCount: 2
        }
      }
    });

    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, true);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /stopped/i);
    assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Maintenance stopped/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("maintenance stop names feed cleanup while compaction is running", () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    f.controller.acceptStorageHealth({
      storage: {
        runtimeRetention: {
          status: "running",
          running: true,
          phase: "prune-source-runs"
        },
        compaction: { status: "running", running: true }
      }
    });

    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, false);
    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.textContent, "Stop feed cleanup");
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /compaction/i);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /cannot be safely interrupted/i);
    assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Compaction is running/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("compaction-only status explains that nothing can be stopped", () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    f.controller.acceptStorageHealth({
      storage: { compaction: { status: "running", running: true } }
    });

    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, true);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Compaction is running/);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /cannot be safely interrupted/);
    assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Cannot be safely interrupted/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});

test("cancelled feed cleanup does not claim compaction stopped", async () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    await f.controller.startMaintenance();
    f.controller.acceptStorageHealth({
      storage: {
        runtimeRetention: {
          status: "cancelled",
          running: false,
          phase: "cancelled"
        },
        compaction: { status: "running", running: true }
      }
    });

    assert.equal(f.refs.adminStorageMaintenanceCancelBtnEl.hidden, true);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /Feed cleanup stopped/);
    assert.match(f.refs.adminStorageMaintenanceStatusEl.textContent, /compaction is still running/i);
    assert.match(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Feed cleanup stopped/);
    assert.doesNotMatch(f.refs.adminStorageMaintenanceProgressEl.innerHTML, /Maintenance stopped/);
  } finally {
    f.controller.stopMaintenancePoll();
    timers.restore();
  }
});
