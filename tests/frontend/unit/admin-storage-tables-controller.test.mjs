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

const DIAGNOSTIC = {
  status: "complete",
  runId: "storage_diag_abc123",
  databaseBytes: 10_000,
  walBytes: 0,
  capturedAt: "2026-11-11T10:00:00.000Z",
  durationMs: 12,
  tableSizes: {
    status: "ok",
    method: "dbstat",
    allocatedBytes: 8_000,
    freelistBytes: 0,
    tables: [
      {
        name: "job_sources",
        tableBytes: 5_000,
        indexBytes: 1_000,
        bytes: 6_000,
        share: 0.75
      }
    ],
    otherBytes: 1_000
  },
  jobSources: {
    rowCount: 2,
    payloadBytes: 400,
    contentBytes: 500,
    generationCount: 1,
    tableBytes: 6_000,
    byGeneration: [
      {
        generation: "gen-current",
        rowCount: 2,
        payloadBytes: 400,
        contentBytes: 500,
        isCurrent: true,
        lastUpdatedAt: "2026-11-11T09:00:00.000Z"
      }
    ]
  }
};

const QUICK_DIAGNOSTIC = {
  mode: "quick",
  status: "complete",
  runId: "storage_diag_quick123",
  databaseBytes: 10_000,
  walBytes: 0,
  pageSize: 4096,
  pageCount: 2,
  freelistBytes: 100,
  capturedAt: "2026-11-11T10:00:00.000Z",
  durationMs: 8,
  feedState: {
    currentGeneration: "gen-current",
    currentRowCount: 2,
    currentSourceCount: 3,
    currentPublishedAt: "2026-11-11T09:00:00.000Z"
  },
  rowBytesProbe: {
    status: "ok",
    method: "bounded-rowid-sample",
    currentBytes: 400,
    oldBytes: 1_600,
    totalBytes: 2_000,
    currentShare: 0.2,
    oldShare: 0.8,
    classification: "old-generations",
    summary: "Most sampled row bytes are in older generations (20.0% current).",
    tables: [
      {
        table: "job_sources",
        currentBytes: 300,
        oldBytes: 1_200,
        totalBytes: 1_500,
        currentRows: 2,
        currentShare: 0.2,
        status: "ok"
      }
    ],
    warnings: []
  },
  jobSourcesByGeneration: {
    listedRowCount: 2,
    generationCount: 1,
    truncated: false,
    scope: "current",
    byGeneration: [
      { generation: "gen-current", rowCount: 2, isCurrent: true, isOrphan: false }
    ]
  },
  jobsByGeneration: {
    listedRowCount: 3,
    generationCount: 2,
    truncated: false,
    byGeneration: [
      { generation: "gen-current", rowCount: 2, isCurrent: true, isOrphan: false },
      { generation: "gen-old", rowCount: 1, isCurrent: false, isOrphan: true }
    ]
  }
};

function fixture({
  payload = { storage: { tableSizes: TABLE_SIZES } },
  reject = null,
  postPayload = { started: true, storageDiagnostic: DIAGNOSTIC },
  postReject = null,
  shouldDefer = () => false,
  downloadJson = null
} = {}) {
  const calls = [];
  const postCalls = [];
  const downloads = [];
  const listeners = {};
  const refs = {
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement(),
    adminStorageTablesRefreshBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.refresh = handler;
      }
    }),
    adminStorageQuickCheckBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.quickCheck = handler;
      }
    }),
    adminStorageDiagnosticBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.diagnostic = handler;
      }
    }),
    adminStorageDiagnosticCancelBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.cancel = handler;
      }
    }),
    adminStorageDiagnosticExportBtnEl: createElement({
      hidden: true,
      addEventListener(type, handler) {
        listeners.export = handler;
      }
    }),
    adminStorageDiagnosticStatusEl: createElement(),
    adminStorageDiagnosticBodyEl: createElement()
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async (path, options) => {
      calls.push({ path, options });
      if (reject) throw reject;
      return payload;
    },
    postBridge: async (path, payloadBody, options) => {
      postCalls.push({ path, payload: payloadBody, options });
      if (postReject) throw postReject;
      return postPayload;
    },
    downloadJson: downloadJson || ((filename, content) => {
      downloads.push({ filename, content });
    }),
    formatBytes: value => `${value} B`,
    renderStorageTablesHtml,
    renderStorageTablesMessageHtml,
    shouldDefer
  });
  return { controller, refs, calls, postCalls, downloads, listeners };
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

  await f.listeners.refresh();

  assert.equal(f.calls.length, 1);
});

test("exact diagnostic starts in the background and renders its result", async () => {
  const f = fixture();
  f.controller.init();

  await f.controller.startExactDiagnostic();

  assert.equal(f.postCalls.length, 1);
  assert.equal(f.postCalls[0].path, "/ops/storage-diagnostic");
  assert.equal(f.postCalls[0].options.affectsReachability, false);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /job_sources by feed generation/);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /gen-current/);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /dbstat exact/);
  assert.equal(f.refs.adminStorageDiagnosticStatusEl.textContent, "Complete");
  assert.equal(f.refs.adminStorageDiagnosticExportBtnEl.hidden, false);
});

test("quick check starts with quick mode and renders generation counts", async () => {
  const f = fixture({
    postPayload: { started: true, storageDiagnostic: QUICK_DIAGNOSTIC }
  });
  f.controller.init();

  await f.controller.startQuickCheck();

  assert.equal(f.postCalls.length, 1);
  assert.equal(f.postCalls[0].path, "/ops/storage-diagnostic");
  assert.deepEqual(f.postCalls[0].payload, { mode: "quick" });
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /Quick check reads current feed counts/i);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /Estimated row bytes/);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /gen-old/);
  assert.match(f.refs.adminStorageDiagnosticBodyEl.innerHTML, /jobs by feed generation/i);
  assert.equal(f.refs.adminStorageDiagnosticStatusEl.textContent, "Quick check complete");
  assert.equal(f.refs.adminStorageDiagnosticExportBtnEl.hidden, false);
});

test("quick check reports partial when the generation count times out", async () => {
  const f = fixture({
    postPayload: {
      started: true,
      storageDiagnostic: {
        ...QUICK_DIAGNOSTIC,
        partial: true,
        exactContent: false,
        jobsByGeneration: {
          ...QUICK_DIAGNOSTIC.jobsByGeneration,
          status: "partial",
          scope: "current"
        }
      }
    }
  });
  f.controller.init();

  await f.controller.startQuickCheck();

  assert.equal(f.refs.adminStorageDiagnosticStatusEl.textContent, "Quick check partial");
  assert.equal(f.refs.adminStorageDiagnosticStatusEl.attributes["data-tone"], "warning");
});

test("exact diagnostic exports table and generation totals as JSON", async () => {
  const f = fixture();
  f.controller.init();

  await f.controller.startExactDiagnostic();
  f.listeners.export();

  assert.equal(f.downloads.length, 1);
  assert.equal(
    f.downloads[0].filename,
    "baluffo-storage-diagnostic-2026-11-11T10-00-00-000Z.json"
  );
  const payload = JSON.parse(f.downloads[0].content);
  assert.equal(payload.kind, "baluffo.storage-diagnostic");
  assert.equal(payload.schemaVersion, 1);
  assert.ok(payload.exportedAt);
  assert.equal(payload.storageDiagnostic.tableSizes.tables[0].name, "job_sources");
  assert.equal(payload.storageDiagnostic.tableSizes.tables[0].bytes, 6_000);
  assert.equal(payload.storageDiagnostic.jobSources.byGeneration[0].generation, "gen-current");
  assert.equal(payload.storageDiagnostic.jobSources.byGeneration[0].contentBytes, 500);
});

test("exact diagnostic export stays unavailable before completion", () => {
  const f = fixture();
  f.controller.init();

  assert.equal(f.refs.adminStorageDiagnosticExportBtnEl.hidden, true);
  f.listeners.export();

  assert.equal(f.downloads.length, 0);
});

test("exact diagnostic cancel posts to the cancel route", async () => {
  const f = fixture({
    postPayload: {
      cancelling: true,
      storageDiagnostic: { ...DIAGNOSTIC, status: "cancelling" }
    }
  });

  await f.controller.cancelExactDiagnostic();

  assert.equal(f.postCalls.length, 1);
  assert.equal(f.postCalls[0].path, "/ops/storage-diagnostic/cancel");
  assert.equal(f.refs.adminStorageDiagnosticStatusEl.textContent, "Cancelling…");
  assert.equal(f.refs.adminStorageDiagnosticExportBtnEl.hidden, true);
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
