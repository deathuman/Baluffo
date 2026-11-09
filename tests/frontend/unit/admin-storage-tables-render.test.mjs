import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml
} from "../../../frontend/admin/render/storage-tables.js";

const formatBytes = value => `${Math.round(Number(value || 0) / 1000)} KB`;

function payload(overrides = {}) {
  return {
    status: "ok",
    method: "rowid-sample",
    estimated: true,
    complete: true,
    capturedAt: "2026-11-09T12:00:00.000Z",
    durationMs: 18,
    budgetMs: 750,
    databaseBytes: 10_000,
    allocatedBytes: 8_000,
    freelistBytes: 2_000,
    tables: [
      { name: "source_runs", bytes: 6_000, share: 0.75, rowCount: 1_200, sampledRows: 16 },
      { name: "task_events", bytes: 1_000, share: 0.125, rowCount: 40, sampledRows: 16 }
    ],
    otherBytes: 1_000,
    truncated: false,
    ...overrides
  };
}

test("renders estimated table sizes with share, row estimates, and pooled bytes", () => {
  const html = renderStorageTablesHtml(payload(), formatBytes);

  assert.match(html, /source_runs/);
  assert.match(html, /task_events/);
  assert.match(html, /6 KB/);
  assert.match(html, /75\.0%/);
  assert.match(html, /~1[.,]200/);
  assert.match(html, /estimated from rowid samples/);
  assert.match(html, /Indexes, schema, and untracked pages/);
  assert.match(html, /750 ms budget/);
});

test("renders exact dbstat sizes without estimate tildes", () => {
  const html = renderStorageTablesHtml(payload({
    estimated: false,
    method: "dbstat",
    tables: [{ name: "source_runs", bytes: 6_000, share: 0.75, rowCount: null }]
  }), formatBytes);

  assert.match(html, /exact dbstat measurement/);
  assert.doesNotMatch(html, /~1[.,]200/);
  assert.match(html, /—/);
});

test("renders partial and unavailable states", () => {
  const partial = renderStorageTablesHtml(payload({ status: "partial", complete: false }), formatBytes);
  assert.match(partial, /time budget expired/);

  const unavailable = renderStorageTablesHtml(
    { status: "unavailable", error: "no dbstat <unsafe>" },
    formatBytes
  );
  assert.match(unavailable, /Table sizes unavailable/);
  assert.match(unavailable, /&lt;unsafe&gt;/);
  assert.doesNotMatch(unavailable, /<unsafe>/);
});

test("escapes table names", () => {
  const html = renderStorageTablesHtml(payload({
    tables: [{ name: "<script>alert(1)</script>", bytes: 1, share: 0.1, rowCount: 1 }]
  }), formatBytes);

  assert.doesNotMatch(html, /<script>alert/i);
  assert.match(html, /&lt;script&gt;/);
});

test("renders a message when the payload is absent", () => {
  assert.match(renderStorageTablesMessageHtml("nothing yet"), /nothing yet/);
  assert.match(renderStorageTablesHtml(null, formatBytes), /did not include/);
});

test("admin shell places the runtime storage panel beside the profiles overview", () => {
  const adminHtml = readFileSync(new URL("../../../admin.html", import.meta.url), "utf8");
  const usersIndex = adminHtml.indexOf('data-ui="admin-users-list"');
  const storageIndex = adminHtml.indexOf('data-ui="admin-storage-tables"');
  const opsIndex = adminHtml.indexOf('id="admin-ops-section"');

  assert.ok(usersIndex >= 0);
  assert.ok(storageIndex > usersIndex);
  assert.ok(opsIndex > storageIndex);
  assert.match(adminHtml, /data-ui="admin-storage-tables-refresh-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-tables-body"/);
});
