import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  renderStorageMaintenanceHtml,
  renderStorageMaintenancePreviewHtml
} from "../../../frontend/admin/render/storage-tables.js";

test("renders stopping and stopped maintenance states", () => {
  const stopping = renderStorageMaintenanceHtml(
    {
      status: "cancelling",
      running: true,
      phase: "cancelling",
      elapsedMs: 2_000
    },
    null,
    value => `${value} B`
  );
  const stopped = renderStorageMaintenanceHtml(
    {
      status: "cancelled",
      running: false,
      phase: "cancelled",
      deletedRunCount: 2
    },
    null,
    value => `${value} B`
  );

  assert.match(stopping, /Maintenance stopping/);
  assert.match(stopping, /Stopping after the current database step/);
  assert.match(stopped, /Maintenance stopped/);
  assert.match(stopped, /Already removed data stays removed/);
  assert.match(stopped, /data-maintenance-running="false"/);
});

test("renders compaction as explicitly non-interruptible", () => {
  const compactionOnly = renderStorageMaintenanceHtml(
    null,
    { status: "running", running: true },
    value => `${value} B`
  );
  const stoppedWithCompaction = renderStorageMaintenanceHtml(
    { status: "cancelled", running: false, phase: "cancelled" },
    { status: "running", running: true },
    value => `${value} B`
  );
  const idleRetentionWithCompaction = renderStorageMaintenanceHtml(
    { status: "not-run", running: false, phase: "not-run" },
    { status: "running", running: true },
    value => `${value} B`
  );

  assert.match(compactionOnly, /Compaction running/);
  assert.match(compactionOnly, /Cannot be safely interrupted/);
  assert.match(compactionOnly, /data-maintenance-running="true"/);
  assert.match(stoppedWithCompaction, /Feed cleanup stopped/);
  assert.match(stoppedWithCompaction, /cannot be safely interrupted/);
  assert.doesNotMatch(stoppedWithCompaction, /Maintenance stopped/);
  assert.match(idleRetentionWithCompaction, /Compacting database/);
  assert.doesNotMatch(idleRetentionWithCompaction, /Not started/);
});

test("renders an explicit in-progress state while the preview waits", () => {
  const html = renderStorageMaintenancePreviewHtml({ status: "loading" });

  assert.match(html, /data-maintenance-preview="loading"/);
  assert.match(html, /aria-busy="true"/);
  assert.match(html, /Preparing cleanup preview/);
  assert.match(html, /bounded payload samples/);
  assert.match(html, /Nothing is deleted/);
});

test("labels sample-only legacy counts as lower bounds instead of zero rows", () => {
  const html = renderStorageMaintenancePreviewHtml(
    {
      status: "ok",
      currentGeneration: "gen-current",
      currentFeedProtected: true,
      countsKnown: false,
      candidateGenerationCount: 1,
      candidateRowCount: 4,
      estimatedBytes: 1024,
      freeDiskBytes: 10_000_000,
      freelistBytes: 0,
      candidates: [
        { generation: "gen-legacy", jobRowCount: 4, sourceRowCount: 0, estimatedBytes: 1024, countsKnown: false }
      ]
    },
    value => `${value} B`
  );

  assert.match(html, /At least 4 jobs/);
  assert.match(html, /≥ 4<\/strong> rows/);
  assert.match(html, /≥1024 B/);
  assert.doesNotMatch(html, /0 jobs/);
});

test("renders read-only cleanup candidates and current-feed protection", () => {
  const html = renderStorageMaintenancePreviewHtml(
    {
      status: "ok",
      generatedAt: "2026-11-13T10:00:00Z",
      currentGeneration: "gen-current",
      currentFeedProtected: true,
      candidateGenerationCount: 2,
      candidateRowCount: 12,
      estimatedBytes: 2048,
      freeDiskBytes: 10_000_000,
      freelistBytes: 4096,
      countsKnown: true,
      capReached: false,
      candidates: [
        { generation: "gen-old", jobRowCount: 3, sourceRowCount: 9, rowCount: 12, estimatedBytes: 2048, sampled: true }
      ]
    },
    value => `${value} B`
  );

  assert.match(html, /Cleanup preview/);
  assert.match(html, /Read-only/);
  assert.match(html, /Current feed/);
  assert.match(html, /gen-current/);
  assert.match(html, /gen-old/);
  assert.match(html, /3 jobs \+ 9 source rows/);
  assert.match(html, /2048 B/);
  assert.match(html, /free disk/);
});

test("Admin shell exposes a hidden maintenance stop control", () => {
  const adminHtml = readFileSync(new URL("../../../admin.html", import.meta.url), "utf8");

  assert.match(adminHtml, /data-ui="admin-storage-maintenance-cancel-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-maintenance-preview-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-maintenance-preview"/);
  assert.match(adminHtml, /Stop maintenance/);
  assert.match(adminHtml, /Preview cleanup/);
});
