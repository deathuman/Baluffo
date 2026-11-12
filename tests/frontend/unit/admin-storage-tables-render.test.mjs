import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  renderStorageMaintenanceHtml,
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml,
  renderStorageWriteLockHtml
} from "../../../frontend/admin/render/storage-tables.js";
import { renderStorageDiagnosticHtml } from "../../../frontend/admin/render/storage-diagnostic.js";

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
  assert.match(html, /Sampled estimate/);
  assert.match(html, /rowid-sample/);
  assert.match(html, /Time budget/);
  assert.match(html, /Technical details/);
  assert.match(html, /data-table-name="source_runs"/);
});

test("renders exact dbstat sizes without estimate tildes", () => {
  const html = renderStorageTablesHtml(payload({
    estimated: false,
    method: "dbstat",
    tables: [{ name: "source_runs", bytes: 6_000, share: 0.75, rowCount: null }]
  }), formatBytes);

  assert.match(html, /Exact allocation/);
  assert.doesNotMatch(html, /~1[.,]200/);
  assert.match(html, /—/);
});

test("renders partial and unavailable states", () => {
  const partial = renderStorageTablesHtml(payload({ status: "partial", complete: false }), formatBytes);
  assert.match(partial, /ran out of time/);

  const unavailable = renderStorageTablesHtml(
    { status: "unavailable", error: "no dbstat <unsafe>" },
    formatBytes
  );
  assert.match(unavailable, /Table sizes unavailable/);
  assert.match(unavailable, /&lt;unsafe&gt;/);
  assert.doesNotMatch(unavailable, /<unsafe>/);
});

test("partial table samples lead with exact allocation and do not claim a full share", () => {
  const html = renderStorageTablesHtml(payload({
    status: "partial",
    complete: false,
    databaseBytes: 210_900_000_000,
    allocatedBytes: 203_900_000_000,
    freelistBytes: 7_000_000_000,
    tables: [{ name: "job_feed_state", bytes: 352, share: 0, rowCount: 1 }],
    otherBytes: 203_899_999_648
  }), formatBytes);

  assert.match(html, /Allocated space/);
  assert.match(html, /Unattributed allocated space/);
  assert.match(html, /Database size, allocated space, and free space are exact/);
  assert.match(html, /data-table-share=""/);
  assert.doesNotMatch(html, /100\.0%/);
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

test("renders exact allocation and job source generations", () => {
  const html = renderStorageDiagnosticHtml({
    status: "complete",
    databaseBytes: 10_000,
    walBytes: 100,
    capturedAt: "2026-11-11T10:00:00Z",
    durationMs: 42,
    tableSizes: {
      status: "ok",
      method: "dbstat",
      allocatedBytes: 8_000,
      freelistBytes: 100,
      tables: [
        {
          name: "job_sources",
          tableBytes: 5_000,
          indexBytes: 1_000,
          bytes: 6_000,
          share: 0.75
        }
      ],
      otherBytes: 2_000
    },
    jobSources: {
      rowCount: 2,
      generationCount: 1,
      payloadBytes: 400,
      contentBytes: 500,
      tableBytes: 6_000,
      byGeneration: [
        {
          generation: "gen-current",
          rowCount: 2,
          payloadBytes: 400,
          contentBytes: 500,
          isCurrent: true,
          lastUpdatedAt: "2026-11-11T09:00:00Z"
        }
      ]
    }
  }, formatBytes);

  assert.match(html, /dbstat exact/);
  assert.match(html, /job_sources/);
  assert.match(html, /gen-current/);
  assert.match(html, /current/);
  assert.match(html, /job_sources by feed generation/);
});

test("renders unavailable allocation without hiding exact job source totals", () => {
  const html = renderStorageDiagnosticHtml({
    status: "complete",
    exactAllocation: false,
    warnings: ["SQLite dbstat is unavailable; table allocation could not be measured exactly."],
    tableSizes: {
      status: "unavailable",
      error: "This SQLite build does not include dbstat; job_sources generation totals are exact."
    },
    jobSources: {
      rowCount: 2,
      generationCount: 1,
      payloadBytes: 400,
      contentBytes: 500,
      tableBytes: null,
      byGeneration: [{ generation: "gen-current", rowCount: 2, payloadBytes: 400, contentBytes: 500 }]
    }
  }, formatBytes);

  assert.match(html, /does not include dbstat/);
  assert.match(html, /gen-current/);
  assert.match(html, /not samples/);
});

test("renders quick generation counts without table allocation", () => {
  const html = renderStorageDiagnosticHtml({
    mode: "quick",
    status: "complete",
    databaseBytes: 10_000,
    walBytes: 100,
    pageSize: 4096,
    pageCount: 2,
    freelistBytes: 100,
    capturedAt: "2026-11-11T10:00:00Z",
    durationMs: 8,
    feedState: {
      currentGeneration: "gen-current",
      currentRowCount: 2,
      currentSourceCount: 3
    },
    rowBytesProbe: {
      status: "ok",
      method: "bounded-rowid-sample",
      currentComplete: true,
      totalComplete: true,
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
        },
        {
          table: "jobs",
          currentBytes: 100,
          oldBytes: 400,
          totalBytes: 500,
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
      scope: "current",
      byGeneration: [
        { generation: "gen-current", rowCount: 2, isCurrent: true, isOrphan: false }
      ]
    },
    jobsByGeneration: {
      listedRowCount: 2,
      generationCount: 1,
      scope: "current",
      byGeneration: [
        { generation: "gen-current", rowCount: 2, isCurrent: true, isOrphan: false }
      ]
    }
  }, formatBytes);

  assert.match(html, /Current feed/);
  assert.match(html, /Space used by saved rows/);
  assert.match(html, /Current stored rows/);
  assert.match(html, /gen-current/);
  assert.match(html, /data-generation-id="gen-current"/);
  assert.match(html, /bounded-rowid-sample/);
  assert.match(html, /Saved source rows by feed version/);
  assert.match(html, /Jobs by feed version/);
  assert.match(html, /Current · 20\.0%/);
  assert.doesNotMatch(html, /dbstat exact/);
});

test("partial quick checks do not claim a current share when older rows are unmeasured", () => {
  const html = renderStorageDiagnosticHtml({
    mode: "quick",
    status: "complete",
    partial: true,
    exactContent: true,
    databaseBytes: 210_900_000_000,
    walBytes: 100,
    pageSize: 4096,
    pageCount: 2,
    freelistBytes: 7_000_000_000,
    capturedAt: "2026-11-12T12:56:21Z",
    durationMs: 6_085,
    feedState: {
      currentGeneration: "gen-current",
      currentRowCount: 39_518,
      currentSourceCount: 159_198,
      currentPublishedAt: "2026-11-11T01:36:37Z"
    },
    rowBytesProbe: {
      status: "unavailable",
      method: "bounded-rowid-sample",
      currentComplete: false,
      totalComplete: false,
      currentBytes: 0,
      oldBytes: 0,
      totalBytes: 0,
      tables: [
        {
          table: "job_sources",
          currentBytes: 90_424_464,
          currentRows: 159_198,
          oldBytes: null,
          totalBytes: null,
          status: "partial"
        },
        {
          table: "jobs",
          currentBytes: null,
          currentRows: 39_518,
          oldBytes: null,
          totalBytes: null,
          status: "partial"
        }
      ],
      warnings: ["job_sources row-byte sample exceeded the 3s table budget."]
    },
    jobSourcesByGeneration: {
      listedRowCount: 159_198,
      generationCount: 1,
      byGeneration: [{ generation: "gen-current", rowCount: 159_198, isCurrent: true }]
    },
    jobsByGeneration: {
      listedRowCount: 39_518,
      generationCount: 1,
      byGeneration: [{ generation: "gen-current", rowCount: 39_518, isCurrent: true }]
    }
  }, formatBytes);

  assert.match(html, /Current stored rows/);
  assert.match(html, /measured tables only/);
  assert.match(html, /Older saved versions/);
  assert.match(html, /older rows not measured/);
  assert.match(html, /current-versus-older share is not available/);
  assert.doesNotMatch(html, /admin-storage-space-bar/);
  assert.doesNotMatch(html, /100\.0%/);
});
test("renders maintenance phase, elapsed time, counters, and compaction", () => {
  const html = renderStorageMaintenanceHtml(
    {
      status: "running",
      running: true,
      phase: "prune-source-runs",
      stage: "deleting",
      elapsedMs: 65_000,
      processedRunCount: 2,
      totalRunCount: 10,
      deletedRunCount: 2,
      deletedRowCount: 20
    },
    { status: "ok", reclaimedBytes: 500 },
    formatBytes
  );

  assert.match(html, /Maintenance running/);
  assert.match(html, /Removing old fetch history/);
  assert.match(html, /Elapsed 1m 5s/);
  assert.match(html, /2 of 10 runs/);
  assert.match(html, /2 runs \/ 20 rows/);
  assert.match(html, /1 KB reclaimed/);
  assert.match(html, /data-maintenance-phase="prune-source-runs"/);
});

test("renders the write-lock holder, operation, and held duration", () => {
  const html = renderStorageWriteLockHtml({
    held: true,
    threadName: "baluffo-sqlite-runtime-retention",
    operation: "prune-job-generations",
    heldMs: 65_000
  });

  assert.match(html, /Write lock/);
  assert.match(html, /Held by baluffo-sqlite-runtime-retention/);
  assert.match(html, /prune-job-generations/);
  assert.match(html, /for 1m 5s/);
});

test("renders a clear fallback when no write lock is held", () => {
  const html = renderStorageWriteLockHtml({ held: false });

  assert.match(html, /No write lock held/);
  assert.match(html, /Database writes can proceed/);
  assert.match(renderStorageWriteLockHtml(null), /Status unavailable/);
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
  assert.match(adminHtml, /data-ui="admin-storage-quick-check-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-maintenance-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-maintenance-status"/);
  assert.match(adminHtml, /data-ui="admin-storage-maintenance-progress"/);
  assert.match(adminHtml, /data-ui="admin-storage-write-lock"/);
  assert.match(adminHtml, /Database storage/);
  assert.match(adminHtml, /Full storage audit/);
  assert.match(adminHtml, /admin-storage-advanced/);
  assert.match(adminHtml, /admin-storage-check-title/);
  assert.match(adminHtml, /data-ui="admin-storage-tables-body"/);
  assert.match(adminHtml, /data-ui="admin-storage-diagnostic-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-diagnostic-cancel-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-diagnostic-export-btn"/);
  assert.match(adminHtml, /data-ui="admin-storage-diagnostic-body"/);
});
