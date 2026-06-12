import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsKpis } from "../../../frontend/admin/render/ops-summary.js";

function makeEl() {
  return { innerHTML: "", dataset: {} };
}

test("admin render: ops KPI panel renders registry sync confidence", () => {
  const el = makeEl();
  renderAdminOpsKpis(el, {
    lastSuccessfulFetchAge: "1h",
    registrySync: {
      activeCount: 12,
      pendingCount: 5,
      hiddenPendingCount: 2,
      deferredPendingCount: 1,
      ignoredRejectedCount: 3,
      ignoredTombstonedCount: 4,
      lastSyncAt: "2026-04-30T10:00:00Z",
      lastSyncStatus: "ok",
      pulledCount: 1,
      pushedCount: 0,
      conflictCount: 0,
      invalidRowsCount: 0,
      summaryExact: false,
      countBasis: "storage"
    }
  }, "healthy");

  assert.match(el.innerHTML, /admin-ops-registry-sync-details/i);
  assert.doesNotMatch(el.innerHTML, /<details[^>]*admin-ops-registry-sync-details[^>]*open/i);
  assert.match(el.innerHTML, /Registry &amp; Sync/i);
  assert.match(el.innerHTML, /storage snapshot counts/i);
  assert.match(el.innerHTML, /Active Sources/i);
  assert.match(el.innerHTML, /Pending Review/i);
  assert.match(el.innerHTML, /rejected local-only 3/i);
  assert.match(el.innerHTML, /tombstones local-only 4/i);
  assert.match(el.innerHTML, /pull 1/i);
});

test("admin render: ops KPI panel renders provider coverage confidence", () => {
  const el = makeEl();
  renderAdminOpsKpis(el, {
    lastSuccessfulFetchAge: "1h",
    providerCoverage: {
      statusCounts: {
        validated_provider: 1,
        probing: 1,
        failed_provider: 1,
        unstable_provider: 1
      },
      readyLaterProviders: [{ name: "Studio Greenhouse" }]
    }
  }, "healthy");

  assert.match(el.innerHTML, /admin-ops-registry-sync-details/i);
  assert.doesNotMatch(el.innerHTML, /<details[^>]*admin-ops-registry-sync-details[^>]*open/i);
  assert.match(el.innerHTML, /Provider coverage/i);
  assert.match(el.innerHTML, /validated 1/i);
  assert.match(el.innerHTML, /probing 1/i);
  assert.match(el.innerHTML, /failed\/unstable 2/i);
  assert.match(el.innerHTML, /Static sources are retained/i);
});

test("admin render: registry sync details omit unrelated pending diagnostics", () => {
  const el = makeEl();
  renderAdminOpsKpis(el, {
    lastSuccessfulFetchAge: "1h",
    registrySync: {
      activeCount: 12,
      pendingCount: 5,
      hiddenPendingCount: 0,
      deferredPendingCount: 0,
      ignoredRejectedCount: 0,
      ignoredTombstonedCount: 0,
      lastSyncAt: "",
      lastSyncStatus: "never",
      pulledCount: 0,
      pushedCount: 0,
      conflictCount: 0,
      invalidRowsCount: 0,
      summaryExact: false,
      countBasis: "storage"
    }
  }, "healthy");

  assert.doesNotMatch(el.innerHTML, /Provider coverage/i);
  assert.doesNotMatch(el.innerHTML, /Dedup review-state/i);
});
