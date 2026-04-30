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
      invalidRowsCount: 0
    }
  }, "healthy");

  assert.match(el.innerHTML, /Registry &amp; Sync/i);
  assert.match(el.innerHTML, /Active Sources/i);
  assert.match(el.innerHTML, /Pending Review/i);
  assert.match(el.innerHTML, /rejected local-only 3/i);
  assert.match(el.innerHTML, /tombstones local-only 4/i);
  assert.match(el.innerHTML, /pull 1/i);
});
