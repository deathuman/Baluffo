import test from "node:test";
import assert from "node:assert/strict";

import { renderStorageMaintenanceHtml } from "../../../frontend/admin/render/storage-tables.js";

const formatBytes = value => `${Math.round(Number(value || 0) / 1_000_000_000)} GB`;

test("renders why a large allocated database is below the compaction ratio", () => {
  const html = renderStorageMaintenanceHtml(
    { status: "not-run", running: false },
    {
      status: "not-run",
      freelistThresholdBytes: 52_428_800,
      space: {
        databaseBytes: 226_491_760_640,
        allocatedBytes: 205_597_204_480,
        freelistBytes: 20_894_556_160,
        observedFreelistRatio: 0.0923,
        effectiveFreelistRatio: 0.35,
        vacuumDueBySize: false,
        vacuumBlockedBy: "freelist-ratio",
        largeAllocatedLowFreelist: true
      }
    },
    formatBytes
  );

  assert.match(html, /Compaction not due/);
  assert.match(html, /data-compaction-suppressed="true"/);
  assert.match(html, /206 GB allocated/);
  assert.match(html, /21 GB free/);
  assert.match(html, /9\.2% freelist/);
  assert.match(html, /35\.0% large-database threshold/);
});

test("renders nothing when the compaction size gate passes", () => {
  const html = renderStorageMaintenanceHtml(
    { status: "not-run", running: false },
    {
      status: "not-run",
      space: {
        allocatedBytes: 1_300,
        freelistBytes: 700,
        observedFreelistRatio: 0.35,
        effectiveFreelistRatio: 0.35,
        vacuumDueBySize: true,
        vacuumBlockedBy: "",
        largeAllocatedLowFreelist: false
      }
    },
    formatBytes
  );

  assert.equal(html, "");
});
