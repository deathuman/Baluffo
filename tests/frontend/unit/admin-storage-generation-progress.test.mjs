import test from "node:test";
import assert from "node:assert/strict";

import { renderStorageMaintenanceHtml } from "../../../frontend/admin/render/storage-tables.js";

const formatBytes = value => `${value} B`;

test("renders saved-version generation and row progress", () => {
  const html = renderStorageMaintenanceHtml(
    {
      status: "running",
      running: true,
      phase: "prune-job-generations",
      stage: "deleting",
      totalGenerationCount: 4,
      processedGenerationCount: 2,
      totalGenerationRowCount: 100,
      deletedGenerationRowCount: 50,
      generationRowCountKnown: true
    },
    null,
    formatBytes
  );

  assert.match(html, /Saved versions 2 of 4/);
  assert.match(html, /50 of 100 saved-version rows \(50%\)/);
});
