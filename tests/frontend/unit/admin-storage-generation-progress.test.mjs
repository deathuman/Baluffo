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

test("renders a pass with a scheduled follow-up as not fully complete", () => {
  const html = renderStorageMaintenanceHtml(
    {
      status: "ok",
      running: false,
      phase: "complete",
      done: false,
      timerScheduled: true,
      nextRunInS: 30,
      followUpScheduled: true,
      followUpInS: 30,
      completionState: "follow-up-scheduled",
      deletedGenerationCount: 2,
      processedGenerationCount: 2,
      totalGenerationCount: 4,
      deletedGenerationRowCount: 289370,
      totalGenerationRowCount: 578740,
      generationRowCountKnown: true
    },
    null,
    formatBytes
  );

  assert.match(html, /Maintenance batch complete/);
  assert.match(html, /Follow-up scheduled/);
  assert.match(html, /Saved versions 2 of 4/);
  assert.match(html, /data-maintenance-follow-up="true"/);
  assert.doesNotMatch(html, /<strong>Maintenance complete<\/strong>/);
});

test("renders a drained pass with a periodic timer as complete", () => {
  const html = renderStorageMaintenanceHtml(
    {
      status: "ok",
      running: false,
      phase: "complete",
      done: true,
      timerScheduled: true,
      nextRunInS: 21600,
      followUpScheduled: false,
      completionState: "complete",
      fullyComplete: true
    },
    null,
    formatBytes
  );

  assert.match(html, /Maintenance complete/);
  assert.doesNotMatch(html, /Follow-up scheduled/);
  assert.match(html, /data-maintenance-follow-up="false"/);
});
