import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

function reviewElement() {
  return { dataset: {}, innerHTML: "", querySelectorAll: () => [] };
}

function payload(jobEvidence) {
  return {
    summary: { conflictCount: 0 },
    triage: { summary: { totalConflictCount: 0, bucketCounts: {} }, buckets: [] },
    review: {
      summary: { totalConflictCount: 0, priorityCounts: {}, queueCounts: {} },
      queues: []
    },
    conflicts: [],
    jobEvidence
  };
}

test("registry conflicts renderer reports SQLite job evidence", () => {
  const reviewEl = reviewElement();
  renderAdminRegistryConflicts(reviewEl, payload({
    source: "sqlite_source_index",
    rowCount: 5,
    truncated: false
  }));

  assert.match(reviewEl.innerHTML, /Job evidence/);
  assert.match(reviewEl.innerHTML, /SQLite source index · 5 source rows matched/);
});

test("registry conflicts renderer reports discarded JSON prefix evidence", () => {
  const reviewEl = reviewElement();
  renderAdminRegistryConflicts(reviewEl, payload({
    source: "jobs_feed_prefix",
    rowCount: 0,
    truncated: true
  }));

  assert.match(reviewEl.innerHTML, /JSON feed prefix · 0 feed rows read/);
  assert.match(reviewEl.innerHTML, /prefix truncated, partial evidence discarded/);
});

test("registry conflicts renderer flags a stale SQLite generation", () => {
  const reviewEl = reviewElement();
  renderAdminRegistryConflicts(reviewEl, payload({
    source: "sqlite_source_index",
    rowCount: 5,
    truncated: false,
    feedPublishedAt: "2026-05-12T10:00:00+00:00",
    stale: true
  }));

  assert.match(reviewEl.innerHTML, /feed older than source state/);
});
