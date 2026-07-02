import test from "node:test";
import assert from "node:assert/strict";

import {
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressCounts,
  formatTaskProgressDetail
} from "../../../frontend/shared/task-progress.js";

test("formatScrapyStaticSourcesTailBadge returns empty when queue item is absent", () => {
  assert.equal(formatScrapyStaticSourcesTailBadge([]), "");
  assert.equal(
    formatScrapyStaticSourcesTailBadge([{ id: "studio_a", status: "running" }]),
    ""
  );
});

test("formatScrapyStaticSourcesTailBadge returns fallback progress for active scrapy queue", () => {
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "running",
        progress: {
          active: true,
          phaseKey: "loading_source",
          phaseLabel: "Processing browser fallback queue",
          counts: {
            completedSources: 19,
            totalSources: 26
          }
        }
      }
    ]),
    "Browser fallback 19/26"
  );
});

test("formatScrapyStaticSourcesTailBadge ignores malformed or inactive queue counts", () => {
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "ok",
        progress: {
          counts: {
            completedSources: 19,
            totalSources: 26
          }
        }
      }
    ]),
    ""
  );
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "running",
        progress: {
          counts: {
            completedSources: "x",
            totalSources: 26
          }
        }
      }
    ]),
    ""
  );
});

test("formatTaskProgressCounts renders pipeline step and output counts", () => {
  assert.equal(
    formatTaskProgressCounts("pipeline", {
      currentStep: 3,
      totalSteps: 7,
      baselineOutputCount: 128,
      finalOutputCount: 256
    }),
    "step 3/7 | output 256 (baseline 128)"
  );
});

test("formatTaskProgressDetail renders fetch preparation counts", () => {
  const label = formatTaskProgressDetail("fetch", {
    active: true,
    phaseKey: "seeding_existing_output",
    phaseLabel: "Seeding existing output",
    counts: {
      seededOutputRows: 47388,
      selectedSourceCount: 333,
      setupElapsedMs: 4500
    }
  });

  assert.equal(
    label,
    "Seeding existing output | seeded 47,388 jobs | selected 333 sources | setup 5s"
  );
  assert.doesNotMatch(label, /0 sources resolved/i);
});

test("formatTaskProgressCounts does not invent zero pipeline counts", () => {
  assert.equal(formatTaskProgressCounts("pipeline", {}), "");
  assert.equal(
    formatTaskProgressCounts("pipeline", {
      currentStep: 3,
      totalSteps: 3,
      baselineOutputCount: 0,
      finalOutputCount: 0
    }),
    "step 3/3"
  );
  assert.equal(
    formatTaskProgressDetail("pipeline", {
      active: true,
      phaseKey: "starting",
      phaseLabel: "Starting pipeline",
      counts: {}
    }),
    "Starting pipeline"
  );
});

test("formatTaskProgressDetail renders sync shard progress", () => {
  assert.equal(
    formatTaskProgressDetail("sync", {
      active: true,
      phaseKey: "remote_write",
      phaseLabel: "Verified shard 25 of 501",
      mode: "determinate",
      ratio: 25 / 501,
      counts: {
        action: "push",
        shardCount: 501,
        changedShardCount: 501,
        completedShardCount: 25,
        verifiedShardCount: 25,
        currentShardIndex: 25,
        currentShardLabel: "active/97",
        manifestCommitted: false,
        gcDeletedCount: 0
      }
    }),
    "Verified shard 25 of 501 (5%) | shards 25/501 | verified 25/501 | current active/97"
  );
  assert.equal(
    formatTaskProgressCounts("sync", {
      shardCount: 501,
      changedShardCount: 501,
      completedShardCount: 501,
      verifiedShardCount: 501,
      manifestCommitted: true,
      gcDeletedCount: 3
    }),
    "shards 501/501 | verified 501/501 | manifest committed | gc deleted 3"
  );
  assert.equal(
    formatTaskProgressDetail("sync", {
      active: true,
      phaseKey: "remote_read",
      phaseLabel: "Read shard 25 of 501",
      mode: "determinate",
      ratio: 25 / 501,
      counts: {
        action: "pull",
        shardCount: 501,
        completedShardCount: 25,
        currentShardIndex: 25,
        currentShardLabel: "active/97",
        shardsReadBytes: 1024,
        totalShardBytes: 2048
      }
    }),
    "Read shard 25 of 501 (5%) | read 25/501 | current active/97"
  );
  assert.equal(
    formatTaskProgressCounts("sync", {
      action: "pull",
      shardCount: 501,
      completedShardCount: 0,
      shardsReadBytes: 0,
      skipped: true,
      skipReason: "remote_manifest_unchanged"
    }),
    "remote manifest unchanged | shards skipped 501"
  );
});

test("formatTaskProgressCounts does not invent zero sync counts", () => {
  assert.equal(formatTaskProgressCounts("sync", {}, null, { action: "pull" }), "");
  assert.equal(
    formatTaskProgressDetail("sync", {
      active: true,
      phaseKey: "startup",
      phaseLabel: "Starting sync",
      counts: {}
    }, { action: "pull" }),
    "Starting sync"
  );
});

test("formatTaskProgressCounts renders GameDevMap dry-run fetch subtask counts", () => {
  assert.equal(
    formatTaskProgressCounts("discovery", {
      stageIndex: 7,
      stageTotal: 11,
      subtaskKey: "gamedevmap_active_audit",
      subtaskLabel: "GameDevMap active audit",
      activeAuditPhase: "recovery_wave1_fetch",
      activeAuditCompletedUrls: 0,
      activeAuditTotalUrls: 7524,
      activeAuditBatch: 2,
      activeAuditPhaseCompleted: 1275,
      activeAuditPhaseTotal: 1277,
      generatedCandidates: 0,
      foundEndpoints: 0,
      survivedDedupeCandidates: 0,
      probedCandidates: 0,
      queuedCandidates: 0,
      deferredCandidates: 0,
      failedProbes: 0
    }),
    "stage 7/11 | GameDevMap active dry run | recovery wave 1 fetch 1,275/1,277 pages | generated 0 | endpoints 0 | survived 0 | probed 0 | queued 0 | deferred 0 | failed 0"
  );
});

test("formatTaskProgressCounts renders GameDevMap active audit subtask counts", () => {
  assert.equal(
    formatTaskProgressCounts("discovery", {
      stageIndex: 7,
      stageTotal: 11,
      subtaskKey: "gamedevmap_active_audit",
      subtaskLabel: "GameDevMap active audit",
      activeAuditPhase: "batch_start",
      activeAuditCompletedUrls: 2000,
      activeAuditTotalUrls: 7524,
      activeAuditBatch: 2,
      generatedCandidates: 0,
      foundEndpoints: 0,
      survivedDedupeCandidates: 0,
      probedCandidates: 0,
      queuedCandidates: 0,
      deferredCandidates: 0,
      failedProbes: 0
    }),
    "stage 7/11 | GameDevMap active audit | batch 2 | 2,000/7,524 URLs | batch start | generated 0 | endpoints 0 | survived 0 | probed 0 | queued 0 | deferred 0 | failed 0"
  );
});
