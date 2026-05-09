import test from "node:test";
import assert from "node:assert/strict";

import {
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressCounts
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

test("formatTaskProgressCounts renders GameDevMap active audit subtask counts", () => {
  assert.equal(
    formatTaskProgressCounts("discovery", {
      stageIndex: 7,
      stageTotal: 11,
      subtaskKey: "gamedevmap_active_audit",
      subtaskLabel: "GameDevMap active audit",
      activeAuditPhase: "recovery_wave1_fetch",
      activeAuditCompletedUrls: 2000,
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
    "stage 7/11 | GameDevMap active audit | batch 2 | 2.000/7.524 URLs | recovery wave1 fetch 1.275/1.277 | generated 0 | endpoints 0 | survived 0 | probed 0 | queued 0 | deferred 0 | failed 0"
  );
});
