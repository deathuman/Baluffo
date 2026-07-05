import test from "node:test";
import assert from "node:assert/strict";

import {
  activeSummaryIndicatesAdminWork,
  deriveAdminActiveWorkContext,
  hasActiveAdminTaskRows,
  pipelineStatusIndicatesActive,
  pipelineStatusIndicatesFetch
} from "../../../frontend/admin/app/active-work-policy.js";

test("admin active-work policy classifies fetch as source-table-delayed and mutation-blocking", () => {
  const context = deriveAdminActiveWorkContext({
    busyState: {
      liveFetchRunning: true,
      livePipelineRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false
    }
  });

  assert.equal(context.isActive, true);
  assert.equal(context.fetchActive, true);
  assert.equal(context.sourceTablesCanLoadCompact, false);
  assert.equal(context.sourceMutationsAllowed, false);
  assert.equal(context.reason, "fetch_running");
});

test("admin active-work policy keeps sync stricter than fetch", () => {
  const context = deriveAdminActiveWorkContext({
    taskStatePayload: {
      tasks: [{ taskType: "sync", active: true, status: "running" }]
    }
  });

  assert.equal(context.isActive, true);
  assert.equal(context.syncActive, true);
  assert.equal(context.sourceTablesCanLoadCompact, false);
  assert.equal(context.sourceMutationsAllowed, false);
  assert.equal(context.reason, "sync_running");
});

test("admin active-work policy ignores terminal active rows", () => {
  const taskStatePayload = {
    tasks: [
      { taskType: "fetch", active: true, status: "canceled" },
      { taskType: "discovery", active: true, finishedAt: "2026-06-15T10:00:00Z" }
    ]
  };

  assert.equal(hasActiveAdminTaskRows(taskStatePayload), false);
  assert.equal(activeSummaryIndicatesAdminWork({ taskStatePayload }), false);
  assert.equal(deriveAdminActiveWorkContext({ taskStatePayload }).isActive, false);
});

test("admin active-work policy accepts active pipeline status children", () => {
  const pipelineStatusPayload = {
    active: false,
    stage: "fetch",
    activeChildren: [{ taskType: "fetch", active: true, status: "running" }]
  };
  const context = deriveAdminActiveWorkContext({ pipelineStatusPayload });

  assert.equal(pipelineStatusIndicatesActive(pipelineStatusPayload), true);
  assert.equal(context.pipelineActive, true);
  assert.equal(context.fetchActive, true);
  assert.equal(context.reason, "fetch_running");
});

test("admin active-work policy ignores inactive pipeline status with stale stage", () => {
  const pipelineStatusPayload = {
    active: false,
    stage: "fetch",
    activeChildren: []
  };

  assert.equal(pipelineStatusIndicatesActive(pipelineStatusPayload), false);
  assert.equal(deriveAdminActiveWorkContext({ pipelineStatusPayload }).isActive, false);
});

test("admin active-work policy reads pipeline progress phase as stage", () => {
  const pipelineStatusPayload = {
    active: true,
    status: "running",
    progress: { phaseKey: "fetch" }
  };
  const context = deriveAdminActiveWorkContext({ pipelineStatusPayload });

  assert.equal(pipelineStatusIndicatesFetch(pipelineStatusPayload), true);
  assert.equal(context.fetchActive, true);
  assert.equal(context.sourceTablesCanLoadCompact, false);
  assert.equal(context.reason, "fetch_running");
});
