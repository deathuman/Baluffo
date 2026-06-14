import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { deriveAdminRunsModel } from "../../../frontend/admin/domain/runs.js";
import {
  createDeferredRenderScheduler,
  createElement,
  stubDateNow,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushBackground() {
  await Promise.resolve();
  await Promise.resolve();
}

function syncTaskState(tasks) {
  return { tasks, count: tasks.length, summary: true };
}

function syncRow() {
  return {
    taskType: "sync",
    type: "sync",
    runId: "sync_live_1",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    status: "running",
    summary: { action: "pull" }
  };
}

function createRefs() {
  return {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
}

test("admin active polling keeps sync rows updating until task-state clears", async () => {
  const timers = stubScheduledTimers();
  let nowMs = Date.parse("2026-03-08T10:02:00.000Z");
  const clock = stubDateNow(() => nowMs);
  try {
    const state = {
      latestOpsHealthCache: { ok: true, summaryView: true, kpis: {}, schedule: {} },
      latestOpsTaskStatePayload: syncTaskState([syncRow()]),
      adminBusyState: {
        opsLoad: false,
        liveFetchRunning: false,
        liveDiscoveryRunning: false,
        liveSyncRunning: true,
        livePipelineRunning: false
      }
    };
    const taskStatePayloads = [
      syncTaskState([syncRow()]),
      syncTaskState([syncRow()]),
      syncTaskState([])
    ];
    const calls = [];
    const renderedCurrentRows = [];
    const renderScheduler = createDeferredRenderScheduler();
    const controller = createAdminOpsController({
      state,
      refs: createRefs(),
      getBridge: async path => {
        calls.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") return taskStatePayloads.shift() || syncTaskState([]);
        if (path === "/ops/fetch-kpis?view=summary") return { ok: true, summaryView: true, kpis: {} };
        throw new Error(`unexpected heavy path ${path}`);
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel,
      getOpsPollIntervalMs: () => 5000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsFetcherMetrics() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory(_el, runModel) {
        renderedCurrentRows.push(
          runModel.currentRows.map(row => ({ runId: row.runId, elapsedMs: row.elapsedMs }))
        );
      },
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      adminDispatch: { dispatch() {} },
      adminActions: { OPS_REFRESHED: "ops/refreshed" },
      escapeHtml: value => String(value || ""),
      onBridgeStatusChange() {},
      bridgeStatusPollIntervalMs: 1000,
      idlePollIntervalMs: 1000,
      renderScheduler: renderScheduler.schedule
    });

    await controller.loadActiveOpsSummaryData({ fromPoll: false });
    await flushBackground();
    renderScheduler.flush();
    nowMs += 5000;
    await controller.loadActiveOpsSummaryData({ fromPoll: true });
    await flushBackground();
    renderScheduler.flush();
    nowMs += 5000;
    await controller.loadActiveOpsSummaryData({ fromPoll: true });
    await flushBackground();
    renderScheduler.flush();
    controller.stopOpsHealthPolling();

    const nonEmptyRows = renderedCurrentRows.filter(rows => rows.length > 0);
    assert.ok(nonEmptyRows.length >= 2);
    assert.equal(nonEmptyRows[0][0].runId, "sync_live_1");
    assert.ok(nonEmptyRows.at(-1)[0].elapsedMs > nonEmptyRows[0][0].elapsedMs);
    assert.deepEqual(renderedCurrentRows.at(-1), []);
    assert.equal(state.adminBusyState.liveSyncRunning, false);
    assert.equal(state.opsActiveAdminWorkLastActive, false);
    assert.equal(calls.includes("/ops/dashboard-health?view=summary"), false);
    assert.equal(calls.includes("/registry/conflicts?view=summary"), false);
    assert.equal(calls.includes("/admin/ops-tab-counts?view=summary"), false);
    assert.ok(calls.filter(path => path === "/ops/task-state?view=summary").length >= 3);
  } finally {
    clock.restore();
    timers.restore();
  }
});
