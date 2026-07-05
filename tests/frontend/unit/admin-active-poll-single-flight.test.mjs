import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createElement,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushMicrotasks(count = 10) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function createState(extra = {}) {
  return {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    },
    ...extra
  };
}

function createRefs() {
  return {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
}

function createController({ state, getBridge }) {
  return createAdminOpsController({
    state,
    refs: createRefs(),
    getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 1000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    renderAdminRegistryConflicts() {},
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
    idlePollIntervalMs: 1000
  });
}

test("admin active recovery collapses concurrent status and task-state requests", async () => {
  const state = createState({
    latestOpsTaskStatePayload: {
      tasks: [
        { taskType: "fetch", type: "fetch", runId: "fetch_live_1", parentTaskType: "pipeline", active: true },
        { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
      ],
      count: 2,
      summary: true
    },
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: true,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: true
    }
  });
  const calls = [];
  const controller = createController({
    state,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") {
        await Promise.resolve();
        return {
          active: true,
          runId: "pipeline_live_1",
          stage: "fetch",
          activeChildren: [{ taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true }]
        };
      }
      if (path === "/ops/task-state?view=summary") {
        await Promise.resolve();
        return {
          tasks: [
            { taskType: "fetch", type: "fetch", runId: "fetch_live_1", parentTaskType: "pipeline", active: true },
            { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
          ],
          count: 2,
          summary: true
        };
      }
      throw new Error(`unexpected path ${path}`);
    }
  });

  await Promise.all([
    controller.loadActiveOpsSummaryData({ fromPoll: true }),
    controller.loadActiveOpsSummaryData({ fromPoll: true }),
    controller.loadPipelineStatusFallbackData(undefined, { fromPoll: true })
  ]);
  controller.stopOpsHealthPolling();

  assert.equal(calls.filter(path => path === "/tasks/run-jobs-pipeline-status").length, 1);
  assert.equal(calls.filter(path => path === "/ops/task-state?view=summary").length, 1);
  assert.equal(calls.some(path => path === "/ops/storage-health"), false);
  assert.equal(calls.some(path => path === "/ops/fetch-report"), false);
});

test("admin abort idle recovery does not restart an active polling storm from stale evidence", async () => {
  const timers = stubScheduledTimers();
  try {
    const state = createState({
      opsActiveAdminWorkLastActive: true,
      opsActivePipelineOrFetchLastActive: true,
      latestOpsTaskStatePayload: {
        tasks: [
          { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true, status: "aborting" }
        ],
        count: 1,
        summary: true
      },
      adminBusyState: {
        opsLoad: false,
        liveFetchRunning: false,
        liveDiscoveryRunning: false,
        liveSyncRunning: false,
        livePipelineRunning: true
      }
    });
    const calls = [];
    const controller = createController({
      state,
      getBridge: async path => {
        calls.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "canceled", activeChildren: [] };
        if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
        if (path === "/ops/history?limit=2") return { runs: [], count: 0, summaryView: true };
        if (path === "/tasks/jobs-pipeline-schedule") return { pipeline: { enabled: true, intervalHours: 24, nextRunAt: "2026-07-05T12:00:00Z" } };
        throw new Error(`unexpected path ${path}`);
      }
    });

    await Promise.all([
      controller.loadActiveOpsSummaryData({ fromPoll: true }),
      controller.loadActiveOpsSummaryData({ fromPoll: true })
    ]);
    await flushMicrotasks();
    controller.stopOpsHealthPolling();

    assert.ok(calls.filter(path => path === "/tasks/run-jobs-pipeline-status").length <= 2);
    assert.ok(calls.filter(path => path === "/ops/task-state?view=summary").length <= 2);
    assert.ok(calls.filter(path => path === "/ops/dashboard-health?view=summary").length <= 1);
    assert.equal(timers.scheduled.length <= 2, true);
  } finally {
    timers.restore();
  }
});
