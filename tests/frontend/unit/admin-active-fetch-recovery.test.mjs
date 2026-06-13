import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import {
  createClassList,
  createElement,
  createRegistryControllerFixture,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushMicrotasks(count = 10) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function createOpsRefs() {
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

function createOpsController({ state, refs = createOpsRefs(), getBridge, onActivePipelineIdle, renderAdminOpsKpisImpl } = {}) {
  return createAdminOpsController({
    state,
    refs,
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
    renderAdminOpsKpis: renderAdminOpsKpisImpl || (() => {}),
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
    onActivePipelineIdle,
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });
}

function activeOpsState(extra = {}) {
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

test("admin active fetch renders loaded KPI values instead of delayed copy", async () => {
  const state = activeOpsState();
  const refs = createOpsRefs();
  const calls = [];
  const controller = createOpsController({
    state,
    refs,
    renderAdminOpsKpisImpl: renderAdminOpsKpis,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") return { active: true, runId: "pipeline_live_1" };
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            { taskType: "fetch", type: "fetch", runId: "fetch_live_1", parentTaskType: "pipeline", active: true },
            { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
          ],
          count: 2,
          summary: true
        };
      }
      if (path === "/ops/fetch-kpis?view=summary") {
        return {
          ok: true,
          summaryView: true,
          status: "warning",
          alertsEvaluated: true,
          alerts: [{ id: "fetch_never_run", severity: "warning", message: "No successful fetch has run yet." }],
          kpis: { sevenDayFetchSuccessRate: 0, avgFetchDurationMs7d: 5220280, pendingApprovalsCount: 813 }
        };
      }
      throw new Error(`unexpected path ${path}`);
    }
  });

  controller.applyBootstrapPayload({
    app: { version: "0.2.67" },
    tasks: {
      current: [
        { taskType: "fetch", type: "fetch", runId: "fetch_live_1", parentTaskType: "pipeline", active: true },
        { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
      ],
      recent: []
    },
    registrySummary: {},
    schedule: {}
  });
  await flushMicrotasks();

  assert.ok(calls.includes("/ops/task-state?view=summary"));
  assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
  assert.match(refs.adminOpsKpisEl.innerHTML, /No successful fetch yet/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /0\.0%/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /87\.0m/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /813/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /Not available/);
  assert.doesNotMatch(refs.adminOpsKpisEl.innerHTML, /Delayed while job update is running\./);
  assert.doesNotMatch(refs.adminOpsKpisEl.innerHTML, /Loading latest fetch KPI/);
  controller.stopOpsHealthPolling();
});

test("admin active poll keeps refreshing compact state when richer rows are preserved", async () => {
  const timers = stubScheduledTimers();
  try {
    const state = activeOpsState({
      latestOpsTaskStatePayload: {
        tasks: [
          { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true, taskProgress: { active: true, ratio: 0.1 } },
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
    let taskRatio = 0.1;
    const controller = createOpsController({
      state,
      getBridge: async path => {
        calls.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_live_1",
            stage: "fetch",
            activeChildren: [{ taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true }]
          };
        }
        if (path === "/ops/task-state?view=summary") {
          taskRatio += 0.1;
          return {
            tasks: [
              { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true, taskProgress: { active: true, ratio: taskRatio } },
              { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
            ],
            count: 2,
            summary: true
          };
        }
        if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: { pendingApprovalsCount: 813 }, summaryView: true };
        throw new Error(`unexpected path ${path}`);
      }
    });

    await controller.loadPipelineStatusFallbackData();
    assert.ok(state.pipelineStatusPollTimer);
    assert.equal(calls.filter(path => path === "/ops/task-state?view=summary").length, 0);

    timers.scheduled.shift()();
    await flushMicrotasks();

    assert.equal(calls.filter(path => path === "/tasks/run-jobs-pipeline-status").length, 2);
    assert.equal(calls.filter(path => path === "/ops/task-state?view=summary").length, 1);
    assert.equal(calls.filter(path => path === "/ops/fetch-kpis?view=summary").length, 1);
    assert.equal(state.latestOpsTaskStatePayload.tasks[0].taskProgress.ratio, 0.2);
    assert.ok(state.pipelineStatusPollTimer);
    controller.stopOpsHealthPolling();
  } finally {
    timers.restore();
  }
});

test("admin active poll notifies source tables when pipeline transitions idle", async () => {
  const timers = stubScheduledTimers();
  try {
    const state = activeOpsState();
    const idleNotifications = [];
    const calls = [];
    const controller = createOpsController({
      state,
      onActivePipelineIdle: payload => idleNotifications.push(payload),
      getBridge: async path => {
        calls.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
        if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: { pendingApprovalsCount: 813 }, summaryView: true };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
        if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
        throw new Error(`unexpected path ${path}`);
      }
    });

    controller.applyBootstrapPayload({
      app: { version: "0.2.67" },
      tasks: {
        current: [
          { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true },
          { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
        ],
        recent: []
      },
      registrySummary: {},
      schedule: {}
    });
    await flushMicrotasks();
    timers.scheduled.shift()();
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(idleNotifications.length, 1);
    assert.equal(idleNotifications[0].reason, "active_pipeline_idle");
    assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
    assert.ok(calls.includes("/ops/task-state?view=summary"));
    assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
    controller.stopOpsHealthPolling();
  } finally {
    timers.restore();
  }
});

test("admin source tables preserve rendered rows while active refresh is delayed", async () => {
  const fixture = createRegistryControllerFixture({
    state: { adminBusyState: { discoveryLoad: false, livePipelineRunning: true, liveFetchRunning: true } },
    options: { getBridge: async path => { throw new Error(`unexpected path ${path}`); } }
  });
  fixture.refs.adminPendingSourcesEl.innerHTML = '<table><tbody><tr><td>Existing Pending Studio</td></tr></tbody></table>';
  fixture.refs.adminActiveSourcesEl.innerHTML = '<table><tbody><tr><td>Existing Active Studio</td></tr></tbody></table>';
  fixture.refs.adminRejectedSourcesEl.innerHTML = '<div class="muted">Loading rejected sources...</div>';
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: false });

  assert.equal(result?.skipped, true);
  assert.equal(result?.reason, "pipeline_running");
  assert.equal(fixture.state.sourceTablesDelayedDuringActiveRun, true);
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Existing Pending Studio/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Existing Active Studio/);
  assert.match(fixture.refs.adminRejectedSourcesEl.innerHTML, /Source tables delayed while job update is running/);
});

test("admin source tables refresh after active pipeline becomes idle", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      sourceTablesDelayedDuringActiveRun: true,
      adminBusyState: { discoveryLoad: false, livePipelineRunning: true, liveFetchRunning: true }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/registry/summary") return { ok: true, summary: { pendingCount: 1 } };
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            sources: {
              pending: [{ name: "Studio Pending", sourceId: "p1", url: "https://pending.example" }],
              active: [{ name: "Studio Active", sourceId: "a1", url: "https://active.example" }],
              rejected: []
            },
            summary: { pendingCount: 1, activeCount: 1, rejectedCount: 0 }
          };
        }
        throw new Error(`unexpected path ${path}`);
      },
      fetchJobsFetchReportJson: async () => ({ sources: [] })
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.refreshSourceTablesAfterActiveRunIdle();
  fixture.renderScheduler.flush();

  assert.equal(result?.partialLoadFailed, false);
  assert.equal(fixture.state.sourceTablesDelayedDuringActiveRun, false);
  assert.equal(fixture.state.adminBusyState.livePipelineRunning, false);
  assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(calls.some(path => path.startsWith("/registry/sources")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Studio Pending/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Studio Active/);
});
