import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import {
  createClassList,
  createElement,
  createRegistryControllerFixture
} from "./helpers/admin-controller-test-helpers.mjs";

function createOpsState() {
  return {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
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

function createOpsController(overrides = {}) {
  const state = overrides.state || createOpsState();
  const refs = overrides.refs || createOpsRefs();
  return createAdminOpsController({
    state,
    refs,
    getBridge: overrides.getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
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
    idlePollIntervalMs: 1000,
    ...overrides,
    state,
    refs
  });
}

test("admin ops controller replaces stale pipeline child rows from fresher pipeline status", async () => {
  const state = createOpsState();
  const refs = createOpsRefs();
  const renderedCurrentRows = [];
  const controller = createOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path !== "/tasks/run-jobs-pipeline-status") throw new Error(`unexpected path ${path}`);
      return {
        active: true,
        runId: "pipeline_live_1",
        startedAt: "2026-06-06T09:00:00.000Z",
        stage: "fetch",
        activeChildren: [{
          taskType: "fetch",
          type: "fetch",
          runId: "fetch_live_1",
          active: true,
          startedAt: "2026-06-06T09:03:00.000Z",
          status: "running",
          taskProgress: { active: true, phaseLabel: "Fetching job listings" }
        }]
      };
    },
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentRows.push(runModel.currentRows);
    }
  });

  controller.applyBootstrapPayload({
    tasks: {
      current: [
        {
          taskType: "discovery",
          type: "discovery",
          runId: "discovery_live_1",
          parentTaskType: "pipeline",
          parentRunId: "pipeline_live_1",
          active: true,
          startedAt: "2026-06-06T09:00:01.000Z"
        },
        { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
      ],
      recent: []
    },
    registrySummary: {},
    schedule: {},
    app: { version: "0.2.66" }
  });
  assert.deepEqual(renderedCurrentRows.at(-1)?.map(row => row.taskType), ["discovery", "pipeline"]);

  await controller.loadPipelineStatusFallbackData();

  assert.deepEqual(renderedCurrentRows.at(-1)?.map(row => row.runId), ["fetch_live_1", "pipeline_live_1"]);
  assert.deepEqual(state.latestOpsTaskStatePayload.tasks.map(row => row.taskType), ["fetch", "pipeline"]);
  controller.stopOpsHealthPolling();
});

test("admin ops KPI cards show active-run delayed copy while pipeline is active", () => {
  const state = createOpsState();
  const refs = createOpsRefs();
  const controller = createOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return { active: true, runId: "pipeline_live_1" };
      throw new Error(`unexpected path ${path}`);
    },
    renderAdminOpsKpis
  });

  controller.applyBootstrapPayload({
    app: { version: "0.2.66" },
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

  assert.match(refs.adminOpsKpisEl.innerHTML, /Delayed while job update is running\./);
  assert.doesNotMatch(refs.adminOpsKpisEl.innerHTML, /Loading latest fetch KPI/);
  controller.stopOpsHealthPolling();
});

test("admin registry controller delays source tables while pipeline fetch is active", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: { adminBusyState: { discoveryLoad: false, livePipelineRunning: true, liveFetchRunning: true } },
    options: {
      getBridge: async path => {
        calls.push(path);
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  fixture.refs.adminPendingSourcesEl.innerHTML = '<div class="muted">Loading pending sources...</div>';
  fixture.refs.adminActiveSourcesEl.innerHTML = '<div class="muted">Loading active sources...</div>';
  fixture.refs.adminRejectedSourcesEl.innerHTML = '<div class="muted">Loading rejected sources...</div>';
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: true });

  assert.equal(result?.skipped, true);
  assert.equal(result?.reason, "pipeline_running");
  assert.deepEqual(calls, []);
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminRejectedSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.doesNotMatch(fixture.refs.adminPendingSourcesEl.innerHTML, /Loading pending sources/);
});
