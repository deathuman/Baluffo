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

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

test("admin ops controller replaces stale pipeline child rows from fresher pipeline status", async () => {
  const state = createOpsState({
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: true,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: true
    }
  });
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

test("admin ops active pipeline lazily requests fetch KPI summary", async () => {
  const state = createOpsState();
  const refs = createOpsRefs();
  const calls = [];
  const controller = createOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") {
        return { active: true, runId: "pipeline_live_1", stage: "fetch" };
      }
      if (path === "/ops/dashboard-health?view=summary") {
        return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    },
    renderAdminOpsKpis(_el, kpis, status, options) {
      renderAdminOpsKpis(refs.adminOpsKpisEl, kpis, status, options);
    },
    renderScheduler: callback => {
      callback();
      return () => {};
    }
  });

  await controller.loadOpsHealthData({ summary: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.equal(calls.includes("/ops/fetch-kpis?view=summary"), true);
  assert.doesNotMatch(refs.adminOpsKpisEl.innerHTML, /Loading latest fetch KPI/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /Updating while job is running\./);
});

test("admin registry controller delays source tables while pipeline fetch is active", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: { adminBusyState: { discoveryLoad: false, livePipelineRunning: true, liveFetchRunning: true } },
    options: {
      getBridge: async (path, requestOptions = {}) => {
        calls.push({ path: String(path), requestOptions });
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            activeCompact: true,
            sources: { pending: [], active: [], rejected: [] },
            summary: {}
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  fixture.refs.adminPendingSourcesEl.innerHTML = '<div class="muted">Loading pending sources...</div>';
  fixture.refs.adminActiveSourcesEl.innerHTML = '<div class="muted">Loading active sources...</div>';
  fixture.refs.adminRejectedSourcesEl.innerHTML = '<div class="muted">Loading rejected sources...</div>';
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: true });
  fixture.renderScheduler.flush();

  assert.notEqual(result?.skipped, true);
  assert.notEqual(result?.sourceTablesDelayed, true);
  assert.ok(calls.some(call => call.path.startsWith("/registry/sources") && call.path.includes("activeCompact=1")));
  assert.ok(!calls.some(call => call.path === "/discovery/report"));
  assert.ok(!calls.some(call => call.path === "/discovery/candidates"));
  assert.equal(fixture.state.sourceTablesLoadState, "loaded");
});

test("admin registry controller preflights pipeline status before source tables", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      latestOpsTaskStatePayload: {
        tasks: [],
        count: 0,
        summary: true,
        sentinel: "keep"
      },
      adminBusyState: {
        discoveryLoad: false,
        livePipelineRunning: false,
        liveFetchRunning: false
      }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_live_1",
            stage: "fetch",
            activeChildren: [
              { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true }
            ]
          };
        }
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            activeCompact: true,
            sources: { pending: [], active: [], rejected: [] },
            summary: {}
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  fixture.refs.adminPendingSourcesEl.innerHTML = '<div class="muted">Loading pending sources...</div>';
  fixture.refs.adminActiveSourcesEl.innerHTML = '<div class="muted">Loading active sources...</div>';
  fixture.refs.adminRejectedSourcesEl.innerHTML = '<div class="muted">Loading rejected sources...</div>';
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: true });
  fixture.renderScheduler.flush();

  assert.notEqual(result?.skipped, true);
  assert.notEqual(result?.sourceTablesDelayed, true);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(calls.some(path => path.startsWith("/registry/sources") && path.includes("activeCompact=1")));
  assert.ok(!calls.includes("/discovery/report"));
  assert.ok(!calls.includes("/discovery/candidates"));
  assert.equal(fixture.state.adminBusyState.livePipelineRunning, true);
  assert.equal(fixture.state.adminBusyState.liveFetchRunning, true);
  assert.equal(fixture.state.latestOpsTaskStatePayload.sentinel, "keep");
  assert.equal(fixture.state.latestOpsTaskStatePayload.tasks.length, 0);
});

test("admin registry controller uses active-safe compact source calls during active pipeline", async () => {
  const calls = [];
  let fixture;
  fixture = createRegistryControllerFixture({
    state: { adminBusyState: { discoveryLoad: false, livePipelineRunning: true, liveFetchRunning: true } },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            activeCompact: true,
            sources: {
              pending: [{ id: "pending_1", name: "Pending Studio" }],
              active: [{ id: "active_1", name: "Active Studio" }],
              rejected: []
            },
            summary: {}
          };
        }
        throw new Error(`unexpected path ${path}`);
      },
      fetchJobsFetchReportJson: async () => ({ sources: [] })
    }
  });
  const logs = [];
  fixture.options.appendDiscoveryLog = message => {
    logs.push(String(message));
  };
  fixture.refs.adminPendingSourcesEl.innerHTML = '<div class="muted">Loading pending sources...</div>';
  fixture.refs.adminActiveSourcesEl.innerHTML = '<div class="muted">Loading active sources...</div>';
  fixture.refs.adminRejectedSourcesEl.innerHTML = '<div class="muted">Loading rejected sources...</div>';
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: true });
  fixture.renderScheduler.flush();

  assert.equal(result.skipped, undefined);
  assert.equal(result.sourceTablesDelayed, undefined);
  assert.ok(calls.some(path => path.includes("/registry/sources?") && path.includes("activeCompact=1")));
  assert.doesNotMatch(logs.join("\n"), /Could not load Admin registry source tables/);
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Pending Studio/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Active Studio/);
});
