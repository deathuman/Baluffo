import test from "node:test";
import assert from "node:assert/strict";

import { createAdminFetcherLogController } from "../../../frontend/admin/app/fetcher/logs.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createOpsState(extra = {}) {
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

function createOpsController({ state, getBridge } = {}) {
  return createAdminOpsController({
    state,
    refs: createOpsRefs(),
    getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 500,
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

async function flushBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

test("admin degraded active skips heavy summaries while allowing KPI hydration", async () => {
  const state = createOpsState({
    latestOpsHealthCache: { ok: true, summaryView: true, kpis: { pendingApprovalsCount: 3 } },
    latestOpsTaskStatePayload: {
      tasks: [
        { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true },
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
  const controller = createOpsController({
    state,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") throw new Error("Bridge error (HTTP 504)");
      if (path === "/ops/task-state?view=summary") throw new Error("Bridge error (HTTP 504)");
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, summaryView: true, kpis: {} };
      throw new Error(`unexpected heavy path ${path}`);
    }
  });

  await controller.loadOpsHealthData({ summary: true });
  await flushBackground();

  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(calls.includes("/ops/task-state?view=summary"));
  assert.equal(calls.includes("/ops/fetch-kpis?view=summary"), true);
  assert.equal(calls.includes("/ops/dashboard-health?view=summary"), false);
  assert.equal(calls.includes("/registry/conflicts?view=summary"), false);
  assert.equal(calls.includes("/admin/ops-tab-counts?view=summary"), false);
  assert.ok(state.pipelineStatusPollTimer);
  controller.stopOpsHealthPolling();
});

test("admin summary route timeouts are backoff skipped on the next poll", async () => {
  const state = createOpsState({
    latestOpsHealthCache: { ok: true, summaryView: true, kpis: { pendingApprovalsCount: 4 }, schedule: {} }
  });
  const calls = [];
  const controller = createOpsController({
    state,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/ops/dashboard-health?view=summary") throw new Error("Bridge error (HTTP 504)");
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, summaryView: true, kpis: {} };
      if (path === "/registry/conflicts?view=summary") throw new Error("Bridge error (HTTP 504)");
      if (path === "/admin/ops-tab-counts?view=summary") throw new Error("Bridge error (HTTP 504)");
      throw new Error(`unexpected path ${path}`);
    }
  });

  await controller.loadOpsHealthData({ summary: true });
  await flushBackground();
  await controller.loadOpsHealthData({ summary: true });
  await flushBackground();

  assert.equal(calls.filter(path => path === "/ops/dashboard-health?view=summary").length, 1);
  assert.equal(calls.filter(path => path === "/registry/conflicts?view=summary").length, 1);
  assert.equal(calls.filter(path => path === "/admin/ops-tab-counts?view=summary").length, 1);
  controller.stopOpsHealthPolling();
});

test("fetcher log offset polling backs off after timeouts and preserves visible text", async () => {
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = (callback, delay = 0) => {
    scheduled.push({ callback, delay: Number(delay) || 0 });
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  try {
    const state = {
      fetcherLogRemoteOffset: 360908,
      fetcherLogPollTimer: null,
      fetcherLiveProgressState: {},
      adminBusyState: { fetcherWatch: true }
    };
    const refs = {
      adminFetcherLogEl: createElement({ textContent: "Fetching source 42" }),
      adminFetcherProgressEl: createElement({ style: {} }),
      adminFetcherProgressBarEl: createElement({ style: {} }),
      adminFetcherProgressLabelEl: createElement({ textContent: "10/551 sources resolved" })
    };
    const calls = [];
    const controller = createAdminFetcherLogController({
      state,
      refs,
      getBridge: async (path, options) => {
        calls.push({ path: String(path), timeoutMs: Number(options?.timeoutMs || 0) });
        throw new Error("Bridge error (HTTP 504)");
      },
      createLogEvent: (_scope, message, level) => ({ message, level }),
      appendLogRow(container, event) {
        container.textContent = `${container.textContent || ""}${event.message || ""}`;
      },
      setFetcherProgress() {}
    });

    controller.scheduleFetcherLogPoll(500);
    await scheduled.shift().callback();
    assert.equal(scheduled.at(-1).delay, 500);
    await scheduled.at(-1).callback();

    assert.equal(calls.length, 2);
    assert.deepEqual(calls.map(call => call.timeoutMs), [3500, 3500]);
    assert.deepEqual(
      calls.map(call => call.path),
      [
        "/fetcher/log?view=tail&limitChars=8192",
        "/fetcher/log?view=tail&limitChars=8192"
      ]
    );
    assert.equal(scheduled.at(-1).delay, 1000);
    assert.equal(refs.adminFetcherLogEl.textContent, "Fetching source 42");
    assert.equal(refs.adminFetcherProgressLabelEl.textContent, "10/551 sources resolved");
    controller.stopFetcherLogPolling();
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});
