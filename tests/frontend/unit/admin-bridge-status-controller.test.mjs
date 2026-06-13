import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createOpsControllerForBridgeStatus({ getBridge } = {}) {
  return createAdminOpsController({
    state: { adminBusyState: {} },
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() })
    },
    getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });
}

test("admin bridge status pill shares an in-flight lightweight health poll", async () => {
  const deferred = createDeferred();
  let callCount = 0;
  const controller = createOpsControllerForBridgeStatus({
    getBridge: async path => {
      if (path !== "/app/ready") throw new Error(`unexpected path ${path}`);
      callCount += 1;
      return deferred.promise;
    }
  });

  const first = controller.pollBridgeStatus();
  const second = controller.pollBridgeStatus();
  await Promise.resolve();
  assert.equal(callCount, 1);

  deferred.resolve({ service: "baluffo-container-gateway", status: "healthy" });
  await Promise.all([first, second]);
});

test("admin bridge status accepts degraded container gateway ready payload", async () => {
  const badge = createElement({ classList: createClassList() });
  const states = [];
  const controller = createAdminOpsController({
    state: { adminBusyState: {} },
    refs: { adminBridgeStatusBadgeEl: badge },
    getBridge: async path => {
      if (path === "/app/ready") {
        return { service: "baluffo-container-gateway", status: "degraded" };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange(state) {
      states.push(state);
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.pollBridgeStatus();

  assert.equal(badge.textContent, "Bridge Degraded");
  assert.equal(badge.classList.contains("degraded"), true);
  assert.deepEqual(states, ["degraded"]);
});

test("admin bridge status degrades when app ready is delayed but pipeline status responds", async () => {
  const badge = createElement({ classList: createClassList() });
  const states = [];
  const controller = createAdminOpsController({
    state: { adminBusyState: {} },
    refs: { adminBridgeStatusBadgeEl: badge },
    getBridge: async path => {
      if (path === "/app/ready") throw new Error("ready delayed");
      if (path === "/tasks/run-jobs-pipeline-status") return { active: true, runId: "pipeline_1" };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange(state) {
      states.push(state);
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.pollBridgeStatus();

  assert.equal(badge.textContent, "Bridge Degraded");
  assert.equal(badge.classList.contains("degraded"), true);
  assert.deepEqual(states, ["degraded"]);
});
