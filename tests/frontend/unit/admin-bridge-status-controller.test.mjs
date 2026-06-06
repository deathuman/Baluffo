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
      if (path !== "/ops/health?view=ready") throw new Error(`unexpected path ${path}`);
      callCount += 1;
      return deferred.promise;
    }
  });

  const first = controller.pollBridgeStatus();
  const second = controller.pollBridgeStatus();
  await Promise.resolve();
  assert.equal(callCount, 1);

  deferred.resolve({ service: "baluffo-bridge", status: "healthy" });
  await Promise.all([first, second]);
});
