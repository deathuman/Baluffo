import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin bootstrap timeout keeps pipeline-status fallback and avoids global offline placeholders", async () => {
  const calls = [];
  const refs = {
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminSyncStatusEl: createElement()
  };
  const controller = createAdminAuthController({
    refs,
    adminDispatch: { dispatch() {} },
    adminActions: { UNLOCKED: "unlocked", LOCKED: "locked" },
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {},
    markAdminStep() {},
    measureAdminStep() {},
    syncAdminBusyUi() {},
    syncDiscoveryLogDisclosure() {},
    resetBusyFlags() {},
    setSourceFilter() {},
    setSourceStatus() {},
    setFetcherLogPlaceholder() {},
    setDiscoveryLogPlaceholder() {},
    clearOptimisticFetchRun() {},
    clearOptimisticDiscoveryRun() {},
    setManualSourceFeedback() {},
    setOpsPlaceholders(message = "") {
      calls.push(`placeholder:${message}`);
    },
    setOpsReadinessShell() {},
    setBridgeStatusBadge(stateValue, label) {
      calls.push(`bridge:${stateValue}:${label}`);
    },
    renderUsersEmpty() {},
    startBridgeStatusWatch() {},
    stopBridgeStatusWatch() {},
    scheduleOpsHealthPolling() {},
    stopOpsHealthPolling() {},
    refreshOverview: async () => {},
    loadOpsHealthData: async () => {},
    loadPipelineStatusFallbackData: async () => {
      calls.push("pipelineStatus");
      return { active: true, runId: "pipeline_live_1" };
    },
    loadSyncStatus: async () => {},
    loadAdminBootstrap: async () => {
      calls.push("bootstrap");
      throw new Error("Bridge request timed out");
    },
    loadCriticalBootstrapFallbacks: async ({ reason } = {}) => {
      calls.push(`criticalFallback:${reason}`);
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    loadDiscoveryConfig: async () => {},
    logAdminError() {},
    showToast() {}
  });

  assert.equal(controller.initAdminPage(), true);
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.ok(calls.includes("pipelineStatus"));
  assert.ok(calls.includes("bootstrap"));
  assert.ok(calls.includes("criticalFallback:Bridge request timed out"));
  assert.ok(calls.includes("bridge:degraded:Bridge Degraded"));
  assert.equal(calls.some(item => item.startsWith("bridge:offline:")), false);
  assert.equal(calls.some(item => item.startsWith("placeholder:Admin bootstrap unavailable")), false);
});
