import test from "node:test";
import assert from "node:assert/strict";
import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import {
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops pipeline schedule hydrates the visible rebound schedule node", async () => {
  const staleScheduleEl = createElement({
    isConnected: false,
    addEventListener() {},
    querySelector() {
      return null;
    }
  });
  const liveScheduleEl = createElement({
    isConnected: true,
    addEventListener() {},
    querySelector() {
      return null;
    }
  });
  const previousDocument = globalThis.document;
  globalThis.document = {
    querySelector(selector) {
      return selector === '[data-ui="admin-ops-schedule"]' ? liveScheduleEl : null;
    }
  };
  try {
    const refs = {
      adminOpsScheduleEl: staleScheduleEl,
      adminOpsAlertsEl: createElement(),
      adminOpsKpisEl: createElement(),
      adminOpsTrendsEl: createElement(),
      adminOpsHistoryEl: createElement(),
      adminOpsFetcherMetricsEl: createElement(),
      adminOpsDedupListsEl: createElement()
    };
    const state = { adminBusyState: {} };
    const controller = createOpsHealthController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/tasks/jobs-pipeline-schedule") {
          return {
            ok: true,
            savedConfig: { enabled: true, intervalHours: 11 },
            status: {
              enabled: true,
              due: false,
              nextRunAt: "2026-06-28T12:51:16Z"
            }
          };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => ({
        historyRuns: [],
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        liveTypes: new Set()
      }),
      getOpsPollIntervalMs: () => 1000,
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      adminDispatch: { dispatch() {} },
      adminActions: { OPS_REFRESHED: "ops/refreshed" },
      escapeHtml: value => String(value || ""),
      idlePollIntervalMs: 1000,
      taskStateController: {
        getActiveTaskRows() { return []; },
        getTaskType() { return ""; },
        syncLiveBusyFlags() {},
        maybeAttachLiveTaskRows() {},
        resolveTaskStatePayload() { return { tasks: [] }; },
        resetLifecycleTaskState() {}
      },
      getBridgeStatus: () => "online",
      awaitBridgeReady: async () => true,
      renderScheduler: createDeferredRenderScheduler()
    });

    await controller.loadPipelineScheduleData({ force: true, silent: true });

    assert.equal(refs.adminOpsScheduleEl, liveScheduleEl);
    assert.match(liveScheduleEl.innerHTML, /Pipeline<\/strong>: every 11h, next /);
    assert.match(liveScheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled" checked/);
    assert.match(liveScheduleEl.innerHTML, /value="11"/);
    assert.equal(staleScheduleEl.innerHTML, "");
  } finally {
    globalThis.document = previousDocument;
  }
});
