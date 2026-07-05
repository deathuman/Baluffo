import test from "node:test";
import assert from "node:assert/strict";
import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import { renderAdminOpsSchedule } from "../../../frontend/admin/render/ops-summary.js";
import {
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops schedule unknown state renders disabled controls instead of editable defaults", () => {
  const scheduleEl = createElement({ dataset: {} });

  renderAdminOpsSchedule(scheduleEl, {
    pipeline: {
      scheduleLoading: true
    }
  });

  assert.match(scheduleEl.innerHTML, /Pipeline<\/strong>: loading schedule\.\.\./);
  assert.match(scheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled"[^>]*disabled/);
  assert.match(scheduleEl.innerHTML, /value=""[^>]*data-ui="admin-pipeline-schedule-interval"[^>]*disabled/);
  assert.match(scheduleEl.innerHTML, /data-action="save-pipeline-schedule"[^>]*disabled/);
  assert.doesNotMatch(scheduleEl.innerHTML, /value="24"/);
});

test("admin ops schedule active refreshing config stays editable", () => {
  const scheduleEl = createElement({ dataset: {} });

  renderAdminOpsSchedule(scheduleEl, {
    pipeline: {
      enabled: true,
      intervalHours: 12,
      nextAfterCurrentCompletes: true,
      scheduleStatusRefreshing: true
    }
  });

  assert.match(scheduleEl.innerHTML, /Pipeline<\/strong>: every 12h, running now; next after this pipeline finishes/);
  assert.match(scheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled"[^>]*checked/);
  assert.match(scheduleEl.innerHTML, /value="12"[^>]*data-ui="admin-pipeline-schedule-interval"/);
  assert.doesNotMatch(scheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled"[^>]*disabled/);
  assert.doesNotMatch(scheduleEl.innerHTML, /loading schedule/);
});

test("admin ops schedule route failure keeps disabled retrying controls", async () => {
  const refs = {
    adminOpsScheduleEl: createElement({ dataset: {} }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {} };
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  globalThis.setTimeout = () => 1;
  globalThis.clearTimeout = () => {};
  try {
    const controller = createOpsHealthController({
      state,
      refs,
      getBridge: async path => {
        throw new Error(`failed ${path}`);
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
        getActiveTaskRows() {
          return [];
        },
        getTaskType() {
          return "";
        },
        syncLiveBusyFlags() {},
        maybeAttachLiveTaskRows() {},
        resolveTaskStatePayload() {
          return { tasks: [] };
        },
        resetLifecycleTaskState() {}
      },
      getBridgeStatus: () => "online",
      awaitBridgeReady: async () => true,
      renderScheduler: createDeferredRenderScheduler()
    });

    const schedule = await controller.loadPipelineScheduleData({ force: true, silent: true });

    assert.equal(schedule, null);
    assert.match(refs.adminOpsScheduleEl.innerHTML, /Pipeline<\/strong>: schedule delayed; retrying/);
    assert.match(refs.adminOpsScheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled"[^>]*disabled/);
    assert.doesNotMatch(refs.adminOpsScheduleEl.innerHTML, /value="24"/);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  }
});
