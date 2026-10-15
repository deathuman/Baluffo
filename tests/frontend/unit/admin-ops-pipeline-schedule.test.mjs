import test from "node:test";
import assert from "node:assert/strict";
import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import { createOpsPipelineSchedule } from "../../../frontend/admin/app/ops/health-pipeline-schedule.js";
import {
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";
import { flushMicrotaskPair as flushAdminOpsBackground } from "./helpers/async-test-helpers.mjs";

test("admin ops pipeline schedule controls post normalized settings", async () => {
  const listeners = {};
  const enabledEl = { checked: true };
  const intervalEl = { value: "12" };
  const saveButton = { disabled: false };
  const refs = {
    adminOpsScheduleEl: {
      addEventListener(type, handler) {
        listeners[type] = handler;
      },
      querySelector(selector) {
        if (selector === '[data-ui="admin-pipeline-schedule-enabled"]') return enabledEl;
        if (selector === '[data-ui="admin-pipeline-schedule-interval"]') return intervalEl;
        return null;
      }
    },
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {} };
  const posts = [];
  const toasts = [];
  createOpsHealthController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/ops/dashboard-health") {
        return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [] };
      if (path === "/registry/conflicts?view=summary") {
        return { summary: { conflictCount: 0 }, summaryView: true };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      posts.push({ path, payload });
      return {
        ok: true,
        savedConfig: { enabled: true, intervalHours: 12 },
        status: { enabled: true }
      };
    },
    deriveAdminRunsModel: () => ({
      historyRuns: [],
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      liveTypes: new Set()
    }),
    getOpsPollIntervalMs: () => 1000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast(message, tone) {
      toasts.push({ message, tone });
    },
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

  listeners.click({
    target: {
      closest(selector) {
        return selector === '[data-action="save-pipeline-schedule"]' ? saveButton : null;
      }
    },
    preventDefault() {}
  });
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.deepEqual(posts, [
    {
      path: "/tasks/jobs-pipeline-schedule",
      payload: { enabled: true, intervalHours: 12 }
    }
  ]);
  assert.equal(toasts.at(-1).tone, "success");
  assert.equal(saveButton.disabled, false);

  intervalEl.value = "169";
  listeners.click({
    target: {
      closest(selector) {
        return selector === '[data-action="save-pipeline-schedule"]' ? saveButton : null;
      }
    },
    preventDefault() {}
  });
  await flushAdminOpsBackground();

  assert.equal(posts.length, 1);
  assert.equal(toasts.at(-1).tone, "error");
  assert.match(toasts.at(-1).message, /between 1 and 168/i);
});

function createPipelineScheduleSaveHarness({ postBridge, loadOpsHealthData = async () => {} }) {
  const listeners = {};
  let controls = createScheduleControls({ enabled: false, intervalHours: 24 });
  const root = {
    dataset: {},
    attributes: {},
    addEventListener(type, handler) {
      listeners[type] = handler;
    },
    querySelector(selector) {
      if (selector === '[data-ui="admin-pipeline-schedule-enabled"]') return controls.enabled;
      if (selector === '[data-ui="admin-pipeline-schedule-interval"]') return controls.interval;
      if (selector === '[data-action="save-pipeline-schedule"]') return controls.save;
      return null;
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    removeAttribute(name) {
      delete this.attributes[name];
    }
  };
  const state = {
    pipelineScheduleModel: { pipeline: { enabled: false, intervalHours: 24 } },
    adminBusyState: {}
  };
  const renderedSchedules = [];
  const toasts = [];
  const controller = createOpsPipelineSchedule({
    state,
    refs: { adminOpsScheduleEl: root },
    postBridge,
    showToast: (message, tone) => toasts.push({ message, tone }),
    getErrorMessage: error => String(error?.message || error || "unknown"),
    markStep() {},
    loadOpsHealthData,
    loadPipelineScheduleData: async () => {},
    maybeUnrefTimer: timer => timer,
    getScheduleElement: () => root,
    renderAdminOpsScheduleImpl(_root, schedule) {
      renderedSchedules.push(schedule);
      controls = createScheduleControls(schedule.pipeline || {});
    }
  });
  controller.setupPipelineScheduleControls();

  function editDraft({ enabled = controls.enabled.checked, interval = controls.interval.value }) {
    controls.enabled.checked = enabled;
    controls.interval.value = String(interval);
    const target = { matches: () => true };
    listeners.input({ target });
  }

  function clickSave() {
    const button = controls.save;
    listeners.click({
      target: { closest: selector => selector === '[data-action="save-pipeline-schedule"]' ? button : null },
      preventDefault() {}
    });
  }

  return {
    controller,
    state,
    toasts,
    renderedSchedules,
    root,
    editDraft,
    clickSave,
    acceptLoadedSchedule: schedule => controller.rememberPipelineSchedule(schedule),
    get controls() { return controls; }
  };
}

function createScheduleControls(pipeline) {
  const enabled = createElement({ checked: Boolean(pipeline.enabled) });
  const interval = createElement({ value: String(pipeline.intervalHours ?? 24) });
  const save = createElement({ textContent: "Save" });
  return { enabled, interval, save };
}

function createDeferredPromise() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

test("admin ops schedule save owns its lock across rerenders and preserves edits newer than the submitted snapshot", async () => {
  const pendingPost = createDeferredPromise();
  const posts = [];
  const harness = createPipelineScheduleSaveHarness({
    postBridge: (path, payload) => {
      posts.push({ path, payload });
      return pendingPost.promise;
    }
  });
  harness.editDraft({ enabled: true, interval: "12" });
  harness.clickSave();
  assert.deepEqual(posts, [{
    path: "/tasks/jobs-pipeline-schedule",
    payload: { enabled: true, intervalHours: 12 }
  }]);

  harness.controller.renderPipelineScheduleModel();
  assert.equal(harness.controls.save.disabled, true);
  assert.equal(harness.controls.save.textContent, "Saving…");
  assert.equal(harness.controls.save.attributes["aria-busy"], "true");
  assert.equal(harness.root.attributes["aria-busy"], "true");
  harness.editDraft({ enabled: false, interval: "18" });
  harness.clickSave();
  assert.equal(posts.length, 1, "re-entry is blocked even with a replacement button");

  pendingPost.resolve({
    ok: true,
    savedConfig: { enabled: true, intervalHours: 12 },
    status: { enabled: true, intervalHours: 12 }
  });
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();

  assert.equal(harness.state.pipelineScheduleModel.pipeline.intervalHours, 12);
  assert.equal(harness.controls.enabled.checked, false);
  assert.equal(harness.controls.interval.value, "18");
  assert.equal(harness.controls.save.disabled, false);
  assert.equal(harness.root.attributes["aria-busy"], undefined);
  assert.equal(harness.toasts.some(toast => toast.message === "Pipeline schedule saved."), true);
});

test("admin ops schedule save retains the attempted draft after a failed POST and permits retry", async () => {
  const firstPost = createDeferredPromise();
  const posts = [];
  const harness = createPipelineScheduleSaveHarness({
    postBridge: (_path, payload) => {
      posts.push(payload);
      return posts.length === 1 ? firstPost.promise : Promise.resolve({
        ok: true,
        savedConfig: { enabled: true, intervalHours: 7 },
        status: { enabled: true, intervalHours: 7 }
      });
    }
  });
  harness.editDraft({ enabled: true, interval: "7" });
  harness.clickSave();
  firstPost.reject(new Error("write failed"));
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();

  assert.equal(harness.controls.enabled.checked, true);
  assert.equal(harness.controls.interval.value, "7");
  assert.equal(harness.controls.save.disabled, false);
  assert.equal(harness.toasts.at(-1).tone, "error");
  harness.clickSave();
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();
  assert.deepEqual(posts, [
    { enabled: true, intervalHours: 7 },
    { enabled: true, intervalHours: 7 }
  ]);
});

test("admin ops schedule save rejects a stale in-flight schedule hydration after persistence", async () => {
  const pendingGet = createDeferredPromise();
  const pendingPost = createDeferredPromise();
  const harness = createPipelineScheduleSaveHarness({ postBridge: () => pendingPost.promise });

  // Simulate a GET begun before submission but accepted after the POST.
  const staleGet = pendingGet.promise.then(schedule => harness.acceptLoadedSchedule(schedule));
  harness.editDraft({ enabled: true, interval: "10" });
  harness.clickSave();
  pendingPost.resolve({
    ok: true,
    savedConfig: { enabled: true, intervalHours: 10 },
    status: { enabled: true, intervalHours: 10 }
  });
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();

  pendingGet.resolve({ pipeline: { enabled: false, intervalHours: 24 } });
  await staleGet;
  await flushAdminOpsBackground();
  assert.equal(harness.state.pipelineScheduleModel.pipeline.enabled, true);
  assert.equal(harness.state.pipelineScheduleModel.pipeline.intervalHours, 10);
  assert.equal(harness.controls.enabled.checked, true);
  assert.equal(harness.controls.interval.value, "10");
});

test("admin ops schedule save reports health refresh failure separately from saved persistence", async () => {
  const harness = createPipelineScheduleSaveHarness({
    postBridge: async () => ({
      ok: true,
      savedConfig: { enabled: true, intervalHours: 9 },
      status: { enabled: true, intervalHours: 9 }
    }),
    loadOpsHealthData: async () => {
      throw new Error("refresh unavailable");
    }
  });
  harness.editDraft({ enabled: true, interval: "9" });
  harness.clickSave();
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();

  assert.equal(harness.state.pipelineScheduleModel.pipeline.intervalHours, 9);
  assert.deepEqual(harness.toasts.map(toast => toast.tone), ["success", "warning"]);
  assert.match(harness.toasts.at(-1).message, /saved, but Ops health could not refresh/i);
  assert.equal(harness.toasts.some(toast => /Could not save pipeline schedule/.test(toast.message)), false);
});

test("admin ops hydrates pipeline schedule fallback and preserves it across empty bootstrap schedules", async () => {
  const refs = {
    adminOpsScheduleEl: createElement({
      addEventListener() {},
      querySelector() {
        return null;
      }
    }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {} };
  const renderedSchedules = [];
  const controller = createOpsHealthController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/jobs-pipeline-schedule") {
        return {
          ok: true,
          savedConfig: { enabled: true, intervalHours: 12 },
          status: { pending: true, nextRunAt: "2026-06-14T12:00:00Z" }
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
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule(_el, schedule) {
      renderedSchedules.push(schedule);
    },
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
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

  assert.equal(schedule.pipeline.enabled, true);
  assert.equal(schedule.pipeline.intervalHours, 12);
  assert.equal(renderedSchedules.at(-1).pipeline.intervalHours, 12);

  controller.applyBootstrapPayload({
    ok: true,
    degraded: true,
    source: "container-gateway-fallback",
    tasks: { current: [], recent: [] },
    schedule: {
      pipeline: {
        enabled: true,
        intervalHours: 11,
        due: true,
        nextRunAt: "2026-06-27T09:14:44+00:00"
      }
    },
    registrySummary: { degraded: true, delayed: true },
    app: { version: "9.9.9" }
  });

  assert.equal(renderedSchedules.at(-1).pipeline.enabled, true);
  assert.equal(renderedSchedules.at(-1).pipeline.intervalHours, 12);
  assert.notEqual(renderedSchedules.at(-1).pipeline.due, true);
});

test("admin ops hydrates degraded active schedule fallback without next unknown", async () => {
  const refs = {
    adminOpsScheduleEl: createElement({
      addEventListener() {},
      querySelector() {
        return null;
      }
    }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {} };
  const renderedSchedules = [];
  const controller = createOpsHealthController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/jobs-pipeline-schedule") {
        return {
          ok: true,
          degraded: true,
          savedConfig: { enabled: true, intervalHours: 12 },
          status: {
            enabled: true,
            pending: false,
            due: false,
            nextRunAt: "",
            nextAfterCurrentCompletes: true,
            pipeline: { active: true, stage: "fetch" }
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
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule(_el, schedule) {
      renderedSchedules.push(schedule);
    },
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
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

  assert.equal(schedule.pipeline.intervalHours, 12);
  assert.equal(schedule.pipeline.nextAfterCurrentCompletes, true);
  assert.equal(renderedSchedules.at(-1).pipeline.nextAfterCurrentCompletes, true);
});
