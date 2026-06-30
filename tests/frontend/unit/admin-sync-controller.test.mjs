import test from "node:test";
import assert from "node:assert/strict";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";
import {
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin sync controller hydrates status and runs save/test/pull/push flows", async () => {
  const toasts = [];
  const paths = [];
  const busyTransitions = [];
  const state = {
    syncConfigDirty: true,
    latestSyncStatusCache: null
  };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const readyPayload = {
    savedConfig: { enabled: true },
    config: {
      enabled: true,
      state: "ready",
      authMode: "github_app",
      configPath: "config/sync.json",
      repo: "org/repo",
      branch: "main",
      path: "baluffo/source-sync.json"
    },
    runtime: {
      lastPullAt: "2026-03-08T10:00:00Z",
      lastPushAt: "2026-03-08T10:05:00Z",
      lastAction: "pull",
      lastResult: "success",
      lastError: ""
    }
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async path => {
      paths.push(path);
      return readyPayload;
    },
    postBridge: async (path, payload) => {
      paths.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/sync/config") return readyPayload;
      if (path === "/sync/test") return { ok: true, remoteFound: true };
      if (path === "/tasks/run-sync-pull") return { started: true };
      if (path === "/tasks/run-sync-push") return { started: true };
      throw new Error(`unexpected path ${path}`);
    },
    isSyncBusy: () => false,
    setBusyFlag(key, value) {
      busyTransitions.push(`${key}:${String(value)}`);
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast(message, level) {
      toasts.push({ message, level });
    },
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {
      paths.push("loadOpsHealthData");
    },
    scheduleOpsHealthPolling(delay) {
      paths.push(`scheduleOpsHealthPolling:${delay}`);
    },
    escapeHtml: value => String(value)
  });

  const payload = await controller.loadSyncStatus({ forceForm: true });
  assert.equal(payload, readyPayload);
  assert.equal(refs.adminSyncEnabledEl.checked, true);
  assert.match(refs.adminSyncConfigHintEl.textContent, /packaged config: available/i);
  assert.doesNotMatch(refs.adminSyncConfigHintEl.textContent, /config\/sync\.json/i);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Connected to org\/repo/i);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Local sync enabled/i);

  await controller.saveSyncConfig();
  await controller.testSyncConfig();
  await controller.pullSourcesSync();
  await controller.pushSourcesSync();

  assert.equal(state.syncConfigDirty, false);
  assert.ok(paths.includes("/sync/status"));
  assert.ok(paths.includes("/ops/task-live/sync?view=summary"));
  assert.equal(paths.includes("/ops/task-live/sync"), false);
  assert.ok(paths.includes("/sync/config:{\"enabled\":true}"));
  assert.ok(paths.includes("/sync/test:{}"));
  assert.ok(paths.includes("/tasks/run-sync-pull:{}"));
  assert.ok(paths.includes("/tasks/run-sync-push:{}"));
  assert.equal(paths.filter(item => item === "loadOpsHealthData").length, 2);
  assert.equal(paths.filter(item => item === "scheduleOpsHealthPolling:900").length, 2);
  assert.ok(toasts.some(item => item.message === "Source sync preference updated." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sync test passed. Remote snapshot found." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sources sync pull started." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sources sync push started." && item.level === "success"));
  assert.deepEqual(busyTransitions, [
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false"
  ]);
});

test("admin sync status hydrates live state from summary task-live route", async () => {
  const paths = [];
  const state = {
    syncConfigDirty: false,
    latestSyncStatusCache: null,
    adminBusyState: { liveSyncRunning: false }
  };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async path => {
      paths.push(path);
      if (path === "/sync/status?view=summary") {
        return {
          savedConfig: { enabled: true },
          config: { enabled: true, state: "ready", repo: "org/repo", branch: "main" },
          runtime: { lastAction: "pull", lastResult: "running" }
        };
      }
      if (path === "/ops/task-live/sync?view=summary") {
        return {
          active: true,
          taskProgress: { active: true, phaseLabel: "Pulling remote source registry" },
          summary: { action: "pull" }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    isSyncBusy: () => false,
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast() {},
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  await controller.loadSyncStatus({ silent: true, forceForm: true, summary: true });

  assert.deepEqual(paths, ["/sync/status?view=summary", "/ops/task-live/sync?view=summary"]);
  assert.equal(state.adminBusyState.liveSyncRunning, true);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Connected to org\/repo/i);
});

test("admin sync status can skip live sync hydration during first boot", async () => {
  const paths = [];
  const state = {
    syncConfigDirty: false,
    latestSyncStatusCache: null,
    adminBusyState: {}
  };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async path => {
      paths.push(path);
      return {
        savedConfig: { enabled: true },
        config: {
          enabled: true,
          ready: true,
          state: "ready",
          repo: "org/repo",
          branch: "main",
          path: "baluffo/source-sync.json"
        },
        runtime: {
          lastPullAt: "2026-06-11T21:39:53Z",
          lastPushAt: "2026-06-04T17:27:36Z",
          lastAction: "pull",
          lastResult: "ok"
        }
      };
    },
    postBridge: async () => ({}),
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast() {},
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  await controller.loadSyncStatus({ silent: true, forceForm: true, includeLive: false });

  assert.deepEqual(paths, ["/sync/status"]);
  assert.equal(Boolean(state.adminBusyState.liveSyncRunning), false);
});

test("admin sync status summary preserves enabled form state during first boot", async () => {
  const paths = [];
  const state = {
    syncConfigDirty: false,
    latestSyncStatusCache: null,
    adminBusyState: {}
  };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async path => {
      paths.push(path);
      return {
        summaryView: true,
        detailLevel: "summary",
        savedConfig: { enabled: true },
        config: {
          enabled: true,
          ready: true,
          state: "ready",
          repo: "org/repo",
          branch: "main",
          path: "baluffo/source-sync.json"
        },
        runtime: {
          lastPullAt: "2026-06-11T21:39:53Z",
          lastPushAt: "2026-06-04T17:27:36Z",
          lastAction: "pull",
          lastResult: "ok"
        }
      };
    },
    postBridge: async () => ({}),
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast() {},
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  await controller.loadSyncStatus({ silent: true, forceForm: true, includeLive: false, summary: true });

  assert.deepEqual(paths, ["/sync/status?view=summary"]);
  assert.equal(refs.adminSyncEnabledEl.checked, true);
  assert.match(refs.adminSyncStatusEl.innerHTML, /2026-06-11T21:39:53\.000Z/);
  assert.match(refs.adminSyncStatusEl.innerHTML, /2026-06-04T17:27:36\.000Z/);
  assert.match(refs.adminSyncStatusEl.innerHTML, /pull/);
  assert.equal(Boolean(state.adminBusyState.liveSyncRunning), false);
});

test("admin sync status renders misconfigured packaged config as needs attention", () => {
  const state = { syncConfigDirty: false, latestSyncStatusCache: null };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async () => ({}),
    postBridge: async () => ({}),
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast() {},
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  controller.renderSyncStatus({
    savedConfig: { enabled: true },
    config: {
      enabled: true,
      ready: false,
      state: "misconfigured",
      repo: "",
      branch: "main",
      path: "baluffo/source-sync.json",
      missing: ["packaged_github_app_config"],
      message: "Missing packaged GitHub App config.",
      credentialsPackaged: false
    },
    runtime: { lastAction: "pull", lastResult: "ok", lastError: "" }
  }, { forceForm: true });

  assert.equal(refs.adminSyncEnabledEl.checked, true);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Needs Attention/);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Missing: packaged_github_app_config/);
  assert.doesNotMatch(refs.adminSyncStatusEl.innerHTML, /Connected to unknown and ready/);
});

test("admin sync status renders degraded bootstrap stub as delayed, not disabled", () => {
  const state = { syncConfigDirty: false, latestSyncStatusCache: null };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: true }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async () => ({}),
    postBridge: async () => ({}),
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast() {},
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  controller.renderSyncStatus({
    ok: true,
    summaryView: true,
    degraded: true,
    delayed: true
  }, { forceForm: true });

  assert.equal(refs.adminSyncEnabledEl.checked, true);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Loading/);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Sync status delayed/);
  assert.doesNotMatch(refs.adminSyncStatusEl.innerHTML, /Disabled/);
  assert.doesNotMatch(refs.adminSyncStatusEl.innerHTML, /Local sync disabled/);
});
