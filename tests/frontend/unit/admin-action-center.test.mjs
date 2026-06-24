import test from "node:test";
import assert from "node:assert/strict";
import { createActionCenterController } from "../../../frontend/admin/app/action-center.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

function cleanHealthPayload() {
  return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0 } };
}

function cleanSyncPayload() {
  return {
    config: { enabled: true, ready: true, state: "ready" },
    runtime: { lastAction: "pull", lastResult: "ok", lastError: "" }
  };
}

function cleanStoragePayload() {
  return { ok: true, storage: { healthy: true, diagnostics: [] } };
}

function createFixture({
  getBridge,
  onSyncStatus,
  shouldDeferStorageHealth
} = {}) {
  const refs = {
    actionCenterItemsEl: createElement(),
    actionCenterCopyBtnEl: createElement()
  };
  const calls = [];
  const controller = createActionCenterController({
    refs,
    getBridge: getBridge || (async path => {
      calls.push(path);
      if (path === "/ops/health?view=ready") {
        return cleanHealthPayload();
      }
      if (path === "/sync/status?view=summary") {
        return cleanSyncPayload();
      }
      if (path === "/ops/storage-health") {
        return cleanStoragePayload();
      }
      return null;
    }),
    postBridge: async () => ({}),
    showToast() {},
    logAdminError() {},
    onSyncStatus,
    shouldDeferStorageHealth
  });
  return { refs, calls, controller };
}

test("action center renders partial state after lightweight clean core poll", async () => {
  const { refs, calls, controller } = createFixture();

  await controller.pollActionCenter({ includeStorage: false });

  assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
  assert.doesNotMatch(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
});

test("action center renders healthy only after storage is checked", async () => {
  const { refs, calls, controller } = createFixture();

  await controller.pollActionCenter({ includeStorage: true });

  assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary", "/ops/storage-health"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
});

test("action center defers storage health while active work is known", async () => {
  const { refs, calls, controller } = createFixture({
    shouldDeferStorageHealth: () => true
  });

  await controller.pollActionCenter({ includeStorage: true });

  assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
});

test("action center renders unavailable state when signal routes fail", async () => {
  const refs = {
    actionCenterItemsEl: createElement(),
    actionCenterCopyBtnEl: createElement()
  };
  const controller = createActionCenterController({
    refs,
    getBridge: async () => {
      throw new Error("timeout");
    },
    postBridge: async () => ({}),
    showToast() {},
    logAdminError() {}
  });

  await controller.pollActionCenter({ includeStorage: false });

  assert.match(refs.actionCenterItemsEl.innerHTML, /Operational signals unavailable/);
  assert.doesNotMatch(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
  assert.notEqual(refs.actionCenterItemsEl.innerHTML.trim(), "");
});

test("action center startPolling runs first lightweight poll immediately", async () => {
  const calls = [];
  const refs = {
    actionCenterItemsEl: createElement({ addEventListener() {} }),
    actionCenterCopyBtnEl: createElement({ addEventListener() {} })
  };
  const controller = createActionCenterController({
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/health?view=ready") return cleanHealthPayload();
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      if (path === "/ops/storage-health") return cleanStoragePayload();
      return null;
    },
    postBridge: async () => ({}),
    showToast() {},
    logAdminError() {}
  });
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousSetInterval = global.setInterval;
  const previousClearInterval = global.clearInterval;
  const scheduledTimeouts = [];
  try {
    global.setTimeout = (callback, delayMs) => {
      scheduledTimeouts.push({ callback, delayMs });
      return scheduledTimeouts.length;
    };
    global.clearTimeout = () => {};
    global.setInterval = () => 1;
    global.clearInterval = () => {};

    controller.startPolling({ initialDelayMs: 5000 });
    await new Promise(resolve => setImmediate(resolve));

    assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary"]);
    assert.equal(scheduledTimeouts[0]?.delayMs, 10000);
    assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
  } finally {
    controller.stopPolling();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    global.setInterval = previousSetInterval;
    global.clearInterval = previousClearInterval;
  }
});

test("action center renders remote sync conflict as reviewable warning", async () => {
  const { refs, calls, controller } = createFixture({
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/health?view=ready") {
        return cleanHealthPayload();
      }
      if (path === "/sync/status?view=summary") {
        return {
          config: { enabled: true, ready: true, state: "remote_conflict" },
          runtime: {
            lastAction: "push",
            lastResult: "error",
            lastError: "is at a8f0ae858e0e7c8ecafe671bf9825f6e7328dd97 but expected db2c4166cf428892f165629d27933ce492d346d1"
          }
        };
      }
      return null;
    }
  });

  await controller.pollActionCenter({ includeStorage: false });

  assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Sync needs attention/);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Sync conflict needs review; data refresh can continue/);
  assert.match(refs.actionCenterItemsEl.innerHTML, /data-preset="sync_pull"/);
});

test("action center publishes fresh sync status for Source Sync panel hydration", async () => {
  const syncPayload = {
    config: {
      enabled: true,
      ready: false,
      state: "misconfigured",
      missing: ["packaged_github_app_config"],
      message: "Missing packaged GitHub App config.",
      credentialsPackaged: false
    },
    runtime: { lastAction: "pull", lastResult: "ok", lastError: "" }
  };
  let publishedSync = null;
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/health?view=ready") return cleanHealthPayload();
      if (path === "/sync/status?view=summary") return syncPayload;
      return null;
    },
    onSyncStatus(payload) {
      publishedSync = payload;
    }
  });

  await controller.pollActionCenter({ includeStorage: false });

  assert.equal(publishedSync, syncPayload);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Sync is enabled but not configured/);
});
