import test from "node:test";
import assert from "node:assert/strict";
import { createActionCenterController } from "../../../frontend/admin/app/action-center.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";
import { cleanHealthPayload, cleanStoragePayload, cleanSyncPayload, createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";

test("action center renders partial state after lightweight clean core poll", async () => {
  const { refs, calls, controller } = createFixture();

  await controller.pollActionCenter({ includeStorage: false });

  assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
  assert.doesNotMatch(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
});

test("action center renders healthy only after storage is checked", async () => {
  const { refs, calls, controller } = createFixture();

  await controller.pollActionCenter({ includeStorage: true });

  assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary", "/ops/storage-health"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
});

test("action center defers storage health while active work is known", async () => {
  const { refs, calls, controller } = createFixture({
    shouldDeferStorageHealth: () => true
  });

  await controller.pollActionCenter({ includeStorage: true });

  assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
});

test("action center skips core signal polls while active work is known", async () => {
  const { refs, calls, controller } = createFixture({
    shouldDeferCoreSignals: () => true,
    shouldDeferStorageHealth: () => true
  });

  await controller.pollActionCenter({ includeStorage: true });

  assert.deepEqual(calls, []);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Operational checks delayed while job update is running\./);
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
      if (path === "/ops/fetch-kpis?view=summary") return cleanHealthPayload();
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

    assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary"]);
    assert.equal(scheduledTimeouts[0]?.delayMs, 30000);
    assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals\. Storage check pending\./);
  } finally {
    controller.stopPolling();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    global.setInterval = previousSetInterval;
    global.clearInterval = previousClearInterval;
  }
});

test("action center defers its first poll to the injected startup lane", async () => {
  // The first poll reads the health route, which is startup-heavy: the bridge has one
  // gateway, so the poll must queue on the serial lane instead of racing the bootstrap
  // loads. Without the injection the poll stays immediate, which is what every other
  // test in this file relies on.
  const queued = [];
  const { calls, refs, controller } = createFixture({
    enqueueStartupTask: task => {
      queued.push(task);
      return Promise.resolve();
    }
  });
  const previousSetTimeout = global.setTimeout;
  const previousSetInterval = global.setInterval;
  const previousClearTimeout = global.clearTimeout;
  const previousClearInterval = global.clearInterval;
  try {
    global.setTimeout = () => 1;
    global.setInterval = () => 1;
    global.clearTimeout = () => {};
    global.clearInterval = () => {};

    controller.startPolling();
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(queued.length, 1, "the first poll must be handed to the startup lane");
    assert.deepEqual(calls, [], "nothing may hit the bridge before the lane runs the task");

    await queued[0]();
    assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary"]);
    assert.match(refs.actionCenterItemsEl.innerHTML, /Storage check pending/);
  } finally {
    controller.stopPolling();
    global.setTimeout = previousSetTimeout;
    global.setInterval = previousSetInterval;
    global.clearTimeout = previousClearTimeout;
    global.clearInterval = previousClearInterval;
  }
});

test("action center renders remote sync conflict as reviewable warning", async () => {
  const { refs, calls, controller } = createFixture({
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/fetch-kpis?view=summary") {
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

  assert.deepEqual(calls, ["/ops/fetch-kpis?view=summary", "/sync/status?view=summary"]);
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
      if (path === "/ops/fetch-kpis?view=summary") return cleanHealthPayload();
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

test("copy all diagnostics falls back to execCommand on non-secure contexts", async () => {
  const { controller, toasts } = createFixture();
  let execCommandArgs = [];
  const fakeTextArea = { value: "", style: {}, focus() {}, select() {}, setAttribute() {} };
  const previousWindow = globalThis.window;
  const previousDocument = globalThis.document;
  globalThis.window = { isSecureContext: false };
  globalThis.document = {
    createElement: () => fakeTextArea,
    body: {
      appendChild() {},
      removeChild() {}
    },
    execCommand: command => {
      execCommandArgs.push(command);
      return true;
    }
  };

  try {
    await controller.copyAllDiagnostics();

    assert.deepEqual(execCommandArgs, ["copy"]);
    assert.equal(fakeTextArea.value.includes('"health"'), true);
    assert.deepEqual(toasts, [["All diagnostics copied", "success"]]);
  } finally {
    if (typeof previousWindow === "undefined") delete globalThis.window;
    else globalThis.window = previousWindow;
    if (typeof previousDocument === "undefined") delete globalThis.document;
    else globalThis.document = previousDocument;
  }
});

test("copy all diagnostics reports failure when no clipboard path works", async () => {
  const { controller, toasts } = createFixture();
  const previousWindow = globalThis.window;
  const previousDocument = globalThis.document;
  globalThis.window = { isSecureContext: false };
  globalThis.document = {
    createElement: () => ({ value: "", style: {}, focus() {}, select() {}, setAttribute() {} }),
    body: { appendChild() {}, removeChild() {} },
    execCommand: () => false
  };

  try {
    await controller.copyAllDiagnostics();

    assert.deepEqual(toasts, [["Could not copy diagnostics", "warn"]]);
  } finally {
    if (typeof previousWindow === "undefined") delete globalThis.window;
    else globalThis.window = previousWindow;
    if (typeof previousDocument === "undefined") delete globalThis.document;
    else globalThis.document = previousDocument;
  }
});

test("action center header chip reflects state and issue count", async () => {
  const { refs, controller } = createFixture();
  await controller.pollActionCenter({ includeStorage: true });
  assert.match(refs.actionCenterStatusChipEl.innerHTML, /Healthy/, "clean poll shows the healthy chip");

  const { refs: signalRefs, controller: signalController } = createFixture({
    getBridge: async path => {
      if (path === "/ops/fetch-kpis?view=summary") {
        return { alerts: [{ id: "stale_fetch" }], kpis: { lastSuccessfulFetchAge: "30h", failedSourceRatioLatest: 0 } };
      }
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      if (path === "/ops/storage-health") return cleanStoragePayload();
      return null;
    }
  });
  await signalController.pollActionCenter({ includeStorage: true });
  assert.match(signalRefs.actionCenterStatusChipEl.innerHTML, /1 issue/, "chip counts visible signals");
  assert.match(signalRefs.actionCenterStatusChipEl.innerHTML, /warning/, "chip takes the worst severity");
});

test("action center checked-at stamp appears only after data arrives", async () => {
  const { refs, controller } = createFixture();
  assert.equal(refs.actionCenterCheckedAtEl.innerHTML, "", "no stamp before the first poll");

  await controller.pollActionCenter({ includeStorage: false });
  assert.match(refs.actionCenterCheckedAtEl.innerHTML, /checked just now/);
});

test("action center renders without the optional header refs", async () => {
  // Guards the legacy two-ref construction: a missing chip or stamp must not throw.
  const { refs, controller } = createFixture({ includeHeaderRefs: false });
  await controller.pollActionCenter({ includeStorage: true });
  assert.match(refs.actionCenterItemsEl.innerHTML, /All systems operational/);
});
