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
  shouldDeferStorageHealth,
  shouldDeferCoreSignals,
  showToast,
  includeHeaderRefs = true
} = {}) {
  const refs = {
    actionCenterItemsEl: createElement(),
    actionCenterCopyBtnEl: createElement()
  };
  // The header chip and stamp are optional at runtime; the default fixture supplies
  // them so their wiring is exercised, and `includeHeaderRefs: false` covers the
  // legacy two-ref shape that must keep working.
  if (includeHeaderRefs) {
    refs.actionCenterStatusChipEl = createElement();
    refs.actionCenterCheckedAtEl = createElement();
  }
  const calls = [];
  const toasts = [];
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
    showToast: showToast || ((message, tone) => toasts.push([message, tone])),
    logAdminError() {},
    onSyncStatus,
    shouldDeferCoreSignals,
    shouldDeferStorageHealth
  });
  return { refs, calls, toasts, controller };
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
      if (path === "/ops/health?view=ready") {
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

test("action center renders every reachable signal and no unreachable overflow row", async () => {
  // Three is the ceiling: `stale_fetch` needs a fetch age above 12h and
  // `failed_sources` needs one at or below 12h, so they are mutually exclusive. The
  // reachable maximum is storage_health + sync_status + failed_sources, and all
  // three must render. The "View all" row that used to follow them could never
  // appear and has been removed.
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/health?view=ready") {
        return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0.5 } };
      }
      if (path === "/sync/status?view=summary") {
        return {
          config: { enabled: true, ready: true, state: "remote_conflict" },
          runtime: { lastAction: "push", lastResult: "error", lastError: "is at a but expected b" }
        };
      }
      if (path === "/ops/storage-health") {
        return { ok: true, storage: { healthy: false, diagnostics: [{ ok: false }] } };
      }
      return null;
    }
  });

  await controller.pollActionCenter({ includeStorage: true });

  const html = refs.actionCenterItemsEl.innerHTML;
  for (const id of ["storage_health", "sync_status", "failed_sources"]) {
    assert.match(html, new RegExp(`data-signal="${id}"`), `${id} must render`);
  }
  assert.equal((html.match(/class="action-center-signal /g) || []).length, 3);
  assert.doesNotMatch(html, /view-all/);
});

test("stale_fetch and failed_sources are mutually exclusive in the live signal set", async () => {
  // This is the invariant that makes the display cap unreachable. If a future change
  // lets both fire together, the signal count reaches four, the cap binds, and the
  // removed overflow row becomes necessary again — so this test is the tripwire.
  const at = async (age, alerts, ratio) => {
    const { refs, controller } = createFixture({
      getBridge: async path => {
        if (path === "/ops/health?view=ready") {
          return { alerts, kpis: { lastSuccessfulFetchAge: age, failedSourceRatioLatest: ratio } };
        }
        if (path === "/sync/status?view=summary") return { config: { enabled: false }, runtime: {} };
        if (path === "/ops/storage-health") return cleanStoragePayload();
        return null;
      }
    });
    await controller.pollActionCenter({ includeStorage: true });
    return [...new Set([...refs.actionCenterItemsEl.innerHTML.matchAll(/data-signal="([^"]+)"/g)].map(m => m[1]))];
  };

  const stale = await at("30h", [{ id: "stale_fetch" }], 0.5);
  assert.deepEqual(stale, ["stale_fetch"], "a stale fetch must not also report failed sources");

  const fresh = await at("1h", [], 0.5);
  assert.deepEqual(fresh, ["failed_sources"], "failed sources require a recent successful fetch");
});
