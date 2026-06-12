import test from "node:test";
import assert from "node:assert/strict";
import { bindAdminRuntimeEvents } from "../../../frontend/admin/app/runtime/events.js";

function createClickableElement() {
  const listeners = new Map();
  return {
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    click() {
      (listeners.get("click") || []).forEach(handler => handler({
        preventDefault() {},
        stopPropagation() {}
      }));
    }
  };
}

function createToggleElement({ matches = () => false, open = false } = {}) {
  const listeners = new Map();
  return {
    open,
    matches,
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    toggleOpen(nextOpen = true, target = this) {
      this.open = Boolean(nextOpen);
      if (target && typeof target === "object") target.open = Boolean(nextOpen);
      (listeners.get("toggle") || []).forEach(handler => handler({ target }));
    }
  };
}

async function flushMicrotasks(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

test("admin load discovery report button also reloads discovery log from the start", async () => {
  const previousWindow = global.window;
  global.window = {
    addEventListener() {}
  };

  try {
    const calls = [];
    const loadDiscoveryBtn = createClickableElement();

    bindAdminRuntimeEvents({
      state: {},
      refs: {
        adminLoadDiscoveryBtnEl: loadDiscoveryBtn
      },
      onRestoreActiveRunWatches: async () => {},
      getLastJobsUrl: () => "",
      onRefreshOverview: async () => {},
      fetcherController: {},
      discoveryController: {
        async loadDiscoveryLogChunk(options) {
          calls.push(["loadDiscoveryLogChunk", options]);
        }
      },
      registryController: {
        async loadDiscoveryData() {
          calls.push(["loadDiscoveryData"]);
        }
      },
      opsController: {},
      syncController: {},
      readShowZeroJobs: () => false,
      writeShowZeroJobs: () => {},
      showZeroJobsKey: "test",
      onSyncDiscoveryLogDisclosure: () => {},
      onSetSourceFilter: () => {}
    });

    loadDiscoveryBtn.click();
    await flushMicrotasks();

    assert.deepEqual(calls, [
      ["loadDiscoveryData"],
      ["loadDiscoveryLogChunk", { reset: true, guarded: false, view: "tail", limitChars: 65536 }]
    ]);
  } finally {
    global.window = previousWindow;
  }
});

test("admin registry and sync diagnostics load when the disclosure opens", async () => {
  const previousWindow = global.window;
  global.window = {
    addEventListener() {}
  };

  try {
    const calls = [];
    const opsKpisEl = createToggleElement();
    const registrySyncDetails = createToggleElement({
      matches: selector => selector === ".admin-ops-registry-sync-details",
      open: false
    });

    bindAdminRuntimeEvents({
      state: {},
      refs: {
        adminOpsKpisEl: opsKpisEl
      },
      onRestoreActiveRunWatches: async () => {},
      getLastJobsUrl: () => "",
      fetcherController: {},
      discoveryController: {},
      registryController: {},
      opsController: {
        async loadRegistrySyncDiagnosticsData(options) {
          calls.push(options);
        }
      },
      syncController: {},
      readShowZeroJobs: () => false,
      writeShowZeroJobs: () => {},
      showZeroJobsKey: "test",
      onSyncDiscoveryLogDisclosure: () => {},
      onSetSourceFilter: () => {}
    });

    opsKpisEl.toggleOpen(true, registrySyncDetails);
    await flushMicrotasks();

    assert.deepEqual(calls, [{ silent: false }]);
  } finally {
    global.window = previousWindow;
  }
});
