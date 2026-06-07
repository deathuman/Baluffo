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
