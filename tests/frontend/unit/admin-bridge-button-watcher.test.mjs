import test from "node:test";
import assert from "node:assert/strict";

import { createAdminBridgeButtonWatcher } from "../../../frontend/shared/admin-bridge-button.js";

function createButton() {
  return { disabled: false };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

test("admin bridge button watcher falls back to desktop bridge URL params", async () => {
  const originalWindow = globalThis.window;
  const states = [];
  const bases = [];
  const paths = [];
  globalThis.window = {
    location: {
      href: "http://127.0.0.1:64432/saved.html?desktop=1&bridgePort=64433&bridgeHost=127.0.0.1"
    },
    sessionStorage: {
      getItem() {
        return "";
      }
    },
    setInterval() {
      return 1;
    },
    clearInterval() {}
  };

  try {
    const watcher = createAdminBridgeButtonWatcher({
      buttonEl: createButton(),
      baseUrl: "http://127.0.0.1:8877",
      fetchJson: async (base, path) => {
        bases.push(base);
        paths.push(path);
        if (String(base).endsWith(":64433")) {
          return { summary: { activeAlertCount: 0 } };
        }
        throw new Error("wrong bridge");
      },
      applyState: state => states.push(state),
      awaitBridgeReady: async () => true
    });

    watcher.startAdminBridgeButtonWatch();
    await new Promise(resolve => setImmediate(resolve));
    await new Promise(resolve => setImmediate(resolve));
    watcher.stopAdminBridgeButtonWatch();

    assert.deepEqual(bases, ["http://127.0.0.1:8877", "http://127.0.0.1:64433"]);
    assert.deepEqual(paths, ["/ops/health?view=ready", "/ops/health?view=ready"]);
    assert.equal(states.at(-1)?.state, "online");
    assert.equal(states.at(-1)?.label, "Admin Online");
  } finally {
    if (originalWindow === undefined) {
      delete globalThis.window;
    } else {
      globalThis.window = originalWindow;
    }
  }
});

test("admin bridge button watcher skips overlapping interval polls", async () => {
  const originalWindow = globalThis.window;
  const deferred = createDeferred();
  const paths = [];
  const states = [];
  let intervalCallback = null;
  let readyCalls = 0;
  globalThis.window = {
    location: {
      href: "http://127.0.0.1:64432/saved.html?desktop=1&bridgePort=64433&bridgeHost=127.0.0.1"
    },
    sessionStorage: {
      getItem() {
        return "";
      }
    },
    setInterval(callback) {
      intervalCallback = callback;
      return 1;
    },
    clearInterval() {}
  };

  try {
    const watcher = createAdminBridgeButtonWatcher({
      buttonEl: createButton(),
      baseUrl: "http://127.0.0.1:64433",
      fetchJson: async (_base, path) => {
        paths.push(path);
        return deferred.promise;
      },
      applyState: state => states.push(state),
      awaitBridgeReady: async () => {
        readyCalls += 1;
        return true;
      }
    });

    watcher.startAdminBridgeButtonWatch();
    assert.equal(typeof intervalCallback, "function");
    intervalCallback();
    await new Promise(resolve => setImmediate(resolve));

    assert.deepEqual(paths, ["/ops/health?view=ready"]);
    assert.equal(readyCalls, 1);

    deferred.resolve({ summary: { activeAlertCount: 0 } });
    await new Promise(resolve => setImmediate(resolve));
    watcher.stopAdminBridgeButtonWatch();

    assert.equal(states.at(-1)?.state, "online");
  } finally {
    if (originalWindow === undefined) {
      delete globalThis.window;
    } else {
      globalThis.window = originalWindow;
    }
  }
});
