import test from "node:test";
import assert from "node:assert/strict";

import { createAdminBridgeButtonWatcher } from "../../../frontend/shared/admin-bridge-button.js";

function createButton() {
  return { disabled: false };
}

test("admin bridge button watcher falls back to desktop bridge URL params", async () => {
  const originalWindow = globalThis.window;
  const states = [];
  const bases = [];
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
      fetchJson: async base => {
        bases.push(base);
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
