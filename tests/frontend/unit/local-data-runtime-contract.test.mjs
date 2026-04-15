import test from "node:test";
import assert from "node:assert/strict";

import {
  LOCAL_DATA_RUNTIME_METHODS,
  assertLocalDataRuntime
} from "../../../frontend/local-data/runtime-contract.js";

function createStorageMock() {
  const map = new Map();
  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

async function importFresh(specifier) {
  return import(`${specifier}?t=${Date.now()}_${Math.random()}`);
}

function setupBrowserGlobals() {
  const localStorage = createStorageMock();
  const sessionStorage = createStorageMock();
  const indexedDb = { open() { throw new Error("unexpected IndexedDB open"); } };
  global.localStorage = localStorage;
  global.indexedDB = indexedDb;
  global.window = {
    indexedDB: indexedDb,
    localStorage,
    sessionStorage,
    addEventListener: () => {},
    prompt: () => "Test User"
  };
}

function setupDesktopGlobals() {
  const localStorage = createStorageMock();
  const sessionStorage = createStorageMock();
  const eventListeners = new Map();
  const intervalHandlers = [];
  global.window = {
    localStorage,
    sessionStorage,
    setInterval(handler) {
      intervalHandlers.push(handler);
      return intervalHandlers.length;
    },
    clearInterval: () => {},
    addEventListener(name, handler) {
      eventListeners.set(name, handler);
    },
    prompt: () => "Desktop User"
  };
  Object.defineProperty(globalThis, "navigator", {
    value: {
      sendBeacon: () => true
    },
    configurable: true,
    writable: true
  });
  global.Blob = class Blob {
    constructor(parts, options = {}) {
      this.parts = parts;
      this.type = options.type || "";
    }
  };
  global.fetch = async url => {
    if (String(url).includes("/desktop-local-data/session")) {
      return {
        ok: true,
        json: async () => ({
          ok: true,
          user: null,
          desktopSession: {
            sessionId: "desktop-session-1",
            ownerToken: "desktop-owner-1",
            lastActivityAt: "2026-04-15T10:00:00Z"
          }
        })
      };
    }
    if (String(url).includes("/app/desktop-session-lifecycle")) {
      return {
        ok: true,
        json: async () => ({ ok: true })
      };
    }
    if (String(url).includes("/ops/task-state")) {
      return {
        ok: true,
        json: async () => ({ tasks: [], count: 0 })
      };
    }
    if (String(url).includes("/app/update-status")) {
      return {
        ok: true,
        json: async () => ({ availability: "unknown", downloadState: "idle", installState: "idle" })
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  return { eventListeners, intervalHandlers };
}

test("assertLocalDataRuntime rejects missing required methods", () => {
  assert.throws(
    () => assertLocalDataRuntime({ APPLICATION_STATUSES: [] }, "test runtime"),
    /missing methods/i
  );
});

test("browser local-data client conforms to shared runtime contract", async () => {
  setupBrowserGlobals();
  const { initBrowserLocalDataClient } = await importFresh("../../../frontend/shared/local-data/browser-client.js");
  const api = initBrowserLocalDataClient();

  assert.equal(assertLocalDataRuntime(api, "browser runtime"), api);
  for (const methodName of LOCAL_DATA_RUNTIME_METHODS) {
    assert.equal(typeof api[methodName], "function", `browser runtime missing ${methodName}`);
  }
  assert.deepEqual(api.APPLICATION_STATUSES, ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"]);
  assert.equal(api.getAttachmentOpenUrl("u1", "job_1", "att_1"), "");
  assert.equal(api.getAttachmentDownloadUrl("u1", "job_1", "att_1"), "");
  assert.equal(api.getBackupExportUrl("u1"), "");
  assert.equal(global.window.JobAppLocalData, api);
});

test("desktop local-data client conforms to shared runtime contract", async () => {
  setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  const api = initDesktopLocalDataClient();
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(assertLocalDataRuntime(api, "desktop runtime"), api);
  for (const methodName of LOCAL_DATA_RUNTIME_METHODS) {
    assert.equal(typeof api[methodName], "function", `desktop runtime missing ${methodName}`);
  }
  assert.deepEqual(api.APPLICATION_STATUSES, ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"]);
  assert.equal(typeof api.getAttachmentOpenUrl("u1", "job_1", "att_1"), "string");
  assert.equal(typeof api.getAttachmentDownloadUrl("u1", "job_1", "att_1"), "string");
  assert.equal(typeof api.getBackupExportUrl("u1"), "string");
  assert.equal(global.window.JobAppLocalData, api);
});
