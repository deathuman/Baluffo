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
  global.window = {
    localStorage,
    sessionStorage,
    setInterval: () => 1,
    prompt: () => "Desktop User"
  };
  global.fetch = async url => {
    if (String(url).includes("/desktop-local-data/session")) {
      return {
        ok: true,
        json: async () => ({ ok: true, user: null })
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
}

test("assertLocalDataRuntime rejects missing required methods", () => {
  assert.throws(
    () => assertLocalDataRuntime({ APPLICATION_STATUSES: [] }, "test runtime"),
    /missing methods/i
  );
});

test("browser local-data client conforms to shared runtime contract", async () => {
  setupBrowserGlobals();
  const { initBrowserLocalDataClient } = await importFresh("../../../local-data-client.js");
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
  const { initDesktopLocalDataClient } = await importFresh("../../../desktop-local-data-client.js");
  const api = initDesktopLocalDataClient();

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
