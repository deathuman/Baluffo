import test from "node:test";
import assert from "node:assert/strict";

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

function createJsonResponse(payload) {
  return {
    ok: true,
    json: async () => payload
  };
}

function createBeforeUnloadEvent() {
  let prevented = false;
  return {
    preventDefault() {
      prevented = true;
    },
    get defaultPrevented() {
      return prevented;
    },
    returnValue: undefined
  };
}

async function flushMicrotasks(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
  await new Promise(resolve => setTimeout(resolve, 0));
}

function setupDesktopGlobals({
  taskPayload = { tasks: [], count: 0 },
  updatePayload = { availability: "unknown", downloadState: "idle", installState: "idle" }
} = {}) {
  const localStorage = createStorageMock();
  const sessionStorage = createStorageMock();
  const eventListeners = new Map();
  const intervalHandlers = [];
  const beaconCalls = [];
  const fetchCalls = [];
  const locationState = {
    href: "http://127.0.0.1:4173/jobs.html?desktop=1",
    assignCalls: [],
    assign(url) {
      const nextUrl = String(url || "");
      this.assignCalls.push(nextUrl);
      this.href = nextUrl;
    }
  };

  global.window = {
    localStorage,
    sessionStorage,
    location: locationState,
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
  global.fetch = async (url, options = {}) => {
    const normalizedUrl = String(url);
    fetchCalls.push({ url: normalizedUrl, options });
    if (normalizedUrl.includes("/desktop-local-data/session")) {
      return createJsonResponse({
        ok: true,
        user: null,
        desktopSession: {
          sessionId: "desktop-session-1",
          ownerToken: "desktop-owner-1",
          lastActivityAt: "2026-04-15T10:00:00Z"
        }
      });
    }
    if (normalizedUrl.includes("/app/desktop-session-lifecycle")) {
      return createJsonResponse({ ok: true });
    }
    if (normalizedUrl.includes("/ops/task-state")) {
      return createJsonResponse(taskPayload);
    }
    if (normalizedUrl.includes("/app/update-status")) {
      return createJsonResponse(updatePayload);
    }
    throw new Error(`unexpected fetch: ${normalizedUrl}`);
  };
  Object.defineProperty(globalThis, "navigator", {
    value: {
      sendBeacon(url, blob) {
        beaconCalls.push({ url: String(url), blob });
        return true;
      }
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

  return {
    beaconCalls,
    eventListeners,
    fetchCalls,
    intervalHandlers,
    locationState
  };
}

test("desktop beforeunload prompts when admin bridge work is active", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "fetch", active: true }],
      count: 1
    }
  });
  const { initDesktopLocalDataClient } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const beforeUnload = eventListeners.get("beforeunload");
  assert.equal(typeof beforeUnload, "function");

  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, "");
  assert.equal(event.defaultPrevented, true);
  assert.equal(event.returnValue, "");
  assert.equal(beaconCalls.length, 0);
});

test("desktop beforeunload prompts when update handoff or install is active", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals({
    updatePayload: {
      availability: "update_ready",
      downloadState: "downloaded",
      installState: "waiting_for_exit"
    }
  });
  const { initDesktopLocalDataClient } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, "");
  assert.equal(event.defaultPrevented, true);
  assert.equal(event.returnValue, "");
  assert.equal(beaconCalls.length, 0);
});

test("approved desktop page navigation bypasses the unload prompt and still signals closing", async () => {
  const { eventListeners, beaconCalls, locationState } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "sync", active: true }],
      count: 1
    }
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("saved.html", { locationObject: locationState, baseHref: locationState.href });

  assert.equal(locationState.assignCalls.length, 1);
  assert.match(locationState.assignCalls[0], /saved\.html$/);

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, undefined);
  assert.equal(event.defaultPrevented, false);
  assert.equal(event.returnValue, undefined);
  assert.equal(beaconCalls.length, 1);
});

test("external navigation does not bypass the unload prompt", async () => {
  const { eventListeners, beaconCalls, locationState } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "pipeline", active: true }],
      count: 1
    }
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("https://example.com/outside", { locationObject: locationState, baseHref: locationState.href });

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, "");
  assert.equal(event.defaultPrevented, true);
  assert.equal(event.returnValue, "");
  assert.equal(beaconCalls.length, 0);
});

test("normal unload without active work signals desktop closing", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh("../../../frontend/shared/local-data/desktop-client.js");
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, undefined);
  assert.equal(event.defaultPrevented, false);
  assert.equal(event.returnValue, undefined);
  assert.equal(beaconCalls.length, 1);
  assert.match(beaconCalls[0].url, /desktop-session-lifecycle/);
});
