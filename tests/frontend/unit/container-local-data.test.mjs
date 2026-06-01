import test from "node:test";
import assert from "node:assert/strict";
import {
  createStorageMock,
  importFresh
} from "./helpers/browser-test-helpers.mjs";

function createJsonResponse(payload) {
  return {
    ok: true,
    json: async () => payload
  };
}

async function flushMicrotasks(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
  await new Promise(resolve => setTimeout(resolve, 0));
}

function setupContainerGlobals() {
  const localStorage = createStorageMock();
  const sessionStorage = createStorageMock();
  const eventListeners = new Map();
  const intervalHandlers = [];
  const beaconCalls = [];
  const fetchCalls = [];
  const locationState = {
    href: "http://192.168.50.61:8877/jobs.html",
    assignCalls: [],
    assign(url) {
      const nextUrl = String(url || "");
      this.assignCalls.push(nextUrl);
      this.href = nextUrl;
    }
  };
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      sameOrigin: true
    },
    runtime: {
      mode: "container",
      localDataMode: "bridge"
    }
  });
  global.window = {
    localStorage,
    sessionStorage,
    location: locationState,
    __baluffoInitErrors: [],
    setInterval(handler) {
      intervalHandlers.push(handler);
      return intervalHandlers.length;
    },
    clearInterval: () => {},
    addEventListener(name, handler) {
      eventListeners.set(name, handler);
    },
    prompt: () => "Container User"
  };
  global.document = { querySelectorAll() { return []; } };
  global.fetch = async (url, options = {}) => {
    const normalizedUrl = String(url);
    fetchCalls.push({ url: normalizedUrl, options });
    if (normalizedUrl.includes("/desktop-local-data/session")) {
      return createJsonResponse({
        ok: true,
        user: null,
        desktopSession: {
          sessionId: "container-session-1",
          ownerToken: "container-owner-1",
          lastActivityAt: "2026-06-01T10:00:00Z"
        }
      });
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

test("container navigation skips desktop runtime query params", async () => {
  const { locationState } = setupContainerGlobals();
  try {
    const { navigateDesktopPage } = await importFresh(
      "../../../frontend/shared/local-data/desktop-client.js",
      { relativeTo: import.meta.url }
    );

    navigateDesktopPage("saved.html", {
      locationObject: locationState,
      baseHref: locationState.href
    });

    const nextUrl = new URL(locationState.assignCalls[0]);
    assert.equal(nextUrl.pathname, "/saved.html");
    assert.equal(nextUrl.searchParams.has("desktop"), false);
    assert.equal(nextUrl.searchParams.has("bridgePort"), false);
    assert.equal(nextUrl.searchParams.has("bridgeHost"), false);
  } finally {
    delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  }
});

test("container app client uses bridge local data without desktop lifecycle", async () => {
  const { beaconCalls, eventListeners, fetchCalls, intervalHandlers } = setupContainerGlobals();
  try {
    await importFresh("../../../frontend/shared/local-data/app-client.js", {
      relativeTo: import.meta.url
    });
    for (let index = 0; index < 20 && !window.__baluffoLocalDataLoaded; index += 1) {
      await flushMicrotasks(5);
    }

    assert.equal(window.__baluffoRuntimeMode, "container");
    assert.equal(window.__baluffoDesktopMode, false);
    assert.equal(window.__baluffoBridgeLocalDataMode, true);
    assert.equal(window.__baluffoLocalDataLoaded, true);
    assert.ok(fetchCalls.some(call => call.url.match(/^\/desktop-local-data\/session/)));
    assert.equal(fetchCalls.some(call => call.url.includes("127.0.0.1:8877")), false);
    assert.equal(fetchCalls.some(call => call.url.includes("/app/desktop-session-lifecycle")), false);
    assert.equal(fetchCalls.some(call => call.url.includes("/ops/task-state")), false);
    assert.equal(fetchCalls.some(call => call.url.includes("/app/update-status")), false);
    assert.equal(eventListeners.size, 0);
    assert.equal(beaconCalls.length, 0);
    assert.equal(intervalHandlers.length, 0);
  } finally {
    delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  }
});
