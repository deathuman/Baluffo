import test from "node:test";
import assert from "node:assert/strict";
import { createStorageMock, importFresh } from "./helpers/browser-test-helpers.mjs";

function jsonResponse(payload) {
  return { ok: true, json: async () => payload };
}

function beforeUnloadEvent() {
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

function setupDesktopLifecycleGlobals() {
  const eventListeners = new Map();
  const beaconCalls = [];
  const fetchCalls = [];
  global.window = {
    localStorage: createStorageMock(),
    sessionStorage: createStorageMock(),
    location: {
      href: "http://127.0.0.1:4173/admin.html?desktop=1"
    },
    __baluffoInitErrors: [],
    setInterval: () => 1,
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
      return jsonResponse({
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
      return jsonResponse({ ok: true });
    }
    if (normalizedUrl.includes("/ops/task-state")) {
      return jsonResponse({ tasks: [{ taskType: "sync", active: true }], count: 1 });
    }
    if (normalizedUrl.includes("/app/update-status")) {
      return jsonResponse({ availability: "unknown", downloadState: "idle", installState: "idle" });
    }
    if (normalizedUrl.includes("/desktop-local-data/profiles")) {
      return jsonResponse({ ok: true, profiles: [] });
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
  return { eventListeners, beaconCalls, fetchCalls };
}

test("desktop reload shortcuts bypass close shutdown while active work is present", async () => {
  const { eventListeners, beaconCalls, fetchCalls } = setupDesktopLifecycleGlobals();
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const keydown = eventListeners.get("keydown");
  const beforeUnload = eventListeners.get("beforeunload");
  const pagehide = eventListeners.get("pagehide");
  assert.equal(typeof keydown, "function");
  assert.equal(typeof beforeUnload, "function");
  assert.equal(typeof pagehide, "function");

  for (const eventPayload of [
    { key: "F5", code: "F5", ctrlKey: false, shiftKey: false },
    { key: "r", code: "KeyR", ctrlKey: true, shiftKey: true }
  ]) {
    keydown(eventPayload);
    const event = beforeUnloadEvent();
    const result = beforeUnload(event);
    assert.equal(result, undefined);
    assert.equal(event.defaultPrevented, false);
    assert.equal(event.returnValue, undefined);
    pagehide();
  }

  assert.equal(beaconCalls.length, 0);
  const closeCalls = fetchCalls.filter(call => {
    if (!call.url.includes("/app/desktop-session-lifecycle")) return false;
    const body = JSON.parse(String(call.options?.body || "{}"));
    return body.state === "closing";
  });
  assert.equal(closeCalls.length, 0);
});
