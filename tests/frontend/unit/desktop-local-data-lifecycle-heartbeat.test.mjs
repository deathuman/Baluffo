import test from "node:test";
import assert from "node:assert/strict";

import { createStorageMock, importFresh } from "./helpers/browser-test-helpers.mjs";

function jsonResponse(payload) {
  return { ok: true, json: async () => payload };
}

async function flushMicrotasks(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
  await new Promise(resolve => setTimeout(resolve, 0));
}

function setupDesktopGlobals() {
  const fetchCalls = [];
  const intervalHandlers = [];
  global.window = {
    localStorage: createStorageMock(),
    sessionStorage: createStorageMock(),
    location: { href: "http://127.0.0.1:4173/jobs.html?desktop=1" },
    __baluffoInitErrors: [],
    setInterval(handler) {
      intervalHandlers.push(handler);
      return intervalHandlers.length;
    },
    clearInterval: () => {},
    addEventListener() {},
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
      return jsonResponse({ tasks: [], count: 0 });
    }
    if (normalizedUrl.includes("/app/update-status")) {
      return jsonResponse({ availability: "unknown", downloadState: "idle", installState: "idle" });
    }
    throw new Error(`unexpected fetch: ${normalizedUrl}`);
  };
  return { fetchCalls, intervalHandlers };
}

test("desktop lifecycle heartbeat keeps an idle visible page alive without keepalive quota", async () => {
  const { fetchCalls, intervalHandlers } = setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const lifecycleCallsBefore = fetchCalls.filter(call => (
    call.url.includes("/app/desktop-session-lifecycle")
  ));
  assert.ok(lifecycleCallsBefore.length >= 1);
  assert.ok(intervalHandlers.length >= 1);

  intervalHandlers[0]();
  await flushMicrotasks();

  const aliveCalls = fetchCalls.filter(call => {
    if (!call.url.includes("/app/desktop-session-lifecycle")) return false;
    return JSON.parse(String(call.options?.body || "{}")).state === "alive";
  });
  assert.ok(aliveCalls.length > lifecycleCallsBefore.length);
  assert.equal(aliveCalls.at(-1).options?.keepalive, undefined);
});
