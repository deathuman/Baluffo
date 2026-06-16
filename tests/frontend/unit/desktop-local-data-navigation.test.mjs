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
function installImmediateTimeoutClock() {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const originalDateNow = Date.now;
  let fakeNow = 0;
  globalThis.setTimeout = (handler, delay = 0, ...args) => {
    fakeNow += Math.max(0, Number(delay) || 0);
    handler(...args);
    return fakeNow || 1;
  };
  globalThis.clearTimeout = () => {};
  Date.now = () => fakeNow;
  return () => {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    Date.now = originalDateNow;
  };
}
function setupDesktopGlobals({
  locationHref = "http://127.0.0.1:4173/jobs.html?desktop=1",
  taskPayload = { tasks: [], count: 0 },
  updatePayload = { availability: "unknown", downloadState: "idle", installState: "idle" },
  profilesPayload = { profiles: [] },
  promptImpl = () => "Desktop User"
} = {}) {
  const localStorage = createStorageMock();
  const sessionStorage = createStorageMock();
  const eventListeners = new Map();
  const intervalHandlers = [];
  const beaconCalls = [];
  const fetchCalls = [];
  const locationState = {
    href: String(locationHref || "http://127.0.0.1:4173/jobs.html?desktop=1"),
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
    __baluffoInitErrors: [],
    setInterval(handler) {
      intervalHandlers.push(handler);
      return intervalHandlers.length;
    },
    clearInterval: () => {},
    addEventListener(name, handler) {
      eventListeners.set(name, handler);
    },
    prompt: promptImpl
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
    if (normalizedUrl.includes("/desktop-local-data/profiles")) {
      const resolvedProfilesPayload = typeof profilesPayload === "function"
        ? profilesPayload({ url: normalizedUrl, options, fetchCalls })
        : profilesPayload;
      if (resolvedProfilesPayload instanceof Error) {
        throw resolvedProfilesPayload;
      }
      return createJsonResponse({
        ok: true,
        profiles: resolvedProfilesPayload?.profiles || []
      });
    }
    if (normalizedUrl.includes("/desktop-local-data/sign-in")) {
      let body = {};
      try {
        body = JSON.parse(String(options?.body || "{}"));
      } catch {
        body = {};
      }
      const displayName = String(body?.name || "Existing User");
      return createJsonResponse({
        ok: true,
        user: {
          uid: "local_existing",
          displayName,
          email: ""
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
test("desktop beforeunload prompts when admin bridge work is active", async () => {
  const { eventListeners, beaconCalls, fetchCalls } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "fetch", lifecycleStatus: "running", status: "running" }],
      count: 1
    }
  });
  const { awaitDesktopBootstrap, initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await awaitDesktopBootstrap({ enableLifecycle: false });
  await flushMicrotasks();

  const taskStateCalls = fetchCalls.filter(call => call.url.includes("/ops/task-state"));
  assert.ok(taskStateCalls.length >= 1);
  assert.ok(taskStateCalls.every(call => call.url.includes("/ops/task-state?view=summary")));
  assert.equal(window.__baluffoDesktopLifecycleBound, true);
  const beforeUnload = eventListeners.get("beforeunload");
  assert.equal(typeof beforeUnload, "function");

  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);
  assert.equal(result, "");
  assert.equal(event.defaultPrevented, true);
  assert.equal(event.returnValue, "");
  assert.equal(beaconCalls.length, 0);
  const closeAttemptCalls = fetchCalls.filter(call => {
    if (!call.url.includes("/app/desktop-session-lifecycle")) return false;
    const body = JSON.parse(String(call.options?.body || "{}"));
    return body.state === "closing";
  });
  assert.equal(closeAttemptCalls.length, 1);
  const closeAttemptPayload = JSON.parse(String(closeAttemptCalls[0].options?.body || "{}"));
  assert.equal(closeAttemptPayload.reason, "active_work_close_attempt");
  const pagehide = eventListeners.get("pagehide");
  pagehide();
  pagehide();
  assert.equal(beaconCalls.length, 1);
  const payload = JSON.parse(String(beaconCalls[0].blob.parts[0] || "{}"));
  assert.equal(payload.reason, "confirmed_active_work_close");
});

test("desktop lifecycle binds beforeunload, pagehide, and focus but not unload", async () => {
  const { eventListeners } = setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();
  assert.equal(typeof eventListeners.get("beforeunload"), "function");
  assert.equal(typeof eventListeners.get("pagehide"), "function");
  assert.equal(typeof eventListeners.get("focus"), "function");
  assert.equal(eventListeners.has("unload"), false);
});

test("desktop bootstrap preserves persisted session hint while restoring auth", async () => {
  setupDesktopGlobals();
  window.localStorage.setItem("baluffo_current_profile_id", "local_packaged_smoke_user");
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );

  initDesktopLocalDataClient();

  assert.equal(
    window.localStorage.getItem("baluffo_current_profile_id"),
    "local_packaged_smoke_user"
  );
});

test("desktop beforeunload prompts when update handoff or install is active", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals({
    updatePayload: {
      availability: "update_ready",
      downloadState: "downloaded",
      installState: "waiting_for_exit"
    }
  });
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
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

test("approved desktop page navigation bypasses the unload prompt without signaling closing", async () => {
  const { eventListeners, beaconCalls, fetchCalls, locationState } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "sync", active: true }],
      count: 1
    }
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("saved.html", { locationObject: locationState, baseHref: locationState.href });

  assert.equal(locationState.assignCalls.length, 1);
  assert.equal(new URL(locationState.assignCalls[0]).pathname, "/saved.html");

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, undefined);
  assert.equal(event.defaultPrevented, false);
  assert.equal(event.returnValue, undefined);
  assert.equal(beaconCalls.length, 0);
  const lifecycleCallCount = fetchCalls.filter(call => (
    call.url.includes("/app/desktop-session-lifecycle")
  )).length;
  eventListeners.get("pagehide")();
  assert.equal(beaconCalls.length, 0);
  assert.equal(
    fetchCalls.filter(call => call.url.includes("/app/desktop-session-lifecycle")).length,
    lifecycleCallCount
  );
});

test("approved desktop page navigation preserves desktop runtime query params", async () => {
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  const { locationState } = setupDesktopGlobals({
    locationHref: "http://127.0.0.1:4173/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1&startupProbe=1"
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("saved.html", { locationObject: locationState, baseHref: locationState.href });

  assert.equal(locationState.assignCalls.length, 1);
  const nextUrl = new URL(locationState.assignCalls[0]);
  assert.equal(nextUrl.pathname, "/saved.html");
  assert.equal(nextUrl.searchParams.get("desktop"), "1");
  assert.equal(nextUrl.searchParams.get("bridgePort"), "8877");
  assert.equal(nextUrl.searchParams.get("bridgeHost"), "127.0.0.1");
  assert.equal(nextUrl.searchParams.get("startupProbe"), "1");
});

test("approved desktop page navigation prefers active runtime config over stale cached bridge", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      host: "127.0.0.1",
      port: 61236
    },
    security: {
      github_app_enabled_default: true
    },
    runtime: {
      desktop: true
    }
  });
  const { locationState } = setupDesktopGlobals({
    locationHref: "http://127.0.0.1:4173/jobs.html?desktop=1"
  });
  window.sessionStorage.setItem("baluffo_runtime_bridge_base", "http://127.0.0.1:8877");
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("saved.html", { locationObject: locationState, baseHref: locationState.href });

  assert.equal(locationState.assignCalls.length, 1);
  const nextUrl = new URL(locationState.assignCalls[0]);
  assert.equal(nextUrl.pathname, "/saved.html");
  assert.equal(nextUrl.searchParams.get("desktop"), "1");
  assert.equal(nextUrl.searchParams.get("bridgePort"), "61236");
  assert.equal(nextUrl.searchParams.get("bridgeHost"), "127.0.0.1");
  assert.equal(
    window.sessionStorage.getItem("baluffo_runtime_bridge_base"),
    "http://127.0.0.1:61236"
  );
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});

test("approved desktop page navigation preserves target query params while appending runtime params", async () => {
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  const { locationState } = setupDesktopGlobals({
    locationHref: "http://127.0.0.1:4173/admin.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1"
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("jobs.html?page=3&search=engineer", {
    locationObject: locationState,
    baseHref: locationState.href
  });

  const nextUrl = new URL(locationState.assignCalls[0]);
  assert.equal(nextUrl.pathname, "/jobs.html");
  assert.equal(nextUrl.searchParams.get("page"), "3");
  assert.equal(nextUrl.searchParams.get("search"), "engineer");
  assert.equal(nextUrl.searchParams.get("desktop"), "1");
  assert.equal(nextUrl.searchParams.get("bridgePort"), "8877");
  assert.equal(nextUrl.searchParams.get("bridgeHost"), "127.0.0.1");
});

test("external navigation does not bypass the unload prompt", async () => {
  const { eventListeners, beaconCalls, locationState } = setupDesktopGlobals({
    taskPayload: {
      tasks: [{ taskType: "pipeline", active: true }],
      count: 1
    }
  });
  const { initDesktopLocalDataClient, navigateDesktopPage } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  navigateDesktopPage("https://example.com/outside", { locationObject: locationState, baseHref: locationState.href });

  assert.equal(locationState.assignCalls.length, 1);
  assert.equal(locationState.assignCalls[0], "https://example.com/outside");

  const beforeUnload = eventListeners.get("beforeunload");
  const event = createBeforeUnloadEvent();
  const result = beforeUnload(event);

  assert.equal(result, "");
  assert.equal(event.defaultPrevented, true);
  assert.equal(event.returnValue, "");
  assert.equal(beaconCalls.length, 0);
});

test("normal beforeunload without active work signals desktop closing", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
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

test("pagehide without beforeunload still signals desktop closing", async () => {
  const { eventListeners, beaconCalls } = setupDesktopGlobals();
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  initDesktopLocalDataClient();
  await flushMicrotasks();

  const pagehide = eventListeners.get("pagehide");
  assert.equal(typeof pagehide, "function");

  pagehide();

  assert.equal(beaconCalls.length, 1);
  assert.match(beaconCalls[0].url, /desktop-session-lifecycle/);
});

test("desktop sign-in loads existing profiles before prompting", async () => {
  const promptCalls = [];
  setupDesktopGlobals({
    profilesPayload: {
      profiles: [
        { uid: "local_existing", displayName: "Existing User", email: "", isCurrent: true },
        { uid: "local_other", displayName: "Other User", email: "", isCurrent: false }
      ]
    },
    promptImpl: (message, initialValue) => {
      promptCalls.push({ message: String(message || ""), initialValue: String(initialValue || "") });
      return "Existing User";
    }
  });
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  const api = initDesktopLocalDataClient();
  await flushMicrotasks();

  const result = await api.signIn();

  assert.equal(result.user.displayName, "Existing User");
  assert.equal(promptCalls.length, 1);
  assert.match(promptCalls[0].message, /Choose an existing local profile/i);
  assert.equal(promptCalls[0].initialValue, "Existing User");
});

test("desktop sign-in retries profile loading before showing the existing-profile picker", async () => {
  const promptCalls = [];
  let profileLoadAttempts = 0;
  setupDesktopGlobals({
    profilesPayload: () => {
      profileLoadAttempts += 1;
      if (profileLoadAttempts === 1) {
        throw new Error("profiles unavailable");
      }
      return {
        profiles: [
          { uid: "local_existing", displayName: "Existing User", email: "", isCurrent: true }
        ]
      };
    },
    promptImpl: (message, initialValue) => {
      promptCalls.push({ message: String(message || ""), initialValue: String(initialValue || "") });
      return promptCalls.length === 1 ? "retry" : "Existing User";
    }
  });
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  const api = initDesktopLocalDataClient();
  await flushMicrotasks();

  const result = await api.signIn();

  assert.equal(result.user.displayName, "Existing User");
  assert.equal(profileLoadAttempts, 2);
  assert.equal(promptCalls.length, 2);
  assert.match(promptCalls[0].message, /Could not load existing local profiles/i);
  assert.equal(promptCalls[0].initialValue, "retry");
  assert.match(promptCalls[1].message, /Choose an existing local profile/i);
});

test("desktop sign-in requires an explicit create action when profile loading fails", async () => {
  const promptCalls = [];
  const { fetchCalls } = setupDesktopGlobals({
    profilesPayload: () => new Error("profiles unavailable"),
    promptImpl: (message, initialValue) => {
      promptCalls.push({ message: String(message || ""), initialValue: String(initialValue || "") });
      return promptCalls.length === 1 ? "create" : "New Desktop User";
    }
  });
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  const api = initDesktopLocalDataClient();
  await flushMicrotasks();

  const result = await api.signIn();

  assert.equal(result.user.displayName, "New Desktop User");
  assert.equal(promptCalls.length, 2);
  assert.match(promptCalls[0].message, /Retry to load them again, create a new local profile, or cancel sign-in/i);
  assert.match(promptCalls[1].message, /Create a new local profile for this device to continue/i);
  const signInCall = fetchCalls.find(call => call.url.includes("/desktop-local-data/sign-in"));
  assert.ok(signInCall);
  assert.match(String(signInCall.options?.body || ""), /New Desktop User/);
});

test("desktop sign-in cancels cleanly when profile loading fails and the user aborts", async () => {
  const { fetchCalls } = setupDesktopGlobals({
    profilesPayload: () => new Error("profiles unavailable"),
    promptImpl: () => "cancel"
  });
  const { initDesktopLocalDataClient } = await importFresh(
    "../../../frontend/shared/local-data/desktop-client.js",
    { relativeTo: import.meta.url }
  );
  const api = initDesktopLocalDataClient();
  await flushMicrotasks();

  await assert.rejects(() => api.signIn(), /Sign-in cancelled\./);
  assert.equal(fetchCalls.filter(call => call.url.includes("/desktop-local-data/sign-in")).length, 0);
});

test("desktop bootstrap retries bridge startup while version labels hydrate independently", async () => {
  const restoreClock = installImmediateTimeoutClock();
  const labels = [{ hidden: true, textContent: "" }];
  const consoleErrors = [];
  const originalConsoleError = console.error;
  console.error = (...args) => {
    consoleErrors.push(args.map(value => String(value)).join(" "));
  };
  try {
    const { fetchCalls } = setupDesktopGlobals({
      updatePayload: {
        currentVersion: "0.1.33",
        availability: "unknown",
        downloadState: "idle",
        installState: "idle"
      }
    });
    global.document = {
      querySelectorAll() {
        return labels;
      }
    };
    const requestOrder = [];
    let sessionAttempts = 0;
    const baseFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      requestOrder.push(String(url));
      if (String(url).includes("/desktop-local-data/session")) {
        sessionAttempts += 1;
        if (sessionAttempts < 3) {
          throw new Error("bridge unavailable");
        }
      }
      return baseFetch(url, options);
    };

    await importFresh("../../../frontend/shared/local-data/app-client.js", {
      relativeTo: import.meta.url
    });
    for (let index = 0; index < 20 && !window.__baluffoLocalDataLoaded; index += 1) {
      await flushMicrotasks(5);
    }
    for (let index = 0; index < 20 && !String(labels[0].textContent || "").includes("0.1.33"); index += 1) {
      await flushMicrotasks(5);
    }

    const firstUpdateIndex = requestOrder.findIndex(url => url.includes("/app/update-status"));
    const lastSessionIndex = requestOrder.reduce(
      (index, url, callIndex) => (url.includes("/desktop-local-data/session") ? callIndex : index),
      -1
    );

    assert.equal(sessionAttempts, 3);
    assert.ok(firstUpdateIndex >= 0);
    assert.ok(lastSessionIndex >= 0);
    assert.equal(
      fetchCalls.some(call => call.url.includes("/app/desktop-session-lifecycle")),
      true
    );
    assert.equal(window.__baluffoLocalDataLoaded, true);
    assert.deepEqual(labels.map(label => label.textContent), ["Version 0.1.33"]);
    assert.ok(labels.every(label => label.hidden === false));
    assert.equal(consoleErrors.length, 0);
  } finally {
    console.error = originalConsoleError;
    restoreClock();
  }
});

test("desktop bootstrap logs one final failure and never starts lifecycle when bridge stays unavailable", async () => {
  const restoreClock = installImmediateTimeoutClock();
  const consoleErrors = [];
  const originalConsoleError = console.error;
  console.error = (...args) => {
    consoleErrors.push(args.map(value => String(value)).join(" "));
  };
  try {
    const { fetchCalls, intervalHandlers } = setupDesktopGlobals();
    global.document = { querySelectorAll() { return []; } };
    global.fetch = async (url, options = {}) => {
      fetchCalls.push({ url: String(url), options });
      if (String(url).includes("/desktop-local-data/session")) {
        throw new Error("bridge unavailable");
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const { awaitDesktopBootstrap, initDesktopLocalDataClient } = await importFresh(
      "../../../frontend/shared/local-data/desktop-client.js",
      { relativeTo: import.meta.url }
    );
    const api = initDesktopLocalDataClient();
    const ready = await awaitDesktopBootstrap();
    await flushMicrotasks(6);

    assert.equal(ready, false);
    assert.equal(api.getCurrentUser(), null);
    assert.equal(fetchCalls.some(call => call.url.includes("/app/desktop-session-lifecycle")), false);
    assert.equal(intervalHandlers.length, 0);
    assert.ok(fetchCalls.filter(call => call.url.includes("/desktop-local-data/session")).length > 1);
    assert.equal(consoleErrors.length, 1);
    assert.match(consoleErrors[0], /bootstrap failed/i);
  } finally {
    console.error = originalConsoleError;
    restoreClock();
  }
});

test("desktop version labels render the installed app version", async () => {
  setupDesktopGlobals({
    updatePayload: {
      currentVersion: "0.1.33",
      availability: "unknown",
      downloadState: "idle",
      installState: "idle"
    }
  });
  const labels = [
    { hidden: true, textContent: "" },
    { hidden: true, textContent: "" }
  ];
  const { hydrateDesktopVersionLabels } = await importFresh(
    "../../../frontend/shared/app-version.js",
    { relativeTo: import.meta.url }
  );

  const version = await hydrateDesktopVersionLabels({
    querySelectorAll() {
      return labels;
    }
  }, { attempts: 1 });

  assert.equal(version, "0.1.33");
  assert.deepEqual(labels.map(label => label.textContent), ["Version 0.1.33", "Version 0.1.33"]);
  assert.ok(labels.every(label => label.hidden === false));
});
