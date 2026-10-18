import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { createClassList, createElement } from "./helpers/admin-controller-test-helpers.mjs";

test("admin discovery controller recovers when launch response is lost but report shows a fresh run", async () => {
  const logs = [];
  const toasts = [];
  const calls = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        discoveryRun: false,
        discoveryWatch: false,
        discoveryLoad: false,
        discoveryWrite: false,
        manualAdd: false,
        manualCheck: false,
        liveDiscoveryRunning: false
      }
    };
    const refs = {
      adminDiscoveryLogEl: createElement(),
      adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminDiscoveryProgressBarEl: createElement({ style: {} }),
      adminDiscoveryProgressLabelEl: createElement()
    };
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
          return {
            startedAt: new Date().toISOString(),
            finishedAt: "",
            summary: { queuedCandidateCount: 0, foundEndpointCount: 0, probedCandidateCount: 0, failedProbeCount: 0 },
            candidates: [],
            failures: []
          };
        }
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => {
        throw new Error("Network error: bridge unreachable");
      },
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast(message, level) {
        toasts.push({ message, level });
      },
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      },
      loadOpsHealthData: async () => {
        calls.push("loadOpsHealthData");
      },
      scheduleOpsHealthPolling(delay) {
        calls.push(`scheduleOpsHealthPolling:${delay}`);
      },
      loadDiscoveryData: async () => {}
    });

    await controller.runDiscoveryTask();

    assert.equal(state.adminBusyState.discoveryWatch, true);
    assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
    assert.ok(calls.includes("/discovery/report"));
    assert.ok(logs.some(line => /response was lost, but the run is active/i.test(line)));
    assert.ok(toasts.some(item => /reattached after a dropped bridge response/i.test(item.message) && item.level === "warning"));
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller still shows error when launch recovery finds no fresh run", async () => {
  const toasts = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
        return {
          startedAt: "2020-01-01T00:00:00.000Z",
          finishedAt: "",
          summary: {},
          candidates: [],
          failures: []
        };
      }
      if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
        return { text: "", nextOffset: 0 };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => {
      throw new Error("Network error: bridge unreachable");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask();

  assert.equal(state.adminBusyState.discoveryWatch, false);
  assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
  assert.ok(toasts.some(item => item.message === "Could not trigger source discovery task." && item.level === "error"));
});

test("admin discovery controller does not reattach when the fresh report is a launch failure", async () => {
  const logs = [];
  const toasts = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
        return {
          startedAt: new Date().toISOString(),
          finishedAt: new Date().toISOString(),
          summary: { failedProbeCount: 1 },
          candidates: [],
          failures: [
            { adapter: "bridge", stage: "launch", error: "WinError 233" }
          ]
        };
      }
      if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
        return { text: "[2026-03-08T10:01:01.000Z] Launch failed: [WinError 233] No process is on the other end of the pipe\n", nextOffset: 99 };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => {
      throw new Error("Network error: bridge unreachable");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow(_container, event) {
      logs.push(String(event.message || ""));
    },
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask();

  assert.equal(state.adminBusyState.discoveryWatch, false);
  assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
  assert.ok(logs.some(line => /launch failed/i.test(line)));
  assert.ok(!logs.some(line => /reattaching to live progress/i.test(line)));
  assert.ok(toasts.some(item => item.message === "Could not trigger source discovery task." && item.level === "error"));
});
