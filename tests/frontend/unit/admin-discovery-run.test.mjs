import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { createDiscoveryControllerFixture, createElement, stubDateNow, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

test("admin discovery controller stores optimistic run metadata while discovery watch is active", async () => {
  const timerStub = stubScheduledTimers();

  try {
    const fixture = createDiscoveryControllerFixture({
      refs: {
        adminDiscoveryLogEl: createElement()
      }
    });
    fixture.options.getBridge = async path => {
      fixture.calls.push(path);
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
    };
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
      return {
        started: true,
        runId: "discovery_123",
        startedAt: "2026-03-08T10:01:00.000Z"
      };
    };
    fixture.options.loadOpsHealthData = async () => {
      fixture.calls.push("loadOpsHealthData");
    };
    fixture.options.scheduleOpsHealthPolling = delay => {
      fixture.calls.push(`scheduleOpsHealthPolling:${delay}`);
    };
    fixture.options.loadDiscoveryData = async () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    await controller.runDiscoveryTask();

    assert.deepEqual(fixture.state.discoveryOptimisticRun, {
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.discoveryWatch, true);
    assert.equal(fixture.state.adminBusyState.liveDiscoveryRunning, false);
    assert.ok(fixture.calls.includes("/tasks/run-discovery"));
    assert.ok(fixture.calls.includes("loadOpsHealthData"));
    assert.ok(fixture.calls.includes("scheduleOpsHealthPolling:250"));
    assert.ok(fixture.logs.some(line => /source discovery task started/i.test(line)));
    assert.ok(fixture.toasts.some(item => item.message === "Source discovery started." && item.level === "success"));
    assert.deepEqual(fixture.busyTransitions, [
      "discoveryRun:true",
      "discoveryWatch:false",
      "discoveryWatch:true",
      "discoveryRun:false"
    ]);
  } finally {
    timerStub.restore();
  }
});

test("admin discovery controller attaches to an already-running bridge task on conflict", async () => {
  const logs = [];
  const toasts = [];
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
      adminDiscoveryLogEl: createElement()
    };
    const busyTransitions = [];
    const calls = [];
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
        }
        return {};
      },
      postBridge: async path => {
        calls.push(path);
        return {
          status: 409,
          data: {
            started: false,
            alreadyRunning: true,
            runId: "discovery_live_1",
            startedAt: "2026-03-08T10:01:00.000Z",
            task: "source_discovery",
            taskType: "discovery",
            pid: 321,
            status: "running"
          }
        };
      },
      setBusyFlag(key, value) {
        busyTransitions.push(`${key}:${String(value)}`);
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

    assert.deepEqual(state.discoveryOptimisticRun, {
      runId: "discovery_live_1",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(state.adminBusyState.discoveryWatch, true);
    assert.ok(calls.includes("/tasks/run-discovery"));
    assert.ok(calls.includes("loadOpsHealthData"));
    assert.ok(calls.includes("scheduleOpsHealthPolling:250"));
    assert.ok(logs.some(line => /discovery already running; attached/i.test(line)));
    assert.ok(toasts.some(item => item.message === "Source discovery already running. Attached to active run." && item.level === "info"));
    assert.deepEqual(busyTransitions, [
      "discoveryRun:true",
      "discoveryWatch:false",
      "discoveryWatch:true",
      "discoveryRun:false"
    ]);
    assert.ok(scheduled.length >= 1);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller emits summary-first live progress and updates progress bar", async () => {
  const timerStub = stubScheduledTimers();
  const dateStub = stubDateNow(Date.parse("2026-03-08T10:01:00.500Z"));

  try {
    const barEl = createElement({ style: {} });
    const fixture = createDiscoveryControllerFixture({
      refs: {
        adminDiscoveryProgressBarEl: barEl
      }
    });
    fixture.options.getBridge = async path => {
        if (path === "/ops/task-live/discovery?view=summary") {
          return {
            runId: "discovery_123",
            startedAt: "2026-03-08T10:01:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "scanning_sources",
              phaseLabel: "Scanning known careers pages",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                foundEndpoints: 12,
                probedCandidates: 5,
                probeTotal: 10,
                queuedCandidates: 3,
                deferredCandidates: 4,
                failedProbes: 1
              }
            },
            summary: {
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1
            },
            workItems: [
              {
                id: "greenhouse",
                name: "greenhouse",
                status: "running",
                progress: {
                  phaseKey: "scanning_sources",
                  phaseLabel: "Scanning known careers pages",
                  counts: { queuedCount: 2 },
                  updatedAt: "2026-03-08T10:01:01.000Z"
                }
              }
            ]
          };
        }
        if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
          return {
            startedAt: "2026-03-08T10:01:00.000Z",
            finishedAt: "",
            summary: {
              phaseLabel: "Scanning known careers pages",
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1,
              skippedDuplicateCount: 2,
              skippedInvalidCount: 0
            },
            candidates: [
              { adapter: "greenhouse" },
              { adapter: "greenhouse" },
              { adapter: "teamtailor" }
            ],
            failures: [
              { stage: "timeout", error: "request timed out" }
            ]
          };
        }
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return {
            text: "[2026-03-08T10:01:01.000Z] Scanning known careers pages from the seed catalog.\n[2026-03-08T10:01:02.000Z] found 12 candidates, probed 5, queued 3\n[2026-03-08T10:01:03.000Z] timeout while probing\n",
            nextOffset: 99
          };
        }
        throw new Error(`unexpected path ${path}`);
    };
    fixture.options.postBridge = async () => ({
      started: true,
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    fixture.options.loadOpsHealthData = async () => {};
    fixture.options.scheduleOpsHealthPolling = () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    await controller.runDiscoveryTask();
    await timerStub.scheduled[0]();

    assert.ok(fixture.logs.some(line => /discovery started\. watching live progress/i.test(line)));
    assert.ok(fixture.logs.some(line => /scanning known careers pages/i.test(line)));
    assert.ok(fixture.logs.some(line => /found 12 candidates, probed 5, queued 3/i.test(line)));
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("determinate"), true);
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("indeterminate"), false);
    assert.equal(fixture.refs.adminDiscoveryProgressBarEl.style.width, "50%");
    assert.match(fixture.refs.adminDiscoveryProgressLabelEl.textContent, /discovery:/i);
  } finally {
    timerStub.restore();
    dateStub.restore();
  }
});

test("admin discovery controller forwards uncapped preset payload", async () => {
  const calls = [];
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
    adminDiscoveryLogEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
        return { text: "", nextOffset: 0 };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      return {
        started: true,
        runId: "discovery_uncapped",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "uncapped"
      };
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast() {},
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask({ preset: "uncapped" });

  assert.ok(calls.includes('/tasks/run-discovery:{"preset":"uncapped"}'));
});
