import test from "node:test";
import assert from "node:assert/strict";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import {
  createClassList,
  createDiscoveryControllerFixture,
  createElement,
  stubDateNow,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

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

test("admin discovery controller applies live progress when runId matches despite startedAt skew", async () => {
  const logs = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:10:00.500Z");

  try {
    const barEl = createElement({ style: {} });
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
      adminDiscoveryProgressBarEl: barEl,
      adminDiscoveryProgressLabelEl: createElement(),
      adminRunDiscoveryUncappedBtnEl: createElement()
    };
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/ops/task-live/discovery?view=summary") {
          return {
            taskType: "discovery",
            active: true,
            runId: "discovery_skew_1",
            startedAt: "2026-03-08T10:05:00.000Z",
            taskProgress: {
              active: true,
              phaseKey: "scanning_sources",
              phaseLabel: "Scanning known careers pages",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                foundEndpoints: 12,
                probedCandidates: 5,
                queuedCandidates: 3,
                deferredCandidates: 4,
                failedProbes: 1
              }
            },
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
            workItems: [
              {
                id: "sheet_directory",
                name: "Sheet directory",
                status: "running",
                progress: {
                  phaseKey: "scanning_sources",
                  phaseLabel: "Scanning known careers pages",
                  counts: {
                    foundEndpoints: 12,
                    probedCandidates: 5,
                    queuedCandidates: 3
                  },
                  updatedAt: "2026-03-08T10:05:01.000Z"
                }
              }
            ]
          };
        }
        if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
          return {
            runId: "discovery_skew_1",
            startedAt: "2026-03-08T10:05:00.000Z",
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
            candidates: [{ adapter: "greenhouse" }],
            failures: [{ stage: "timeout", error: "request timed out" }]
          };
        }
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({
        started: true,
        runId: "discovery_skew_1",
        startedAt: "2026-03-08T10:10:00.000Z"
      }),
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast() {},
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:10:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      },
      loadOpsHealthData: async () => {},
      scheduleOpsHealthPolling() {},
    });

    await controller.runDiscoveryTask();
    await scheduled[0]();
    await new Promise(resolve => setImmediate(resolve));

    assert.ok(logs.some(line => /discovery started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /scanning known careers pages/i.test(line)));
    assert.ok(logs.some(line => /Discovery: .*Scanning known careers pages/i.test(line) && /endpoints 12/i.test(line) && /probed 5/i.test(line) && /queued 3/i.test(line)));
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin discovery controller syncs source tables once after completion", async () => {
  const syncCalls = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: {
        runId: "discovery_done_1",
        startedAt: "2026-03-08T10:00:00.000Z"
      },
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
        if (path === "/ops/task-live/discovery?view=summary") {
          return null;
        }
        if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
          return {
            runId: "discovery_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:03:00.000Z",
            summary: {
              foundEndpointCount: 4,
              probedCandidateCount: 6,
              queuedCandidateCount: 1,
              discoverableButDeferredCount: 0,
              failedProbeCount: 0
            }
          };
        }
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({
        started: true,
        runId: "discovery_done_1",
        startedAt: "2026-03-08T10:00:00.000Z"
      }),
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast() {},
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {},
      loadOpsHealthData: async () => {},
      scheduleOpsHealthPolling() {},
      syncSourceTablesAfterTaskCompletion: async payload => {
        syncCalls.push(payload);
      },
    });

    controller.startDiscoveryCompletionWatch();
    await scheduled[scheduled.length - 1]();

    assert.equal(syncCalls.length, 1);
    assert.equal(syncCalls[0].taskType, "discovery");
    assert.equal(syncCalls[0].completionSignature, "discovery_done_1|2026-03-08T10:03:00.000Z");
    assert.equal(state.adminBusyState.discoveryWatch, false);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller waits for registry finalization before source table sync", async () => {
  const timerStub = stubScheduledTimers();
  const syncCalls = [];
  try {
    const fixture = createDiscoveryControllerFixture({
      state: {
        discoveryLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
        discoveryOptimisticRun: {
          runId: "discovery_finalizing_1",
          startedAt: "2026-03-08T10:00:00.000Z"
        }
      },
      options: {
        getBridge: async path => {
          fixture.calls.push(path);
          if (path === "/ops/task-live/discovery?view=summary") return null;
          if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
            return {
              runId: "discovery_finalizing_1",
              startedAt: "2026-03-08T10:00:00.000Z",
              finishedAt: "2026-03-08T10:03:00.000Z",
              runtime: {
                registryFinalization: { status: "running" },
                autoApproval: { enabled: true, status: "completed" }
              },
              summary: { foundEndpointCount: 1, probedCandidateCount: 1, failedProbeCount: 0 }
            };
          }
          if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) return { text: "", nextOffset: 0 };
          throw new Error(`unexpected path ${path}`);
        },
        syncSourceTablesAfterTaskCompletion: async payload => {
          syncCalls.push(payload);
        }
      }
    });
    const controller = createAdminDiscoveryController(fixture.options);

    controller.startDiscoveryCompletionWatch();
    await timerStub.scheduled[timerStub.scheduled.length - 1]();

    assert.equal(syncCalls.length, 0);
    assert.equal(fixture.state.adminBusyState.discoveryWatch, true);
    assert.ok(fixture.logs.some(line => /finalizing source registries/i.test(line)));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    timerStub.restore();
  }
});

test("admin discovery controller completes when finalization is terminal and auto approval failed", async () => {
  const timerStub = stubScheduledTimers();
  const syncCalls = [];
  try {
    const fixture = createDiscoveryControllerFixture({
      state: {
        discoveryLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
        discoveryOptimisticRun: {
          runId: "discovery_done_failed_auto_approval",
          startedAt: "2026-03-08T10:00:00.000Z"
        }
      },
      options: {
        getBridge: async path => {
          fixture.calls.push(path);
          if (path === "/ops/task-live/discovery?view=summary") return null;
          if (path === "/discovery/report" || path === "/discovery/report?view=summary") {
            return {
              runId: "discovery_done_failed_auto_approval",
              startedAt: "2026-03-08T10:00:00.000Z",
              finishedAt: "2026-03-08T10:03:00.000Z",
              runtime: {
                registryFinalization: { status: "completed" },
                autoApproval: { enabled: true, status: "failed" }
              },
              summary: { foundEndpointCount: 1, probedCandidateCount: 1, failedProbeCount: 0 }
            };
          }
          if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) return { text: "", nextOffset: 0 };
          throw new Error(`unexpected path ${path}`);
        },
        syncSourceTablesAfterTaskCompletion: async payload => {
          syncCalls.push(payload);
        }
      }
    });
    const controller = createAdminDiscoveryController(fixture.options);

    controller.startDiscoveryCompletionWatch();
    await timerStub.scheduled[timerStub.scheduled.length - 1]();

    assert.equal(syncCalls.length, 1);
    assert.equal(syncCalls[0].taskType, "discovery");
    assert.equal(fixture.state.adminBusyState.discoveryWatch, false);
  } finally {
    timerStub.restore();
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

test("admin discovery controller loads and saves auto-approve config", async () => {
  const calls = [];
  const toasts = [];
  const state = {
    latestDiscoveryConfigCache: null,
    discoveryConfigDirty: false,
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
    adminDiscoveryAutoApproveToggleEl: createElement({ checked: true })
  };

  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: false
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: Boolean(payload?.autoApproveHealthyPendingOnComplete)
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
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

  const loaded = await controller.loadDiscoveryConfig({ forceForm: true });
  assert.equal(loaded.savedConfig.autoApproveHealthyPendingOnComplete, false);
  assert.equal(refs.adminDiscoveryAutoApproveToggleEl.checked, false);

  refs.adminDiscoveryAutoApproveToggleEl.checked = true;
  state.discoveryConfigDirty = true;
  await controller.saveDiscoveryConfig();

  assert.ok(calls.includes("/discovery/config"));
  assert.ok(calls.includes('/discovery/config:{"autoApproveHealthyPendingOnComplete":true}'));
  assert.equal(state.discoveryConfigDirty, false);
  assert.equal(state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, true);
  assert.ok(
    toasts.some(
      item =>
        item.message === "Discovery auto-approve preference updated." &&
        item.level === "success"
    )
  );
});

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

test("admin discovery controller updates progress label from server log phases before report progress arrives", () => {
  const logs = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: {
      phaseLabel: "",
      summarySignature: "",
      candidateCount: 0,
      failureCount: 0,
      serverPhaseLabel: "",
      serverLogSignatures: new Set(),
      lastHeartbeatAtMs: 0
    },
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: true,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: true
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
    getBridge: async () => ({}),
    postBridge: async () => ({}),
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast() {},
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

  controller.appendDiscoveryServerLogText("[2026-03-08T10:01:01.000Z] Scanning known careers pages from the seed catalog.\n");

  assert.match(refs.adminDiscoveryProgressLabelEl.textContent, /Scanning known careers pages/i);
  assert.ok(logs.some(line => /Scanning known careers pages/i.test(line)));
});


test("admin discovery controller hydrates progress from the report when live payload is empty", async () => {
  const timerStub = stubScheduledTimers();

  try {
    const fixture = createDiscoveryControllerFixture({
      state: {
        latestDiscoveryReportCache: null,
        adminBusyState: {
          discoveryRun: false,
          discoveryWatch: false,
          liveDiscoveryRunning: false
        }
      }
    });
    fixture.options.getBridge = async path => {
        if (path === "/ops/task-live/discovery?view=summary") return {};
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
              failedProbeCount: 1
            },
            candidates: [{ adapter: "greenhouse" }],
            failures: []
          };
        }
        if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
          return { text: "", nextOffset: 0 };
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
    assert.match(String(fixture.refs.adminDiscoveryProgressLabelEl.textContent || ""), /scanning known careers pages/i);
    assert.ok(!fixture.logs.some(line => /discovery: endpoints 12/i.test(line)));
  } finally {
    timerStub.restore();
  }
});

test("admin discovery controller skips overlapping live and log polls", async () => {
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let resolveLive;
  let resolveLog;
  let liveCalls = 0;
  let logCalls = 0;

  try {
    const fixture = createDiscoveryControllerFixture();
    fixture.options.getBridge = async path => {
      if (path === "/ops/task-live/discovery?view=summary") {
        liveCalls += 1;
        return new Promise(resolve => {
          resolveLive = resolve;
        });
      }
      if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
        logCalls += 1;
        return new Promise(resolve => {
          resolveLog = resolve;
        });
      }
      if (path === "/discovery/report" || path === "/discovery/report?view=summary") return {};
      throw new Error(`unexpected path ${path}`);
    };
    const controller = createAdminDiscoveryController(fixture.options);

    controller.startDiscoveryCompletionWatch();
    const firstPoll = scheduled[0]();
    const secondPoll = scheduled[0]();
    await Promise.resolve();

    assert.equal(liveCalls, 1);
    assert.equal(logCalls, 1);

    resolveLive({});
    resolveLog({ text: "", nextOffset: 0 });
    await Promise.all([firstPoll, secondPoll]);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller backs off after transport failures and resets after success", async () => {
  const scheduled = [];
  const delays = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = (callback, delay) => {
    scheduled.push(callback);
    delays.push(delay);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let failLive = true;

  try {
    const fixture = createDiscoveryControllerFixture();
    fixture.options.getBridge = async path => {
      if (path === "/ops/task-live/discovery?view=summary") {
        if (failLive) throw new Error("Network error: bridge unreachable");
        return {};
      }
      if ((String(path).startsWith("/discovery/log?offset=") || String(path).startsWith("/discovery/log?view=tail"))) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/discovery/report" || path === "/discovery/report?view=summary") return {};
      throw new Error(`unexpected path ${path}`);
    };
    const controller = createAdminDiscoveryController(fixture.options);

    controller.startDiscoveryCompletionWatch();
    await scheduled[0]();
    await scheduled[1]();
    failLive = false;
    await scheduled[2]();

    assert.equal(delays[0], 0);
    assert.ok(delays[1] >= 500);
    assert.ok(delays[2] >= 500);
    assert.equal(delays[3], 500);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});
