import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { createClassList, createDiscoveryControllerFixture, createElement, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

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
