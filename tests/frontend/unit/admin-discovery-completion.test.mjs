import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { createClassList, createDiscoveryControllerFixture, createElement, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

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
