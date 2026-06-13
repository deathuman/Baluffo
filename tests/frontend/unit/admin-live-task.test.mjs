import test from "node:test";
import assert from "node:assert/strict";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature,
  createBoundedSignatureSet,
  createLiveTaskPollGuard,
  getLiveTaskPollBackoffDelay,
  pickTaskLivePayload,
  createRestoreActiveRunWatches,
  runGuardedLiveTaskPoll,
  scheduleAsyncWatchTimer
} from "../../../frontend/admin/app/live-task.js";
import { applyAdminTaskProgress } from "../../../frontend/admin/app/progress-ui.js";
import { getLiveTaskWorkItems } from "../../../frontend/shared/live-task.js";
import { createClassList, createElement, createFetcherControllerFixture, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

test("pickTaskLivePayload returns null for empty live payloads and prefers meaningful live payloads", () => {
  const fallbackPayload = { runId: "fallback_1", taskProgress: { active: true } };
  const livePayload = {
    workItems: [{ id: "source_1", status: "running" }]
  };

  assert.strictEqual(pickTaskLivePayload({}, fallbackPayload), null);
  assert.strictEqual(pickTaskLivePayload(null, fallbackPayload), null);
  assert.strictEqual(pickTaskLivePayload(livePayload, fallbackPayload), livePayload);
});

test("getLiveTaskWorkItems reads only canonical workItems", () => {
  assert.deepEqual(getLiveTaskWorkItems({ workItems: [{ id: "source_1" }] }), [{ id: "source_1" }]);
  assert.deepEqual(getLiveTaskWorkItems({ tasks: [{ id: "legacy_source" }] }), []);
});

test("appendLiveTaskActivity dedupes events, tracks signatures, and emits idle heartbeats", () => {
  const appendedEvents = [];
  const summaryChanges = [];
  const heartbeats = [];
  const liveState = {};
  const payload = {
    recentEvents: [
      {
        timestamp: "2026-03-08T10:00:00.000Z",
        phaseKey: "details",
        workItemId: "source_1",
        message: "Fetching details"
      }
    ],
    workItems: [
      {
        id: "source_1",
        name: "Source 1",
        status: "running",
        progress: {
          phaseKey: "details",
          phaseLabel: "Fetching details",
          counts: { emittedJobs: 3 },
          updatedAt: "2026-03-08T10:00:00.000Z"
        }
      }
    ]
  };
  const workItemSignature = buildTaskWorkItemActivitySignature(payload);

  const firstResult = appendLiveTaskActivity({
    payload,
    liveState,
    nowMs: 1_000,
    appendEvent: event => appendedEvents.push(event),
    scope: "fetcher",
    summarySignature: "summary:running",
    workItemSignature,
    onSummaryChange: () => summaryChanges.push("summary"),
    onHeartbeat: () => heartbeats.push("heartbeat"),
    heartbeatIntervalMs: 500
  });

  assert.equal(firstResult, true);
  assert.equal(appendedEvents.length, 1);
  assert.equal(appendedEvents[0].scope, "fetcher");
  assert.equal(appendedEvents[0].sourceId, "source_1");
  assert.deepEqual(summaryChanges, ["summary"]);
  assert.deepEqual(heartbeats, []);
  assert.equal(liveState.summarySignature, "summary:running");
  assert.equal(liveState.workItemSignature, workItemSignature);
  assert.equal(liveState.lastActivityAtMs, 1_000);

  const secondResult = appendLiveTaskActivity({
    payload,
    liveState,
    nowMs: 1_200,
    appendEvent: event => appendedEvents.push(event),
    scope: "fetcher",
    summarySignature: "summary:running",
    workItemSignature,
    onSummaryChange: () => summaryChanges.push("summary"),
    onHeartbeat: () => heartbeats.push("heartbeat"),
    heartbeatIntervalMs: 500
  });

  assert.equal(secondResult, false);
  assert.equal(appendedEvents.length, 1);
  assert.deepEqual(summaryChanges, ["summary"]);

  const heartbeatResult = appendLiveTaskActivity({
    payload: { workItems: payload.workItems, recentEvents: [] },
    liveState,
    nowMs: 2_100,
    appendEvent: event => appendedEvents.push(event),
    scope: "fetcher",
    summarySignature: "summary:running",
    workItemSignature,
    onSummaryChange: () => summaryChanges.push("summary"),
    onHeartbeat: () => heartbeats.push("heartbeat"),
    heartbeatIntervalMs: 500
  });

  assert.equal(heartbeatResult, true);
  assert.deepEqual(heartbeats, ["heartbeat"]);
  assert.equal(liveState.lastHeartbeatAtMs, 2_100);
});

test("createBoundedSignatureSet caps unique signatures without growing unbounded", () => {
  const tracker = createBoundedSignatureSet(3);

  tracker.add("a");
  tracker.add("b");
  tracker.add("c");
  tracker.add("d");
  tracker.add("d");

  assert.equal(tracker.size, 3);
  assert.equal(tracker.has("a"), false);
  assert.equal(tracker.has("b"), true);
  assert.equal(tracker.has("c"), true);
  assert.equal(tracker.has("d"), true);
});

test("live task poll guard skips overlaps, backs off, and resets on success", async () => {
  const guard = createLiveTaskPollGuard({ baseDelayMs: 500, maxDelayMs: 5000 });
  let resolveFirst;
  let calls = 0;

  const first = runGuardedLiveTaskPoll(guard, () => {
    calls += 1;
    return new Promise(resolve => {
      resolveFirst = resolve;
    });
  });
  const overlapping = await runGuardedLiveTaskPoll(guard, async () => {
    calls += 1;
    return {};
  });

  assert.equal(overlapping.skipped, true);
  assert.equal(calls, 1);

  resolveFirst({});
  assert.equal((await first).ok, true);
  assert.equal(getLiveTaskPollBackoffDelay(guard, 500), 500);

  const failedOnce = await runGuardedLiveTaskPoll(guard, async () => {
    throw new Error("network");
  });
  const failedTwice = await runGuardedLiveTaskPoll(guard, async () => {
    throw new Error("network");
  });

  assert.equal(failedOnce.ok, false);
  assert.equal(failedTwice.ok, false);
  assert.equal(getLiveTaskPollBackoffDelay(guard, 500), 1000);

  const recovered = await runGuardedLiveTaskPoll(guard, async () => ({}));
  assert.equal(recovered.ok, true);
  assert.equal(getLiveTaskPollBackoffDelay(guard, 500), 500);
});

test("scheduleAsyncWatchTimer exposes the inner async task through the timer callback", async () => {
  const originalSetTimeout = globalThis.setTimeout;
  const state = {};
  const taskSteps = [];
  const errors = [];
  const fakeTimer = {
    unrefCalls: 0,
    unref() {
      this.unrefCalls += 1;
    }
  };
  let capturedCallback = null;
  let capturedDelay = null;

  globalThis.setTimeout = (callback, delay) => {
    capturedCallback = callback;
    capturedDelay = delay;
    return fakeTimer;
  };

  try {
    scheduleAsyncWatchTimer({
      state,
      timerKey: "completionPollTimer",
      delayMs: 25,
      task: async () => {
        taskSteps.push("start");
        await Promise.resolve();
        taskSteps.push("finish");
      },
      onError: error => errors.push(error)
    });

    assert.equal(capturedDelay, 25);
    assert.strictEqual(state.completionPollTimer, fakeTimer);
    assert.equal(fakeTimer.unrefCalls, 1);
    assert.equal(typeof capturedCallback, "function");

    const taskPromise = capturedCallback();

    assert.equal(typeof taskPromise?.then, "function");
    await taskPromise;
    assert.deepEqual(taskSteps, ["start", "finish"]);
    assert.deepEqual(errors, []);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
  }
});

test("admin live-task restore helper restarts active fetch and discovery watches", async () => {
  const calls = [];
  let fetchLiveLoads = 0;
  let discoveryLiveLoads = 0;

  const restoreActiveRunWatches = createRestoreActiveRunWatches({
    loadFetcherLivePayload: async () => {
      fetchLiveLoads += 1;
      return {
        active: true,
        runId: "fetch_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: ""
      };
    },
    loadLatestFetcherReport: async options => {
      calls.push(`loadLatestFetcherReport:${String(Boolean(options?.silent))}`);
      return {
        runId: "fetch_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: "",
        taskProgress: { active: true, phaseKey: "executing_sources", phaseLabel: "Executing sources" }
      };
    },
    fetcherController: {
      attachToActiveFetchRun(runMeta, options) {
        calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    },
    loadDiscoveryLivePayload: async () => {
      discoveryLiveLoads += 1;
      return {
        active: true,
        runId: "discovery_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: ""
      };
    },
    loadLatestDiscoveryReport: async () => {
      throw new Error("discovery report fallback should not run");
    },
    discoveryController: {
      attachToActiveDiscoveryRun(runMeta, options) {
        calls.push(`attachToActiveDiscoveryRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    }
  });

  await Promise.all([restoreActiveRunWatches(), restoreActiveRunWatches()]);

  assert.equal(fetchLiveLoads, 1);
  assert.equal(discoveryLiveLoads, 1);
  assert.equal(
    calls.filter(line => line === "attachToActiveFetchRun:fetch_restore_2:false").length,
    1
  );
  assert.equal(
    calls.filter(line => line === "attachToActiveDiscoveryRun:discovery_restore_2:false").length,
    1
  );
  assert.ok(calls.includes("loadLatestFetcherReport:true"));
});

test("admin live-task restore helper reattaches fetch watch from an active cold-load report", async () => {
  const calls = [];

  const restoreActiveRunWatches = createRestoreActiveRunWatches({
    loadFetcherLivePayload: async () => null,
    loadLatestFetcherReport: async () => ({
      runId: "fetch_restore_cold_1",
      startedAt: "2026-03-29T11:49:22+02:00",
      finishedAt: "",
      taskProgress: { active: true, phaseKey: "executing_sources", phaseLabel: "Executing sources" },
      summary: { outputCount: 10, failedSources: 0, sourceCount: 10 }
    }),
    fetcherController: {
      getRestorableFetcherRunMeta(report) {
        calls.push(`restoreMeta:${String(report?.runId || "")}`);
        return null;
      },
      attachToActiveFetchRun(runMeta, options) {
        calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    },
    loadDiscoveryLivePayload: async () => null,
    loadLatestDiscoveryReport: async () => null,
    discoveryController: {}
  });

  await restoreActiveRunWatches();

  assert.ok(calls.includes("restoreMeta:fetch_restore_cold_1"));
  assert.ok(calls.includes("attachToActiveFetchRun:fetch_restore_cold_1:false"));
});

test("admin live-task restore helper silently hydrates fetch progress on first boot attach", async () => {
  const timerStub = stubScheduledTimers();
  let controller;
  try {
    const fixture = createFetcherControllerFixture();
    fixture.options.getBridge = async path => {
      if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/task-live/fetch?view=summary") {
        return {
          active: true,
          runId: "fetch_boot_restore_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: ""
        };
      }
      return {};
    };
    fixture.options.fetchJobsFetchReportJson = async () => ({
      runId: "fetch_boot_restore_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 6,
          sourceCount: 12,
          runningTasks: 6,
          queuedTasks: 0,
          outputCount: 18,
          failedSources: 1,
          excludedSources: 0
        }
      },
      summary: { outputCount: 18, failedSources: 1, excludedSources: 0, sourceCount: 12 },
      sources: [{ name: "Studio A", status: "running" }]
    });
    fixture.options.loadOpsHealthData = async () => {};
    controller = createAdminFetcherController(fixture.options);

    const restoreActiveRunWatches = createRestoreActiveRunWatches({
      loadFetcherLivePayload: (...args) => controller.loadFetcherLivePayload(...args),
      loadLatestFetcherReport: options => controller.loadLatestFetcherReport(options),
      fetcherController: controller,
      loadDiscoveryLivePayload: async () => null,
      loadLatestDiscoveryReport: async () => null,
      discoveryController: {}
    });

    await restoreActiveRunWatches();

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /executing sources/i);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    await timerStub.scheduled[1]();
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_boot_restore_1",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    assert.deepEqual(fixture.logs, []);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("shared admin task progress renderer resets indeterminate state before determinate fill", () => {
  const rootEl = createElement({ style: {}, classList: createClassList(["hidden"]) });
  const barEl = createElement({ style: {} });
  const labelEl = createElement();

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: false,
    label: "Fetcher: Executing sources"
  });
  assert.equal(rootEl.classList.contains("indeterminate"), true);
  assert.equal(barEl.style.width, "36%");
  assert.equal(rootEl.attributes["aria-hidden"], "false");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: Executing sources");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 0.65,
    label: "Fetcher: 65% complete"
  });
  assert.equal(rootEl.classList.contains("determinate"), true);
  assert.equal(rootEl.classList.contains("indeterminate"), false);
  assert.equal(barEl.style.width, "65%");
  assert.equal(barEl.style.left, "0");
  assert.equal(barEl.style.animation, "none");
  assert.equal(rootEl.attributes["aria-valuenow"], "65");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: 65% complete");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: false
  });
  assert.equal(rootEl.classList.contains("hidden"), true);
  assert.equal(barEl.style.width, "0%");
  assert.equal(rootEl.attributes["aria-hidden"], "true");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 1,
    label: "Discovery: Discovery completed"
  });
  assert.equal(rootEl.classList.contains("complete"), true);
  assert.equal(barEl.style.width, "100%");
  assert.equal(rootEl.attributes["aria-valuenow"], "100");
});
