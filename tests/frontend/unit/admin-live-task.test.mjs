import test from "node:test";
import assert from "node:assert/strict";
import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature,
  createBoundedSignatureSet,
  pickTaskLivePayload,
  scheduleAsyncWatchTimer
} from "../../../frontend/admin/app/live-task.js";
import { getLiveTaskWorkItems } from "../../../frontend/shared/live-task.js";

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
