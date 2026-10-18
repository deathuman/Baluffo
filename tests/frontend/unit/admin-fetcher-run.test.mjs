import test from "node:test";

import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";

import { createElement, createFetcherControllerFixture, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller stores optimistic run metadata while fetch watch is active", async () => {
  const timerStub = stubScheduledTimers();

  let controller;
  try {
    const fixture = createFetcherControllerFixture({
      refs: {
        adminFetcherLogEl: createElement()
      }
    });
    fixture.options.getBridge = async path => {
      fixture.calls.push(path);
        if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
          return { text: "", nextOffset: 0 };
        }
        return {};
    };
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
      return {
        started: true,
        runId: "fetch_123",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "default",
        args: ["--quiet"]
      };
    };
    fixture.options.loadOpsHealthData = async () => {
      fixture.calls.push("loadOpsHealthData");
    };
    controller = createAdminFetcherController(fixture.options);

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.ok(fixture.calls.includes("/tasks/run-fetcher"));
    assert.ok(fixture.logs.some(line => /triggered fetcher via local admin bridge/i.test(line)));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller attaches to an already-running bridge task on conflict", async () => {
  const timerStub = stubScheduledTimers();
  let controller;

  try {
    const fixture = createFetcherControllerFixture();
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
        return {
          status: 409,
          data: {
            started: false,
            alreadyRunning: true,
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:01:00.000Z",
            task: "jobs_fetcher",
            taskType: "fetch",
            pid: 654,
            status: "running"
          }
        };
    };
    controller = createAdminFetcherController(fixture.options);

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_live_1",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.ok(fixture.calls.includes("/tasks/run-fetcher"));
    assert.ok(fixture.calls.includes("loadOpsHealthData"));
    assert.ok(fixture.logs.some(line => /fetcher already running; attached/i.test(line)));
    assert.ok(fixture.toasts.some(item => item.message === "Fetcher already running. Attached to active run." && item.level === "info"));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller starts live progress watching for an explicit bridge-launched fetch", async () => {
  const timerStub = stubScheduledTimers();
  const activeReport = {
    startedAt: "2026-03-08T10:00:00.000Z",
    finishedAt: "",
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "indeterminate",
      ratio: 0,
      counts: {}
    },
    runtime: {},
    summary: {},
    sources: []
  };
  const fixture = createFetcherControllerFixture({
    options: {
      fetchJobsFetchReportJson: async () => activeReport
    }
  });
  const controller = createAdminFetcherController(fixture.options);

  try {
    controller.attachToActiveFetchRun({
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    await Promise.resolve();

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    assert.ok(fixture.logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(!fixture.logs.some(line => /timeout window/i.test(line)));
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(fixture.calls.includes("/fetcher/log?view=tail&limitChars=8192"));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller can restore a live watch from local state when the latest report is stale", async () => {
  const fixture = createFetcherControllerFixture({
    state: {
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherLiveProgressState: {
      summarySignature: "",
      sourceSignatures: new Map(),
      reportedSlowSources: new Set(),
      serverLogSignatures: new Set(),
      slowSourceSummarySignature: "",
      slowStageSummarySignature: "",
      lastHeartbeatAtMs: 0,
      lastActivityAtMs: Date.now()
    },
    fetchOptimisticRun: {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: true,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
    },
    options: {
      getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
        return { text: "", nextOffset: 0 };
      }
      return {
        runId: "fetch_stale_2",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: false,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "indeterminate",
          ratio: 0,
          counts: {}
        },
        runtime: {},
        summary: {},
        sources: []
      };
    },
      fetchJobsFetchReportJson: async () => ({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: false,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "indeterminate",
        ratio: 0,
        counts: {}
      },
      runtime: {},
      summary: {},
      sources: []
    })
    }
  });
  const timerStub = stubScheduledTimers();
  const controller = createAdminFetcherController(fixture.options);

  try {
    const meta = controller.getRestorableFetcherRunMeta({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: { active: false }
    });

    assert.deepEqual(meta, {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    });

    controller.restartFetcherCompletionWatch(meta);

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});
