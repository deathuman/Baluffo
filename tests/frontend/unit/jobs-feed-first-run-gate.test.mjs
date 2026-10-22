import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

async function waitFor(predicate, { timeoutMs = 2000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition not met in time");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

test("initJobsFeed paints the startup snapshot without waiting for the report probe", async () => {
  const { localStorage } = createLocalStorage();
  let resolveReport;
  const pendingReport = new Promise(resolve => {
    resolveReport = resolve;
  });
  let startupPreviewCalled = false;
  let bootstrapStarts = 0;
  const lastUpdated = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true, desktopJobsColdStart: true,
    windowObject: { localStorage, setTimeout: () => 0 },
    fetchJobsReport: () => pendingReport,
    updateLastUpdatedText: value => lastUpdated.push(value),
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true };
    },
    loadStartupPreviewJobs: async () => {
      startupPreviewCalled = true;
      deps.setAllJobs([{ id: "snapshot-1" }]);
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(startupPreviewCalled, true, "the snapshot is what the first render waits on");
  assert.deepEqual(calls.allJobs.at(-1), [{ id: "snapshot-1" }]);
  assert.equal(bootstrapStarts, 0);

  resolveReport({
    finishedAt: "2026-05-17T10:00:00+00:00",
    summary: { status: "ok", outputCount: 5 }
  });
  await waitFor(() => calls.metrics.some(metric => metric.event === "jobs_first_run_gate_evaluated"));

  const gate = calls.metrics.find(metric => metric.event === "jobs_first_run_gate_evaluated");
  assert.equal(gate.payload.action, "skip");
  assert.equal(gate.payload.late, true);
  assert.equal(gate.payload.rowsRendered, 1);
  assert.equal(bootstrapStarts, 0);
  assert.deepEqual(calls.notices, []);
  assert.deepEqual(
    lastUpdated,
    [Date.parse("2026-05-17T10:00:00+00:00")],
    "the late report fills in the stamp the snapshot could not show"
  );
});

test("initJobsFeed applies a late first-run gate behind rendered rows without clearing them", async () => {
  const { storage, localStorage } = createLocalStorage();
  let reportCalls = 0;
  let resolveFirstReport;
  const firstReport = new Promise(resolve => {
    resolveFirstReport = resolve;
  });
  let bootstrapStarts = 0;
  const refreshCalls = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true, desktopJobsColdStart: true,
    windowObject: { localStorage, setTimeout: () => 0 },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return firstReport;
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true, runId: "jobs_bootstrap_late" };
    },
    loadStartupPreviewJobs: async () => {
      deps.setAllJobs([{ id: "snapshot-1" }]);
      return true;
    },
    refreshJobsNow: async options => {
      refreshCalls.push(options);
      return true;
    }
  });

  await initJobsFeed(deps);
  assert.deepEqual(
    calls.rendered.filter(entry => entry.stage === "first_run_bootstrap"),
    [],
    "nothing may paint the empty bootstrap state before the gate answers"
  );

  resolveFirstReport({ summary: { outputCount: 0 } });
  await waitFor(() => refreshCalls.length === 1);

  assert.equal(bootstrapStarts, 1);
  assert.deepEqual(refreshCalls, [{ manual: false, firstLoad: true }]);
  assert.deepEqual(calls.notices, [], "a late gate must not raise the first-run modal over rows");
  assert.equal(
    calls.allJobs.some(rows => rows.length === 0),
    false,
    "rendered rows are never cleared by the late gate"
  );
  const late = calls.metrics.find(metric => metric.event === "jobs_first_run_gate_applied_late");
  assert.equal(late.payload.action, "start");
  assert.equal(late.payload.rowsRendered, 1);
  assert.deepEqual(calls.startupStates.at(-1), {
    state: "interactive",
    detail: "first_run_bootstrap"
  });
  assert.equal(
    calls.sourceStatus.at(-1),
    "Refreshing first-run sheet jobs. This can take several minutes..."
  );
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});
