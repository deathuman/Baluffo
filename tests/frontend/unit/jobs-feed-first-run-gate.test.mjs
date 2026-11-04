import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

test("initJobsFeed waits for the first-run gate before loading the feed", async () => {
  const { localStorage } = createLocalStorage();
  let resolveReport;
  const pendingReport = new Promise(resolve => {
    resolveReport = resolve;
  });
  let refreshCalls = 0;
  let previewCalls = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    fetchJobsReport: () => pendingReport,
    loadStartupPreviewJobs: async () => {
      previewCalls += 1;
      return true;
    },
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    }
  });

  const init = initJobsFeed(deps);
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(refreshCalls, 0, "the feed must not load before the gate answers");
  assert.equal(previewCalls, 0, "no partial snapshot is painted ahead of the gate");
  assert.deepEqual(calls.allJobs, []);

  resolveReport({
    finishedAt: "2026-05-17T10:00:00+00:00",
    summary: { status: "ok", outputCount: 5 }
  });
  await init;

  assert.equal(refreshCalls, 1);
  assert.equal(previewCalls, 0);
  assert.deepEqual(calls.notices, []);
  assert.deepEqual(calls.showError, []);
});

test("initJobsFeed prefetches the full feed while the report probe is pending", async () => {
  const { localStorage } = createLocalStorage();
  let resolveReport;
  const pendingReport = new Promise(resolve => {
    resolveReport = resolve;
  });
  let prefetchCalls = 0;
  let refreshCalls = 0;
  let refreshOptions = null;
  const { deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    fetchJobsReport: () => pendingReport,
    prefetchJobsFeed: () => {
      prefetchCalls += 1;
      return Promise.resolve({ jobs: [{ id: "job-1" }], sourceName: "test" });
    },
    refreshJobsNow: async options => {
      refreshCalls += 1;
      refreshOptions = options;
      return true;
    }
  });

  const init = initJobsFeed(deps);
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(prefetchCalls, 1, "the feed read starts without waiting for the probe");
  assert.equal(refreshCalls, 0, "the feed commit still waits for the gate");

  resolveReport({
    finishedAt: "2026-05-17T10:00:00+00:00",
    summary: { status: "ok", outputCount: 5 }
  });
  await init;

  assert.equal(refreshCalls, 1);
  assert.equal(refreshOptions.prefetchedFeed instanceof Promise, true);
});

test("initJobsFeed shows the first-run tutorial instead of loading the feed on a cold start", async () => {
  const { localStorage } = createLocalStorage();
  let bootstrapStarts = 0;
  let reportCalls = 0;
  let refreshCalls = 0;
  let previewCalls = 0;
  let prefetchCalls = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    desktopJobsColdStart: true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return { summary: { outputCount: 0 } };
      return {
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      return { started: true, runId: "jobs_bootstrap_first_run" };
    },
    prefetchJobsFeed: () => {
      prefetchCalls += 1;
      return Promise.resolve({ jobs: [{ id: "job-1" }] });
    },
    loadStartupPreviewJobs: async () => {
      previewCalls += 1;
      return true;
    },
    refreshJobsNow: async () => {
      refreshCalls += 1;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(previewCalls, 0);
  assert.equal(prefetchCalls, 0, "the tutorial owns the first feed load");
  assert.equal(bootstrapStarts, 1);
  assert.equal(refreshCalls, 1, "the completed bootstrap feed is loaded once");
  assert.equal(calls.notices.length, 1, "the first-run tutorial is shown");
  assert.deepEqual(calls.rendered[0], { stage: "first_run_bootstrap", rowCount: 0 });
  assert.deepEqual(calls.startupStates.at(-1), {
    state: "interactive",
    detail: "first_run_bootstrap"
  });
  assert.deepEqual(calls.showError, []);
});
