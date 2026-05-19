import test from "node:test";
import assert from "node:assert/strict";

import { initJobsFeed } from "../../../frontend/jobs/app/feed.js";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

test("initJobsFeed confirms a timed-out bootstrap start from the active report", async () => {
  const { storage, localStorage } = createLocalStorage();
  let reportCalls = 0;
  let bootstrapStarts = 0;
  let refreshOptions = null;
  const startOptions = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    bootstrapConfirmTimeoutMs: 0,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls === 1) return { summary: { outputCount: 0 } };
      if (reportCalls === 2) {
        return {
          runId: "jobs_bootstrap_timeout",
          summary: { outputCount: 0, coverageScope: "bootstrap_sheets" }
        };
      }
      return {
        runId: "jobs_bootstrap_timeout",
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async options => {
      bootstrapStarts += 1;
      startOptions.push(options);
      throw new Error("Bridge request timed out");
    },
    refreshJobsNow: async options => {
      refreshOptions = options;
      return true;
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 1);
  assert.equal(startOptions[0].timeoutMs, 30000);
  assert.deepEqual(refreshOptions, { manual: false, firstLoad: true });
  assert.deepEqual(calls.showError, []);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});

test("initJobsFeed shows an unconfirmed start error without writing failed marker", async () => {
  const { storage, localStorage } = createLocalStorage();
  let reportCalls = 0;
  let bootstrapStarts = 0;
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapConfirmTimeoutMs: 0,
    fetchJobsReport: async () => {
      reportCalls += 1;
      return { summary: { outputCount: 0 } };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      throw new Error("Bridge request timed out");
    }
  });

  await initJobsFeed(deps);

  assert.equal(bootstrapStarts, 2);
  assert.equal(reportCalls, 2);
  assert.equal(calls.showError.length, 1);
  assert.match(calls.showError[0], /Could not confirm first-run sheet refresh started/);
  assert.equal(storage.has("baluffo_jobs_bootstrap_auto_started"), false);
});

test("initJobsFeed retry re-renders first-run progress and reattaches to running bootstrap", async () => {
  const { localStorage } = createLocalStorage();
  let reportCalls = 0;
  let bootstrapStarts = 0;
  let retryCallback = null;
  const renderedStates = [];
  const progressCalls = [];
  const { calls, deps } = createBaseDeps({
    isDesktopRuntimeMode: () => true,
    windowObject: { localStorage },
    bootstrapPollIntervalMs: 0,
    bootstrapTimeoutMs: 1000,
    bootstrapConfirmTimeoutMs: 0,
    fetchJobsReport: async () => {
      reportCalls += 1;
      if (reportCalls <= 2) return { summary: { outputCount: 0 } };
      return {
        runId: "jobs_bootstrap_retry",
        finishedAt: "2026-05-17T10:00:00+00:00",
        summary: { status: "ok", outputCount: 3, coverageScope: "bootstrap_sheets" }
      };
    },
    startJobsBootstrap: async () => {
      bootstrapStarts += 1;
      if (bootstrapStarts <= 2) throw new Error("Bridge request timed out");
      return { alreadyRunning: true, runId: "jobs_bootstrap_retry" };
    },
    setProgress: visible => progressCalls.push(Boolean(visible)),
    applyFiltersAndRender: options => renderedStates.push(options),
    showError: (message, onRetry) => {
      calls.showError.push(String(message || ""));
      retryCallback = onRetry;
    }
  });

  await initJobsFeed(deps);
  assert.equal(typeof retryCallback, "function");

  const retryButton = {
    disabled: false,
    isConnected: true,
    attrs: new Map(),
    setAttribute(name, value) {
      this.attrs.set(name, String(value));
    },
    removeAttribute(name) {
      this.attrs.delete(name);
    }
  };
  await retryCallback({ currentTarget: retryButton });

  assert.equal(bootstrapStarts, 3);
  assert.equal(renderedStates.length, 2);
  assert.equal(renderedStates[0].emptyStateReason, "first_run_bootstrap");
  assert.equal(renderedStates[1].emptyStateReason, "first_run_bootstrap");
  assert.deepEqual(calls.showError, [
    "Could not confirm first-run sheet refresh started. Retry quick refresh or open Admin."
  ]);
  assert.equal(progressCalls.includes(true), true);
  assert.equal(retryButton.disabled, false);
  assert.equal(retryButton.attrs.has("aria-busy"), false);
});
