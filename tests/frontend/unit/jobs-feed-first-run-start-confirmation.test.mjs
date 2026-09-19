import test from "node:test";
import assert from "node:assert/strict";

import { createFirstRunBootstrapFlow } from "../../../frontend/jobs/app/feed-first-run-flow.js";
import { createBaseDeps, createLocalStorage } from "./helpers/jobs-feed-test-helpers.mjs";

const UNCERTAIN_MESSAGE = "Bridge request timed out";

function createFlow(overrides = {}) {
  const { localStorage } = createLocalStorage();
  const { calls, deps } = createBaseDeps({
    windowObject: { localStorage },
    ...overrides
  });
  return { calls, flow: createFirstRunBootstrapFlow(deps) };
}

function metricsFor(calls, event) {
  return calls.metrics.filter(entry => entry.event === event);
}

test("startBootstrapWithConfirmation returns the payload identity when running evidence is present", async () => {
  const payload = { started: true, runId: "jobs_bootstrap_identity_running" };
  let startCalls = 0;
  const { calls, flow } = createFlow({
    startJobsBootstrap: async () => {
      startCalls += 1;
      return payload;
    }
  });

  const result = await flow.startBootstrapWithConfirmation();

  assert.equal(result, payload);
  assert.equal(startCalls, 1);
  assert.deepEqual(metricsFor(calls, "jobs_first_run_bootstrap_start_uncertain"), []);
});

test("startBootstrapWithConfirmation returns the payload identity when running evidence is absent", async () => {
  const payload = { started: false, runId: "" };
  let startCalls = 0;
  const { calls, flow } = createFlow({
    startJobsBootstrap: async () => {
      startCalls += 1;
      return payload;
    }
  });

  const result = await flow.startBootstrapWithConfirmation({ explicit: true });

  assert.equal(result, payload);
  assert.equal(startCalls, 1);
  assert.deepEqual(metricsFor(calls, "jobs_first_run_bootstrap_start_uncertain"), []);
  assert.deepEqual(calls.sourceStatus, []);
});

test("startBootstrapWithConfirmation passes the start timeout and leaves the payload untouched", async () => {
  const payload = { started: true, runId: "jobs_bootstrap_identity_timeout" };
  const options = [];
  const { flow } = createFlow({
    bootstrapStartTimeoutMs: 12345,
    startJobsBootstrap: async request => {
      options.push(request);
      return payload;
    }
  });

  const result = await flow.startBootstrapWithConfirmation();

  assert.deepEqual(options, [{ timeoutMs: 12345 }]);
  assert.equal(result, payload);
  assert.deepEqual(Object.keys(result), Object.keys(payload));
});

test("startBootstrapWithConfirmation still reports uncertainty when the start route fails", async () => {
  let startCalls = 0;
  const { calls, flow } = createFlow({
    bootstrapConfirmTimeoutMs: 0,
    bootstrapConfirmIntervalMs: 0,
    fetchJobsReport: async () => null,
    startJobsBootstrap: async () => {
      startCalls += 1;
      throw new Error(UNCERTAIN_MESSAGE);
    }
  });

  await assert.rejects(
    flow.startBootstrapWithConfirmation({ explicit: true }),
    err => err?.bootstrapStartUnconfirmed === true
  );

  const uncertain = metricsFor(calls, "jobs_first_run_bootstrap_start_uncertain");
  assert.equal(uncertain.length, 1);
  assert.equal(uncertain[0].payload.explicit, true);
  assert.match(uncertain[0].payload.error, /timed out/);
  assert.equal(startCalls, 2);
});
