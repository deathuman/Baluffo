import test from "node:test";
import assert from "node:assert/strict";

import {
  availabilityCheckResultLabel,
  availabilityCheckWasApplied,
  runJobAvailabilityCheck
} from "../../../frontend/shared/job-availability-check.js";

test("availability check polls through terminal completion", async () => {
  let statusCalls = 0;
  const service = {
    async checkJobAvailability() {
      return { ok: true, data: { runId: "run-1" }, error: "" };
    },
    async getJobAvailabilityCheckStatus() {
      statusCalls += 1;
      return statusCalls === 1
        ? { ok: true, data: { status: "running" }, error: "" }
        : {
            ok: true,
            data: { status: "succeeded", result: { classification: "direct_live" } },
            error: ""
          };
    }
  };

  const result = await runJobAvailabilityCheck(service, "availability-1", {
    wait: async () => {},
    maxPolls: 3
  });

  assert.equal(result.ok, true);
  assert.equal(statusCalls, 2);
  assert.equal(availabilityCheckResultLabel(result.data), "Availability check completed: direct live.");
  assert.equal(availabilityCheckWasApplied(result.data), false);
});

test("availability check reports when a transition was applied", () => {
  assert.equal(
    availabilityCheckWasApplied({ status: "succeeded", result: { applied: true } }),
    true
  );
  assert.equal(
    availabilityCheckWasApplied({ status: "failed", result: { applied: true } }),
    false
  );
});

test("availability check returns a bounded timeout", async () => {
  const service = {
    async checkJobAvailability() {
      return { ok: true, data: { runId: "run-1" }, error: "" };
    },
    async getJobAvailabilityCheckStatus() {
      return { ok: true, data: { status: "running" }, error: "" };
    }
  };

  const result = await runJobAvailabilityCheck(service, "availability-1", {
    wait: async () => {},
    maxPolls: 2
  });

  assert.equal(result.ok, false);
  assert.match(result.error, /timed out/i);
});
