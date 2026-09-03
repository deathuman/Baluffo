import test from "node:test";
import assert from "node:assert/strict";

import {
  availabilityCheckResultLabel,
  availabilityCheckVerdict,
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
    wait: async () => {}
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

test("availability check keeps polling while the run is running", async () => {
  let statusCalls = 0;
  const service = {
    async checkJobAvailability() {
      return { ok: true, data: { runId: "run-1" }, error: "" };
    },
    async getJobAvailabilityCheckStatus() {
      statusCalls += 1;
      return statusCalls < 12
        ? { ok: true, data: { status: "running" }, error: "" }
        : {
            ok: true,
            data: { status: "succeeded", result: { classification: "direct_live" } },
            error: ""
          };
    }
  };

  const progressEvents = [];
  const result = await runJobAvailabilityCheck(service, "availability-1", {
    wait: async () => {},
    onProgress: progress => progressEvents.push(progress)
  });

  assert.equal(result.ok, true);
  assert.equal(statusCalls, 12);
  assert.ok(progressEvents.length >= 12);
  assert.ok(progressEvents.every(event => event.status === "running"));
});

test("availability check honors an explicit wall-clock backstop", async () => {
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
    maxWallMs: 1
  });

  assert.equal(result.ok, false);
  assert.match(result.error, /timed out/i);
});

test("availability check surfaces backend errors", async () => {
  const service = {
    async checkJobAvailability() {
      return { ok: false, data: null, error: "availability_id_not_found" };
    }
  };

  const result = await runJobAvailabilityCheck(service, "availability-1", {
    wait: async () => {}
  });

  assert.equal(result.ok, false);
  assert.equal(result.error, "availability_id_not_found");
});

test("verdict maps definitive live evidence to a plain green message", () => {
  const verdict = availabilityCheckVerdict({
    status: "succeeded",
    result: { classification: "direct_live", availabilityEvidence: { confidence: "definitive" } }
  });
  assert.equal(verdict.tone, "success");
  assert.match(verdict.message, /verified live/i);
  assert.equal(verdict.conclusive, true);
});

test("verdict maps unverified classifications to neutral inconclusive messages", () => {
  for (const classification of ["direct_unverified", "generic_redirect", "anti_bot", "invalid_public_url"]) {
    const verdict = availabilityCheckVerdict({
      status: "succeeded",
      result: { classification }
    });
    assert.equal(verdict.tone, "info", classification);
    assert.equal(verdict.conclusive, false, classification);
    assert.match(verdict.message, /(couldn't verify|can't be checked)/i, classification);
  }
});

test("verdict maps closed evidence to red messages", () => {
  const definitive = availabilityCheckVerdict({
    status: "succeeded",
    result: {
      classification: "direct_closed",
      availabilityEvidence: { confidence: "definitive" }
    }
  });
  assert.equal(definitive.tone, "error");
  assert.equal(definitive.conclusive, true);

  const ambiguous = availabilityCheckVerdict({
    status: "succeeded",
    result: {
      classification: "direct_closed",
      availabilityEvidence: { confidence: "ambiguous" }
    }
  });
  assert.equal(ambiguous.tone, "error");
  assert.match(ambiguous.message, /suggests/i);
});

test("verdict maps failed runs to a retryable red message", () => {
  const verdict = availabilityCheckVerdict({ status: "failed", result: {} });
  assert.equal(verdict.tone, "error");
  assert.equal(verdict.conclusive, false);
  assert.match(verdict.message, /failed/i);
});

test("verdict reports superseded definitive live evidence as no change", () => {
  const verdict = availabilityCheckVerdict({
    status: "succeeded",
    result: {
      classification: "direct_live",
      availabilityEvidence: { confidence: "definitive" },
      enforced: true,
      applied: false
    }
  });
  assert.equal(verdict.tone, "info");
  assert.match(verdict.message, /no change/i);
});
