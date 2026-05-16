import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  canSetOutcomeStatus,
  canTransitionPipelinePhase,
  normalizeTrackingFields,
  splitApplicationStatus,
  toApplicationStatusMirror
} from "../../../frontend/local-data/tracking.js";

const CASES = JSON.parse(
  readFileSync(new URL("../../fixtures/saved_job_tracking_cases.json", import.meta.url), "utf8")
);

test("local-data tracking normalizes shared parity fixtures", () => {
  for (const item of CASES) {
    const normalized = normalizeTrackingFields(item.input, {}, {
      savedAt: item.input.savedAt,
      nowIso: () => "2026-04-01T00:00:00.000Z",
      normalizeIsoOrNow: (value, fallback = "") => String(value || fallback)
    });
    assert.equal(normalized.pipelinePhase, item.expected.pipelinePhase, item.name);
    assert.equal(normalized.outcomeStatus, item.expected.outcomeStatus, item.name);
    assert.equal(normalized.applicationStatus, item.expected.applicationStatus, item.name);
    assert.equal(normalized.phaseTimestamps.bookmark, item.input.savedAt, item.name);
    if (item.expected.outcomeTimestampKey) {
      assert.ok(
        normalized.outcomeTimestamps[item.expected.outcomeTimestampKey],
        item.name
      );
    }
  }
});

test("local-data tracking applies legacy source status before base split fields", () => {
  const normalized = normalizeTrackingFields({
    applicationStatus: "rejected",
    savedAt: "2026-03-08T09:00:00.000Z"
  }, {
    pipelinePhase: "interview_2",
    outcomeStatus: "active",
    applicationStatus: "interview_2",
    savedAt: "2026-03-08T09:00:00.000Z",
    phaseTimestamps: {
      bookmark: "2026-03-08T09:00:00.000Z",
      interview_2: "2026-03-10T09:00:00.000Z"
    }
  }, {
    savedAt: "2026-03-08T09:00:00.000Z",
    nowIso: () => "2026-04-01T00:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback)
  });

  assert.equal(normalized.pipelinePhase, "interview_2");
  assert.equal(normalized.outcomeStatus, "rejected");
  assert.equal(normalized.applicationStatus, "rejected");
  assert.ok(normalized.outcomeTimestamps.rejected);
});

test("local-data tracking keeps applicationStatus as derived compatibility mirror", () => {
  assert.deepEqual(splitApplicationStatus("rejected", {
    phaseTimestamps: { applied: "2026-03-08T10:00:00.000Z" }
  }), {
    pipelinePhase: "applied",
    outcomeStatus: "rejected"
  });
  assert.equal(toApplicationStatusMirror("offer", "active"), "offer");
  assert.equal(toApplicationStatusMirror("offer", "accepted"), "accepted");
});

test("local-data tracking blocks skipped phases and terminal rewrites without override", () => {
  assert.equal(canTransitionPipelinePhase("bookmark", "applied", "active"), true);
  assert.equal(canTransitionPipelinePhase("bookmark", "interview_1", "active"), false);
  assert.equal(canTransitionPipelinePhase("offer", "final", "active"), false);
  assert.equal(canTransitionPipelinePhase("offer", "offer", "accepted"), false);
  assert.equal(canSetOutcomeStatus("active", "rejected"), true);
  assert.equal(canSetOutcomeStatus("rejected", "accepted"), false);
  assert.equal(canSetOutcomeStatus("rejected", "accepted", { override: true }), true);
});
