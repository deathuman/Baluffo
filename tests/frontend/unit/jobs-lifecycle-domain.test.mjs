import test from "node:test";
import assert from "node:assert/strict";
import { normalizeJobs } from "../../../frontend/jobs/domain.js";

test("jobs domain normalizes lifecycle event and reason fields", () => {
  const rows = normalizeJobs([{
    title: "Animator",
    company: "Studio",
    lifecycleEvent: "REAPPEARED",
    lifecycleReason: "SOURCE_FAILED"
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });

  assert.equal(rows[0].lifecycleEvent, "reappeared");
  assert.equal(rows[0].lifecycleReason, "source_failed");
});

test("jobs domain normalizes canonical availability fields", () => {
  const rows = normalizeJobs([{
    title: "Animator",
    company: "Studio",
    availabilityId: "availability_1",
    availabilityStatus: "VERIFICATION_OVERDUE",
    availabilityCheckedAt: "2026-07-01T10:00:00Z",
    availabilityEvidence: { kind: "source_failed", confidence: "unknown" }
  }], { professionLabels: {}, sanitizeUrl: value => value });

  assert.equal(rows[0].availabilityStatus, "verification_overdue");
  assert.equal(rows[0].availabilityId, "availability_1");
  assert.equal(rows[0].availabilityEvidence.kind, "source_failed");
});
