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
