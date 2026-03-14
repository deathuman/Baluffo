import test from "node:test";
import assert from "node:assert/strict";

import { createJobsStartupMetrics } from "../../../frontend/jobs/app/runtime/effects.js";
import { createSavedStartupMetrics } from "../../../frontend/saved/app/runtime/effects.js";
import { createAdminStartupMetrics } from "../../../frontend/admin/app/runtime/effects.js";

test("jobs startup metrics add elapsedMs to first render and first interactive", () => {
  const calls = [];
  let nowMs = 1000;
  const metrics = createJobsStartupMetrics({
    emitMetric: (event, payload) => calls.push({ event, payload }),
    now: () => nowMs
  });

  nowMs = 1012;
  metrics.markRendered("startup_preview", 11);
  nowMs = 1025;
  metrics.markInteractive("startup_preview");

  assert.equal(calls[0].event, "jobs_first_render");
  assert.equal(calls[0].payload.elapsedMs, 12);
  assert.equal(calls[1].event, "jobs_first_interactive");
  assert.equal(calls[1].payload.elapsedMs, 25);
});

test("saved startup metrics preserve provided elapsedMs", () => {
  const calls = [];
  const metrics = createSavedStartupMetrics({
    emitMetric: (event, payload) => calls.push({ event, payload }),
    now: () => 500
  });

  metrics.emit("saved_boot_step", { elapsedMs: 77, phase: "boot" });

  assert.equal(calls[0].event, "saved_boot_step");
  assert.equal(calls[0].payload.elapsedMs, 77);
  assert.equal(calls[0].payload.phase, "boot");
});

test("admin startup metrics add elapsedMs to first interactive", () => {
  const calls = [];
  let nowMs = 200;
  const metrics = createAdminStartupMetrics({
    emitStartupMetric: (event, payload) => calls.push({ event, payload }),
    now: () => nowMs
  });

  nowMs = 245;
  metrics.markFirstInteractive("unlock");

  assert.equal(calls[0].event, "admin_first_interactive");
  assert.equal(calls[0].payload.elapsedMs, 45);
  assert.equal(calls[0].payload.reason, "unlock");
});
