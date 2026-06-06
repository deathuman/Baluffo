import test from "node:test";
import assert from "node:assert/strict";

import { createAdminStartupMetrics } from "../../../frontend/admin/app/runtime/effects.js";
import { createPerfMarks } from "../../../frontend/shared/perf-marks.js";

function withPerformance(performanceValue, run) {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, "performance");
  Object.defineProperty(globalThis, "performance", {
    configurable: true,
    value: performanceValue
  });
  try {
    return run();
  } finally {
    if (descriptor) {
      Object.defineProperty(globalThis, "performance", descriptor);
    } else {
      delete globalThis.performance;
    }
  }
}

test("perf marks call performance.mark and emit startup metrics", () => {
  const marks = [];
  const emitted = [];
  const metrics = {
    emit(event, payload) {
      emitted.push({ event, payload });
    }
  };

  withPerformance({
    mark(name) {
      marks.push(name);
    }
  }, () => {
    createPerfMarks(metrics).markStep("admin_dom_cache_start", { phase: "dom" });
  });

  assert.deepEqual(marks, ["admin_dom_cache_start"]);
  assert.deepEqual(emitted, [{
    event: "admin_dom_cache_start",
    payload: { phase: "dom" }
  }]);
});

test("perf measures call performance.measure and emit duration payloads", async () => {
  const measures = [];
  const emitted = [];
  let nowMs = 100;
  const startupMetrics = createAdminStartupMetrics({
    emitStartupMetric(event, payload) {
      emitted.push({ event, payload });
    },
    now: () => nowMs
  });

  withPerformance({
    mark() {},
    measure(name, startMark, endMark) {
      measures.push({ name, startMark, endMark });
      return { duration: 12.4 };
    }
  }, () => {
    const perfMarks = createPerfMarks(startupMetrics);
    nowMs = 110;
    perfMarks.markStep("admin_overview_fetch_start");
    nowMs = 125;
    perfMarks.measureStep(
      "admin_overview_fetch",
      "admin_overview_fetch_start",
      "admin_overview_fetch_done",
      { ok: true }
    );
  });
  nowMs = 130;
  startupMetrics.markFirstInteractive("test");
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.deepEqual(measures, [{
    name: "admin_overview_fetch",
    startMark: "admin_overview_fetch_start",
    endMark: "admin_overview_fetch_done"
  }]);
  assert.equal(emitted[0].event, "admin_overview_fetch_start");
  assert.equal(emitted[0].payload.elapsedMs, 10);
  assert.equal(emitted[1].event, "admin_overview_fetch");
  assert.equal(emitted[1].payload.ok, true);
  assert.equal(emitted[1].payload.durationMs, 12);
  assert.equal(emitted[1].payload.elapsedMs, 25);
});

test("perf marks no-op safely without browser timing APIs or startup metrics", () => {
  assert.doesNotThrow(() => {
    withPerformance(undefined, () => {
      const perfMarks = createPerfMarks(null);
      perfMarks.markStep("admin_missing_perf");
      perfMarks.measureStep("admin_missing_measure", "start", "end");
    });
  });
});
