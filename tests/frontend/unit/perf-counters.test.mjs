import assert from "node:assert/strict";
import test from "node:test";
import {
  recordFrontendDuration,
  setTimedInnerHTML,
  snapshotFrontendPerfCounters,
  timeFrontendSync
} from "../../../frontend/shared/perf-counters.js";

test("frontend perf counters record and summarize fetch/render durations", () => {
  globalThis.__baluffoFrontendPerfCounters = {};

  recordFrontendDuration("Frontend Fetch /Bridge Test", 12, { path: "/test" });
  recordFrontendDuration("frontend_fetch_bridge_test", 24);
  timeFrontendSync("frontend_render_example", () => "ok");
  setTimedInnerHTML({ innerHTML: "" }, "<p>Rendered</p>", "frontend_render_html");

  const snapshot = snapshotFrontendPerfCounters();

  assert.equal(snapshot.frontend_fetch_bridge_test.count, 2);
  assert.equal(snapshot.frontend_fetch_bridge_test.sumMs, 36);
  assert.equal(snapshot.frontend_fetch_bridge_test.p50Ms, 12);
  assert.equal(snapshot.frontend_fetch_bridge_test.p95Ms, 24);
  assert.equal(snapshot.frontend_render_example.count, 1);
  assert.equal(snapshot.frontend_render_html.count, 1);
  assert.equal(typeof globalThis.__baluffoSnapshotFrontendPerfCounters, "function");
});
