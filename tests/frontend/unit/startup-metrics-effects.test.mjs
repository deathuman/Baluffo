import test from "node:test";
import assert from "node:assert/strict";

import { createJobsStartupMetrics } from "../../../frontend/jobs/app/runtime/effects.js";
import { createSavedStartupMetrics } from "../../../frontend/saved/app/runtime/effects.js";
import { createAdminStartupMetrics } from "../../../frontend/admin/app/runtime/effects.js";
import {
  emitStartupProbeMetric,
  flushStartupProbeMetricQueue
} from "../../../probes/startup-probe.js";

function createStorageMock() {
  const map = new Map();
  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

function setupStartupProbeGlobals(href = "http://127.0.0.1:8080/jobs.html?desktop=1&startupProbe=1&bridgePort=8877&bridgeHost=127.0.0.1") {
  const sessionStorage = createStorageMock();
  global.window = {
    location: { href },
    sessionStorage,
    addEventListener: () => {}
  };
  return { sessionStorage };
}

test("jobs startup metrics add elapsedMs to first render and first interactive", () => {
  const calls = [];
  let nowMs = 1000;
  const metrics = createJobsStartupMetrics({
    emitMetric: (event, payload) => calls.push({ event, payload }),
    now: () => nowMs
  });

  nowMs = 1012;
  metrics.markRendered("startup_preview", 11);
  nowMs = 1018;
  metrics.markRendered("first_load_refresh", 42);
  nowMs = 1025;
  metrics.markInteractive("startup_preview");
  nowMs = 1032;
  metrics.markInteractive("first_load_refresh");

  assert.equal(calls[0].event, "jobs_first_render");
  assert.equal(calls[0].payload.elapsedMs, 12);
  assert.equal(calls[1].event, "jobs_first_interactive");
  assert.equal(calls[1].payload.elapsedMs, 25);
  assert.equal(calls.length, 2);
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

test("saved startup metrics emit first render once", () => {
  const calls = [];
  let nowMs = 700;
  const metrics = createSavedStartupMetrics({
    emitMetric: (event, payload) => calls.push({ event, payload }),
    now: () => nowMs
  });

  nowMs = 718;
  metrics.markRendered("auth_required", 0);
  nowMs = 725;
  metrics.markRendered("saved_jobs", 3);

  assert.equal(calls.length, 1);
  assert.equal(calls[0].event, "saved_first_render");
  assert.equal(calls[0].payload.stage, "auth_required");
  assert.equal(calls[0].payload.rowCount, 0);
  assert.equal(calls[0].payload.elapsedMs, 18);
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

test("startup probe metric transport retries queued posts after an early bridge failure", async () => {
  setupStartupProbeGlobals();
  const requests = [];
  let attempt = 0;
  global.fetch = async (url, init) => {
    requests.push({ url, init });
    attempt += 1;
    if (attempt === 1) {
      throw new Error("bridge not ready");
    }
    return { ok: true, status: 200 };
  };

  emitStartupProbeMetric("jobs_module_boot_start", { phase: "boot" });
  await flushStartupProbeMetricQueue();
  await flushStartupProbeMetricQueue();

  assert.equal(requests.length, 2);
  const payload = JSON.parse(String(requests[1].init?.body || "{}"));
  assert.equal(payload.event, "jobs_module_boot_start");
  assert.equal(payload.payload.phase, "boot");
  assert.equal(Number.isFinite(Number(payload.payload.browserCreatedAtMs)), true);
});
