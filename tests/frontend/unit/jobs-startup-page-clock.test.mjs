import test from "node:test";
import assert from "node:assert/strict";

import { createJobsStartupMetrics } from "../../../frontend/jobs/app/runtime/effects.js";
import { createJobsFeedController } from "../../../frontend/jobs/app/runtime/feed-controller.js";

test("jobs startup metrics stamp every row with a page clock", () => {
  const calls = [];
  let clock = 1000;
  const metrics = createJobsStartupMetrics({
    emitMetric: (event, payload) => calls.push({ event, payload }),
    now: () => clock
  });

  clock = 1012;
  metrics.emit("jobs_init_start");
  clock = 1020;
  metrics.markRendered("startup_preview", 4);

  assert.equal(calls.length, 2);
  assert.equal(calls[0].payload.pageTsMs, Math.round(Number(performance.timeOrigin) + 1012));
  assert.equal(calls[1].payload.pageTsMs, Math.round(Number(performance.timeOrigin) + 1020));
  assert.equal(calls[0].payload.elapsedMs, 12);
  assert.equal(calls[1].payload.elapsedMs, 20);
  assert.equal(Number.isInteger(calls[0].payload.pageTsMs), true);
});

test("the report probe emits a span per candidate plus a total", async () => {
  const calls = [];
  const candidates = [];
  let clock = 5000;
  const controller = createJobsFeedController({
    runtimeState: {},
    jobsFetchReportUrls: ["data/jobs-fetch-report.json", "jobs-fetch-report.json"],
    fetchJsonFromCandidatesFromSources: async (urls, options) => {
      assert.deepEqual(urls, ["data/jobs-fetch-report.json", "jobs-fetch-report.json"]);
      candidates.push(options.timeoutMs);
      options.onCandidate({ url: urls[0], status: 404, ok: false, ms: 6 });
      options.onCandidate({ url: urls[1], status: 200, ok: true, ms: 3 });
      return { finishedAt: "2026-10-20T09:00:00+00:00", summary: { outputCount: 12 } };
    },
    emitDesktopStartupMetric: (event, payload) => calls.push({ event, payload }),
    now: () => (clock += 10)
  });

  const report = await controller.fetchJobsReport({ timeoutMs: 1500 });

  assert.equal(report.summary.outputCount, 12);
  assert.deepEqual(
    calls.map(call => call.event),
    [
      "jobs_report_probe_start",
      "jobs_report_probe_candidate",
      "jobs_report_probe_candidate",
      "jobs_report_probe_done"
    ]
  );
  assert.equal(calls[0].payload.candidateCount, 2);
  assert.equal(calls[0].payload.timeoutMs, 1500);
  assert.deepEqual(candidates, [1500]);
  assert.equal(calls[1].payload.status, 404);
  assert.equal(calls[1].payload.ok, false);
  assert.equal(calls[2].payload.status, 200);
  assert.equal(calls[3].payload.found, true);
  assert.equal(calls[3].payload.ms, 10);
});

test("the report probe still reports a miss when every candidate fails", async () => {
  const calls = [];
  const controller = createJobsFeedController({
    runtimeState: {},
    jobsFetchReportUrls: ["data/jobs-fetch-report.json"],
    fetchJsonFromCandidatesFromSources: async (urls, options) => {
      options.onCandidate({ url: urls[0], status: 0, ok: false, ms: 800 });
      return null;
    },
    emitDesktopStartupMetric: (event, payload) => calls.push({ event, payload }),
    now: () => 100
  });

  const report = await controller.fetchJobsReport();

  assert.equal(report, null);
  const done = calls[calls.length - 1];
  assert.equal(done.event, "jobs_report_probe_done");
  assert.equal(done.payload.found, false);
  assert.equal(done.payload.candidateCount, 1);
});
