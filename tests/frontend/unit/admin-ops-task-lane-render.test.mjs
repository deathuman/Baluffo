import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: fetcher metrics render compact task lane", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 240000
    },
    history: {}
  }, null, {
    runModel: {
      currentRows: [
        {
          type: "fetch",
          displayStatus: "running",
          isLive: true,
          elapsedMs: 125000,
          taskProgress: {
            phaseLabel: "Executing sources"
          },
          summary: {
            outputCount: 42,
            failedSources: 1
          }
        }
      ],
      visibleCompletedRows: [
        {
          type: "discovery",
          status: "ok",
          durationMs: 45000,
          summary: {
            queuedCandidateCount: 7,
            failedProbeCount: 2
          }
        },
        {
          type: "sync",
          status: "warning",
          durationMs: 18000,
          summary: {
            action: "push",
            activeCount: 120,
            pendingCount: 14,
            rejectedCount: 8
          }
        }
      ],
      olderCompletedRows: []
    }
  });

  assert.match(metricsEl.innerHTML, /admin-ops-task-lane/i);
  assert.match(metricsEl.innerHTML, /Task Status/i);
  assert.match(metricsEl.innerHTML, /Review queue 7; failed probes 2/i);
  assert.match(metricsEl.innerHTML, /Executing sources/i);
  assert.match(metricsEl.innerHTML, /push: active 120, pending 14, rejected 8/i);
  assert.ok(metricsEl.innerHTML.indexOf("admin-ops-task-lane") < metricsEl.innerHTML.indexOf("admin-ops-metrics-section-runtime"));
  assert.doesNotMatch(metricsEl.innerHTML, /start-btn|stop-btn|retry-btn|clear-btn|merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});

test("admin render: fetcher metrics task lane renders waiting defaults", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, { latestRun: {}, history: {} });

  assert.match(metricsEl.innerHTML, /admin-ops-task-lane-card-discovery/i);
  assert.match(metricsEl.innerHTML, /admin-ops-task-lane-card-fetch/i);
  assert.match(metricsEl.innerHTML, /admin-ops-task-lane-card-sync/i);
  assert.match(metricsEl.innerHTML, /waiting/i);
  assert.match(metricsEl.innerHTML, /No run yet/i);
  assert.match(metricsEl.innerHTML, /Waiting for the next run/i);
});
