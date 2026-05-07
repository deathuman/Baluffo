import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

function makeEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: selector => buttonsBySelector[selector] || []
  };
}

function makeButton(attributeValue) {
  return {
    getAttribute(name) {
      return name === "data-ops-diagnostics-copy" ? attributeValue : "";
    },
    addEventListener(_event, handler) {
      this.click = handler;
    }
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
          lifecycleStatus: "succeeded",
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
  assert.match(metricsEl.innerHTML, /data-ops-diagnostics-copy="taskStatus"/i);
  assert.match(metricsEl.innerHTML, /data-ops-diagnostics-copy="runtime"/i);
  assert.match(metricsEl.innerHTML, /data-ops-diagnostics-copy="failures"/i);
  assert.doesNotMatch(metricsEl.innerHTML, /data-ops-diagnostics-copy="dedup"/i);
  assert.match(metricsEl.innerHTML, /data-ops-diagnostics-copy="sourceHealth"/i);
  assert.match(metricsEl.innerHTML, /data-ops-diagnostics-copy="sourcePolicy"/i);
  assert.match(metricsEl.innerHTML, /Review queue 7; failed probes 2/i);
  assert.match(metricsEl.innerHTML, />succeeded</i);
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

test("admin render: diagnostics copy passes bounded section payload", () => {
  const runtimeButton = makeButton("runtime");
  const metricsEl = makeEl({
    "[data-ops-diagnostics-copy]": [runtimeButton]
  });
  const copied = [];
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 240000,
      duplicateRate: 0.1,
      dedupEvidence: {
        rawLargeThing: [{ hidden: true }]
      },
      slowestSources: Array.from({ length: 8 }, (_row, index) => ({
        name: `Source ${index}`,
        status: "ok",
        keptCount: index
      }))
    },
    history: {
      windowRuns: 3,
      medianDurationMs: 120000,
      averageDurationMs: 180000
    }
  }, null, {
    onCopySectionDiagnostics: section => copied.push(section)
  });

  runtimeButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "runtime");
  assert.equal(copied[0].title, "Runtime");
  assert.equal(copied[0].latestDurationMs, 240000);
  assert.equal(copied[0].examples.length, 5);
  const serialized = JSON.stringify(copied[0]);
  assert.doesNotMatch(serialized, /latestRun|dedupEvidence|rawLargeThing|recommendedApiPayload/i);
});
