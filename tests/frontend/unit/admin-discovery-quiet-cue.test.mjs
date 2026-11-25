import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { deriveDiscoveryProgressModel } from "../../../frontend/admin/domain/progress.js";

import { createDiscoveryControllerFixture, stubDateNow, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

test("admin discovery quiet cue ignores a sub-minute gap", () => {
  const report = {
    taskProgress: {
      active: true,
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      ratio: 0.5,
      counts: { probedCandidates: 512, probeTotal: 2135 }
    }
  };
  const fresh = deriveDiscoveryProgressModel(report, { running: true, quietMs: 30_000 });
  const quiet = deriveDiscoveryProgressModel(report, { running: true, quietMs: 12 * 60_000 });

  // A sub-minute gap is not a quiet cue; claiming "1m" at 5s would be a lie.
  assert.doesNotMatch(fresh.label, /counts unchanged/i);
  assert.match(quiet.label, /counts unchanged 12m/i);
});

test("admin discovery controller shows counts unchanged from the server counts stamp", async () => {
  const timerStub = stubScheduledTimers();
  const dateStub = stubDateNow(() => Date.parse("2026-03-08T10:20:00.000Z"));

  try {
    const fixture = createDiscoveryControllerFixture();
    fixture.options.getBridge = async path => {
      if (path === "/ops/task-live/discovery?view=summary") {
        return {
          taskType: "discovery",
          active: true,
          runId: "discovery_quiet_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          // The snapshot heartbeat is fresh; only the counters are frozen.
          heartbeatAt: "2026-03-08T10:19:59.000Z",
          taskProgress: {
            active: true,
            phaseKey: "probing_candidates",
            phaseLabel: "Probing 2135 candidate(s)",
            mode: "determinate",
            ratio: 0.5,
            counts: {
              foundEndpoints: 2400,
              probedCandidates: 512,
              queuedCandidates: 200,
              deferredCandidates: 0,
              failedProbes: 0
            },
            countsUpdatedAt: "2026-03-08T10:08:00.000Z",
            updatedAt: "2026-03-08T10:19:59.000Z"
          },
          summary: { phaseLabel: "Probing 2135 candidate(s)" },
          workItems: [],
          recentEvents: []
        };
      }
      if (String(path).startsWith("/discovery/log?")) return { text: "", nextOffset: 0 };
      if (path === "/discovery/report" || path === "/discovery/report?view=summary") return {};
      throw new Error(`unexpected path ${path}`);
    };
    fixture.options.loadOpsHealthData = async () => {};
    fixture.options.scheduleOpsHealthPolling = () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    controller.startDiscoveryCompletionWatch();
    await timerStub.scheduled[0]();
    await new Promise(resolve => setImmediate(resolve));

    assert.match(
      String(fixture.refs.adminDiscoveryProgressLabelEl.textContent || ""),
      /counts unchanged 12m/i
    );
  } finally {
    dateStub.restore();
    timerStub.restore();
  }
});
