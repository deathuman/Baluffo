import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { applyAdminTaskProgress } from "../../../frontend/admin/app/progress-ui.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";
import { createRestoreActiveRunWatches } from "../../../frontend/admin/app/live-task.js";
import { appendAdminLogRow } from "../../../frontend/admin/render.js";
import {
  FakeInputElement,
  createClassList,
  createDiscoveryControllerFixture,
  createElement,
  createFetcherControllerFixture,
  createRegistryControllerFixture,
  stubDateNow,
  stubScheduledTimers,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin live-task restore helper restarts active fetch and discovery watches", async () => {
  const calls = [];
  let fetchLiveLoads = 0;
  let discoveryLiveLoads = 0;

  const restoreActiveRunWatches = createRestoreActiveRunWatches({
    loadFetcherLivePayload: async () => {
      fetchLiveLoads += 1;
      return {
        active: true,
        runId: "fetch_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: ""
      };
    },
    loadLatestFetcherReport: async options => {
      calls.push(`loadLatestFetcherReport:${String(Boolean(options?.silent))}`);
      return {
        runId: "fetch_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: "",
        taskProgress: { active: true, phaseKey: "executing_sources", phaseLabel: "Executing sources" }
      };
    },
    fetcherController: {
      attachToActiveFetchRun(runMeta, options) {
        calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    },
    loadDiscoveryLivePayload: async () => {
      discoveryLiveLoads += 1;
      return {
        active: true,
        runId: "discovery_restore_2",
        startedAt: "2026-03-29T11:49:22+02:00",
        finishedAt: ""
      };
    },
    loadLatestDiscoveryReport: async () => {
      throw new Error("discovery report fallback should not run");
    },
    discoveryController: {
      attachToActiveDiscoveryRun(runMeta, options) {
        calls.push(`attachToActiveDiscoveryRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    }
  });

  await Promise.all([restoreActiveRunWatches(), restoreActiveRunWatches()]);

  assert.equal(fetchLiveLoads, 1);
  assert.equal(discoveryLiveLoads, 1);
  assert.equal(
    calls.filter(line => line === "attachToActiveFetchRun:fetch_restore_2:false").length,
    1
  );
  assert.equal(
    calls.filter(line => line === "attachToActiveDiscoveryRun:discovery_restore_2:false").length,
    1
  );
  assert.ok(calls.includes("loadLatestFetcherReport:true"));
});

test("admin live-task restore helper reattaches fetch watch from an active cold-load report", async () => {
  const calls = [];

  const restoreActiveRunWatches = createRestoreActiveRunWatches({
    loadFetcherLivePayload: async () => null,
    loadLatestFetcherReport: async () => ({
      runId: "fetch_restore_cold_1",
      startedAt: "2026-03-29T11:49:22+02:00",
      finishedAt: "",
      taskProgress: { active: true, phaseKey: "executing_sources", phaseLabel: "Executing sources" },
      summary: { outputCount: 10, failedSources: 0, sourceCount: 10 }
    }),
    fetcherController: {
      getRestorableFetcherRunMeta(report) {
        calls.push(`restoreMeta:${String(report?.runId || "")}`);
        return null;
      },
      attachToActiveFetchRun(runMeta, options) {
        calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
      }
    },
    loadDiscoveryLivePayload: async () => null,
    loadLatestDiscoveryReport: async () => null,
    discoveryController: {}
  });

  await restoreActiveRunWatches();

  assert.ok(calls.includes("restoreMeta:fetch_restore_cold_1"));
  assert.ok(calls.includes("attachToActiveFetchRun:fetch_restore_cold_1:false"));
});

test("admin live-task restore helper silently hydrates fetch progress on first boot attach", async () => {
  const timerStub = stubScheduledTimers();
  let controller;
  try {
    const fixture = createFetcherControllerFixture();
    fixture.options.getBridge = async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/task-live/fetch") {
        return {
          active: true,
          runId: "fetch_boot_restore_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: ""
        };
      }
      return {};
    };
    fixture.options.fetchJobsFetchReportJson = async () => ({
      runId: "fetch_boot_restore_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 6,
          sourceCount: 12,
          runningTasks: 6,
          queuedTasks: 0,
          outputCount: 18,
          failedSources: 1,
          excludedSources: 0
        }
      },
      summary: { outputCount: 18, failedSources: 1, excludedSources: 0, sourceCount: 12 },
      sources: [{ name: "Studio A", status: "running" }]
    });
    fixture.options.loadOpsHealthData = async () => {};
    controller = createAdminFetcherController(fixture.options);

    const restoreActiveRunWatches = createRestoreActiveRunWatches({
      loadFetcherLivePayload: (...args) => controller.loadFetcherLivePayload(...args),
      loadLatestFetcherReport: options => controller.loadLatestFetcherReport(options),
      fetcherController: controller,
      loadDiscoveryLivePayload: async () => null,
      loadLatestDiscoveryReport: async () => null,
      discoveryController: {}
    });

    await restoreActiveRunWatches();

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /executing sources/i);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    await timerStub.scheduled[1]();
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_boot_restore_1",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    assert.deepEqual(fixture.logs, []);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

