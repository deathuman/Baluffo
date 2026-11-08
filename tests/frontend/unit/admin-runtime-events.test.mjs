import test from "node:test";
import assert from "node:assert/strict";
import { bindAdminRuntimeEvents } from "../../../frontend/admin/app/runtime/events.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";
import { createDeferred, flushMicrotasks as flushMicrotasksCore } from "./helpers/async-test-helpers.mjs";
import { createDeferredRenderScheduler, createElement } from "./helpers/admin-controller-test-helpers.mjs";
const flushMicrotasks = (count = 5) => flushMicrotasksCore(count);

function createClickableElement() {
  const listeners = new Map();
  return {
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    click() {
      (listeners.get("click") || []).forEach(handler => handler({
        preventDefault() {},
        stopPropagation() {}
      }));
    }
  };
}

function createToggleElement({ matches = () => false, open = false } = {}) {
  const listeners = new Map();
  return {
    open,
    matches,
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    toggleOpen(nextOpen = true, target = this) {
      this.open = Boolean(nextOpen);
      if (target && typeof target === "object") target.open = Boolean(nextOpen);
      (listeners.get("toggle") || []).forEach(handler => handler({ target }));
    }
  };
}

test("admin load discovery report button also reloads discovery log from the start", async () => {
  const previousWindow = global.window;
  global.window = {
    addEventListener() {}
  };

  try {
    const calls = [];
    const loadDiscoveryBtn = createClickableElement();

    bindAdminRuntimeEvents({
      state: {},
      refs: {
        adminLoadDiscoveryBtnEl: loadDiscoveryBtn
      },
      onRestoreActiveRunWatches: async () => {},
      getLastJobsUrl: () => "",
      onRefreshOverview: async () => {},
      fetcherController: {},
      discoveryController: {
        async loadDiscoveryLogChunk(options) {
          calls.push(["loadDiscoveryLogChunk", options]);
        }
      },
      registryController: {
        async loadDiscoveryData() {
          calls.push(["loadDiscoveryData"]);
        }
      },
      opsController: {},
      syncController: {},
      readShowZeroJobs: () => false,
      writeShowZeroJobs: () => {},
      showZeroJobsKey: "test",
      onSyncDiscoveryLogDisclosure: () => {},
      onSetSourceFilter: () => {}
    });

    loadDiscoveryBtn.click();
    await flushMicrotasks();

    assert.deepEqual(calls, [
      ["loadDiscoveryData"],
      ["loadDiscoveryLogChunk", { reset: true, guarded: false, view: "tail", limitChars: 65536 }]
    ]);
  } finally {
    global.window = previousWindow;
  }
});

test("admin registry and sync diagnostics load when the disclosure opens", async () => {
  const previousWindow = global.window;
  global.window = {
    addEventListener() {}
  };

  try {
    const calls = [];
    const opsKpisEl = createToggleElement();
    const registrySyncDetails = createToggleElement({
      matches: selector => selector === ".admin-ops-registry-sync-details",
      open: false
    });

    bindAdminRuntimeEvents({
      state: {},
      refs: {
        adminOpsKpisEl: opsKpisEl
      },
      onRestoreActiveRunWatches: async () => {},
      getLastJobsUrl: () => "",
      fetcherController: {},
      discoveryController: {},
      registryController: {},
      opsController: {
        async loadRegistrySyncDiagnosticsData(options) {
          calls.push(options);
        }
      },
      syncController: {},
      readShowZeroJobs: () => false,
      writeShowZeroJobs: () => {},
      showZeroJobsKey: "test",
      onSyncDiscoveryLogDisclosure: () => {},
      onSetSourceFilter: () => {}
    });

    opsKpisEl.toggleOpen(true, registrySyncDetails);
    await flushMicrotasks();

    assert.deepEqual(calls, [{ silent: false }]);
  } finally {
    global.window = previousWindow;
  }
});

test("opening fetcher diagnostics reuses the dedup tab's in-flight metrics request", async () => {
  const previousWindow = global.window;
  global.window = {
    addEventListener() {}
  };

  try {
    const state = {
      latestOpsHealthCache: null,
      latestOpsFetcherMetricsPayload: null,
      opsFetcherDiagnosticsLoaded: false,
      opsFetcherDiagnosticsLoading: false,
      opsFetcherDiagnosticsOpen: false,
      adminSectionLoadState: {},
      adminBusyState: {
        opsLoad: false,
        liveFetchRunning: false,
        liveDiscoveryRunning: false,
        liveSyncRunning: false,
        livePipelineRunning: false
      }
    };
    const metricsEl = Object.assign(createToggleElement(), {
      dataset: {},
      querySelector: () => null
    });
    const diagnosticsDetails = createToggleElement({
      matches: selector => selector === ".admin-ops-fetcher-diagnostics",
      open: false
    });
    const calls = [];
    const deferred = createDeferred();
    const renderScheduler = createDeferredRenderScheduler();
    const payload = {
      latestRun: {
        browserFallbackHealth: {
          ok: false,
          stage: "launch",
          error: "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
          errorType: "Error",
          durationMs: 281
        }
      },
      history: {}
    };
    const controller = createAdminOpsController({
      state,
      refs: {
        adminOpsFetcherMetricsEl: metricsEl,
        adminOpsDedupListsEl: createElement(),
        adminOpsHistoryEl: createElement(),
        adminOpsTrendsEl: createElement()
      },
      getBridge: async path => {
        calls.push(path);
        if (path === "/ops/browser-fallback-queue") {
          return { ok: true, exists: false, duplicatePages: [], parseErrorRows: [], warnings: ["missing"] };
        }
        return deferred.promise;
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => ({
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: false,
        liveTypes: []
      }),
      getOpsPollIntervalMs: () => 5000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsFetcherMetrics,
      renderAdminOpsDedupLists() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      adminDispatch: { dispatch() {} },
      adminActions: { OPS_REFRESHED: "ops/refreshed" },
      escapeHtml: value => String(value || ""),
      onBridgeStatusChange() {},
      bridgeStatusPollIntervalMs: 1000,
      idlePollIntervalMs: 1000,
      renderScheduler: renderScheduler.schedule
    });

    renderAdminOpsFetcherMetrics(metricsEl, null, null, { fetcherDiagnosticsOpen: false });
    assert.match(metricsEl.innerHTML, /Open to load browser fallback health/i);

    const dedupLoad = controller.selectOpsTab("dedup");
    assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80"]);

    bindAdminRuntimeEvents({
      state,
      refs: { adminOpsFetcherMetricsEl: metricsEl },
      onRestoreActiveRunWatches: async () => {},
      getLastJobsUrl: () => "",
      fetcherController: {},
      discoveryController: {},
      registryController: {},
      opsController: controller,
      syncController: {},
      readShowZeroJobs: () => false,
      writeShowZeroJobs: () => {},
      showZeroJobsKey: "test",
      onSyncDiscoveryLogDisclosure: () => {},
      onSetSourceFilter: () => {}
    });

    metricsEl.toggleOpen(true, diagnosticsDetails);
    await flushMicrotasks();

    assert.equal(state.opsFetcherDiagnosticsOpen, true);
    assert.equal(state.opsFetcherDiagnosticsLoading, true);
    assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80", "/ops/browser-fallback-queue"]);

    deferred.resolve(payload);
    await dedupLoad;
    await flushMicrotasks(20);
    renderScheduler.flush();

    assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80", "/ops/browser-fallback-queue"]);
    assert.equal(state.opsFetcherDiagnosticsLoading, false);
    assert.match(metricsEl.innerHTML, /<details[^>]*admin-ops-fetcher-diagnostics[^>]* open/i);
    assert.match(metricsEl.innerHTML, /Browser fallback health/);
    assert.match(metricsEl.innerHTML, /Executable doesn/);
    assert.doesNotMatch(metricsEl.innerHTML, /Open to load browser fallback health/i);
  } finally {
    global.window = previousWindow;
  }
});
