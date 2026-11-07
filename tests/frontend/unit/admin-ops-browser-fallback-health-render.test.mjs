import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";
import { createButtonMapEl as makeEl } from "./helpers/dom-test-helpers.mjs";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin render: browser fallback health exposes the raw launch error", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      browserFallbackHealth: {
        ok: false,
        stage: "launch",
        error: "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
        errorType: "Error",
        durationMs: 281,
        lastFailureAt: "2026-11-06T10:53:32+00:00",
        lastLaunchError: "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
        lastNavigationError: ""
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Browser fallback health/);
  assert.match(metricsEl.innerHTML, /failed at launch \(Error\)/);
  assert.match(metricsEl.innerHTML, /281ms/);
  assert.match(metricsEl.innerHTML, /Executable doesn/);
  assert.match(metricsEl.innerHTML, /ms-playwright\/chromium-1200/);
});

test("admin render: browser fallback health is omitted when no probe ran", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, { latestRun: {}, history: {} });
  assert.doesNotMatch(metricsEl.innerHTML, /Browser fallback health/i);
});

test("admin render: browser fallback health renders before debug diagnostics load", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      browserFallbackHealth: {
        ok: false,
        stage: "launch",
        error: "BrowserType.launch: Executable doesn't exist",
        errorType: "Error"
      }
    },
    history: {}
  }, null, { includeDebugDiagnostics: false });

  assert.match(metricsEl.innerHTML, /Browser fallback health/);
  assert.match(metricsEl.innerHTML, /Executable doesn/);
  assert.doesNotMatch(metricsEl.innerHTML, /Runtime diagnostics/);
});

test("admin ops controller opens fetcher diagnostics after dedup loaded fetcher metrics", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsFetcherMetricsPayload: null,
    opsFetcherDiagnosticsLoaded: false,
    opsFetcherDiagnosticsLoading: false,
    opsFetcherDiagnosticsOpen: false,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const metricsEl = Object.assign(makeEl(), {
    dataset: {},
    querySelector: () => null
  });
  const refs = {
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: metricsEl,
    adminOpsDedupListsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const calls = [];
  const renderScheduler = createDeferredRenderScheduler();
  const payload = {
    latestRun: {
      durationMs: 1200,
      sourceCount: 4,
      failedSources: 0,
      dedupEvidence: {
        dedupAuditGate: { status: "blocked" }
      },
      browserFallbackHealth: {
        ok: false,
        stage: "launch",
        error: "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
        errorType: "Error",
        durationMs: 281
      }
    },
    history: { windowRuns: 1 }
  };
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/fetcher-metrics?windowRuns=80") return payload;
      throw new Error(`unexpected path ${path}`);
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

  await controller.selectOpsTab("dedup");
  assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80"]);

  // Opening the disclosure is the runtime event's job; it sets this flag before
  // invoking the loader. Dedup's fetcher-metrics read is the same payload, so the
  // loader must paint it without issuing a second request.
  state.opsFetcherDiagnosticsOpen = true;
  const loaded = await controller.loadFetcherDiagnosticsData();
  renderScheduler.flush();

  assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80"]);
  assert.equal(loaded, payload);
  assert.equal(state.opsFetcherDiagnosticsLoading, false);
  assert.match(metricsEl.innerHTML, /<details[^>]*admin-ops-fetcher-diagnostics[^>]* open/i);
  assert.match(metricsEl.innerHTML, /Browser fallback health/);
  assert.match(metricsEl.innerHTML, /Executable doesn/);
  assert.doesNotMatch(metricsEl.innerHTML, /Open to load browser fallback health/i);
});

test("admin ops controller loads fetcher diagnostics on demand", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsFetcherMetricsPayload: null,
    opsFetcherDiagnosticsLoaded: false,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const calls = [];
  const renderedMetrics = [];
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
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/fetcher-metrics?windowRuns=80") return payload;
      throw new Error(`unexpected path ${path}`);
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
    renderAdminOpsFetcherMetrics(_el, metrics) {
      renderedMetrics.push(metrics);
    },
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

  const loaded = await controller.loadFetcherDiagnosticsData();
  renderScheduler.flush();

  assert.deepEqual(calls, ["/ops/fetcher-metrics?windowRuns=80"]);
  assert.equal(loaded, payload);
  assert.equal(state.opsFetcherDiagnosticsLoaded, true);
  assert.equal(state.opsFetcherDiagnosticsLoading, false);
  assert.equal(renderedMetrics.at(-1)?.latestRun?.browserFallbackHealth?.ok, false);
  assert.match(
    renderedMetrics.at(-1)?.latestRun?.browserFallbackHealth?.error || "",
    /Executable doesn't exist/
  );
});
