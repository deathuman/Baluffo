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

test("admin auth controller initializes the composed admin view immediately", async () => {
  const dispatched = [];
  const toasts = [];
  const calls = [];
  const refs = {
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminSyncStatusEl: createElement()
  };

  const controller = createAdminAuthController({
    refs,
    adminDispatch: {
      dispatch(action) {
        dispatched.push(action);
      }
    },
    adminActions: {
      UNLOCKED: "unlocked",
      LOCKED: "locked"
    },
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {},
    syncAdminBusyUi() {
      calls.push("syncAdminBusyUi");
    },
    syncDiscoveryLogDisclosure() {
      calls.push("syncDiscoveryLogDisclosure");
    },
    resetBusyFlags() {
      calls.push("resetBusyFlags");
    },
    setSourceFilter(value) {
      calls.push(`setSourceFilter:${value}`);
    },
    setSourceStatus(text) {
      refs.adminSourceStatusEl = { textContent: text };
    },
    setFetcherLogPlaceholder(message) {
      calls.push(`fetcherPlaceholder:${message}`);
    },
    attachToActiveFetchRun(runMeta) {
      calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}`);
    },
    setDiscoveryLogPlaceholder(message) {
      calls.push(`discoveryPlaceholder:${message}`);
    },
    clearOptimisticFetchRun() {
      calls.push("clearOptimisticFetchRun");
    },
    clearOptimisticDiscoveryRun() {
      calls.push("clearOptimisticDiscoveryRun");
    },
    setManualSourceFeedback(message) {
      calls.push(`manualFeedback:${message}`);
    },
    setOpsPlaceholders(message = "") {
      calls.push(`opsPlaceholder:${message}`);
    },
    setBridgeStatusBadge(stateValue, label) {
      calls.push(`bridge:${stateValue}:${label}`);
    },
    renderUsersEmpty(message) {
      calls.push(`renderEmpty:${message}`);
    },
    startBridgeStatusWatch() {
      calls.push("startBridgeStatusWatch");
    },
    stopBridgeStatusWatch() {
      calls.push("stopBridgeStatusWatch");
    },
    scheduleOpsHealthPolling(delay) {
      calls.push(`scheduleOpsHealthPolling:${delay}`);
    },
    stopOpsHealthPolling() {
      calls.push("stopOpsHealthPolling");
    },
    refreshOverview: async () => {
      calls.push("refreshOverview");
    },
    loadLatestFetcherReport: async options => {
      calls.push(`loadLatestFetcherReport:${String(Boolean(options?.silent))}`);
    },
    loadDiscoveryData: async () => {
      calls.push("loadDiscoveryData");
    },
    loadOpsHealthData: async () => {
      calls.push("loadOpsHealthData");
    },
    loadSyncStatus: async options => {
      calls.push(`loadSyncStatus:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
    },
    loadDiscoveryConfig: async options => {
      calls.push(`loadDiscoveryConfig:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    }
  });

  const initReady = controller.initAdminPage();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(initReady, true);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("hidden"), false);
  assert.deepEqual(dispatched.map(item => item.type), []);
  assert.ok(calls.includes("resetBusyFlags"));
  assert.ok(calls.includes("startBridgeStatusWatch"));
  assert.ok(calls.includes("refreshOverview"));
  assert.ok(calls.includes("loadDiscoveryData"));
  assert.ok(calls.includes("loadOpsHealthData"));
  assert.ok(calls.includes("loadDiscoveryConfig:true:true"));
  assert.ok(calls.includes("scheduleOpsHealthPolling:900"));
  assert.equal(refs.adminSyncStatusEl.textContent, "Loading sync status...");
  assert.equal(toasts.length, 0);
});

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

test("admin auth controller session view model tracks bridge badge state", async () => {
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["online"]) }),
    adminContentEl: createElement()
  };
  const controller = createAdminAuthController({
    refs,
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {},
    syncAdminBusyUi() {},
    syncDiscoveryLogDisclosure() {},
    resetBusyFlags() {},
    setSourceFilter() {},
    setSourceStatus() {},
    setFetcherLogPlaceholder() {},
    setDiscoveryLogPlaceholder() {},
    clearOptimisticFetchRun() {},
    clearOptimisticDiscoveryRun() {},
    setManualSourceFeedback() {},
    setOpsPlaceholders() {},
    setBridgeStatusBadge() {},
    renderUsersEmpty() {},
    startBridgeStatusWatch() {},
    stopBridgeStatusWatch() {},
    scheduleOpsHealthPolling() {},
    stopOpsHealthPolling() {},
    refreshOverview: async () => {},
    loadLatestFetcherReport: async () => {},
    loadDiscoveryData: async () => {},
    loadOpsHealthData: async () => {},
    loadSyncStatus: async () => {},
    loadDiscoveryConfig: async () => {},
    logAdminError() {},
    showToast() {}
  });

  assert.deepEqual(controller.toAdminSessionViewModel(), {
    isUnlocked: true,
    apiReady: true,
    bridgeStatus: "online"
  });
});

test("admin registry controller loads filtered discovery state and dispatches refresh", async () => {
  const state = {
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    adminBusyState: {
      discoveryLoad: false
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    adminManualSourceFeedbackEl: createElement()
  };
  const dispatched = [];
  const logs = [];
  const busyTransitions = [];
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report") {
        return {
          summary: {
            foundEndpointCount: 4,
            probedCandidateCount: 3,
            queuedCandidateCount: 2,
            discoverableButDeferredCount: 1,
            validatedCandidateCount: 3,
            liveCandidateCount: 2,
            skippedDuplicateCount: 1,
            failedProbeCount: 1
          },
          topFailures: [{ key: "dns_error", count: 2 }]
        };
      }
      if (path === "/registry/pending") {
        return {
          summary: { pendingCount: 2 },
          sources: [
            { id: "p1", name: "One", jobsFound: 2, status: "healthy" },
            { id: "p2", name: "Zero", jobsFound: 0, status: "healthy" }
          ]
        };
      }
      if (path === "/registry/active") {
        return {
          summary: { activeCount: 1 },
          sources: [{ id: "a1", name: "Active", jobsFound: 3, status: "healthy" }]
        };
      }
      if (path === "/registry/rejected") {
        return {
          summary: { rejectedCount: 1 },
          sources: [{ id: "r1", name: "Rejected", jobsFound: 1, status: "error" }]
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceStatusFromReport: rows => rows,
    applySourceFilter: rows => rows,
    getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
    deriveSourceStatus: row => String(row?.status || "unknown"),
    renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
    readShowZeroJobs: () => false,
    normalizeSourceFilter: value => value,
    adminDispatch: {
      dispatch(action) {
        dispatched.push(action);
      }
    },
    adminActions: {
      DISCOVERY_REFRESHED: "discovery/refreshed"
    },
    appendDiscoveryLog(message) {
      logs.push(String(message));
    },
    formatManualCheckFailureMessage: () => "failed",
    loadOpsHealthData: async () => {},
    setBusyFlag(key, value) {
      busyTransitions.push(`${key}:${String(value)}`);
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown")
  });

  await controller.loadDiscoveryData();

  assert.match(refs.adminDiscoverySummaryEl.textContent, /Found 4 \| Probed 3 \| Queued \(new\) 2/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Deferred review 1/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Validated 3/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Live 2/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Hidden zero-jobs 1/);
  assert.equal(refs.adminPendingSourcesEl.innerHTML, "One");
  assert.equal(refs.adminActiveSourcesEl.innerHTML, "Active");
  assert.equal(refs.adminRejectedSourcesEl.innerHTML, "Rejected");
  assert.deepEqual(dispatched.map(item => item.type), ["discovery/refreshed"]);
  assert.ok(logs.some(line => /source discovery data loaded/i.test(line)));
  assert.deepEqual(busyTransitions, ["discoveryLoad:true", "discoveryLoad:false"]);
});

test("admin registry controller only logs discovery refreshes when the registry snapshot changes", async () => {
  const logs = [];
  const state = {
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement()
  };
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report") return { summary: {} };
      if (path === "/registry/pending") {
        return {
          summary: { pendingCount: 1 },
          sources: [{ id: "p1", name: "Pending", jobsFound: 1, status: "pending" }]
        };
      }
      if (path === "/registry/active") return { summary: { activeCount: 0 }, sources: [] };
      if (path === "/registry/rejected") return { summary: { rejectedCount: 0 }, sources: [] };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceStatusFromReport: rows => rows,
    applySourceFilter: rows => rows,
    getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
    deriveSourceStatus: row => String(row?.status || "unknown"),
    renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
    readShowZeroJobs: () => false,
    normalizeSourceFilter: value => value,
    adminDispatch: { dispatch() {} },
    adminActions: { DISCOVERY_REFRESHED: "discovery/refreshed" },
    appendDiscoveryLog(message) {
      logs.push(String(message));
    },
    formatManualCheckFailureMessage: () => "failed",
    loadOpsHealthData: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown")
  });

  await controller.loadDiscoveryData();
  await controller.loadDiscoveryData();

  assert.equal(logs.filter(line => /source discovery data loaded/i.test(line)).length, 1);
  assert.equal(logs.filter(line => /loading source discovery report and registries/i.test(line)).length, 1);
  assert.ok(logs.some(line => /discovery summary:/i.test(line)));
});

test("admin registry controller syncs source tables once per completed task signature", async () => {
  const fetchReportCalls = [];
  const fixture = createRegistryControllerFixture();
  fixture.options.getBridge = async path => {
    if (path === "/discovery/report") {
      return {
        summary: {
          foundEndpointCount: 4,
          probedCandidateCount: 3,
          queuedCandidateCount: 1,
          discoverableButDeferredCount: 0,
          failedProbeCount: 0,
          skippedDuplicateCount: 0
        }
      };
    }
    if (path === "/registry/pending") return { sources: [{ id: "pending_1", name: "Pending" }], summary: { pendingCount: 1 } };
    if (path === "/registry/active") return { sources: [{ id: "active_1", name: "Active" }], summary: { activeCount: 1 } };
    if (path === "/registry/rejected") return { sources: [], summary: { rejectedCount: 0 } };
    throw new Error(`unexpected path ${path}`);
  };
  fixture.options.fetchJobsFetchReportJson = async () => {
    fetchReportCalls.push("fetch");
    return { sources: [{ name: "Active", status: "ok" }] };
  };
  fixture.options.getSourceJobsFoundCount = () => 1;
  const controller = createAdminRegistryController(fixture.options);

  await controller.syncSourceTablesAfterTaskCompletion({
    taskType: "fetch",
    completionSignature: "fetch_run_1|2026-03-08T10:10:00.000Z",
    fetchReport: { sources: [{ name: "Active", status: "ok" }] }
  });
  await controller.syncSourceTablesAfterTaskCompletion({
    taskType: "fetch",
    completionSignature: "fetch_run_1|2026-03-08T10:10:00.000Z",
    fetchReport: { sources: [{ name: "Active", status: "ok" }] }
  });

  assert.equal(fetchReportCalls.length, 0);
  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "Pending");
  assert.equal(fixture.refs.adminActiveSourcesEl.innerHTML, "Active");
  assert.equal(fixture.logs.filter(line => /source discovery data loaded/i.test(line)).length, 0);
});

test("admin registry controller adds a manual source and runs the follow-up check", async () => {
  const toasts = [];
  const logs = [];
  const state = {
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminManualSourceUrlEl: createElement({ value: "https://studio.example/jobs" }),
    adminManualSourceFeedbackEl: createElement(),
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement()
  };
  const calls = [];
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/discovery/report") {
        return { summary: {} };
      }
      if (path === "/registry/pending" || path === "/registry/active" || path === "/registry/rejected") {
        return { summary: {}, sources: [] };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/sources/manual") {
        return {
          status: "added",
          sourceId: "src_1",
          source: { adapter: "static" }
        };
      }
      if (path === "/discovery/check-source") {
        return {
          started: true,
          ok: true,
          jobsFound: 5,
          weakSignal: false,
          browserFallbackUsed: true
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceStatusFromReport: rows => rows,
    applySourceFilter: rows => rows,
    getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
    deriveSourceStatus: row => String(row?.status || "unknown"),
    renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
    readShowZeroJobs: () => false,
    normalizeSourceFilter: value => value,
    adminDispatch: { dispatch() {} },
    adminActions: { DISCOVERY_REFRESHED: "discovery/refreshed" },
    appendDiscoveryLog(message) {
      logs.push(String(message));
    },
    formatManualCheckFailureMessage: () => "failed",
    loadOpsHealthData: async () => {
      calls.push("loadOpsHealthData");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown")
  });

  await controller.addManualSource();

  assert.equal(refs.adminManualSourceUrlEl.value, "");
  assert.equal(refs.adminManualSourceFeedbackEl.textContent, "check started");
  assert.equal(refs.adminManualSourceFeedbackEl.classList.contains("muted"), true);
  assert.ok(calls.includes("/sources/manual:{\"url\":\"https://studio.example/jobs\"}"));
  assert.ok(calls.includes("/discovery/check-source:{\"sourceId\":\"src_1\"}"));
  assert.ok(calls.includes("/discovery/report"));
  assert.ok(calls.includes("loadOpsHealthData"));
  assert.ok(logs.some(line => /manual source added/i.test(line)));
  assert.ok(logs.some(line => /source discovery data loaded/i.test(line)));
  assert.ok(logs.some(line => /browser fallback was used/i.test(line)));
  assert.ok(toasts.some(item => item.message === "Manual source added and checked." && item.level === "success"));
});

test("admin registry controller approves selected pending rows", async () => {
  await withDom(
    new Map([
      [
        ".pending-source-checkbox",
        [
          new FakeInputElement({ checked: true, sourceId: "pending_1" }),
          new FakeInputElement({ checked: false, sourceId: "pending_2" })
        ]
      ]
    ]),
    async () => {
      const posts = [];
      const logs = [];
      const state = {
        adminBusyState: {
          discoveryRun: false,
          discoveryWatch: false,
          discoveryLoad: false,
          discoveryWrite: false,
          manualAdd: false,
          manualCheck: false,
          liveDiscoveryRunning: false
        }
      };
      const controller = createAdminRegistryController({
        state,
        refs: {
          adminManualSourceFeedbackEl: createElement(),
          adminDiscoverySummaryEl: createElement(),
          adminPendingSourcesEl: createElement({
            querySelectorAll: selector => global.document.querySelectorAll(selector)
          }),
          adminActiveSourcesEl: createElement(),
          adminRejectedSourcesEl: createElement()
        },
        getBridge: async path => {
          posts.push({ path, payload: null });
          if (path === "/discovery/report") return { summary: {} };
          return { summary: {}, sources: [] };
        },
        postBridge: async (path, payload) => {
          posts.push({ path, payload });
          return { approved: 1 };
        },
        fetchJobsFetchReportJson: async () => ({ sources: [] }),
        mergeSourceStatusFromReport: rows => rows,
        applySourceFilter: rows => rows,
        getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
        deriveSourceStatus: row => String(row?.status || "unknown"),
        renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
        readShowZeroJobs: () => false,
        normalizeSourceFilter: value => value,
        adminDispatch: { dispatch() {} },
        adminActions: { DISCOVERY_REFRESHED: "discovery/refreshed" },
        appendDiscoveryLog(message) {
          logs.push(String(message));
        },
        formatManualCheckFailureMessage: () => "failed",
        loadOpsHealthData: async () => {
          posts.push({ path: "ops", payload: null });
        },
        setBusyFlag(key, value) {
          state.adminBusyState[key] = value;
        },
        showToast() {},
        getErrorMessage: err => String(err?.message || err || "unknown")
      });

      await controller.approveSelectedSources();

      assert.deepEqual(posts[0], {
        path: "/registry/approve",
        payload: { ids: ["pending_1"] }
      });
      assert.equal(posts.some(item => item.path === "/discovery/report"), true);
      assert.equal(posts.some(item => item.path === "ops"), true);
      assert.ok(logs.some(line => /source discovery data loaded/i.test(line)));
    }
  );
});

test("admin discovery controller stores optimistic run metadata while discovery watch is active", async () => {
  const timerStub = stubScheduledTimers();

  try {
    const fixture = createDiscoveryControllerFixture({
      refs: {
        adminDiscoveryLogEl: createElement()
      }
    });
    fixture.options.getBridge = async path => {
      fixture.calls.push(path);
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
    };
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
      return {
        started: true,
        runId: "discovery_123",
        startedAt: "2026-03-08T10:01:00.000Z"
      };
    };
    fixture.options.loadOpsHealthData = async () => {
      fixture.calls.push("loadOpsHealthData");
    };
    fixture.options.scheduleOpsHealthPolling = delay => {
      fixture.calls.push(`scheduleOpsHealthPolling:${delay}`);
    };
    fixture.options.loadDiscoveryData = async () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    await controller.runDiscoveryTask();

    assert.deepEqual(fixture.state.discoveryOptimisticRun, {
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.discoveryWatch, true);
    assert.equal(fixture.state.adminBusyState.liveDiscoveryRunning, false);
    assert.ok(fixture.calls.includes("/tasks/run-discovery"));
    assert.ok(fixture.calls.includes("loadOpsHealthData"));
    assert.ok(fixture.calls.includes("scheduleOpsHealthPolling:250"));
    assert.ok(fixture.logs.some(line => /source discovery task started/i.test(line)));
    assert.ok(fixture.toasts.some(item => item.message === "Source discovery started." && item.level === "success"));
    assert.deepEqual(fixture.busyTransitions, [
      "discoveryRun:true",
      "discoveryWatch:false",
      "discoveryWatch:true",
      "discoveryRun:false"
    ]);
  } finally {
    timerStub.restore();
  }
});

test("admin discovery controller attaches to an already-running bridge task on conflict", async () => {
  const logs = [];
  const toasts = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        discoveryRun: false,
        discoveryWatch: false,
        discoveryLoad: false,
        discoveryWrite: false,
        manualAdd: false,
        manualCheck: false,
        liveDiscoveryRunning: false
      }
    };
    const refs = {
      adminDiscoveryLogEl: createElement()
    };
    const busyTransitions = [];
    const calls = [];
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        return {};
      },
      postBridge: async path => {
        calls.push(path);
        return {
          status: 409,
          data: {
            started: false,
            alreadyRunning: true,
            runId: "discovery_live_1",
            startedAt: "2026-03-08T10:01:00.000Z",
            task: "source_discovery",
            taskType: "discovery",
            pid: 321,
            status: "running"
          }
        };
      },
      setBusyFlag(key, value) {
        busyTransitions.push(`${key}:${String(value)}`);
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast(message, level) {
        toasts.push({ message, level });
      },
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      },
      loadOpsHealthData: async () => {
        calls.push("loadOpsHealthData");
      },
      scheduleOpsHealthPolling(delay) {
        calls.push(`scheduleOpsHealthPolling:${delay}`);
      },
      loadDiscoveryData: async () => {}
    });

    await controller.runDiscoveryTask();

    assert.deepEqual(state.discoveryOptimisticRun, {
      runId: "discovery_live_1",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(state.adminBusyState.discoveryWatch, true);
    assert.ok(calls.includes("/tasks/run-discovery"));
    assert.ok(calls.includes("loadOpsHealthData"));
    assert.ok(calls.includes("scheduleOpsHealthPolling:250"));
    assert.ok(logs.some(line => /discovery already running; attached/i.test(line)));
    assert.ok(toasts.some(item => item.message === "Source discovery already running. Attached to active run." && item.level === "info"));
    assert.deepEqual(busyTransitions, [
      "discoveryRun:true",
      "discoveryWatch:false",
      "discoveryWatch:true",
      "discoveryRun:false"
    ]);
    assert.ok(scheduled.length >= 1);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller emits summary-first live progress and updates progress bar", async () => {
  const timerStub = stubScheduledTimers();
  const dateStub = stubDateNow(Date.parse("2026-03-08T10:01:00.500Z"));

  try {
    const barEl = createElement({ style: {} });
    const fixture = createDiscoveryControllerFixture({
      refs: {
        adminDiscoveryProgressBarEl: barEl
      }
    });
    fixture.options.getBridge = async path => {
        if (path === "/ops/task-live/discovery") {
          return {
            runId: "discovery_123",
            startedAt: "2026-03-08T10:01:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "scanning_sources",
              phaseLabel: "Scanning known careers pages",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                foundEndpoints: 12,
                probedCandidates: 5,
                probeTotal: 10,
                queuedCandidates: 3,
                deferredCandidates: 4,
                failedProbes: 1
              }
            },
            summary: {
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1
            },
            workItems: [
              {
                id: "greenhouse",
                name: "greenhouse",
                status: "running",
                progress: {
                  phaseKey: "scanning_sources",
                  phaseLabel: "Scanning known careers pages",
                  counts: { queuedCount: 2 },
                  updatedAt: "2026-03-08T10:01:01.000Z"
                }
              }
            ]
          };
        }
        if (path === "/discovery/report") {
          return {
            startedAt: "2026-03-08T10:01:00.000Z",
            finishedAt: "",
            summary: {
              phaseLabel: "Scanning known careers pages",
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1,
              skippedDuplicateCount: 2,
              skippedInvalidCount: 0
            },
            candidates: [
              { adapter: "greenhouse" },
              { adapter: "greenhouse" },
              { adapter: "teamtailor" }
            ],
            failures: [
              { stage: "timeout", error: "request timed out" }
            ]
          };
        }
        if (String(path).startsWith("/discovery/log?offset=")) {
          return {
            text: "[2026-03-08T10:01:01.000Z] Scanning known careers pages from the seed catalog.\n[2026-03-08T10:01:02.000Z] found 12 candidates, probed 5, queued 3\n[2026-03-08T10:01:03.000Z] timeout while probing\n",
            nextOffset: 99
          };
        }
        throw new Error(`unexpected path ${path}`);
    };
    fixture.options.postBridge = async () => ({
      started: true,
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    fixture.options.loadOpsHealthData = async () => {};
    fixture.options.scheduleOpsHealthPolling = () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    await controller.runDiscoveryTask();
    await timerStub.scheduled[0]();

    assert.ok(fixture.logs.some(line => /discovery started\. watching live progress/i.test(line)));
    assert.ok(fixture.logs.some(line => /scanning known careers pages/i.test(line)));
    assert.ok(fixture.logs.some(line => /found 12 candidates, probed 5, queued 3/i.test(line)));
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("determinate"), true);
    assert.equal(fixture.refs.adminDiscoveryProgressEl.classList.contains("indeterminate"), false);
    assert.equal(fixture.refs.adminDiscoveryProgressBarEl.style.width, "50%");
    assert.match(fixture.refs.adminDiscoveryProgressLabelEl.textContent, /discovery:/i);
  } finally {
    timerStub.restore();
    dateStub.restore();
  }
});

test("admin discovery controller applies live progress when runId matches despite startedAt skew", async () => {
  const logs = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:10:00.500Z");

  try {
    const barEl = createElement({ style: {} });
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        discoveryRun: false,
        discoveryWatch: false,
        discoveryLoad: false,
        discoveryWrite: false,
        manualAdd: false,
        manualCheck: false,
        liveDiscoveryRunning: false
      }
    };
    const refs = {
      adminDiscoveryLogEl: createElement(),
      adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminDiscoveryProgressBarEl: barEl,
      adminDiscoveryProgressLabelEl: createElement(),
      adminRunDiscoveryUncappedBtnEl: createElement()
    };
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/ops/task-live/discovery") {
          return {
            taskType: "discovery",
            active: true,
            runId: "discovery_skew_1",
            startedAt: "2026-03-08T10:05:00.000Z",
            taskProgress: {
              active: true,
              phaseKey: "scanning_sources",
              phaseLabel: "Scanning known careers pages",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                foundEndpoints: 12,
                probedCandidates: 5,
                queuedCandidates: 3,
                deferredCandidates: 4,
                failedProbes: 1
              }
            },
            summary: {
              phaseLabel: "Scanning known careers pages",
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1,
              skippedDuplicateCount: 2,
              skippedInvalidCount: 0
            },
            workItems: [
              {
                id: "sheet_directory",
                name: "Sheet directory",
                status: "running",
                progress: {
                  phaseKey: "scanning_sources",
                  phaseLabel: "Scanning known careers pages",
                  counts: {
                    foundEndpoints: 12,
                    probedCandidates: 5,
                    queuedCandidates: 3
                  },
                  updatedAt: "2026-03-08T10:05:01.000Z"
                }
              }
            ]
          };
        }
        if (path === "/discovery/report") {
          return {
            runId: "discovery_skew_1",
            startedAt: "2026-03-08T10:05:00.000Z",
            finishedAt: "",
            summary: {
              phaseLabel: "Scanning known careers pages",
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1,
              skippedDuplicateCount: 2,
              skippedInvalidCount: 0
            },
            candidates: [{ adapter: "greenhouse" }],
            failures: [{ stage: "timeout", error: "request timed out" }]
          };
        }
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({
        started: true,
        runId: "discovery_skew_1",
        startedAt: "2026-03-08T10:10:00.000Z"
      }),
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast() {},
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:10:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      },
      loadOpsHealthData: async () => {},
      scheduleOpsHealthPolling() {},
    });

    await controller.runDiscoveryTask();
    await scheduled[0]();
    await new Promise(resolve => setImmediate(resolve));

    assert.ok(logs.some(line => /discovery started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /scanning known careers pages/i.test(line)));
    assert.ok(logs.some(line => /endpoints 12, probed 5, queued 3/i.test(line)));
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin discovery controller syncs source tables once after completion", async () => {
  const syncCalls = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: {
        runId: "discovery_done_1",
        startedAt: "2026-03-08T10:00:00.000Z"
      },
      adminBusyState: {
        discoveryRun: false,
        discoveryWatch: false,
        discoveryLoad: false,
        discoveryWrite: false,
        manualAdd: false,
        manualCheck: false,
        liveDiscoveryRunning: false
      }
    };
    const refs = {
      adminDiscoveryLogEl: createElement(),
      adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminDiscoveryProgressBarEl: createElement({ style: {} }),
      adminDiscoveryProgressLabelEl: createElement()
    };
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/ops/task-live/discovery") {
          return null;
        }
        if (path === "/discovery/report") {
          return {
            runId: "discovery_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:03:00.000Z",
            summary: {
              foundEndpointCount: 4,
              probedCandidateCount: 6,
              queuedCandidateCount: 1,
              discoverableButDeferredCount: 0,
              failedProbeCount: 0
            }
          };
        }
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({
        started: true,
        runId: "discovery_done_1",
        startedAt: "2026-03-08T10:00:00.000Z"
      }),
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast() {},
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {},
      loadOpsHealthData: async () => {},
      scheduleOpsHealthPolling() {},
      syncSourceTablesAfterTaskCompletion: async payload => {
        syncCalls.push(payload);
      },
    });

    controller.startDiscoveryCompletionWatch();
    await scheduled[scheduled.length - 1]();

    assert.equal(syncCalls.length, 1);
    assert.equal(syncCalls[0].taskType, "discovery");
    assert.equal(syncCalls[0].completionSignature, "discovery_done_1|2026-03-08T10:03:00.000Z");
    assert.equal(state.adminBusyState.discoveryWatch, false);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller forwards uncapped preset payload", async () => {
  const calls = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if (String(path).startsWith("/discovery/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      return {
        started: true,
        runId: "discovery_uncapped",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "uncapped"
      };
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast() {},
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask({ preset: "uncapped" });

  assert.ok(calls.includes('/tasks/run-discovery:{"preset":"uncapped"}'));
});

test("admin discovery controller loads and saves auto-approve config", async () => {
  const calls = [];
  const toasts = [];
  const state = {
    latestDiscoveryConfigCache: null,
    discoveryConfigDirty: false,
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryAutoApproveToggleEl: createElement({ checked: true })
  };

  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: false
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: Boolean(payload?.autoApproveHealthyPendingOnComplete)
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  const loaded = await controller.loadDiscoveryConfig({ forceForm: true });
  assert.equal(loaded.savedConfig.autoApproveHealthyPendingOnComplete, false);
  assert.equal(refs.adminDiscoveryAutoApproveToggleEl.checked, false);

  refs.adminDiscoveryAutoApproveToggleEl.checked = true;
  state.discoveryConfigDirty = true;
  await controller.saveDiscoveryConfig();

  assert.ok(calls.includes("/discovery/config"));
  assert.ok(calls.includes('/discovery/config:{"autoApproveHealthyPendingOnComplete":true}'));
  assert.equal(state.discoveryConfigDirty, false);
  assert.equal(state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, true);
  assert.ok(
    toasts.some(
      item =>
        item.message === "Discovery auto-approve preference updated." &&
        item.level === "success"
    )
  );
});

test("admin discovery controller recovers when launch response is lost but report shows a fresh run", async () => {
  const logs = [];
  const toasts = [];
  const calls = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollTimer: null,
      discoveryLiveProgressState: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        discoveryRun: false,
        discoveryWatch: false,
        discoveryLoad: false,
        discoveryWrite: false,
        manualAdd: false,
        manualCheck: false,
        liveDiscoveryRunning: false
      }
    };
    const refs = {
      adminDiscoveryLogEl: createElement(),
      adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminDiscoveryProgressBarEl: createElement({ style: {} }),
      adminDiscoveryProgressLabelEl: createElement()
    };
    const controller = createAdminDiscoveryController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if (path === "/discovery/report") {
          return {
            startedAt: new Date().toISOString(),
            finishedAt: "",
            summary: { queuedCandidateCount: 0, foundEndpointCount: 0, probedCandidateCount: 0, failedProbeCount: 0 },
            candidates: [],
            failures: []
          };
        }
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => {
        throw new Error("Network error: bridge unreachable");
      },
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast(message, level) {
        toasts.push({ message, level });
      },
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      },
      loadOpsHealthData: async () => {
        calls.push("loadOpsHealthData");
      },
      scheduleOpsHealthPolling(delay) {
        calls.push(`scheduleOpsHealthPolling:${delay}`);
      },
      loadDiscoveryData: async () => {}
    });

    await controller.runDiscoveryTask();

    assert.equal(state.adminBusyState.discoveryWatch, true);
    assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
    assert.ok(calls.includes("/discovery/report"));
    assert.ok(logs.some(line => /response was lost, but the run is active/i.test(line)));
    assert.ok(toasts.some(item => /reattached after a dropped bridge response/i.test(item.message) && item.level === "warning"));
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller still shows error when launch recovery finds no fresh run", async () => {
  const toasts = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report") {
        return {
          startedAt: "2020-01-01T00:00:00.000Z",
          finishedAt: "",
          summary: {},
          candidates: [],
          failures: []
        };
      }
      if (String(path).startsWith("/discovery/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => {
      throw new Error("Network error: bridge unreachable");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask();

  assert.equal(state.adminBusyState.discoveryWatch, false);
  assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
  assert.ok(toasts.some(item => item.message === "Could not trigger source discovery task." && item.level === "error"));
});

test("admin discovery controller does not reattach when the fresh report is a launch failure", async () => {
  const logs = [];
  const toasts = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/discovery/report") {
        return {
          startedAt: new Date().toISOString(),
          finishedAt: new Date().toISOString(),
          summary: { failedProbeCount: 1 },
          candidates: [],
          failures: [
            { adapter: "bridge", stage: "launch", error: "WinError 233" }
          ]
        };
      }
      if (String(path).startsWith("/discovery/log?offset=")) {
        return { text: "[2026-03-08T10:01:01.000Z] Launch failed: [WinError 233] No process is on the other end of the pipe\n", nextOffset: 99 };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => {
      throw new Error("Network error: bridge unreachable");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow(_container, event) {
      logs.push(String(event.message || ""));
    },
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  await controller.runDiscoveryTask();

  assert.equal(state.adminBusyState.discoveryWatch, false);
  assert.equal(state.adminBusyState.liveDiscoveryRunning, false);
  assert.ok(logs.some(line => /launch failed/i.test(line)));
  assert.ok(!logs.some(line => /reattaching to live progress/i.test(line)));
  assert.ok(toasts.some(item => item.message === "Could not trigger source discovery task." && item.level === "error"));
});

test("admin discovery controller updates progress label from server log phases before report progress arrives", () => {
  const logs = [];
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: {
      phaseLabel: "",
      summarySignature: "",
      candidateCount: 0,
      failureCount: 0,
      serverPhaseLabel: "",
      serverLogSignatures: new Set(),
      lastHeartbeatAtMs: 0
    },
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: true,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: true
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement()
  };
  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async () => ({}),
    postBridge: async () => ({}),
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast() {},
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow(_container, event) {
      logs.push(String(event.message || ""));
    },
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  controller.appendDiscoveryServerLogText("[2026-03-08T10:01:01.000Z] Scanning known careers pages from the seed catalog.\n");

  assert.match(refs.adminDiscoveryProgressLabelEl.textContent, /Scanning known careers pages/i);
  assert.ok(logs.some(line => /Scanning known careers pages/i.test(line)));
});

test("admin ops controller preserves optimistic rows while history lags", async () => {
  const cases = [
    {
      label: "discovery",
      optimisticKey: "discoveryOptimisticRun",
      busyKey: "liveDiscoveryRunning",
      runId: "discovery_123"
    },
    {
      label: "fetch",
      optimisticKey: "fetchOptimisticRun",
      busyKey: "liveFetchRunning",
      runId: "fetch_123"
    }
  ];

  for (const { label, optimisticKey, busyKey, runId } of cases) {
    const state = {
      latestOpsHealthCache: null,
      discoveryOptimisticRun: null,
      fetchOptimisticRun: null,
      [optimisticKey]: {
        runId,
        startedAt: "2026-03-08T10:01:00.000Z"
      },
      adminBusyState: {
        opsLoad: false,
        liveFetchRunning: false,
        liveDiscoveryRunning: false,
        liveSyncRunning: false,
        livePipelineRunning: false
      }
    };
    const refs = {
      adminSyncStatusEl: createElement(),
      adminSyncConfigHintEl: createElement(),
      adminOpsAlertsEl: createElement(),
      adminOpsKpisEl: createElement(),
      adminOpsScheduleEl: createElement(),
      adminOpsFetcherMetricsEl: createElement(),
      adminOpsHistoryEl: createElement(),
      adminOpsTrendsEl: createElement()
    };
    const runModels = [];
    let optimisticApplied = 0;
    const controller = createAdminOpsController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
        if (path === "/ops/history?limit=80") return { runs: [] };
        if (path === "/ops/task-state") return { tasks: [] };
        if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => {
        optimisticApplied += 1;
        return {
          currentRows: [],
          visibleCompletedRows: [],
          olderCompletedRows: [],
          hasLiveRuns: false,
          liveTypes: []
        };
      },
      getOpsPollIntervalMs: () => 5000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsFetcherMetrics() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory(_el, runModel) {
        runModels.push(runModel);
      },
      loadSyncStatus: async () => {},
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
      idlePollIntervalMs: 1000
    });

    await controller.loadOpsHealthData();
    controller.stopOpsHealthPolling();

    assert.equal(optimisticApplied, 1, label);
    assert.equal(state.adminBusyState[busyKey], false, label);
    assert.equal(runModels.length, 1, label);
    assert.equal(runModels[0].currentRows.length, 0, label);
  }
});

test("admin ops controller renders bridge task-state without reattaching from history-only rows", async () => {
  const cases = [
    {
      label: "fetch",
      taskType: "fetch",
      busyKey: "liveFetchRunning",
      watcherKey: "fetcherWatch",
      liveTypes: ["fetch"],
      runId: "fetch_123"
    },
    {
      label: "discovery",
      taskType: "discovery",
      busyKey: "liveDiscoveryRunning",
      watcherKey: "discoveryWatch",
      liveTypes: ["discovery"],
      runId: "discovery_123"
    }
  ];

  for (const { label, taskType, busyKey, watcherKey, liveTypes, runId } of cases) {
    const state = {
      latestOpsHealthCache: null,
      fetchOptimisticRun: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        opsLoad: false,
        fetcherWatch: false,
        discoveryWatch: false,
        liveFetchRunning: false,
        liveDiscoveryRunning: false,
        liveSyncRunning: false,
        livePipelineRunning: false
      }
    };
    const refs = {
      adminSyncStatusEl: createElement(),
      adminSyncConfigHintEl: createElement(),
      adminOpsAlertsEl: createElement(),
      adminOpsKpisEl: createElement(),
      adminOpsScheduleEl: createElement(),
      adminOpsFetcherMetricsEl: createElement(),
      adminOpsHistoryEl: createElement(),
      adminOpsTrendsEl: createElement()
    };
    const runModels = [];
    const calls = [];
    let controller;
    try {
      controller = createAdminOpsController({
        state,
        refs,
        getBridge: async path => {
          if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
          if (path === "/ops/history?limit=80") return { runs: [] };
          if (path === "/ops/task-state") return {
            tasks: [
              {
                taskType,
                type: taskType,
                runId,
                active: true,
                startedAt: "2026-03-08T10:01:00.000Z",
                status: "running"
              }
            ]
          };
          if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
          throw new Error(`unexpected path ${path}`);
        },
        postBridge: async () => ({}),
        deriveAdminRunsModel: ({ taskState }) => ({
          currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
          visibleCompletedRows: [],
          olderCompletedRows: [],
          hasLiveRuns: true,
          liveTypes
        }),
        getOpsPollIntervalMs: () => 5000,
        renderAdminOpsAlerts() {},
        renderAdminOpsKpis() {},
        renderAdminOpsSchedule() {},
        renderAdminOpsFetcherMetrics() {},
        renderAdminOpsTrends() {},
        renderAdminOpsHistory(_el, runModel) {
          runModels.push(runModel);
        },
        loadSyncStatus: async () => {},
        setBusyFlag(key, value) {
          state.adminBusyState[key] = value;
        },
        showToast() {},
        getErrorMessage: err => String(err?.message || err || "unknown"),
        adminDispatch: { dispatch() {} },
        adminActions: { OPS_REFRESHED: "ops/refreshed" },
        escapeHtml: value => String(value || ""),
        onBridgeStatusChange() {},
        loadDiscoveryData: async () => {
          calls.push("loadDiscoveryData");
        },
        bridgeStatusPollIntervalMs: 1000,
        idlePollIntervalMs: 1000
      });

      await controller.loadOpsHealthData();

      assert.equal(state.adminBusyState[busyKey], true, label);
      assert.equal(state.adminBusyState[watcherKey], false, label);
      assert.equal(runModels.length, 1, label);
      assert.equal(runModels[0].currentRows.length, 1, label);
      assert.deepEqual(calls, label === "discovery" ? ["loadDiscoveryData"] : [], label);
    } finally {
      controller?.stopOpsHealthPolling?.();
    }
  }
});

test("admin ops controller quietly auto-attaches active fetch and discovery task-state rows", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
    taskStateMissingStreakByType: {},
    adminBusyState: {
      opsLoad: false,
      fetcherWatch: false,
      discoveryWatch: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const calls = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") {
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_attach_1",
              active: true,
              startedAt: "2026-03-08T10:01:00.000Z",
              status: "running"
            },
            {
              taskType: "discovery",
              type: "discovery",
              runId: "discovery_live_attach_1",
              active: true,
              startedAt: "2026-03-08T10:02:00.000Z",
              status: "running"
            }
          ]
        };
      }
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: true,
      liveTypes: ["fetch", "discovery"]
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    loadDiscoveryData: async () => {},
    attachToActiveFetchRun(runMeta, options) {
      calls.push(`fetch:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
    },
    loadLatestFetcherReport: async options => {
      calls.push(`fetchReport:${String(Boolean(options?.silent))}:${String(Boolean(options?.hydrateActiveProgress))}`);
      return {};
    },
    attachToActiveDiscoveryRun(runMeta, options) {
      calls.push(`discovery:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
    },
    loadLatestDiscoveryReport: async options => {
      calls.push(`discoveryReport:${String(Boolean(options?.silent))}`);
      return {};
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.ok(calls.includes("fetch:fetch_live_attach_1:false"));
  assert.ok(calls.includes("fetchReport:true:true"));
  assert.ok(calls.includes("discovery:discovery_live_attach_1:false"));
  assert.ok(calls.includes("discoveryReport:true"));
});

test("admin ops controller retains current live rows across one empty task-state sample and clears on the second", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
    taskStateMissingStreakByType: {},
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const taskStatePayloads = [
    {
      tasks: [
        {
          taskType: "fetch",
          type: "fetch",
          runId: "fetch_live_stable_1",
          active: true,
          startedAt: "2026-03-08T10:01:00.000Z",
          status: "running"
        }
      ]
    },
    { tasks: [] },
    { tasks: [] }
  ];
  const renderedCurrentCounts = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") return taskStatePayloads.shift() || { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentCounts.push(runModel.currentRows.length);
    },
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    loadDiscoveryData: async () => {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.loadOpsHealthData();
  await controller.loadOpsHealthData();
  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.deepEqual(renderedCurrentCounts, [1, 1, 0]);
  assert.equal(state.adminBusyState.liveFetchRunning, false);
});

test("admin ops controller keeps the last live rows rendered on transient ops polling failure", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
    taskStateMissingStreakByType: {},
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  let callCount = 0;
  const renderedCurrentCounts = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      callCount += 1;
      if (callCount > 4 && path === "/ops/health") {
        throw new Error("transient bridge error");
      }
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") {
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_error_hold_1",
              active: true,
              startedAt: "2026-03-08T10:01:00.000Z",
              status: "running"
            }
          ]
        };
      }
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentCounts.push(runModel.currentRows.length);
    },
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    loadDiscoveryData: async () => {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.loadOpsHealthData();
  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.deepEqual(renderedCurrentCounts, [1, 1]);
  assert.equal(state.adminBusyState.liveFetchRunning, true);
});

test("admin fetcher controller stores optimistic run metadata while fetch watch is active", async () => {
  const timerStub = stubScheduledTimers();

  let controller;
  try {
    const fixture = createFetcherControllerFixture({
      refs: {
        adminFetcherLogEl: createElement()
      }
    });
    fixture.options.getBridge = async path => {
      fixture.calls.push(path);
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        return {};
    };
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
      return {
        started: true,
        runId: "fetch_123",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "default",
        args: ["--quiet"]
      };
    };
    fixture.options.loadOpsHealthData = async () => {
      fixture.calls.push("loadOpsHealthData");
    };
    controller = createAdminFetcherController(fixture.options);

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.ok(fixture.calls.includes("/tasks/run-fetcher"));
    assert.ok(fixture.logs.some(line => /triggered fetcher via local admin bridge/i.test(line)));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller attaches to an already-running bridge task on conflict", async () => {
  const logs = [];
  const toasts = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  let controller;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const state = {
      latestFetcherReportCache: null,
      fetcherLaunchAtMs: 0,
      fetcherLogRemoteOffset: 0,
      fetcherCompletionPollTimer: null,
      fetcherLogPollTimer: null,
      fetcherLiveProgressState: null,
      fetchOptimisticRun: null,
      adminBusyState: {
        fetcherRun: false,
        fetcherWatch: false,
        fetcherReportLoad: false,
        liveFetchRunning: false
      }
    };
    const refs = {
      adminFetcherLogEl: createElement(),
      adminRunFetcherBtnEl: createElement(),
      adminRunFetcherIncrementalBtnEl: createElement(),
      adminRunFetcherUncappedBtnEl: createElement(),
      adminRunFetcherForceBtnEl: createElement(),
      adminRetryFailedBtnEl: createElement()
    };
    const calls = [];
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        return {};
      },
      postBridge: async path => {
        calls.push(path);
        return {
          status: 409,
          data: {
            started: false,
            alreadyRunning: true,
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:01:00.000Z",
            task: "jobs_fetcher",
            taskType: "fetch",
            pid: 654,
            status: "running"
          }
        };
      },
      fetchJobsFetchReportJson: async () => ({}),
      writeJobsAutoRefreshSignal() {},
      showToast(message, level) {
        toasts.push({ message, level });
      },
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {
        calls.push("loadOpsHealthData");
      },
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(state.fetchOptimisticRun, {
      runId: "fetch_live_1",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(state.adminBusyState.fetcherWatch, true);
    assert.ok(calls.includes("/tasks/run-fetcher"));
    assert.ok(calls.includes("loadOpsHealthData"));
    assert.ok(logs.some(line => /fetcher already running; attached/i.test(line)));
    assert.ok(toasts.some(item => item.message === "Fetcher already running. Attached to active run." && item.level === "info"));
    assert.ok(scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller starts live progress watching for an explicit bridge-launched fetch", async () => {
  const logs = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: 0,
    fetcherLogRemoteOffset: 0,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement()
  };
  const controller = createAdminFetcherController({
    state,
    refs,
    getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/health") return {};
      return {
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "indeterminate",
          ratio: 0,
          counts: {}
        },
        runtime: {},
        summary: {},
        sources: []
      };
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "indeterminate",
        ratio: 0,
        counts: {}
      },
      runtime: {},
      summary: {},
      sources: []
    }),
    writeJobsAutoRefreshSignal() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getSourceStatusSetter: () => () => {},
    loadOpsHealthData: async () => {},
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
    },
    appendLogRow(_container, event) {
      logs.push(String(event.message || ""));
    }
  });

  try {
    controller.attachToActiveFetchRun({
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });

    assert.equal(state.adminBusyState.fetcherWatch, true);
    assert.equal(state.adminBusyState.liveFetchRunning, false);
    assert.deepEqual(state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    assert.ok(logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(!logs.some(line => /timeout window/i.test(line)));
    assert.equal(refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller can restore a live watch from local state when the latest report is stale", async () => {
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherLogRemoteOffset: 0,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: {
      summarySignature: "",
      sourceSignatures: new Map(),
      reportedSlowSources: new Set(),
      serverLogSignatures: new Set(),
      slowSourceSummarySignature: "",
      slowStageSummarySignature: "",
      lastHeartbeatAtMs: 0,
      lastActivityAtMs: Date.now()
    },
    fetchOptimisticRun: {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: true,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  const controller = createAdminFetcherController({
    state,
    refs,
    getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      return {
        runId: "fetch_stale_2",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: false,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "indeterminate",
          ratio: 0,
          counts: {}
        },
        runtime: {},
        summary: {},
        sources: []
      };
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: false,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "indeterminate",
        ratio: 0,
        counts: {}
      },
      runtime: {},
      summary: {},
      sources: []
    }),
    writeJobsAutoRefreshSignal() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getSourceStatusSetter: () => () => {},
    loadOpsHealthData: async () => {},
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
    },
    appendLogRow(_container, event) {
      // Intentionally unused in this regression.
      void event;
    }
  });

  try {
    const meta = controller.getRestorableFetcherRunMeta({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: { active: false }
    });

    assert.deepEqual(meta, {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    });

    controller.restartFetcherCompletionWatch(meta);

    assert.equal(state.adminBusyState.fetcherWatch, true);
    assert.equal(state.adminBusyState.liveFetchRunning, false);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller renders progress from the shared task progress contract", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLiveProgressState: null,
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:00:00.500Z");

  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return {
            text: "[2026-03-08T10:00:01.000Z] [jobs_fetcher] START source=Studio A\n[2026-03-08T10:00:02.000Z] [jobs_fetcher] WARN source=Studio B HTTP 403\n",
            nextOffset: 120
          };
        }
        if (path === "/ops/task-live/fetch") {
          return {
            runId: "fetch_live_1",
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
                outputCount: 18,
                failedSources: 1,
                excludedSources: 1
              }
            },
            summary: {
              outputCount: 18,
              failedSources: 1,
              sourceCount: 10
            },
            workItems: [
              {
                id: "studio_a",
                name: "Studio A",
                status: "running",
                progress: {
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  counts: { resolvedSources: 6, sourceCount: 12 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              },
              {
                id: "scrapy_static_sources",
                name: "scrapy_static_sources",
                status: "running",
                progress: {
                  phaseKey: "loading_source",
                  phaseLabel: "Processing browser fallback queue",
                  counts: { completedSources: 19, totalSources: 26 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              }
            ]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
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
            outputCount: 18,
            failedSources: 1,
            excludedSources: 1
          }
        },
        runtime: {
          selectedSourceCount: 10,
          timingSummary: {
            stageTop: [{ stage: "detailFetch", durationMs: 47000 }]
          }
        },
        summary: {
          successfulSources: 4,
          failedSources: 1,
          excludedSources: 1,
          outputCount: 18,
          sourceCount: 10
        },
        sources: [
          { name: "Studio A", status: "ok", keptCount: 4, durationMs: 1200 },
          { name: "Studio B", status: "error", keptCount: 0, durationMs: 2200, error: "HTTP 403" },
          { name: "Studio C", status: "running", keptCount: 0, durationMs: 26000 }
        ]
      }),
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    await scheduled[0]();
    await scheduled[1]();

    assert.ok(logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /start source=studio a/i.test(line)));
    assert.ok(logs.some(line => /warn source=studio b http 403/i.test(line)));
    assert.ok(logs.some(line => /6\/12 sources resolved/i.test(line)));
    assert.ok(logs.some(line => /Browser fallback 19\/26/i.test(line)));
    assert.equal(refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("determinate"), true);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("indeterminate"), false);
    assert.equal(refs.adminFetcherProgressBarEl.style.width, "50%");
    assert.equal(refs.adminFetcherProgressBarEl.style.left, "0");
    assert.equal(refs.adminFetcherProgressEl.attributes["aria-valuenow"], "50");
    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /executing sources/i);
    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.ok(!logs.some(line => /timeout window/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin fetcher controller keeps current live detail when an active report refresh lags behind", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: {
      runId: "fetch_live_current_1",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return {
            runId: "fetch_live_current_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 10 / 551,
              counts: {
                resolvedSources: 10,
                sourceCount: 551,
                runningTasks: 541,
                queuedTasks: 0,
                outputCount: 34081,
                failedSources: 0,
                excludedSources: 0
              }
            },
            workItems: [
              {
                id: "studio_a",
                name: "Studio A",
                status: "running",
                progress: {
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  counts: { emittedJobs: 17 },
                  updatedAt: "2026-03-08T10:03:00.000Z"
                }
              }
            ]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        runId: "fetch_live_current_1",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 9 / 551,
          counts: {
            resolvedSources: 9,
            sourceCount: 551,
            runningTasks: 542,
            queuedTasks: 0,
            outputCount: 29957,
            failedSources: 0,
            excludedSources: 0
          }
        },
        summary: {
          successfulSources: 9,
          failedSources: 0,
          excludedSources: 0,
          outputCount: 29957,
          sourceCount: 551
        },
        sources: []
      }),
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    await scheduled[scheduled.length - 1]();
    await scheduled[1]();
    await controller.loadLatestFetcherReport({ silent: true });

    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /10\/551 sources resolved/i);
    assert.equal((state.latestFetcherReportCache || {}).summary?.outputCount, 29957);
    assert.ok(!logs.some(line => /No source entries found in report/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller hydrates progress from the report without replaying summary noise when live payload is empty", async () => {
  const timerStub = stubScheduledTimers();

  let controller;
  try {
    const fixture = createFetcherControllerFixture({
      state: {
        fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z")
      }
    });
    fixture.options.getBridge = async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return {};
        }
        return {};
    };
    fixture.options.postBridge = async () => ({});
    fixture.options.fetchJobsFetchReportJson = async () => ({
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: { resolvedSources: 6, sourceCount: 12 }
      },
      summary: { outputCount: 18, failedSources: 1, sourceCount: 12 },
      sources: [{ name: "Studio A", status: "running" }]
    });
    fixture.options.loadOpsHealthData = async () => {};
    controller = createAdminFetcherController(fixture.options);

    controller.startFetcherCompletionWatch();
    await timerStub.scheduled[0]();
    await timerStub.scheduled[1]();
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.ok(!fixture.logs.some(line => /6\/12 sources resolved/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin discovery controller hydrates progress from the report when live payload is empty", async () => {
  const timerStub = stubScheduledTimers();

  try {
    const fixture = createDiscoveryControllerFixture({
      state: {
        latestDiscoveryReportCache: null,
        adminBusyState: {
          discoveryRun: false,
          discoveryWatch: false,
          liveDiscoveryRunning: false
        }
      }
    });
    fixture.options.getBridge = async path => {
        if (path === "/ops/task-live/discovery") return {};
        if (path === "/discovery/report") {
          return {
            startedAt: "2026-03-08T10:01:00.000Z",
            finishedAt: "",
            summary: {
              phaseLabel: "Scanning known careers pages",
              foundEndpointCount: 12,
              probedCandidateCount: 5,
              queuedCandidateCount: 3,
              discoverableButDeferredCount: 4,
              failedProbeCount: 1
            },
            candidates: [{ adapter: "greenhouse" }],
            failures: []
          };
        }
        if (String(path).startsWith("/discovery/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        throw new Error(`unexpected path ${path}`);
    };
    fixture.options.postBridge = async () => ({
      started: true,
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    fixture.options.loadOpsHealthData = async () => {};
    fixture.options.scheduleOpsHealthPolling = () => {};
    const controller = createAdminDiscoveryController(fixture.options);

    await controller.runDiscoveryTask();
    await timerStub.scheduled[0]();
    assert.match(String(fixture.refs.adminDiscoveryProgressLabelEl.textContent || ""), /scanning known careers pages/i);
    assert.ok(!fixture.logs.some(line => /discovery: endpoints 12/i.test(line)));
  } finally {
    timerStub.restore();
  }
});

test("shared admin task progress renderer resets indeterminate state before determinate fill", () => {
  const rootEl = createElement({ style: {}, classList: createClassList(["hidden"]) });
  const barEl = createElement({ style: {} });
  const labelEl = createElement();

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: false,
    label: "Fetcher: Executing sources"
  });
  assert.equal(rootEl.classList.contains("indeterminate"), true);
  assert.equal(barEl.style.width, "36%");
  assert.equal(rootEl.attributes["aria-hidden"], "false");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: Executing sources");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 0.65,
    label: "Fetcher: 65% complete"
  });
  assert.equal(rootEl.classList.contains("determinate"), true);
  assert.equal(rootEl.classList.contains("indeterminate"), false);
  assert.equal(barEl.style.width, "65%");
  assert.equal(barEl.style.left, "0");
  assert.equal(barEl.style.animation, "none");
  assert.equal(rootEl.attributes["aria-valuenow"], "65");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: 65% complete");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: false
  });
  assert.equal(rootEl.classList.contains("hidden"), true);
  assert.equal(barEl.style.width, "0%");
  assert.equal(rootEl.attributes["aria-hidden"], "true");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 1,
    label: "Discovery: Discovery completed"
  });
  assert.equal(rootEl.classList.contains("complete"), true);
  assert.equal(barEl.style.width, "100%");
  assert.equal(rootEl.attributes["aria-valuenow"], "100");
});

test("admin fetcher controller only emits generic active heartbeat after sustained idle time", async () => {
  const logs = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  let nowMs = Date.parse("2026-03-08T10:00:00.000Z");
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => nowMs;

  const report = {
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
        outputCount: 18,
        failedSources: 1,
        excludedSources: 1
      }
    },
    runtime: {
      selectedSourceCount: 12
    },
    summary: {
      successfulSources: 4,
      failedSources: 1,
      excludedSources: 1,
      outputCount: 18,
      sourceCount: 12
    },
    sources: [
      { name: "Studio A", status: "ok", keptCount: 4, durationMs: 1200 }
    ]
  };

  let controller;
  try {
    const state = {
      latestFetcherReportCache: null,
      fetcherLaunchAtMs: nowMs,
      fetcherLogRemoteOffset: 0,
      fetcherCompletionPollTimer: null,
      fetcherLogPollTimer: null,
      fetcherLiveProgressState: null,
      fetchOptimisticRun: null,
      adminBusyState: {
        fetcherRun: false,
        fetcherWatch: false,
        fetcherReportLoad: false,
        liveFetchRunning: false
      }
    };
    const refs = {
      adminFetcherLogEl: createElement(),
      adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminFetcherProgressBarEl: createElement({ style: {} }),
      adminFetcherProgressLabelEl: createElement(),
      adminRunFetcherBtnEl: createElement(),
      adminRunFetcherIncrementalBtnEl: createElement(),
      adminRunFetcherUncappedBtnEl: createElement(),
      adminRunFetcherForceBtnEl: createElement(),
      adminRetryFailedBtnEl: createElement()
    };

    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return {
            taskType: "fetch",
            active: true,
            runId: "fetch_heartbeat_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                resolvedSources: 6,
                sourceCount: 12,
                outputCount: 18,
                failedSources: 1,
                excludedSources: 1
              }
            },
            summary: {
              outputCount: 18,
              failedSources: 1,
              excludedSources: 1,
              sourceCount: 12
            },
            workItems: [
              {
                id: "studio_a",
                name: "Studio A",
                status: "running",
                progress: {
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  counts: { resolvedSources: 6, sourceCount: 12 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              },
              {
                id: "scrapy_static_sources",
                name: "scrapy_static_sources",
                status: "running",
                progress: {
                  phaseKey: "loading_source",
                  phaseLabel: "Processing browser fallback queue",
                  counts: { completedSources: 19, totalSources: 26 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              }
            ]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => report,
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();

    const runLatestScheduled = async () => {
      const callback = scheduled[scheduled.length - 1];
      await callback();
    };

    await runLatestScheduled();
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 0);

    nowMs += 30_000;
    await runLatestScheduled();
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 0);

    nowMs += 31_000;
    await runLatestScheduled();
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 1);
    assert.ok(logs.some(line => /Fetcher: 6\/12 sources resolved/i.test(line)));
    assert.ok(logs.some(line => /Fetcher active: .*Browser fallback 19\/26/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin fetcher controller treats scrapy fallback progress changes as summary activity", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherCompletionPollAttempts: 0,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    jobsAutoRefreshSignalKey: "k",
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const livePayloads = [
    {
      taskType: "fetch",
      active: true,
      runId: "fetch_tail_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 550,
          sourceCount: 551,
          outputCount: 40_279,
          failedSources: 69,
          excludedSources: 0
        }
      },
      workItems: [
        {
          id: "scrapy_static_sources",
          name: "scrapy_static_sources",
          status: "running",
          progress: {
            phaseKey: "loading_source",
            phaseLabel: "Processing browser fallback queue",
            counts: { completedSources: 19, totalSources: 26 },
            updatedAt: "2026-03-08T10:00:01.000Z"
          }
        }
      ]
    },
    {
      taskType: "fetch",
      active: true,
      runId: "fetch_tail_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 550,
          sourceCount: 551,
          outputCount: 40_279,
          failedSources: 69,
          excludedSources: 0
        }
      },
      workItems: [
        {
          id: "scrapy_static_sources",
          name: "scrapy_static_sources",
          status: "running",
          progress: {
            phaseKey: "loading_source",
            phaseLabel: "Processing browser fallback queue",
            counts: { completedSources: 24, totalSources: 26 },
            updatedAt: "2026-03-08T10:00:05.000Z"
          }
        }
      ]
    }
  ];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:00:00.500Z");

  let livePayloadIndex = 0;
  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return livePayloads[Math.min(livePayloadIndex++, livePayloads.length - 1)];
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            resolvedSources: 550,
            sourceCount: 551,
            outputCount: 40_279,
            failedSources: 69,
            excludedSources: 0
          }
        },
        summary: { outputCount: 40_279, failedSources: 69, sourceCount: 551 }
      }),
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    const runLatestScheduled = async () => {
      const callback = scheduled[scheduled.length - 1];
      await callback();
    };
    await runLatestScheduled();
    await runLatestScheduled();

    assert.ok(logs.some(line => /Browser fallback 19\/26/i.test(line)));
    assert.ok(logs.some(line => /Browser fallback 24\/26/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin fetcher controller prefers task-live payload during active runs and keeps manual report loads full-fidelity", async () => {
  const fetchReportCalls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null,
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return {
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.5,
              counts: { resolvedSources: 4, sourceCount: 8, outputCount: 12 }
            },
            workItems: [{ id: "studio_a", name: "Studio A", status: "running" }]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async options => {
        fetchReportCalls.push(options || {});
        return {
          runId: "fetch_live_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: "",
          taskProgress: {
            active: true,
            phaseKey: "executing_sources",
            phaseLabel: "Executing sources",
            mode: "determinate",
            ratio: 0.5,
            counts: { resolvedSources: 4, sourceCount: 8, outputCount: 12 }
          },
          summary: { outputCount: 12, failedSources: 0, sourceCount: 8 },
          sources: [{ name: "Studio A", status: "running", details: [{ url: "https://example.com/job/1" }] }]
        };
      },
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {}
    });

    controller.startFetcherCompletionWatch();
    for (const callback of [...scheduled]) {
      await callback();
    }
    await controller.loadLatestFetcherReport({ silent: true });

    assert.deepEqual(fetchReportCalls, [{}]);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller syncs source tables once after completion", async () => {
  const syncCalls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: {
      runId: "fetch_done_1",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let manualReportLoads = 0;
  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
          return {
            runId: "fetch_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:05:00.000Z",
            taskProgress: {
              active: false,
              phaseKey: "completed",
              phaseLabel: "Completed",
              mode: "determinate",
              ratio: 1,
              counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
            }
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async options => {
        if (options?.live) {
          return {
            runId: "fetch_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:05:00.000Z",
            summary: { outputCount: 12, failedSources: 0, excludedSources: 0, sourceCount: 8 },
            taskProgress: {
              active: false,
              phaseKey: "completed",
              phaseLabel: "Completed",
              mode: "determinate",
              ratio: 1,
              counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
            }
          };
        }
        manualReportLoads += 1;
        return {
          runId: "fetch_done_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: "2026-03-08T10:05:00.000Z",
          summary: { outputCount: 12, failedSources: 0, excludedSources: 0, sourceCount: 8 },
          taskProgress: {
            active: false,
            phaseKey: "completed",
            phaseLabel: "Completed",
            mode: "determinate",
            ratio: 1,
            counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
          },
          sources: [{ name: "Studio A", status: "ok", durationMs: 1200 }]
        };
      },
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      syncSourceTablesAfterTaskCompletion: async payload => {
        syncCalls.push(payload);
      },
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {}
    });

    controller.startFetcherCompletionWatch();
    await scheduled[scheduled.length - 1]();

    assert.equal(manualReportLoads, 1);
    assert.equal(syncCalls.length, 1);
    assert.equal(syncCalls[0].taskType, "fetch");
    assert.equal(syncCalls[0].completionSignature, "fetch_done_1|2026-03-08T10:05:00.000Z");
    assert.equal((state.latestFetcherReportCache || {}).runId, "fetch_done_1");
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller forwards uncapped preset payload", async () => {
  const calls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: 0,
    fetcherLogRemoteOffset: 0,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const controller = createAdminFetcherController({
    state,
    refs,
    getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      return {
        started: true,
        runId: "fetch_uncapped",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "uncapped",
        args: ["--force-refresh-all", "--ignore-circuit-breaker"]
      };
    },
    fetchJobsFetchReportJson: async () => ({}),
    writeJobsAutoRefreshSignal() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getSourceStatusSetter: () => () => {},
    loadOpsHealthData: async () => {},
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher --social-enabled",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {}
  });

  await controller.triggerJobsFetcherTask({ preset: "uncapped" });

  assert.ok(calls.includes('/tasks/run-fetcher:{"preset":"uncapped"}'));
});

test("admin sync controller hydrates status and runs save/test/pull/push flows", async () => {
  const toasts = [];
  const paths = [];
  const busyTransitions = [];
  const state = {
    syncConfigDirty: true,
    latestSyncStatusCache: null
  };
  const refs = {
    adminSyncEnabledEl: createElement({ checked: false }),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement()
  };
  const readyPayload = {
    savedConfig: { enabled: true },
    config: {
      enabled: true,
      state: "ready",
      authMode: "github_app",
      configPath: "config/sync.json",
      repo: "org/repo",
      branch: "main",
      path: "baluffo/source-sync.json"
    },
    runtime: {
      lastPullAt: "2026-03-08T10:00:00Z",
      lastPushAt: "2026-03-08T10:05:00Z",
      lastAction: "pull",
      lastResult: "success",
      lastError: ""
    }
  };
  const controller = createAdminSyncController({
    state,
    refs,
    getBridge: async path => {
      paths.push(path);
      return readyPayload;
    },
    postBridge: async (path, payload) => {
      paths.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/sync/config") return readyPayload;
      if (path === "/sync/test") return { ok: true, remoteFound: true };
      if (path === "/tasks/run-sync-pull") return { started: true };
      if (path === "/tasks/run-sync-push") return { started: true };
      throw new Error(`unexpected path ${path}`);
    },
    isSyncBusy: () => false,
    setBusyFlag(key, value) {
      busyTransitions.push(`${key}:${String(value)}`);
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast(message, level) {
      toasts.push({ message, level });
    },
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {
      paths.push("loadOpsHealthData");
    },
    scheduleOpsHealthPolling(delay) {
      paths.push(`scheduleOpsHealthPolling:${delay}`);
    },
    escapeHtml: value => String(value)
  });

  const payload = await controller.loadSyncStatus({ forceForm: true });
  assert.equal(payload, readyPayload);
  assert.equal(refs.adminSyncEnabledEl.checked, true);
  assert.match(refs.adminSyncConfigHintEl.textContent, /packaged config: config\/sync\.json/i);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Connected to org\/repo/i);
  assert.match(refs.adminSyncStatusEl.innerHTML, /Local sync enabled/i);

  await controller.saveSyncConfig();
  await controller.testSyncConfig();
  await controller.pullSourcesSync();
  await controller.pushSourcesSync();

  assert.equal(state.syncConfigDirty, false);
  assert.ok(paths.includes("/sync/status"));
  assert.ok(paths.includes("/sync/config:{\"enabled\":true}"));
  assert.ok(paths.includes("/sync/test:{}"));
  assert.ok(paths.includes("/tasks/run-sync-pull:{}"));
  assert.ok(paths.includes("/tasks/run-sync-push:{}"));
  assert.equal(paths.filter(item => item === "loadOpsHealthData").length, 2);
  assert.equal(paths.filter(item => item === "scheduleOpsHealthPolling:900").length, 2);
  assert.ok(toasts.some(item => item.message === "Source sync preference updated." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sync test passed. Remote snapshot found." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sources sync pull started." && item.level === "success"));
  assert.ok(toasts.some(item => item.message === "Sources sync push started." && item.level === "success"));
  assert.deepEqual(busyTransitions, [
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false",
    "syncRun:true",
    "syncRun:false"
  ]);
});

test("appendAdminLogRow passes Date to custom toLocalTime and renders its result", () => {
  const created = [];
  const previousDocument = global.document;
  global.document = {
    createElement(tagName) {
      const el = createElement({
        tagName,
        dataset: {},
        children: [],
        append(...nodes) {
          this.children.push(...nodes);
        },
        appendChild(node) {
          this.children.push(node);
        },
        removeChild(node) {
          const index = this.children.indexOf(node);
          if (index >= 0) this.children.splice(index, 1);
        },
        scrollTop: 0,
        scrollHeight: 0
      });
      created.push(el);
      return el;
    }
  };

  try {
    const container = createElement({
      children: [],
      appendChild(node) {
        this.children.push(node);
      },
      removeChild(node) {
        const index = this.children.indexOf(node);
        if (index >= 0) this.children.splice(index, 1);
      },
      scrollTop: 0,
      scrollHeight: 0
    });

    const event = {
      timestamp: "2026-03-08T10:00:00.000Z",
      level: "info",
      scope: "fetcher",
      sourceId: "",
      message: "START source=test"
    };

    let receivedValue = null;
    function toLocalTimeSpy(value) {
      receivedValue = value;
      return "10:00:00";
    }

    appendAdminLogRow(container, event, {
      normalizeLogLevel: value => value,
      toLocalTime: toLocalTimeSpy,
      formatLogEventText: row => String(row?.message || "")
    });

    assert.ok(receivedValue instanceof Date);
    assert.equal(container.children.length, 1);
    const rowEl = container.children[0];
    assert.ok(Array.isArray(rowEl.children));
    assert.ok(rowEl.children.length >= 1);
    const stampEl = rowEl.children[0];
    assert.equal(String(stampEl.textContent), "10:00:00");
  } finally {
    global.document = previousDocument;
  }
});

test("appendAdminLogRow falls back safely when timestamp is invalid", () => {
  const previousDocument = global.document;
  global.document = {
    createElement(tagName) {
      const el = createElement({
        tagName,
        dataset: {},
        children: [],
        append(...items) {
          this.children.push(...items);
        }
      });
      return el;
    }
  };
  try {
    const container = createElement({
      children: [],
      firstChild: null,
      appendChild(child) {
        this.children.push(child);
        this.firstChild = this.children[0] || null;
      },
      removeChild(child) {
        const index = this.children.indexOf(child);
        if (index >= 0) {
          this.children.splice(index, 1);
        }
        this.firstChild = this.children[0] || null;
      },
      scrollTop: 0,
      scrollHeight: 0
    });
    appendAdminLogRow(
      container,
      {
        timestamp: "[[2026-03-08T10:01:00.000Z",
        level: "info",
        scope: "fetcher",
        sourceId: "",
        message: "Broken timestamp"
      },
      {
        normalizeLogLevel: value => value,
        toLocalTime: value => value.toString(),
        formatLogEventText: row => String(row?.message || "")
      }
    );

    const rowEl = container.children[0];
    const stampEl = rowEl.children[0];
    assert.notEqual(String(stampEl.textContent), "Invalid Date");
  } finally {
    global.document = previousDocument;
  }
});
