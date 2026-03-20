import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";
import { appendAdminLogRow } from "../../../frontend/admin/render.js";

class FakeInputElement {
  constructor({ checked = false, sourceId = "", sourceUrl = "" } = {}) {
    this.checked = checked;
    this.dataset = {
      sourceId,
      sourceUrl
    };
  }
}

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === true) {
        values.add(token);
        return true;
      }
      if (force === false) {
        values.delete(token);
        return false;
      }
      if (values.has(token)) {
        values.delete(token);
        return false;
      }
      values.add(token);
      return true;
    },
    contains(token) {
      return values.has(token);
    },
    toArray() {
      return Array.from(values);
    }
  };
}

function createElement(overrides = {}) {
  return {
    textContent: "",
    innerHTML: "",
    value: "",
    disabled: false,
    title: "",
    checked: false,
    classList: createClassList(),
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    removeAttribute(name) {
      delete this.attributes[name];
    },
    ...overrides
  };
}

function withDom(queryMap, fn) {
  const previousDocument = global.document;
  const previousInput = global.HTMLInputElement;
  global.document = {
    querySelectorAll(selector) {
      return queryMap.get(selector) || [];
    }
  };
  global.HTMLInputElement = FakeInputElement;
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      global.document = previousDocument;
      global.HTMLInputElement = previousInput;
    });
}

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
  assert.ok(calls.includes("scheduleOpsHealthPolling:900"));
  assert.equal(refs.adminSyncStatusEl.textContent, "Loading sync status...");
  assert.equal(toasts.length, 0);
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
    adminPin: "1234",
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
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Hidden zero-jobs 1/);
  assert.equal(refs.adminPendingSourcesEl.innerHTML, "One");
  assert.equal(refs.adminActiveSourcesEl.innerHTML, "Active");
  assert.equal(refs.adminRejectedSourcesEl.innerHTML, "Rejected");
  assert.deepEqual(dispatched.map(item => item.type), ["discovery/refreshed"]);
  assert.ok(logs.some(line => /source discovery data loaded/i.test(line)));
  assert.deepEqual(busyTransitions, ["discoveryLoad:true", "discoveryLoad:false"]);
});

test("admin registry controller adds a manual source and runs the follow-up check", async () => {
  const toasts = [];
  const logs = [];
  const state = {
    adminPin: "1234",
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
        adminPin: "1234",
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
          adminPendingSourcesEl: createElement(),
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
  const toasts = [];
  const logs = [];
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
      adminPin: "1234",
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollDeadline: 0,
      discoveryReportPollTimeoutMs: 60000,
      discoveryReportPollIntervalMs: 5000,
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
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async path => {
        calls.push(path);
        return {
          started: true,
          runId: "discovery_123",
          startedAt: "2026-03-08T10:01:00.000Z"
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
      runId: "discovery_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(state.adminBusyState.discoveryWatch, true);
    assert.equal(state.adminBusyState.liveDiscoveryRunning, true);
    assert.ok(calls.includes("/tasks/run-discovery"));
    assert.ok(calls.includes("loadOpsHealthData"));
    assert.ok(calls.includes("scheduleOpsHealthPolling:250"));
    assert.ok(logs.some(line => /source discovery task started/i.test(line)));
    assert.ok(toasts.some(item => item.message === "Source discovery started." && item.level === "success"));
    assert.deepEqual(busyTransitions, [
      "discoveryRun:true",
      "liveDiscoveryRunning:true",
      "discoveryWatch:false",
      "discoveryWatch:true",
      "discoveryRun:false"
    ]);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin discovery controller emits summary-first live progress and updates progress bar", async () => {
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
  Date.now = () => Date.parse("2026-03-08T10:01:00.500Z");

  try {
    const barEl = createElement({ style: {} });
    const state = {
      adminPin: "1234",
      discoveryLogRemoteOffset: 0,
      discoveryLaunchAtMs: 0,
      discoveryCompletionPollDeadline: 0,
      discoveryReportPollTimeoutMs: 600000,
      discoveryReportPollIntervalMs: 5000,
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
      },
      postBridge: async () => ({
        started: true,
        runId: "discovery_123",
        startedAt: "2026-03-08T10:01:00.000Z"
      }),
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

    await controller.runDiscoveryTask();
    await scheduled[0]();

    assert.ok(logs.some(line => /discovery started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /scanning known careers pages/i.test(line)));
    assert.ok(logs.some(line => /found 12 candidates, probed 5, queued 3/i.test(line)));
    assert.equal(refs.adminDiscoveryProgressEl.classList.contains("hidden"), false);
    assert.match(refs.adminDiscoveryProgressLabelEl.textContent, /discovery:/i);
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin discovery controller forwards uncapped preset payload", async () => {
  const calls = [];
  const state = {
    adminPin: "1234",
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollDeadline: 0,
    discoveryReportPollTimeoutMs: 60000,
    discoveryReportPollIntervalMs: 5000,
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

test("admin ops controller preserves optimistic discovery row while history lags", async () => {
  const state = {
    adminPin: "1234",
    latestOpsHealthCache: null,
    discoveryOptimisticRun: {
      runId: "discovery_123",
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
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    normalizeOpsRuns: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    applyOptimisticDiscoveryRun: (_model, optimisticRun) => {
      optimisticApplied += 1;
      return {
        currentRows: [{ type: "discovery", startedAt: optimisticRun.startedAt, isLive: true }],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: true,
        liveTypes: ["discovery"]
      };
    },
    applyOptimisticFetchRun: model => model,
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

  assert.equal(optimisticApplied, 1);
  assert.equal(state.adminBusyState.liveDiscoveryRunning, true);
  assert.equal(runModels.length, 1);
  assert.equal(runModels[0].currentRows[0].type, "discovery");
  assert.equal(runModels[0].currentRows[0].isLive, true);
});

test("admin ops controller preserves optimistic fetch row while history lags", async () => {
  const state = {
    adminPin: "1234",
    latestOpsHealthCache: null,
    fetchOptimisticRun: {
      runId: "fetch_123",
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
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    normalizeOpsRuns: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    applyOptimisticDiscoveryRun: model => model,
    applyOptimisticFetchRun: (_model, optimisticRun) => {
      optimisticApplied += 1;
      return {
        currentRows: [{ type: "fetch", startedAt: optimisticRun.startedAt, runId: optimisticRun.runId, isLive: true }],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: true,
        liveTypes: ["fetch"]
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

  assert.equal(optimisticApplied, 1);
  assert.equal(state.adminBusyState.liveFetchRunning, true);
  assert.equal(runModels.length, 1);
  assert.equal(runModels[0].currentRows[0].type, "fetch");
  assert.equal(runModels[0].currentRows[0].isLive, true);
});

test("admin fetcher controller stores optimistic run metadata while fetch watch is active", async () => {
  const logs = [];
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
    const state = {
      adminPin: "1234",
      latestFetcherReportCache: null,
      fetcherLaunchAtMs: 0,
      fetcherCompletionPollDeadline: 0,
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
          started: true,
          runId: "fetch_123",
          startedAt: "2026-03-08T10:01:00.000Z",
          preset: "default",
          args: ["--quiet"]
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
      loadOpsHealthData: async () => {
        calls.push("loadOpsHealthData");
      },
      startOpsHealthPolling() {},
      fetchReportPollIntervalMs: 5000,
      fetchReportPollTimeoutMs: 60000,
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      jobsFetchReportUrl: "data/jobs-fetch-report.json",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(state.adminBusyState.fetcherWatch, true);
    assert.equal(state.adminBusyState.liveFetchRunning, true);
    assert.ok(calls.includes("/tasks/run-fetcher"));
    assert.ok(logs.some(line => /triggered fetcher via local admin bridge/i.test(line)));
    assert.ok(scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller emits summary-first progress and updates progress bar", async () => {
  const logs = [];
  const state = {
    adminPin: "1234",
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollDeadline: Date.parse("2026-03-08T10:10:00.000Z"),
    fetchReportPollIntervalMs: 5000,
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
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
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
      startOpsHealthPolling() {},
      fetchReportPollIntervalMs: 5000,
      fetchReportPollTimeoutMs: 600000,
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      jobsFetchReportUrl: "data/jobs-fetch-report.json",
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    await scheduled[0]();

    assert.ok(logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /start source=studio a/i.test(line)));
    assert.ok(logs.some(line => /warn source=studio b http 403/i.test(line)));
    assert.equal(refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /fetcher:/i);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin ops controller auto-attaches discovery watch when a live discovery run exists", async () => {
  const state = {
    adminPin: "1234",
    latestOpsHealthCache: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      opsLoad: false,
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
  const attached = [];

  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    normalizeOpsRuns: () => ({
      currentRows: [{ type: "discovery", startedAt: "2026-03-08T10:01:00.000Z", isLive: true }],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: true,
      liveTypes: ["discovery"]
    }),
    applyOptimisticDiscoveryRun: model => model,
    applyOptimisticFetchRun: model => model,
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
    onLiveDiscoveryDetected(runMeta) {
      attached.push(runMeta);
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.equal(state.adminBusyState.liveDiscoveryRunning, true);
  assert.equal(attached.length, 1);
  assert.equal(attached[0].type, "discovery");
  assert.equal(attached[0].startedAt, "2026-03-08T10:01:00.000Z");
});

test("admin fetcher controller forwards uncapped preset payload", async () => {
  const calls = [];
  const state = {
    adminPin: "1234",
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: 0,
    fetcherCompletionPollDeadline: 0,
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
    startOpsHealthPolling() {},
    fetchReportPollIntervalMs: 5000,
    fetchReportPollTimeoutMs: 60000,
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher --social-enabled",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    jobsFetchReportUrl: "data/jobs-fetch-report.json",
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
    adminPin: "1234",
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
