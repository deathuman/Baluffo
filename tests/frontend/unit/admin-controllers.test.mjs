import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";

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

test("admin auth controller unlocks and locks the composed admin view", async () => {
  const dispatched = [];
  const toasts = [];
  const calls = [];
  const state = {
    activeSourceFilter: "all",
    adminPin: "",
    syncConfigDirty: false,
    latestSyncStatusCache: { stale: true },
    adminBusyState: {}
  };
  const refs = {
    adminUnlockBtnEl: createElement(),
    adminLockBtnEl: createElement({ classList: createClassList(["hidden"]) }),
    adminPinGateEl: createElement(),
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminPinInputEl: createElement({ value: "1234" }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminTotalsEl: createElement({ innerHTML: "stale" }),
    adminUsersListEl: createElement({ innerHTML: "stale" }),
    adminSyncStatusEl: createElement()
  };

  const controller = createAdminAuthController({
    state,
    refs,
    services: {
      adminService: {
        verifyAdminPin: pin => pin === "1234"
      },
      adminPageService: {
        isAvailable: () => true
      }
    },
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
  assert.equal(initReady, true);

  controller.unlockAdmin();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(state.adminPin, "1234");
  assert.equal(refs.adminPinInputEl.value, "");
  assert.equal(refs.adminPinGateEl.classList.contains("hidden"), true);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  assert.equal(refs.adminLockBtnEl.classList.contains("hidden"), false);
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("hidden"), false);
  assert.deepEqual(dispatched.map(item => item.type), ["unlocked"]);
  assert.ok(calls.includes("resetBusyFlags"));
  assert.ok(calls.includes("startBridgeStatusWatch"));
  assert.ok(calls.includes("refreshOverview"));
  assert.ok(calls.includes("loadDiscoveryData"));
  assert.ok(calls.includes("loadOpsHealthData"));
  assert.ok(calls.includes("scheduleOpsHealthPolling:900"));

  controller.lockAdmin();

  assert.equal(state.adminPin, "");
  assert.equal(state.latestSyncStatusCache, null);
  assert.equal(refs.adminPinGateEl.classList.contains("hidden"), false);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), true);
  assert.equal(refs.adminLockBtnEl.classList.contains("hidden"), true);
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("hidden"), true);
  assert.equal(refs.adminTotalsEl.innerHTML, "");
  assert.equal(refs.adminUsersListEl.innerHTML, "");
  assert.deepEqual(dispatched.map(item => item.type), ["unlocked", "locked"]);
  assert.ok(calls.includes("stopBridgeStatusWatch"));
  assert.ok(calls.includes("stopOpsHealthPolling"));
  assert.equal(toasts.length, 0);
});

test("admin auth controller polls for api readiness while locked", async () => {
  const scheduled = [];
  const refs = {
    adminUnlockBtnEl: createElement(),
    adminLockBtnEl: createElement({ classList: createClassList(["hidden"]) }),
    adminPinGateEl: createElement(),
    adminContentEl: createElement({ classList: createClassList(["hidden"]) })
  };
  const state = {
    activeSourceFilter: "all",
    adminPin: "",
    adminApiReadyPollTimer: null,
    adminBusyState: {}
  };
  let available = false;
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  try {
    const sourceStatus = { text: "" };
    const controller = createAdminAuthController({
      state,
      refs,
      services: {
        adminService: { verifyAdminPin: () => true },
        adminPageService: { isAvailable: () => available }
      },
      adminDispatch: { dispatch() {} },
      adminActions: { UNLOCKED: "u", LOCKED: "l" },
      emitAdminStartupMetric() {},
      markAdminFirstInteractive() {},
      syncAdminBusyUi() {},
      syncDiscoveryLogDisclosure() {},
      resetBusyFlags() {},
      setSourceFilter() {},
      setSourceStatus(text) {
        sourceStatus.text = text;
      },
      setFetcherLogPlaceholder() {},
      setDiscoveryLogPlaceholder() {},
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
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      showToast() {}
    });

    const initReady = controller.initAdminPage();
    assert.equal(initReady, false);
    assert.equal(refs.adminUnlockBtnEl.disabled, true);
    assert.match(refs.adminUnlockBtnEl.title, /waiting for local storage provider/i);
    assert.equal(scheduled.length > 0, true);

    available = true;
    scheduled[0]();

    assert.equal(refs.adminUnlockBtnEl.disabled, false);
    assert.equal(refs.adminUnlockBtnEl.attributes["aria-disabled"], "false");
    assert.equal(sourceStatus.text, "Enter admin PIN to access user overview.");
  } finally {
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
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
      adminDiscoveryProgressLabelEl: createElement()
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

  try {
    const controller = createAdminFetcherController({
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
        runtime: { selectedSourceCount: 10 },
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
      jobsFetcherCommand: "python scripts/jobs_fetcher.py",
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
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
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
