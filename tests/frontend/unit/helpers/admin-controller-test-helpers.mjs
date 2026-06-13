class FakeInputElement {
  constructor({ checked = false, sourceId = "", sourceUrl = "" } = {}) {
    this.checked = checked;
    this.dataset = {
      sourceId,
      sourceUrl
    };
  }
}

function mergeBusyState(baseState, overrides = {}) {
  return {
    ...baseState,
    ...(overrides || {})
  };
}

export function createClassList(initial = []) {
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

export function createElement(overrides = {}) {
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

export function withDom(queryMap, fn) {
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

export function stubScheduledTimers({
  setTimeoutImpl,
  clearTimeoutImpl
} = {}) {
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = setTimeoutImpl || (callback => {
    scheduled.push(callback);
    return scheduled.length;
  });
  global.clearTimeout = clearTimeoutImpl || (() => {});
  return {
    scheduled,
    restore() {
      global.setTimeout = previousSetTimeout;
      global.clearTimeout = previousClearTimeout;
    }
  };
}

export function stubDateNow(nowValue) {
  const previousDateNow = Date.now;
  Date.now = typeof nowValue === "function" ? nowValue : () => nowValue;
  return {
    restore() {
      Date.now = previousDateNow;
    }
  };
}

export function createDeferredRenderScheduler() {
  const queue = [];
  return {
    schedule(callback) {
      const entry = { callback, cancelled: false };
      queue.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    flush() {
      while (queue.length) {
        const entry = queue.shift();
        if (!entry.cancelled) entry.callback();
      }
    },
    get pendingCount() {
      return queue.filter(entry => !entry.cancelled).length;
    }
  };
}

export function createRegistryControllerFixture({
  state: stateOverrides = {},
  refs: refOverrides = {},
  options: optionOverrides = {}
} = {}) {
  const state = {
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    fetcherSourceSyncSignature: "",
    discoverySourceSyncSignature: "",
    discoveryRegistrySignature: "",
    adminBusyState: mergeBusyState({
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      discoveryRun: false,
      discoveryWatch: false,
      liveDiscoveryRunning: false
    }, stateOverrides.adminBusyState),
    ...stateOverrides
  };
  const refs = {
    adminManualSourceUrlEl: createElement(),
    adminManualSourceFeedbackEl: createElement(),
    adminDiscoverySummaryEl: createElement(),
    adminDiscoveryReviewEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    ...refOverrides
  };
  const logs = [];
  const toasts = [];
  const bridgeCalls = [];
  const bridgePosts = [];
  const busyTransitions = [];
  const dispatched = [];
  const renderScheduler = createDeferredRenderScheduler();
  const options = {
    state,
    refs,
    getBridge: async path => {
      bridgeCalls.push(path);
      if (path === "/discovery/report") return { summary: {} };
      if (path === "/registry/summary") {
        return {
          ok: true,
          summary: { activeCount: 0, pendingCount: 0, rejectedCount: 0, hiddenPendingCount: 0 }
        };
      }
      if (String(path).startsWith("/registry/sources")) {
        return {
          ok: true,
          sources: { pending: [], active: [], rejected: [] },
          summary: { activeCount: 0, pendingCount: 0, rejectedCount: 0, hiddenPendingCount: 0 }
        };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      bridgePosts.push({ path, payload });
      return {};
    },
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceStatusFromReport: rows => rows,
    applySourceFilter: rows => rows,
    getSourceJobsFoundCount: () => 0,
    deriveSourceStatus: row => String(row?.status || "unknown"),
    renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
    readShowZeroJobs: () => true,
    normalizeSourceFilter: value => value,
    adminDispatch: {
      dispatch(action) {
        dispatched.push(action);
      }
    },
    adminActions: { DISCOVERY_REFRESHED: "discovery/refreshed" },
    appendDiscoveryLog(message) {
      logs.push(String(message || ""));
    },
    formatManualCheckFailureMessage: () => "",
    loadOpsHealthData: async () => {},
    setBusyFlag(key, value) {
      busyTransitions.push(`${key}:${String(value)}`);
      state.adminBusyState[key] = value;
    },
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: renderScheduler.schedule,
    ...optionOverrides
  };
  return {
    state,
    refs,
    logs,
    toasts,
    bridgeCalls,
    bridgePosts,
    busyTransitions,
    dispatched,
    renderScheduler,
    options
  };
}

export function createDiscoveryControllerFixture({
  state: stateOverrides = {},
  refs: refOverrides = {},
  options: optionOverrides = {}
} = {}) {
  const state = {
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    latestDiscoveryReportCache: null,
    adminBusyState: mergeBusyState({
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }, stateOverrides.adminBusyState),
    ...stateOverrides
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminDiscoveryProgressBarEl: createElement({ style: {} }),
    adminDiscoveryProgressLabelEl: createElement(),
    adminDiscoveryAutoApproveToggleEl: createElement({ checked: false }),
    adminRunDiscoveryUncappedBtnEl: createElement(),
    ...refOverrides
  };
  const logs = [];
  const toasts = [];
  const calls = [];
  const busyTransitions = [];
  const options = {
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (String(path).startsWith("/discovery/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/task-live/discovery?view=summary") return {};
      if (path === "/discovery/report") return {};
      return {};
    },
    postBridge: async path => {
      calls.push(path);
      return {};
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
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    activeProgressPollIntervalMs: 500,
    loadDiscoveryData: async () => {
      calls.push("loadDiscoveryData");
    },
    syncSourceTablesAfterTaskCompletion: async () => {},
    ...optionOverrides
  };
  return {
    state,
    refs,
    logs,
    toasts,
    calls,
    busyTransitions,
    options
  };
}

export function createFetcherControllerFixture({
  state: stateOverrides = {},
  refs: refOverrides = {},
  options: optionOverrides = {}
} = {}) {
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: 0,
    fetcherLogRemoteOffset: 0,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    adminBusyState: mergeBusyState({
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }, stateOverrides.adminBusyState),
    ...stateOverrides
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
    adminRetryFailedBtnEl: createElement(),
    ...refOverrides
  };
  const logs = [];
  const toasts = [];
  const calls = [];
  const busyTransitions = [];
  const options = {
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/task-live/fetch?view=summary") return {};
      return {};
    },
    postBridge: async path => {
      calls.push(path);
      return {};
    },
    fetchJobsFetchReportJson: async () => ({}),
    writeJobsAutoRefreshSignal() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    setBusyFlag(key, value) {
      busyTransitions.push(`${key}:${String(value)}`);
      state.adminBusyState[key] = value;
    },
    getSourceStatusSetter: () => () => {},
    loadOpsHealthData: async () => {
      calls.push("loadOpsHealthData");
    },
    activeProgressPollIntervalMs: 500,
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    syncSourceTablesAfterTaskCompletion: async () => {},
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
    },
    appendLogRow(_container, event) {
      logs.push(String(event.message || ""));
    },
    ...optionOverrides
  };
  return {
    state,
    refs,
    logs,
    toasts,
    calls,
    busyTransitions,
    options
  };
}

export { FakeInputElement };
