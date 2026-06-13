import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  FakeInputElement,
  createDeferredRenderScheduler,
  createElement,
  createRegistryControllerFixture,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

function registrySourcesPayload({
  pending = [],
  active = [],
  rejected = [],
  summary = {}
} = {}) {
  return {
    ok: true,
    sources: { pending, active, rejected },
    summary: {
      activeCount: active.length,
      pendingCount: pending.length,
      rejectedCount: rejected.length,
      hiddenPendingCount: 0,
      ...summary
    }
  };
}

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
  const renderScheduler = createDeferredRenderScheduler();
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
      if (path === "/discovery/candidates") {
        return {
          candidates: [
            { id: "p2", jobsFound: 1, status: "ok" },
            { id: "d1", jobsFound: 3, deferred: true, deferReason: "adapter_cap" },
            { id: "d2", jobsFound: 0, deferred: true, deferReason: "domain_cap" }
          ]
        };
      }
      if (String(path).startsWith("/registry/sources")) {
        return registrySourcesPayload({
          pending: [
            { id: "p1", name: "One", jobsFound: 2, status: "healthy" },
            { id: "p2", name: "Zero", jobsFound: 0, status: "healthy" }
          ],
          active: [{ id: "a1", name: "Active", jobsFound: 3, status: "healthy" }],
          rejected: [{ id: "r1", name: "Rejected", jobsFound: 1, status: "error" }],
          summary: { pendingCount: 2, activeCount: 1, rejectedCount: 1, summaryExact: true, countBasis: "normalized" }
        });
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceDiscoveryCandidates: (rows, payload) => rows.map(row => {
      const match = (payload.candidates || []).find(candidate => candidate.id === row.id);
      return match ? { ...row, ...match } : row;
    }),
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
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: renderScheduler.schedule
  });

  await controller.loadDiscoveryData();
  renderScheduler.flush();

  assert.match(refs.adminDiscoverySummaryEl.textContent, /Found 4 \| Probed 3 \| Review queue 2/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Deferred review 1/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Deferred by caps 2/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Job-positive deferred 1/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Auto-approved this run 0/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Active registry 1 \(normalized counts\)/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Validated 3/);
  assert.match(refs.adminDiscoverySummaryEl.textContent, /Hidden zero-jobs 0/);
  assert.equal(refs.adminPendingSourcesEl.innerHTML, "One|Zero");
  assert.equal(refs.adminActiveSourcesEl.innerHTML, "Active");
  assert.equal(refs.adminRejectedSourcesEl.innerHTML, "Rejected");
  assert.deepEqual(dispatched.map(item => item.type), ["discovery/refreshed"]);
  assert.ok(logs.some(line => /source discovery data loaded/i.test(line)));
  assert.ok(logs.some(line => /review queue 2/i.test(line)));
  assert.deepEqual(busyTransitions, ["discoveryLoad:true", "discoveryLoad:false"]);
});

test("admin registry controller defers heavy discovery loads while discovery is running", async () => {
  const state = {
    activeSourceFilter: "all",
    latestDiscoveryReportCache: { runId: "discovery_live_1", summary: {} },
    adminBusyState: {
      discoveryLoad: false,
      discoveryWatch: true,
      liveDiscoveryRunning: true
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    adminManualSourceFeedbackEl: createElement()
  };
  const calls = [];
  const logs = [];
  const busyTransitions = [];
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => {
      calls.push("fetchReport");
      return {};
    },
    mergeSourceDiscoveryCandidates: rows => rows,
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
      busyTransitions.push(`${key}:${String(value)}`);
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: callback => callback()
  });

  const result = await controller.loadDiscoveryData();

  assert.equal(result?.skipped, true);
  assert.equal(result?.reason, "discovery_running");
  assert.deepEqual(calls, []);
  assert.deepEqual(busyTransitions, []);
  assert.ok(logs.some(line => /tables will refresh after this run completes/i.test(line)));
});

test("admin registry controller allows completion refresh while discovery watch is still active", async () => {
  const state = {
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    adminBusyState: {
      discoveryLoad: false,
      discoveryWatch: true,
      liveDiscoveryRunning: true
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    adminManualSourceFeedbackEl: createElement()
  };
  const calls = [];
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async (path, requestOptions = {}) => {
      calls.push({ path, requestOptions });
      if (path === "/discovery/report") return { summary: {}, finishedAt: "2026-03-08T10:05:00Z" };
      if (path === "/registry/summary") return { ok: true, summary: {} };
      if (path === "/discovery/candidates") return { candidates: [] };
      if (String(path).startsWith("/registry/sources")) return registrySourcesPayload();
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => ({ sources: [] }),
    mergeSourceDiscoveryCandidates: rows => rows,
    mergeSourceStatusFromReport: rows => rows,
    applySourceFilter: rows => rows,
    getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
    deriveSourceStatus: row => String(row?.status || "unknown"),
    renderSourcesTableHtml: rows => rows.map(row => row.name).join("|"),
    readShowZeroJobs: () => false,
    normalizeSourceFilter: value => value,
    adminDispatch: { dispatch() {} },
    adminActions: { DISCOVERY_REFRESHED: "discovery/refreshed" },
    appendDiscoveryLog() {},
    formatManualCheckFailureMessage: () => "failed",
    loadOpsHealthData: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: callback => callback()
  });

  const result = await controller.loadDiscoveryData({ background: true, completionRefresh: true });

  assert.equal(result?.skipped, undefined);
  assert.ok(calls.some(call => call.path === "/discovery/report"));
  const summaryIndex = calls.findIndex(call => call.path === "/registry/summary");
  const sourcesIndex = calls.findIndex(call => String(call.path).startsWith("/registry/sources"));
  assert.ok(summaryIndex >= 0);
  assert.ok(sourcesIndex > summaryIndex);
  assert.equal(calls[sourcesIndex].requestOptions.timeoutMs, 60000);
});

test("admin registry controller treats background registry source timeout as delayed partial load", async () => {
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") {
          return {
            summary: {
              foundEndpointCount: 7,
              probedCandidateCount: 6,
              queuedCandidateCount: 5
            }
          };
        }
        if (path === "/registry/summary") return { ok: true, summary: { activeCount: 1 } };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) throw new Error("Bridge request timed out");
        throw new Error(`unexpected path ${path}`);
      },
      fetchJobsFetchReportJson: async () => ({ sources: [] })
    }
  });
  const logs = [];
  fixture.options.appendDiscoveryLog = message => {
    logs.push(String(message));
  };
  fixture.refs.adminPendingSourcesEl.innerHTML = "Existing pending rows";
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData({ background: true });
  fixture.renderScheduler.flush();

  assert.equal(result.partialLoadFailed, true);
  assert.match(
    logs.join("\n"),
    /Source table refresh delayed; retrying/
  );
  assert.doesNotMatch(
    logs.join("\n"),
    /Could not load pending registry/
  );
  assert.doesNotMatch(
    fixture.refs.adminDiscoverySummaryEl.textContent,
    /Source discovery bridge unavailable/
  );
  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "Existing pending rows");
});

test("admin registry controller explains hidden zero-job pending rows", async () => {
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") return { summary: { queuedCandidateCount: 0 } };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          return registrySourcesPayload({
            pending: [
              { id: "p1", name: "Zero One", jobsFound: 0 },
              { id: "p2", name: "Zero Two", sampleCount: 0 }
            ],
            summary: { pendingCount: 2, hiddenPendingCount: 2 }
          });
        }
        throw new Error(`unexpected path ${path}`);
      },
      readShowZeroJobs: () => false,
      renderSourcesTableHtml: rows => rows.map(row => row.name).join("|")
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.match(
    fixture.refs.adminPendingSourcesEl.innerHTML,
    /2 pending sources have 0 discovery jobs and are hidden/
  );
});

test("admin registry controller renders registry buckets when the combined source payload resolves", async () => {
  const report = createDeferred();
  const candidates = createDeferred();
  const sources = createDeferred();
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: path => {
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/discovery/report") return report.promise;
        if (path === "/discovery/candidates") return candidates.promise;
        if (String(path).startsWith("/registry/sources")) return sources.promise;
        throw new Error(`unexpected path ${path}`);
      },
      getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
      renderSourcesTableHtml: rows => rows.map(row => row.name).join("|")
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const loadPromise = controller.loadDiscoveryData(); await flushMicrotasks(2);
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Loading pending sources/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Loading active sources/);
  assert.match(fixture.refs.adminRejectedSourcesEl.innerHTML, /Loading rejected sources/);

  report.resolve({ summary: {} });
  candidates.resolve({ candidates: [] });
  sources.resolve(registrySourcesPayload({
    pending: [{ id: "pending_ready", name: "Pending Ready", jobsFound: 1 }],
    active: [{ id: "active_ready", name: "Active Ready", jobsFound: 1 }],
    rejected: [{ id: "rejected_ready", name: "Rejected Ready", jobsFound: 1 }]
  }));
  await flushMicrotasks(8);
  fixture.renderScheduler.flush();

  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "Pending Ready");
  await loadPromise;
  fixture.renderScheduler.flush();

  assert.equal(fixture.refs.adminActiveSourcesEl.innerHTML, "Active Ready");
  assert.equal(fixture.refs.adminRejectedSourcesEl.innerHTML, "Rejected Ready");
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
      if (String(path).startsWith("/registry/sources")) {
        return registrySourcesPayload({
          pending: [{ id: "p1", name: "Pending", jobsFound: 1, status: "pending" }]
        });
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

test("admin registry controller keeps registry signature stable across source row order changes", async () => {
  const pendingRowsByCall = [
    [
      { id: "p1", name: "Pending One", adapter: "static", studio: "One", status: "pending", jobsFound: 1, sourceId: "src_1", url: "https://one.example/jobs" },
      { id: "p2", name: "Pending Two", adapter: "greenhouse", studio: "Two", status: "pending", jobsFound: 2, sourceId: "src_2", sourceUrl: "https://two.example/jobs" }
    ],
    [
      { id: "p2", name: "Pending Two", adapter: "greenhouse", studio: "Two", status: "pending", jobsFound: 2, sourceId: "src_2", sourceUrl: "https://two.example/jobs" },
      { id: "p1", name: "Pending One", adapter: "static", studio: "One", status: "pending", jobsFound: 1, sourceId: "src_1", url: "https://one.example/jobs" }
    ]
  ];
  let pendingCallIndex = 0;
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") return { summary: {} };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          const rows = pendingRowsByCall[Math.min(pendingCallIndex, pendingRowsByCall.length - 1)];
          pendingCallIndex += 1;
          return registrySourcesPayload({ pending: rows, summary: { pendingCount: rows.length } });
        }
        throw new Error(`unexpected path ${path}`);
      },
      getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
      renderSourcesTableHtml: rows => rows.map(row => row.name).join("|")
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();
  await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(fixture.logs.filter(line => /source discovery data loaded/i.test(line)).length, 1);
  assert.equal(fixture.logs.filter(line => /loading source discovery report and registries/i.test(line)).length, 1);
  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "Pending Two|Pending One");
});

test("admin registry controller changes registry signature when a tracked source field changes", async () => {
  const pendingRowsByCall = [
    [{ id: "p1", name: "Pending", adapter: "static", studio: "One", status: "pending", jobsFound: 1, sourceId: "src_1", url: "https://one.example/jobs" }],
    [{ id: "p1", name: "Pending", adapter: "static", studio: "One", status: "healthy", jobsFound: 1, sourceId: "src_1", url: "https://one.example/jobs" }]
  ];
  let pendingCallIndex = 0;
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") return { summary: {} };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          const rows = pendingRowsByCall[Math.min(pendingCallIndex, pendingRowsByCall.length - 1)];
          pendingCallIndex += 1;
          return registrySourcesPayload({ pending: rows, summary: { pendingCount: rows.length } });
        }
        throw new Error(`unexpected path ${path}`);
      },
      getSourceJobsFoundCount: row => Number(row?.jobsFound || 0)
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();
  await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(fixture.logs.filter(line => /source discovery data loaded/i.test(line)).length, 2);
  assert.equal(fixture.logs.filter(line => /loading source discovery report and registries/i.test(line)).length, 2);
});

test("admin registry controller keeps registry signature stable for empty and malformed buckets", async () => {
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") return { summary: {} };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          return { sources: { pending: null, active: { bad: true } }, summary: { pendingCount: 0, activeCount: 0, rejectedCount: 0 } };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();
  await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(fixture.logs.filter(line => /source discovery data loaded/i.test(line)).length, 1);
  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "");
  assert.equal(fixture.refs.adminActiveSourcesEl.innerHTML, "");
  assert.equal(fixture.refs.adminRejectedSourcesEl.innerHTML, "");
});

test("admin registry controller skips stale deferred source table renders after a newer refresh", async () => {
  const pendingRowsByCall = [
    [{ id: "old", name: "Old Pending", jobsFound: 1 }],
    [{ id: "new", name: "New Pending", jobsFound: 1 }]
  ];
  let pendingCallIndex = 0;
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") return { summary: {} };
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          const rows = pendingRowsByCall[Math.min(pendingCallIndex, pendingRowsByCall.length - 1)];
          pendingCallIndex += 1;
          return registrySourcesPayload({ pending: rows, summary: { pendingCount: rows.length } });
        }
        throw new Error(`unexpected path ${path}`);
      },
      getSourceJobsFoundCount: row => Number(row?.jobsFound || 0),
      renderSourcesTableHtml: rows => rows.map(row => row.name).join("|")
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();
  await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "New Pending");
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
    if (path === "/registry/summary") return { ok: true, summary: { pendingCount: 1, activeCount: 1 } };
    if (String(path).startsWith("/registry/sources")) {
      return registrySourcesPayload({
        pending: [{ id: "pending_1", name: "Pending" }],
        active: [{ id: "active_1", name: "Active" }],
        summary: { pendingCount: 1, activeCount: 1 }
      });
    }
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
  fixture.renderScheduler.flush();

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
      if (String(path).startsWith("/registry/sources")) {
        return registrySourcesPayload();
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
