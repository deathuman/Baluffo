import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  FakeInputElement,
  createElement,
  createRegistryControllerFixture,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

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
