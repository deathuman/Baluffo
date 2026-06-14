import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller defers source tables while sync is active", async () => {
  const state = {
    activeSourceFilter: "all",
    latestOpsTaskStatePayload: {
      tasks: [{ taskType: "sync", type: "sync", runId: "sync_live_1", active: true }],
      count: 1,
      summary: true
    },
    adminBusyState: {
      discoveryLoad: false,
      liveSyncRunning: true
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement(),
    adminPendingSourcesEl: createElement({ innerHTML: "Existing pending sources" }),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    adminManualSourceFeedbackEl: createElement()
  };
  const calls = [];
  const logs = [];
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
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: callback => callback()
  });

  const result = await controller.loadDiscoveryData();

  assert.equal(result?.skipped, true);
  assert.equal(result?.reason, "sync_running");
  assert.deepEqual(calls, []);
  assert.equal(refs.adminPendingSourcesEl.innerHTML, "Existing pending sources");
  assert.match(refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(refs.adminRejectedSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.ok(logs.some(line => /Source tables delayed while job update is running/i.test(line)));
});
