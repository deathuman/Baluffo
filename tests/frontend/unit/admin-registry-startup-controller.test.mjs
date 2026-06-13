import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

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

test("admin registry controller can refresh source tables without full discovery diagnostics", async () => {
  const state = {
    activeSourceFilter: "all",
    latestFetcherReportCache: { sources: [] },
    adminBusyState: {
      discoveryLoad: false
    }
  };
  const refs = {
    adminDiscoverySummaryEl: createElement({
      textContent: "Discovery report not loaded yet.",
      innerHTML: "<div>Discovery report not loaded yet.</div>"
    }),
    adminPendingSourcesEl: createElement(),
    adminActiveSourcesEl: createElement(),
    adminRejectedSourcesEl: createElement(),
    adminDiscoveryReviewEl: createElement({ innerHTML: "keep review" }),
    adminManualSourceFeedbackEl: createElement()
  };
  const calls = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminRegistryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/discovery/report?view=summary") {
        throw new Error("source table refresh should not depend on discovery summary");
      }
      if (String(path).startsWith("/registry/sources")) {
        return registrySourcesPayload({
          pending: [{ id: "p1", name: "Pending", jobsFound: 2, status: "healthy" }],
          active: [{ id: "a1", name: "Active", jobsFound: 3, status: "healthy" }],
          rejected: []
        });
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    fetchJobsFetchReportJson: async () => {
      throw new Error("fetch report should not be loaded for sourceTablesOnly");
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
    appendDiscoveryLog() {},
    formatManualCheckFailureMessage: () => "failed",
    loadOpsHealthData: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    renderScheduler: renderScheduler.schedule
  });

  await controller.loadDiscoveryData({
    background: true,
    sourceTablesOnly: true,
    suppressPlaceholders: true,
    logChanges: false
  });
  renderScheduler.flush();

  assert.deepEqual(calls, [
    "/tasks/run-jobs-pipeline-status",
    "/registry/sources?buckets=pending,active,rejected&includeHiddenPending=0"
  ]);
  assert.equal(refs.adminPendingSourcesEl.innerHTML, "Pending");
  assert.equal(refs.adminActiveSourcesEl.innerHTML, "Active");
  assert.equal(refs.adminDiscoveryReviewEl.innerHTML, "keep review");
  assert.equal(refs.adminDiscoverySummaryEl.textContent, "Discovery report not loaded yet.");
});
