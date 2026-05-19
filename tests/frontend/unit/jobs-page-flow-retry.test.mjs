import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPageFlow } from "../../../frontend/jobs/app/runtime/page-flow.js";

function createDeps(overrides = {}) {
  const bodyAttrs = new Map();
  const deps = {
    pipelineController: {
      ensureJobsPipelineStatusWatch: () => {},
      triggerJobsPipelineRun: async () => {}
    },
    runtimeState: {
      allJobs: [],
      filteredJobs: [],
      hasInitializedJobsFeed: false,
      lastHandledAutoRefreshSignalId: "",
      pendingAutoRefreshSignal: null
    },
    state: { currentPage: 1, filters: { countries: new Set(), sort: "" } },
    userState: { currentUser: null, seenJobKeys: new Set(), savedJobKeys: new Set() },
    dom: { jobsList: {}, pagination: {}, resultsSummary: { textContent: "" } },
    documentObject: {
      body: {
        setAttribute(name, value) {
          bodyAttrs.set(name, String(value));
        },
        removeAttribute(name) {
          bodyAttrs.delete(name);
        }
      }
    },
    windowObject: { location: { search: "", reload: () => {} } },
    defaultFilters: {},
    jobsUrlPersistence: { writeStateToUrl: () => {}, rememberCurrentJobsUrl: () => {} },
    startupPreviewController: {
      clearPendingStartupPreviewMaterialization: () => {},
      materializePendingStartupPreview: () => {}
    },
    filtersController: { syncStateFromFilters: () => {} },
    feedController: {
      setSourceStatus: () => {},
      setProgress: () => {},
      refreshJobsNow: async () => true
    },
    emitDesktopStartupMetric: () => {},
    normalizeLifecycleStatus: value => value,
    writeAutoRefreshAppliedId: () => {},
    readAutoRefreshSignal: () => "",
    jobsAutoRefreshAppliedKey: "jobs-auto-refresh-applied",
    jobsAutoRefreshSignalKey: "jobs-auto-refresh-signal",
    logJobsError: () => {},
    showJobsError: () => {},
    retryInit: async () => {},
    isJobsApiReady: () => true,
    getJobKeyForJob: job => String(job?.id || ""),
    getJobLocationCities: () => [],
    getJobLocationCountries: () => [],
    isInternshipJob: () => false,
    fullCountryName: value => value,
    renderJobRowHtml: () => "",
    ...overrides
  };
  return { deps, bodyAttrs };
}

test("Jobs page-flow forwards retry click events to custom retry handlers", async () => {
  let retryHandler = null;
  let receivedEvent = null;
  const { deps, bodyAttrs } = createDeps({
    runtimeState: { allJobs: [{ id: "loaded" }], filteredJobs: [] },
    showJobsError: (_jobsList, _pagination, message, onRetry) => {
      assert.equal(message, "Unable to confirm first-run refresh.");
      retryHandler = onRetry;
    }
  });
  const flow = createJobsPageFlow(deps);

  flow.showError("Unable to confirm first-run refresh.", event => {
    receivedEvent = event;
  });

  assert.equal(typeof retryHandler, "function");
  const clickEvent = { type: "click", currentTarget: { id: "jobs-retry-btn" } };
  await retryHandler(clickEvent);

  assert.equal(receivedEvent, clickEvent);
  assert.equal(bodyAttrs.get("data-jobs-startup-state"), "error");
  assert.equal(bodyAttrs.get("data-jobs-startup-detail"), "load_error");
  assert.equal(deps.dom.resultsSummary.textContent, "Showing 0 jobs (1 loaded)");
});
