import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createJobsPageFlow } from "../../../frontend/jobs/app/runtime/page-flow.js";
import {
  JOBS_FETCH_REPORT_URLS,
  STARTUP_PREVIEW_JSON_URLS,
  getJobsFetchReportUrlsForRuntime,
  getStartupPreviewJsonUrlsForRuntime
} from "../../../frontend/jobs/app/sources.js";

// Regression guard for the silent dep-forwarding drop found during container
// verification: compose/boot layers must actually forward isContainerRuntimeMode.
const readSource = relativePath => readFileSync(new URL(relativePath, import.meta.url), "utf8");

test("container runtime flag is forwarded through compose, boot, and composition wiring", () => {
  const runtimeJs = readSource("../../../frontend/jobs/app/runtime.js");
  const bootJs = readSource("../../../frontend/jobs/app/runtime/boot.js");
  const compositionJs = readSource("../../../frontend/jobs/app/runtime/composition.js");

  assert.match(
    runtimeJs,
    /composeJobsRuntime\(\{[\s\S]*?isContainerRuntimeMode:\s*\(\)\s*=>\s*resolveContainerRuntimeMode\(\)/,
    "runtime.js must pass isContainerRuntimeMode into composeJobsRuntime"
  );
  assert.match(
    bootJs,
    /initJobsFeed\(\{[\s\S]*?isContainerRuntimeMode:\s*deps\.isContainerRuntimeMode/,
    "boot.js init() must forward isContainerRuntimeMode into initJobsFeed"
  );
  const forwardingCount = compositionJs.match(/isContainerRuntimeMode:\s*\(\)\s*=>\s*Boolean\(deps\.isContainerRuntimeMode\?\.\(\)\)/g)?.length ?? 0;
  assert.ok(
    forwardingCount >= 2,
    `composition.js must forward the flag to pipeline and feed controllers (found ${forwardingCount})`
  );
});

function createDeps(overrides = {}) {
  const recorded = {
    statusTexts: [],
    needsAttention: [],
    appliedIds: [],
    triggered: []
  };
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
        setAttribute() {},
        removeAttribute() {}
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
      setSourceStatus: text => recorded.statusTexts.push(String(text || "")),
      setProgress: () => {},
      setRefreshJobsNeedsAttention: value => recorded.needsAttention.push(Boolean(value)),
      refreshJobsNow: async () => {
        recorded.triggered.push("refresh");
        return false;
      }
    },
    emitDesktopStartupMetric: () => {},
    normalizeLifecycleStatus: value => value,
    writeAutoRefreshAppliedId: (_key, id) => recorded.appliedIds.push(id),
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
  return { deps, recorded };
}

function signalRaw(id = "sig-container-1") {
  return JSON.stringify({ id, source: "admin_fetcher", finishedAt: "2026-08-26T08:00:00.000Z" });
}

test("container boot acknowledges a stored admin signal as reload-needed without fetching the feed", async () => {
  const { deps, recorded } = createDeps({
    readAutoRefreshSignal: () => signalRaw()
  });
  const flow = createJobsPageFlow(deps);

  await flow.applyPendingAutoRefreshSignal({ acknowledgeOnly: true });

  assert.deepEqual(recorded.triggered, []);
  assert.deepEqual(recorded.needsAttention, [true]);
  assert.deepEqual(recorded.appliedIds, ["sig-container-1"]);
  assert.match(recorded.statusTexts.at(-1), /reload/i);
  assert.equal(deps.runtimeState.pendingAutoRefreshSignal, null);

  await flow.applyPendingAutoRefreshSignal({ acknowledgeOnly: true });

  assert.deepEqual(recorded.needsAttention, [true]);
  assert.deepEqual(recorded.appliedIds, ["sig-container-1"]);
});

test("container boot acknowledges an in-memory pending signal without another storage fetch", async () => {
  let storageReads = 0;
  const { deps, recorded } = createDeps({
    runtimeState: {
      allJobs: [],
      filteredJobs: [],
      hasInitializedJobsFeed: false,
      lastHandledAutoRefreshSignalId: "",
      pendingAutoRefreshSignal: { id: "sig-pending-1", finishedAt: "" }
    },
    readAutoRefreshSignal: () => {
      storageReads += 1;
      return "";
    }
  });
  const flow = createJobsPageFlow(deps);

  await flow.applyPendingAutoRefreshSignal({ acknowledgeOnly: true });

  assert.equal(storageReads, 0);
  assert.deepEqual(recorded.triggered, []);
  assert.deepEqual(recorded.needsAttention, [true]);
  assert.deepEqual(recorded.appliedIds, ["sig-pending-1"]);
  assert.equal(deps.runtimeState.pendingAutoRefreshSignal, null);
});

test("container boot with no pending signal leaves the boot path untouched", async () => {
  const { deps, recorded } = createDeps();
  const flow = createJobsPageFlow(deps);

  await flow.applyPendingAutoRefreshSignal({ acknowledgeOnly: true });

  assert.deepEqual(recorded.triggered, []);
  assert.deepEqual(recorded.needsAttention, []);
  assert.deepEqual(recorded.appliedIds, []);
});

test("signals arriving after the page is interactive still auto-refresh normally", async () => {
  const { deps, recorded } = createDeps({
    runtimeState: {
      allJobs: [],
      filteredJobs: [],
      hasInitializedJobsFeed: true,
      lastHandledAutoRefreshSignalId: "",
      pendingAutoRefreshSignal: null
    },
    readAutoRefreshSignal: () => ""
  });
  const flow = createJobsPageFlow(deps);

  flow.handleAutoRefreshSignalValue(signalRaw("sig-live-1"));
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.deepEqual(recorded.triggered, ["refresh"]);
  assert.deepEqual(recorded.appliedIds, ["sig-live-1"]);
});

test("container runtime wiring supplies startup-only preview URLs and no report URLs", () => {
  const previousConfig = globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = { runtime: { mode: "container" } };
  try {
    assert.deepEqual(getJobsFetchReportUrlsForRuntime(), []);
    assert.deepEqual(getStartupPreviewJsonUrlsForRuntime(), ["data/jobs-unified-startup.json"]);
  } finally {
    globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = previousConfig;
  }
});

test("browser runtime keeps report URLs and both startup preview candidates", () => {
  const previousConfig = globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  try {
    assert.deepEqual(getJobsFetchReportUrlsForRuntime(), JOBS_FETCH_REPORT_URLS);
    assert.deepEqual(getStartupPreviewJsonUrlsForRuntime(), STARTUP_PREVIEW_JSON_URLS);
  } finally {
    globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = previousConfig;
  }
});
