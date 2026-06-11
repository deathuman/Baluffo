import test from "node:test";
import assert from "node:assert/strict";

import { createJobsFeedController } from "../../../frontend/jobs/app/runtime/feed-controller.js";
import { createElement } from "./helpers/jobs-runtime-helpers.mjs";

function createFeedController(overrides = {}) {
  const dispatches = [];
  const perfCalls = [];
  const runtimeState = overrides.runtimeState || {
    refreshInFlight: false,
    allJobs: [],
    skipInitialGuestAuthRerender: false
  };
  const dom = overrides.dom || {
    refreshJobsBtn: createElement(),
    refreshJobsNeededBadgeEl: createElement({ classList: createElement().classList }),
    fetchProgress: createElement(),
    sourceStatus: createElement(),
    jobsLastUpdatedEl: createElement(),
    dataSourcesListEl: createElement(),
    dataSourcesCaptionEl: createElement()
  };
  return {
    dispatches,
    perfCalls,
    runtimeState,
    dom,
    controller: createJobsFeedController({
      dom,
      runtimeState,
      pageState: { itemsPerPage: 10, filters: {} },
      defaultFilters: {},
      professionLabels: {},
      sanitizeUrl: value => value,
      jobsParsing: {},
      startupPreviewJsonUrls: ["startup.json"],
      jobsDispatch: {
        dispatch(action) {
          dispatches.push(action);
        }
      },
      jobsActions: {
        REFRESH_REQUESTED: "REFRESH_REQUESTED",
        REFRESH_FAILED: "REFRESH_FAILED",
        REFRESH_COMPLETED: "REFRESH_COMPLETED"
      },
      filtersController: {
        updateFilterOptions: () => {},
        applyStateToFilters: () => {}
      },
      showToast: () => {},
      emitDesktopStartupMetric: () => {},
      markJobsStep: (name, payload = {}) => perfCalls.push({ type: "mark", name, payload }),
      measureJobsStep: (name, startMark, endMark, payload = {}) =>
        perfCalls.push({ type: "measure", name, startMark, endMark, payload }),
      markStartupRendered: () => {},
      markJobsFirstInteractive: () => {},
      applyFiltersAndRender: () => {},
      isDesktopRuntimeMode: () => false,
      logJobsError: () => {},
      logJobsInfo: () => {},
      getJobsLastUpdatedText: timestamp => `updated:${timestamp}`,
      normalizeJobs: rows => rows.map(row => ({ ...row, normalized: true })),
      parseUnifiedJobsPayload: payload => payload,
      openJobsCacheDbFromModule: options => options,
      readJobsCache: async options => options,
      writeJobsCache: async (jobs, options) => ({ jobs, options }),
      refreshJobsFeed: async (_request, deps) => {
        deps.setRefreshButtonDisabled(true);
        deps.setProgress(true);
        deps.setSourceStatus("Refreshing feed");
        deps.dispatchRefreshRequested();
        deps.setRefreshJobsNeedsAttention(true);
        deps.dispatchRefreshCompleted();
        return true;
      },
      loadStartupPreviewJobsFeed: async deps => {
        deps.markJobsStep("jobs_forwarded_preview_start");
        deps.measureJobsStep(
          "jobs_forwarded_preview",
          "jobs_forwarded_preview_start",
          "jobs_forwarded_preview_done"
        );
        const normalized = deps.normalizeRows([{ id: "job-1" }]);
        deps.renderStartupPreviewFastPath({ pageJobs: normalized, filteredCount: 1 });
        return true;
      },
      fetchUnifiedJobsFromSources: async options => options,
      fetchJsonFromCandidatesFromSources: async (urls, options) => ({ urls, options }),
      renderDataSourcesFromSources: async refs => refs,
      mapProfession: value => value,
      normalizeSector: value => value,
      classifyCompanyType: value => value,
      detectWorkType: value => value,
      setProgressVisibility: (_setText, element, visible) => {
        element.visible = visible;
      },
      setStatusText: (_setText, element, text) => {
        element.textContent = text;
      },
      setText: (element, text) => {
        element.textContent = text;
      },
      jobsCacheDb: "jobs-cache",
      jobsCacheDbVersion: 2,
      jobsCacheStore: "jobs-feed",
      jobsSeenStore: "jobs-seen",
      jobsCacheKey: "latest",
      jobsFirstLoadRequestTimeoutMs: 4500,
      windowObject: { indexedDB: { name: "db" } },
      now: () => 1234,
      nowIso: () => "2026-04-21T12:00:00.000Z",
      recalculateItemsPerPage: () => 12,
      startupPreviewController: {
        buildStartupPreviewFastPathPlan: jobs => ({ pageJobs: jobs, filteredCount: jobs.length }),
        renderStartupPreviewFastPath: plan => {
          runtimeState.previewPlan = plan;
        },
        scheduleStartupPreviewMaterialization: materialize => {
          runtimeState.materialize = materialize;
        }
      },
      ...overrides
    })
  };
}

test("jobs feed controller refresh wiring updates bridge state and dispatches completion", async () => {
  const { controller, dom, dispatches } = createFeedController();

  const ok = await controller.refreshJobsNow({ manual: true, firstLoad: true });

  assert.equal(ok, true);
  assert.equal(dom.refreshJobsBtn.disabled, true);
  assert.equal(dom.fetchProgress.visible, true);
  assert.equal(dom.sourceStatus.textContent, "Refreshing feed");
  assert.equal(dom.refreshJobsBtn.classList.contains("needs-refresh"), true);
  assert.equal(dom.refreshJobsBtn.getAttribute("aria-live"), "polite");
  assert.equal(dom.refreshJobsNeededBadgeEl.classList.contains("hidden"), false);
  assert.deepEqual(dispatches, [
    { type: "REFRESH_REQUESTED" },
    {
      type: "REFRESH_COMPLETED",
      payload: { finishedAt: "2026-04-21T12:00:00.000Z" }
    }
  ]);
});

test("jobs feed controller source metadata load is lazy and deduped", async () => {
  let resolveLoad;
  const calls = [];
  const loadPromise = new Promise(resolve => {
    resolveLoad = resolve;
  });
  const { controller, dom, runtimeState } = createFeedController({
    renderDataSourcesFromSources: async refs => {
      calls.push(refs);
      return loadPromise;
    }
  });

  const first = controller.renderDataSources();
  const second = controller.renderDataSources();

  assert.equal(calls.length, 1);
  assert.equal(dom.dataSourcesCaptionEl.textContent, "Loading source metadata...");

  resolveLoad({ ok: true });
  await Promise.all([first, second]);

  assert.equal(runtimeState.dataSourcesLoaded, true);
  await controller.renderDataSources();
  assert.equal(calls.length, 1);
});

test("jobs feed controller preview wiring normalizes rows and forwards startup preview hooks", async () => {
  const { controller, runtimeState, dom, perfCalls } = createFeedController();

  const loaded = await controller.loadStartupPreviewJobs();
  controller.updateLastUpdatedText(1234);

  assert.equal(loaded, true);
  assert.deepEqual(runtimeState.allJobs, [{ id: "job-1", normalized: true }]);
  assert.deepEqual(runtimeState.previewPlan, {
    pageJobs: [{ id: "job-1", normalized: true }],
    filteredCount: 1
  });
  assert.equal(dom.jobsLastUpdatedEl.textContent, "updated:1234");
  assert.deepEqual(perfCalls.map(item => `${item.type}:${item.name}`), [
    "mark:jobs_forwarded_preview_start",
    "measure:jobs_forwarded_preview"
  ]);
});

test("jobs feed controller forwards allowSheetsFallback to source fetcher", async () => {
  let forwarded = null;
  const { controller } = createFeedController({
    refreshJobsFeed: async (_request, deps) => {
      forwarded = await deps.fetchUnifiedJobs({
        timeoutMs: 100,
        allowSheetsFallback: false
      });
      return true;
    },
    fetchUnifiedJobsFromSources: async options => options
  });

  await controller.refreshJobsNow({ manual: false, firstLoad: true });

  assert.equal(forwarded.allowSheetsFallback, false);
  assert.equal(forwarded.timeoutMs, 100);
});
