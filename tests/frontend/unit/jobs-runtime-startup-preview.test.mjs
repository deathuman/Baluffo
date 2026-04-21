import test from "node:test";
import assert from "node:assert/strict";

import { createJobsStartupPreviewController } from "../../../frontend/jobs/app/runtime/startup-preview.js";

test("startup preview controller builds the active-job fast path without dropping filter metadata", () => {
  const runtimeState = {
    filteredJobs: [],
    startupPreviewMaterialize: null,
    startupPreviewMaterializeTimer: null,
    startupPreviewFilteredCount: 0
  };
  const renders = [];
  const controller = createJobsStartupPreviewController({
    runtimeState,
    pageState: { itemsPerPage: 2 },
    displayJobs: (jobs, options = {}) => renders.push({ jobs, options }),
    createFilterOptionsAccumulator: () => ({ ids: [] }),
    addJobToFilterOptions: (accumulator, job) => {
      accumulator.ids.push(job.id);
    },
    finalizeFilterOptions: accumulator => ({ ids: [...accumulator.ids] }),
    compareJobsForSort: (left, right) => right.score - left.score,
    sortJobs: jobs => [...jobs].sort((left, right) => right.score - left.score),
    getJobLocationCities: () => [],
    getJobLocationCountries: () => [],
    isSemanticallyValidLocationValue: () => true,
    isValidCountry: () => true,
    getAvailableRegionOptions: () => [],
    fullCountryName: value => String(value || "")
  });

  const plan = controller.buildStartupPreviewFastPathPlan([
    { id: "job-low", status: "active", score: 2 },
    { id: "job-inactive", status: "inactive", score: 99 },
    { id: "job-high", status: "active", score: 8 },
    { id: "job-mid", status: "active", score: 5 }
  ]);

  assert.deepEqual(plan.filterOptions, { ids: ["job-low", "job-inactive", "job-high", "job-mid"] });
  assert.equal(plan.filteredCount, 3);
  assert.deepEqual(plan.pageJobs.map(job => job.id), ["job-high", "job-mid"]);
  assert.deepEqual(plan.materializeFilteredJobs().map(job => job.id), ["job-high", "job-mid", "job-low"]);

  controller.renderStartupPreviewFastPath(plan);

  assert.deepEqual(runtimeState.filteredJobs.map(job => job.id), ["job-high", "job-mid"]);
  assert.equal(runtimeState.startupPreviewFilteredCount, 3);
  assert.deepEqual(renders, [{
    jobs: plan.pageJobs,
    options: {
      pageJobsOverride: plan.pageJobs,
      totalCountOverride: 3
    }
  }]);
});

test("startup preview controller schedules and materializes deferred jobs", () => {
  const runtimeState = {
    filteredJobs: ["old"],
    startupPreviewMaterialize: null,
    startupPreviewMaterializeTimer: null,
    startupPreviewFilteredCount: 4
  };
  const scheduled = [];
  const cleared = [];
  const renders = [];
  const controller = createJobsStartupPreviewController({
    runtimeState,
    pageState: { itemsPerPage: 5 },
    displayJobs: jobs => renders.push([...jobs]),
    windowObject: {
      setTimeout(callback) {
        scheduled.push(callback);
        return scheduled.length;
      },
      clearTimeout(timerId) {
        cleared.push(timerId);
      }
    },
    createFilterOptionsAccumulator: () => ({}),
    addJobToFilterOptions: () => {},
    finalizeFilterOptions: () => ({}),
    compareJobsForSort: () => 0,
    sortJobs: jobs => [...jobs],
    getJobLocationCities: () => [],
    getJobLocationCountries: () => [],
    isSemanticallyValidLocationValue: () => true,
    isValidCountry: () => true,
    getAvailableRegionOptions: () => [],
    fullCountryName: value => String(value || "")
  });

  controller.scheduleStartupPreviewMaterialization(() => ["job-1", "job-2"]);
  assert.equal(typeof runtimeState.startupPreviewMaterialize, "function");
  assert.equal(runtimeState.startupPreviewMaterializeTimer, 1);

  controller.materializePendingStartupPreview({ render: true });

  assert.deepEqual(runtimeState.filteredJobs, ["job-1", "job-2"]);
  assert.equal(runtimeState.startupPreviewMaterialize, null);
  assert.equal(runtimeState.startupPreviewMaterializeTimer, null);
  assert.equal(runtimeState.startupPreviewFilteredCount, 0);
  assert.deepEqual(cleared, [1]);
  assert.deepEqual(renders, [["job-1", "job-2"]]);
});
