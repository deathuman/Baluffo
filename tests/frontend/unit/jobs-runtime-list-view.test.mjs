import test from "node:test";
import assert from "node:assert/strict";

import { displayJobs } from "../../../frontend/jobs/app/runtime/list-view.js";
import { createElement } from "./helpers/jobs-runtime-helpers.mjs";

function renderJobsList(jobs, options = {}) {
  const jobsList = createElement();
  const pagination = createElement();
  const resultsSummary = createElement();
  const metrics = [];

  displayJobs(
    jobs,
    {
      jobsList,
      pagination,
      resultsSummary,
      state: { currentPage: 1, itemsPerPage: 10 },
      allJobs: options.allJobs || jobs,
      currentUser: null,
      seenJobKeys: new Set(),
      savedJobKeys: new Set(),
      isJobsApiReady: () => true,
      getJobKeyForJob: job => String(job.id || ""),
      fullCountryName: value => String(value || ""),
      goToPage: () => {},
      emitDesktopStartupMetric: (event, payload = {}) => metrics.push({ event, payload }),
      renderJobRowHtml: job => `<article>${job.title}</article>`
    },
    options.displayOptions || {}
  );

  return { jobsList, pagination, resultsSummary, metrics };
}

test("jobs list view uses the jobs length when no totalCountOverride is provided", () => {
  const previousWindow = globalThis.window;
  globalThis.window = {
    requestAnimationFrame: callback => {
      if (typeof callback === "function") callback();
      return 1;
    }
  };

  try {
    const result = renderJobsList(
      [
        {
          id: "1",
          title: "Gameplay Engineer",
          company: "Studio",
          country: "NL"
        }
      ]
    );
    assert.match(result.jobsList.innerHTML, /Gameplay Engineer/);
    assert.equal(
      result.metrics.find(entry => entry.event === "jobs_display_start")?.payload?.totalCount,
      1
    );
    assert.match(result.resultsSummary.textContent, /Showing 1-1 of 1 jobs/);
  } finally {
    globalThis.window = previousWindow;
  }
});

test("jobs list view keeps the generic empty message for filter misses", () => {
  const { jobsList, resultsSummary } = renderJobsList([], {
    allJobs: [{ id: "1" }]
  });

  assert.match(jobsList.innerHTML, /No jobs found matching your filters\./);
  assert.doesNotMatch(jobsList.innerHTML, /Preparing first-run jobs/);
  assert.equal(resultsSummary.textContent, "Showing 0 jobs (1 loaded)");
});

test("jobs list view explains the expected first-run empty state", () => {
  const { jobsList, resultsSummary } = renderJobsList([], {
    displayOptions: { emptyStateReason: "first_run_bootstrap" }
  });

  assert.match(jobsList.innerHTML, /Preparing first-run jobs\./);
  assert.match(jobsList.innerHTML, /starter Google Sheets feed/);
  assert.match(jobsList.innerHTML, /several minutes/);
  assert.doesNotMatch(jobsList.innerHTML, /No jobs found matching your filters/);
  assert.equal(resultsSummary.textContent, "0 jobs");
});
