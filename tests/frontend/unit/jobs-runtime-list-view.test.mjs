import test from "node:test";
import assert from "node:assert/strict";

import { displayJobs } from "../../../frontend/jobs/app/runtime/list-view.js";

function createElement() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("jobs list view uses the jobs length when no totalCountOverride is provided", () => {
  const jobsList = createElement();
  const pagination = createElement();
  const resultsSummary = createElement();
  const metrics = [];
  const previousWindow = globalThis.window;
  globalThis.window = {
    requestAnimationFrame: callback => {
      if (typeof callback === "function") callback();
      return 1;
    }
  };

  try {
    displayJobs(
      [
        {
          id: "1",
          title: "Gameplay Engineer",
          company: "Studio",
          country: "NL"
        }
      ],
      {
        jobsList,
        pagination,
        resultsSummary,
        state: { currentPage: 1, itemsPerPage: 10 },
        allJobs: [{ id: "1" }],
        currentUser: null,
        seenJobKeys: new Set(),
        savedJobKeys: new Set(),
        isJobsApiReady: () => true,
        getJobKeyForJob: job => String(job.id || ""),
        fullCountryName: value => String(value || ""),
        goToPage: () => {},
        emitDesktopStartupMetric: (event, payload = {}) => metrics.push({ event, payload }),
        renderJobRowHtml: job => `<article>${job.title}</article>`
      }
    );
  } finally {
    globalThis.window = previousWindow;
  }

  assert.match(jobsList.innerHTML, /Gameplay Engineer/);
  assert.equal(
    metrics.find(entry => entry.event === "jobs_display_start")?.payload?.totalCount,
    1
  );
  assert.match(resultsSummary.textContent, /Showing 1-1 of 1 jobs/);
});
