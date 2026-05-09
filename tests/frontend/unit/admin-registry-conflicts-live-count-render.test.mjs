import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

function createReviewElement() {
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll() {
      return [];
    }
  };
}

test("registry conflicts renderer separates live and registry job counts", () => {
  const reviewEl = createReviewElement();
  const payload = {
    summary: { conflictCount: 1 },
    conflicts: [
      {
        familyKey: "Azra Games",
        triageBucket: "active_active_likely_duplicate",
        triageLabel: "Active-active",
        triageRisk: "high",
        triageReason: "2 active rows share this source family.",
        reviewPriority: 0,
        reviewQueue: "p0_multi_active_provider",
        reviewLabel: "Multiple active providers",
        reviewReason: "Provider/static replacement.",
        suggestedDisposition: "Review duplicate active provider sources",
        suggestedConfidence: "high",
        effectiveWinnerSource: "live_adjudication",
        winner: { id: "greenhouse:slug:azragames", name: "Azra Games (Greenhouse)" },
        rows: [
          {
            id: "greenhouse:slug:azragames",
            name: "Azra Games (Greenhouse)",
            jobsFound: 1,
            liveJobsFound: 1,
            registryJobsFound: 1
          },
          {
            id: "static:listing_url:https://azragames.com/careers/",
            name: "Azra Games (GameDevMap)",
            jobsFound: 0,
            liveJobsFound: 0,
            registryJobsFound: 5
          }
        ]
      }
    ]
  };

  renderAdminRegistryConflicts(reviewEl, payload);

  assert.match(reviewEl.innerHTML, /Live counts applied/);
  assert.match(reviewEl.innerHTML, /Effective jobs found<\/strong> 0/);
  assert.match(reviewEl.innerHTML, /Registry jobs found<\/strong> 5/);
  assert.match(reviewEl.innerHTML, /Live jobs found<\/strong> 0/);
});
