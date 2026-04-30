import test from "node:test";
import assert from "node:assert/strict";
import { renderDiscoveryCandidateReviewHtml } from "../../../frontend/admin/render.js";

test("admin render: discovery candidate review panel shows review lanes", () => {
  const html = renderDiscoveryCandidateReviewHtml({
    totalCandidates: 3,
    recommendationCounts: {
      promote_candidate: 1,
      needs_browser_probe: 1,
      duplicate_candidate: 1
    },
    topCandidates: [
      {
        name: "Live Studio",
        adapter: "greenhouse",
        jobsFound: 5,
        rankScore: 90,
        promotionRecommendation: "promote_candidate"
      }
    ],
    providerBackedCandidates: [
      { name: "Live Studio", adapter: "greenhouse", providerFamily: "greenhouse" }
    ],
    candidatesWithJobs: [{ name: "Live Studio", adapter: "greenhouse", jobsFound: 5 }],
    duplicateCandidates: [{ name: "Duplicate", adapter: "lever", jobsFound: 2 }],
    hiddenOrDeferredCandidates: [{ name: "Deferred", adapter: "static", jobsFound: 0 }],
    needsBrowserProbeCandidates: [
      { name: "Blocked", adapter: "static", lastProbeError: "HTTP 403" }
    ],
    likelyRejectCandidates: [{ name: "Noise", adapter: "static", jobsFound: 0 }]
  });

  assert.match(html, /Discovery Review Quality/);
  assert.match(html, /Provider-backed/);
  assert.match(html, /Needs browser probe/);
  assert.match(html, /Likely reject\/noise/);
  assert.match(html, /Live Studio/);
  assert.match(html, /HTTP 403/);
});
