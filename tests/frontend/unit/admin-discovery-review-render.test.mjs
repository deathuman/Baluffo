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
    likelyRejectCandidates: [{ name: "Noise", adapter: "static", jobsFound: 0 }],
    providerMigration: {
      totalCandidates: 3,
      providerMigrationCandidates: [
        {
          name: "Static Provider",
          currentAdapter: "static",
          detectedProviderFamily: "greenhouse",
          migrationConfidence: 85,
          recommendedAction: "add_provider_source"
        }
      ],
      stagedProviderCandidates: [
        {
          name: "Staged Provider",
          currentAdapter: "greenhouse",
          detectedProviderFamily: "greenhouse",
          migrationConfidence: 90,
          recommendedAction: "add_provider_source"
        }
      ],
      alreadyCoveredByProvider: [
        {
          name: "Covered Static",
          currentAdapter: "static",
          detectedProviderFamily: "lever",
          existingProviderSourceState: "active",
          migrationConfidence: 95,
          recommendedAction: "already_covered_by_provider"
        }
      ],
      addProviderSourceCandidates: [],
      unsupportedProviderCandidates: [
        {
          name: "Unsupported ATS",
          currentAdapter: "static",
          detectedProviderFamily: "jobvite",
          migrationConfidence: 45,
          recommendedAction: "unsupported_provider"
        }
      ],
      needsProbeCandidates: [],
      keepStaticOrInsufficientEvidence: []
    }
  });

  assert.match(html, /Discovery Review Quality/);
  assert.match(html, /Provider Migration Advisory/);
  assert.match(html, /Staged provider candidates/);
  assert.match(html, /Already covered by provider/);
  assert.match(html, /Unsupported provider candidates/);
  assert.match(html, /Provider-backed/);
  assert.match(html, /Needs browser probe/);
  assert.match(html, /Likely reject\/noise/);
  assert.match(html, /Live Studio/);
  assert.match(html, /Static Provider/);
  assert.match(html, /Staged Provider/);
  assert.match(html, /HTTP 403/);
});
