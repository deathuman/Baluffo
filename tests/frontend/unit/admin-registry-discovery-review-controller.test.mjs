import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  createRegistryControllerFixture
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller renders discovery review outside the source summary", async () => {
  const fixture = createRegistryControllerFixture({
    options: {
      getBridge: async path => {
        if (path === "/discovery/report") {
          return {
            summary: {
              foundEndpointCount: 1,
              queuedCandidateCount: 1
            },
            candidateReview: {
              totalCandidates: 1,
              recommendationCounts: { promote_candidate: 1 },
              topCandidates: [
                {
                  name: "Review Studio",
                  adapter: "greenhouse",
                  jobsFound: 4,
                  rankScore: 90,
                  promotionRecommendation: "promote_candidate"
                }
              ]
            }
          };
        }
        if (path === "/discovery/candidates") return { candidates: [] };
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            sources: { pending: [], active: [], rejected: [] },
            summary: { pendingCount: 0, activeCount: 0, rejectedCount: 0, hiddenPendingCount: 0 }
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  await controller.loadDiscoveryData();

  assert.match(fixture.refs.adminDiscoverySummaryEl.textContent, /Found 1/);
  assert.doesNotMatch(fixture.refs.adminDiscoverySummaryEl.innerHTML, /Discovery Review Quality/);
  assert.match(fixture.refs.adminDiscoveryReviewEl.innerHTML, /Discovery Review Quality/);
  assert.match(fixture.refs.adminDiscoveryReviewEl.innerHTML, /Review Studio/);
  assert.match(fixture.refs.adminDiscoveryReviewEl.innerHTML, /admin-source-review-lane-details/);
});
