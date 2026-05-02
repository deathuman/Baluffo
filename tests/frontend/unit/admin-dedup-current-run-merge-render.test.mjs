import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: fetcher metrics render current-run known mirror merge examples", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 1000,
      sourceCount: 1,
      outputCount: 1,
      mergedCount: 1,
      dedupEvidence: {
        mergeReasonCounts: {
          primaryUrl: 0,
          secondaryKey: 0,
          knownMirrorPair: 1,
          socialKey: 0,
          sparseIdentity: 0,
          unknown: 0
        },
        dedupAuditGate: {
          status: "pass",
          lifecycleUxReady: true,
          currentRunMergedCount: 1,
          currentRunSourceBundleCollisionCount: 0,
          carriedSourceBundleCollisionCount: 0,
          highRiskReviewQueueCount: 0,
          currentRunHighRiskReviewQueueCount: 0,
          carriedHighRiskReviewQueueCount: 0,
          providerStaticDisagreementCount: 0,
          providerStaticDisagreementCurrentRunCount: 0,
          providerStaticDisagreementCarriedCount: 0,
          googleSheetsGenericRoleGuardActive: true,
          blockers: [],
          warnings: [],
          examples: []
        },
        currentRunMergeExamples: [
          {
            title: "Senior Foundational Tools Programmer",
            company: "Guerrilla Games",
            incomingSource:
              "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-8608_guerrilla-games?page=1",
            mergeReason: "known_mirror_pair",
            recommendedReviewAction: "monitor",
            nonBlockingReason: "known_gracklehq_gamesjobsdirect_mirror_pair",
            blocksLifecycle: false,
            bundleEvidenceOrigin: "current_run"
          }
        ]
      }
    }
  });

  assert.match(metricsEl.innerHTML, /known mirror pair 1/i);
  assert.match(metricsEl.innerHTML, /Dedup current-run merge examples/i);
  assert.match(metricsEl.innerHTML, /Senior Foundational Tools Programmer @ Guerrilla Games/i);
  assert.match(metricsEl.innerHTML, /known gracklehq gamesjobsdirect mirror pair/i);
});
