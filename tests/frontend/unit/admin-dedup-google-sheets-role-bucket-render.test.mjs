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

test("admin render: Google Sheets role-bucket audit summary stays read-only", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      dedupEvidence: {
        googleSheetsRoleBucketAudit: {
          totalRoleBucketCount: 8,
          currentRunRoleBucketCount: 4,
          carriedHistoricalRoleBucketCount: 2,
          blockedByDifferentPrimaryUrlCount: 1,
          allowedSamePrimaryUrlCount: 1,
          likelyHistoricalCollisionCount: 2,
          likelyParserCategoryBucketCount: 3,
          unresolvedRoleBucketCount: 4,
          classificationCounts: {
            fixed_by_generic_role_guard: 1,
            allowed_same_primary_url: 1,
            historical_carried_bundle: 2,
            unresolved_current_run_role_bucket: 1,
            parser_or_sheet_category_noise: 2,
            needs_narrow_dedup_guard: 1
          },
          examples: [
            {
              classification: "fixed_by_generic_role_guard",
              title: "Product-management",
              company: "eBay",
              bundleEvidenceOrigin: "current_run",
              evidence: ["different_concrete_primary_urls"]
            }
          ]
        },
        dedupAuditGate: {
          status: "blocked",
          lifecycleUxReady: false,
          googleSheetsGenericRoleGuardActive: true,
          googleSheetsRoleBucketUnresolvedCount: 4,
          googleSheetsRoleBucketGuardBlockedCount: 1,
          googleSheetsRoleBucketHistoricalCount: 2,
          blockers: ["high_risk_review_queue_causes_need_review"],
          warnings: [],
          examples: []
        }
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup Google Sheets role-bucket audit summary/i);
  assert.match(metricsEl.innerHTML, /Sheets role unresolved 4/i);
  assert.match(metricsEl.innerHTML, /Sheets guard-blocked 1/i);
  assert.match(metricsEl.innerHTML, /Sheets historical 2/i);
  assert.match(metricsEl.innerHTML, /guard-blocked different URL 1/i);
  assert.match(metricsEl.innerHTML, /allowed same URL 1/i);
  assert.match(metricsEl.innerHTML, /parser\/category 3/i);
  assert.match(metricsEl.innerHTML, /unresolved 4/i);
  assert.match(metricsEl.innerHTML, /fixed by guard 1/i);
  assert.match(metricsEl.innerHTML, /needs narrow guard 1/i);
  assert.match(metricsEl.innerHTML, /fixed by generic role guard/i);
  assert.match(metricsEl.innerHTML, /different concrete primary urls/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});
