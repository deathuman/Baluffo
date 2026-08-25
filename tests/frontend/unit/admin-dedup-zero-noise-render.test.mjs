import test from "node:test";
import assert from "node:assert/strict";

import {
  formatDedupRiskReasonCounts,
  formatDedupReviewQueueCounts,
  formatDedupReviewQueueRows,
  formatDedupAuditGateCard,
  formatNonZeroCounts
} from "../../../frontend/admin/render/ops-summary-dedup.js";

test("non-zero count formatters suppress zero buckets and fall back to none", () => {
  assert.equal(formatDedupRiskReasonCounts({ same_title_company_different_location: 2 }), "location 2");
  assert.equal(formatDedupRiskReasonCounts({}), "none");
  assert.equal(formatNonZeroCounts({ a: 0, b: undefined }), "none");
  assert.doesNotMatch(formatDedupReviewQueueCounts({ monitor: 0 }), /monitor 0/);
});

test("review queue table renders structured evidence details and caps at 10 rows", () => {
  const row = index => ({
    title: `Role ${index}`,
    company: "Studio",
    sourceBundleCount: 2,
    recommendedReviewAction: "needs_review",
    suspectedCause: "category_or_department_bucket",
    identityShape: "many_unique_urls_same_title",
    causeEvidence: ["bucket:engineering"],
    sampleSources: ["a", "b"]
  });
  const html = formatDedupReviewQueueRows(
    Array.from({ length: 12 }, (_, index) => row(index)),
    "empty"
  );

  assert.match(html, /admin-dedup-review-evidence/);
  assert.match(html, /<strong>Suspected cause<\/strong> category or department bucket/);
  assert.match(html, /<strong>Cause evidence<\/strong> bucket:engineering/);
  assert.doesNotMatch(html, /Role 10</);
  assert.match(html, /Role 9</);
});

test("gate card hides zero-count metric chips and keeps the guard flag", () => {
  const html = formatDedupAuditGateCard({
    status: "blocked",
    lifecycleUxReady: false,
    googleSheetsGenericRoleGuardActive: true,
    providerStaticDisagreementCount: 3,
    currentRunMergedCount: 0
  });

  assert.match(html, /Google Sheets guard active/);
  assert.match(html, /provider\/static 3/);
  assert.doesNotMatch(html, /current-run merges 0/);
});
