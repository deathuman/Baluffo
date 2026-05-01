import test from "node:test";
import assert from "node:assert/strict";

import {
  filterSourcePolicyReviewPairs,
  getSourcePolicyReviewActions,
  renderAdminSourcePolicyReview
} from "../../../frontend/admin/render/source-policy-review.js";
import { UI_TOKENS, ui } from "../../../frontend/shared/ui/selectors.js";

function makePair(overrides = {}) {
  return {
    staticSourceId: "static:listing_url:https://studio.example/jobs",
    staticSourceName: "static_source::studio",
    providerSourceId: "provider:greenhouse:studio",
    providerSourceName: "Studio Greenhouse",
    currentRecommendation: "stable_safe_redundant",
    currentRecommendedAction: "keep_runtime_suppression",
    confidence: 0.83,
    reviewState: "new",
    manualSuppressionOverride: "none",
    snoozedUntil: "",
    safeRunCount: 4,
    consecutiveSafeRunCount: 3,
    staticOnlyDetectedRunCount: 0,
    providerUnstableRunCount: 0,
    lastProposal: "safe_redundant_static",
    lastAuditStatus: "safe",
    ...overrides
  };
}

function makeEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    dataset: {},
    querySelectorAll(selector) {
      return buttonsBySelector[selector] || [];
    }
  };
}

function makeButton(dataset) {
  return {
    dataset,
    addEventListener(_event, handler) {
      this.click = handler;
    }
  };
}

test("admin source policy review renders required row fields and non-destructive copy", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    recommendations: {
      pairs: [makePair()]
    }
  });

  assert.match(reviewEl.innerHTML, /Recommendations are advisory/i);
  assert.match(reviewEl.innerHTML, /Review state is local to this machine/i);
  assert.match(reviewEl.innerHTML, /included in explicit backups/i);
  assert.match(reviewEl.innerHTML, /not source-synced/i);
  assert.match(reviewEl.innerHTML, /Force pause is reversible and conservative/i);
  assert.match(reviewEl.innerHTML, /No action deletes or hides sources/i);
  assert.match(reviewEl.innerHTML, /static_source::studio/);
  assert.match(reviewEl.innerHTML, /static:listing_url:https:\/\/studio\.example\/jobs/);
  assert.match(reviewEl.innerHTML, /Studio Greenhouse/);
  assert.match(reviewEl.innerHTML, /provider:greenhouse:studio/);
  assert.match(reviewEl.innerHTML, /stable safe redundant/);
  assert.match(reviewEl.innerHTML, /keep runtime suppression/);
  assert.match(reviewEl.innerHTML, /83%/);
  assert.match(reviewEl.innerHTML, /Safe runs/);
  assert.match(reviewEl.innerHTML, /Safe streak/);
  assert.match(reviewEl.innerHTML, /Static-only runs/);
  assert.match(reviewEl.innerHTML, /Provider unstable runs/);
  assert.match(reviewEl.innerHTML, /safe redundant static/);
  assert.match(reviewEl.innerHTML, /Last audit/);
});

test("admin source policy review filters use deterministic row sets", () => {
  const stableSafe = makePair({ staticSourceId: "static:safe", currentRecommendation: "stable_safe_redundant" });
  const staticOnly = makePair({
    staticSourceId: "static:only",
    currentRecommendation: "static_only_detected",
    staticOnlyDetectedRunCount: 1,
    reviewState: "acknowledged"
  });
  const providerUnstable = makePair({
    staticSourceId: "static:unstable",
    currentRecommendation: "needs_review",
    providerUnstableRunCount: 1,
    lastProposal: "provider_unstable",
    reviewState: "reviewed"
  });
  const forcePaused = makePair({
    staticSourceId: "static:force",
    manualSuppressionOverride: "force_pause",
    currentRecommendation: "needs_more_history",
    reviewState: "snoozed"
  });
  const rows = [stableSafe, staticOnly, providerUnstable, forcePaused];

  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "all"), rows);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "needs_action").map(row => row.staticSourceId), ["static:safe", "static:only"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "stable_safe").map(row => row.staticSourceId), ["static:safe"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "static_only_detected").map(row => row.staticSourceId), ["static:only"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "provider_unstable").map(row => row.staticSourceId), ["static:unstable"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "force_paused").map(row => row.staticSourceId), ["static:force"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "snoozed").map(row => row.staticSourceId), ["static:force"]);
  assert.deepEqual(filterSourcePolicyReviewPairs(rows, "reviewed").map(row => row.staticSourceId), ["static:unstable"]);
});

test("admin source policy review action visibility follows review state rules", () => {
  assert.deepEqual(
    getSourcePolicyReviewActions(makePair({ reviewState: "new", manualSuppressionOverride: "none" })).map(action => action.key),
    ["acknowledge", "reviewed", "snooze", "force_pause"]
  );
  assert.deepEqual(
    getSourcePolicyReviewActions(makePair({ reviewState: "acknowledged", manualSuppressionOverride: "none" })).map(action => action.key),
    ["reviewed", "snooze", "force_pause"]
  );
  assert.deepEqual(
    getSourcePolicyReviewActions(makePair({ reviewState: "reviewed", manualSuppressionOverride: "none" })).map(action => action.key),
    ["snooze", "force_pause"]
  );
  assert.deepEqual(
    getSourcePolicyReviewActions(makePair({ reviewState: "snoozed", manualSuppressionOverride: "force_pause" })).map(action => action.key),
    ["acknowledge", "clear_override"]
  );
});

test("admin source policy review never renders force suppress or destructive actions", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    recommendations: {
      pairs: [
        makePair({ manualSuppressionOverride: "force_pause" }),
        makePair({ currentRecommendation: "static_only_detected", staticOnlyDetectedRunCount: 1 })
      ]
    }
  });

  assert.doesNotMatch(reviewEl.innerHTML, /force_suppress/i);
  assert.doesNotMatch(reviewEl.innerHTML, />\s*delete\s*</i);
  assert.doesNotMatch(reviewEl.innerHTML, />\s*hide\s*</i);
  assert.doesNotMatch(reviewEl.innerHTML, />\s*reject\s*</i);
  assert.doesNotMatch(reviewEl.innerHTML, />\s*demote\s*</i);
  assert.doesNotMatch(reviewEl.innerHTML, />\s*tombstone\s*</i);
});

test("admin source policy review buttons call filter and action handlers", () => {
  const calls = [];
  const filterButton = makeButton({ sourcePolicyFilter: "provider_unstable" });
  const actionButton = makeButton({
    sourcePolicyAction: "clear_override",
    sourcePolicyIndex: "0"
  });
  const reviewEl = makeEl({
    [ui(UI_TOKENS.admin.sourcePolicyFilterBtn)]: [filterButton],
    [ui(UI_TOKENS.admin.sourcePolicyActionBtn)]: [actionButton]
  });
  renderAdminSourcePolicyReview(reviewEl, {
    recommendations: {
      pairs: [makePair({ manualSuppressionOverride: "force_pause" })]
    }
  }, {
    onSourcePolicyFilter(filter) {
      calls.push({ filter });
    },
    onSourcePolicyAction(row, action) {
      calls.push({ action, staticSourceId: row.staticSourceId });
    }
  });

  filterButton.click();
  actionButton.click();

  assert.deepEqual(calls, [
    { filter: "provider_unstable" },
    { action: "clear_override", staticSourceId: "static:listing_url:https://studio.example/jobs" }
  ]);
});
