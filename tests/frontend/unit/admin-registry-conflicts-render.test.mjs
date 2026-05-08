import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

function createReviewElement({
  filterButtons = [],
  reviewFilterButtons = [],
  safeAutomationButtons = [],
  actionButtons = []
} = {}) {
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll(selector) {
      if (selector === ".admin-registry-conflict-filter-btn") return filterButtons;
      if (selector === ".admin-registry-conflict-review-filter-btn") return reviewFilterButtons;
      if (selector === ".admin-registry-conflict-safe-automation-btn") return safeAutomationButtons;
      if (selector === '[data-ui="admin-registry-conflict-action-btn"]') return actionButtons;
      return [];
    }
  };
}

function createButton(dataset = {}) {
  let clickHandler = null;
  return {
    dataset,
    addEventListener(type, handler) {
      if (type === "click") clickHandler = handler;
    },
    click() {
      if (clickHandler) clickHandler();
    }
  };
}

function triagePayload() {
  return {
    summary: { conflictCount: 3 },
    triage: {
      summary: {
        totalConflictCount: 3,
        bucketCounts: {
          exact_duplicate_auto_healable: 0,
          active_active_likely_duplicate: 2,
          pending_duplicate_of_active: 1,
          rejected_historical_noise: 0,
          ambiguous_manual_review: 0
        }
      },
      buckets: [
        {
          bucket: "exact_duplicate_auto_healable",
          label: "Exact duplicate",
          risk: "low",
          description: "Exact duplicates",
          count: 0
        },
        {
          bucket: "active_active_likely_duplicate",
          label: "Active-active",
          risk: "high",
          description: "Active duplicate",
          count: 2
        },
        {
          bucket: "pending_duplicate_of_active",
          label: "Pending duplicate",
          risk: "medium",
          description: "Pending duplicate",
          count: 1
        }
      ]
    },
    review: {
      summary: {
        totalConflictCount: 3,
        priorityCounts: { 0: 1, 1: 1, 2: 1, 3: 0 },
        queueCounts: {
          p0_multi_active_provider: 1,
          p1_active_provider_static: 1,
          p2_pending_static_variant: 1
        }
      },
      queues: [
        {
          queue: "p0_multi_active_provider",
          priority: 0,
          label: "Multiple active providers",
          description: "Multiple active provider rows",
          count: 1
        },
        {
          queue: "p1_active_provider_static",
          priority: 1,
          label: "Active provider + static",
          description: "Active provider static",
          count: 1
        },
        {
          queue: "p2_pending_static_variant",
          priority: 2,
          label: "Pending static variant",
          description: "Pending static variant",
          count: 1
        }
      ]
    },
    conflicts: [
      {
        familyKey: "Active Studio",
        triageBucket: "active_active_likely_duplicate",
        triageLabel: "Active-active",
        triageRisk: "high",
        triageReason: "2 active rows share this source family.",
        reviewPriority: 1,
        reviewQueue: "p1_active_provider_static",
        reviewLabel: "Active provider + static",
        reviewReason: "Active provider rows coexist with active static rows.",
        suggestedDisposition: "Review provider/static replacement",
        suggestedConfidence: "medium",
        evidenceFlags: ["active_rows:2"],
        safeAutomation: {
          eligible: false,
          blockedReasons: ["requires_same_adapter"]
        },
        winner: { name: "Winner", health: "healthy" },
        rows: []
      },
      {
        familyKey: "Pending Studio",
        triageBucket: "pending_duplicate_of_active",
        triageLabel: "Pending duplicate",
        triageRisk: "medium",
        triageReason: "1 pending row matches an active source.",
        reviewPriority: 2,
        reviewQueue: "p2_pending_static_variant",
        reviewLabel: "Pending static variant",
        reviewReason: "A pending static row competes with one active source.",
        suggestedDisposition: "Review pending static duplicate",
        suggestedConfidence: "medium",
        evidenceFlags: ["pending_static_rows:1"],
        safeAutomation: {
          eligible: false,
          blockedReasons: ["requires_active_rows_only"]
        },
        winner: { name: "Pending Winner", health: "unknown" },
        rows: []
      },
      {
        familyKey: "Provider Studio",
        triageBucket: "active_active_likely_duplicate",
        triageLabel: "Active-active",
        triageRisk: "high",
        triageReason: "2 active rows share this source family.",
        reviewPriority: 0,
        reviewQueue: "p0_multi_active_provider",
        reviewLabel: "Multiple active providers",
        reviewReason: "2 active provider rows can duplicate fetches.",
        suggestedDisposition: "Review duplicate active provider sources",
        suggestedConfidence: "high",
        evidenceFlags: ["active_provider_rows:2"],
        safeAutomation: {
          eligible: true,
          action: "auto_demote_same_adapter_provider_alias",
          label: "Auto-demote safe duplicate",
          reason: "Provider Studio has a safe duplicate provider alias.",
          route: "/registry/conflicts/auto-demote-safe",
          targetIds: ["provider-loser"]
        },
        winner: { name: "Pending Winner", health: "unknown" },
        rows: []
      }
    ]
  };
}

test("registry conflicts renderer shows triage summary and card badges", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(reviewEl, triagePayload());

  assert.match(reviewEl.innerHTML, /Triage report/);
  assert.match(reviewEl.innerHTML, /Active-active · 2/);
  assert.match(reviewEl.innerHTML, /Pending duplicate · 1/);
  assert.match(reviewEl.innerHTML, /Active-active · high/);
  assert.match(reviewEl.innerHTML, /2 active rows share this source family/);
});

test("registry conflicts renderer shows review queue counts and suggestions", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(reviewEl, triagePayload());

  assert.match(reviewEl.innerHTML, /Review queue/);
  assert.match(reviewEl.innerHTML, /Multiple active providers · 1/);
  assert.match(reviewEl.innerHTML, /Active provider \+ static · 1/);
  assert.match(reviewEl.innerHTML, /Review provider\/static replacement/);
});

test("registry conflicts renderer locks check controls while adjudication is running", () => {
  const reviewEl = createReviewElement();
  const payload = {
    ...triagePayload(),
    adjudication: {
      status: "running",
      startedAt: "2026-05-08T12:00:00+00:00",
      applyAutopilot: true,
      summary: { recommendedDemotion: 36 },
      demoted: 0
    }
  };

  renderAdminRegistryConflicts(reviewEl, payload);

  assert.match(reviewEl.innerHTML, /Check running/);
  assert.match(reviewEl.innerHTML, /Applying recommendations/);
  assert.match(reviewEl.innerHTML, /Buttons are locked until the bridge reports completion/);
  assert.match(reviewEl.innerHTML, /data-registry-conflict-apply-autopilot="false"[\s\S]*disabled/);
  assert.match(reviewEl.innerHTML, /data-registry-conflict-apply-autopilot="true"[\s\S]*disabled/);
  assert.match(reviewEl.innerHTML, /admin-registry-conflict-safe-automation-btn[\s\S]*disabled/);
});

test("registry conflicts renderer filters cards by triage bucket", () => {
  const filterButton = createButton({
    registryConflictFilterBucket: "active_active_likely_duplicate"
  });
  const reviewEl = createReviewElement({ filterButtons: [filterButton] });

  renderAdminRegistryConflicts(reviewEl, triagePayload());
  filterButton.click();

  assert.match(reviewEl.innerHTML, /Active Studio/);
  assert.doesNotMatch(reviewEl.innerHTML, /Pending Studio/);
  assert.equal(reviewEl.dataset.registryConflictTriageFilter, "active_active_likely_duplicate");
});

test("registry conflicts renderer filters cards by review queue", () => {
  const reviewFilterButton = createButton({
    registryConflictReviewFilterQueue: "p0_multi_active_provider"
  });
  const reviewEl = createReviewElement({ reviewFilterButtons: [reviewFilterButton] });

  renderAdminRegistryConflicts(reviewEl, triagePayload());
  reviewFilterButton.click();

  assert.match(reviewEl.innerHTML, /Provider Studio/);
  assert.doesNotMatch(reviewEl.innerHTML, /Active Studio/);
  assert.doesNotMatch(reviewEl.innerHTML, /Pending Studio/);
  assert.equal(reviewEl.dataset.registryConflictReviewFilter, "p0_multi_active_provider");
});

test("registry conflicts renderer sorts higher-priority review queues first", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(reviewEl, triagePayload());

  assert.ok(reviewEl.innerHTML.indexOf("Provider Studio") < reviewEl.innerHTML.indexOf("Active Studio"));
  assert.ok(reviewEl.innerHTML.indexOf("Active Studio") < reviewEl.innerHTML.indexOf("Pending Studio"));
});

test("registry conflicts renderer collapses lower-priority review groups", () => {
  const reviewEl = createReviewElement();
  const payload = {
    summary: { conflictCount: 1 },
    review: {
      summary: {
        totalConflictCount: 1,
        priorityCounts: { 3: 1 },
        queueCounts: { p3_pending_only_intake: 1 }
      },
      queues: [
        {
          queue: "p3_pending_only_intake",
          priority: 3,
          label: "Pending-only intake",
          description: "Pending-only duplicates",
          count: 1
        }
      ]
    },
    conflicts: [
      {
        familyKey: "Pending Only Studio",
        triageBucket: "ambiguous_manual_review",
        triageLabel: "Manual review",
        triageRisk: "medium",
        triageReason: "Pending-only conflict.",
        reviewPriority: 3,
        reviewQueue: "p3_pending_only_intake",
        reviewLabel: "Pending-only intake",
        reviewReason: "2 pending rows are not active fetch duplication.",
        suggestedDisposition: "Pending-only intake",
        suggestedConfidence: "low",
        winner: { name: "Winner" },
        rows: [
          {
            id: "source-1",
            name: "Winner",
            actions: [{ action: "reject", route: "/registry/reject" }]
          }
        ]
      }
    ]
  };

  renderAdminRegistryConflicts(reviewEl, payload);

  assert.match(
    reviewEl.innerHTML,
    /data-registry-conflict-review-queue="p3_pending_only_intake"\s*>/
  );
  assert.doesNotMatch(
    reviewEl.innerHTML,
    /data-registry-conflict-review-queue="p3_pending_only_intake"[^>]*open/
  );
  assert.match(reviewEl.innerHTML, /admin-registry-conflict-action-btn/);
});

test("registry conflicts renderer preserves existing row action callback", () => {
  const actionButton = createButton({
    registryConflictCardIndex: "0",
    registryConflictRowIndex: "0",
    registryConflictActionIndex: "0"
  });
  const reviewEl = createReviewElement({ actionButtons: [actionButton] });
  const calls = [];
  const payload = {
    summary: { conflictCount: 1 },
    conflicts: [
      {
        familyKey: "Studio",
        triageBucket: "pending_duplicate_of_active",
        triageLabel: "Pending duplicate",
        triageRisk: "medium",
        triageReason: "Pending duplicate.",
        reviewPriority: 2,
        reviewQueue: "p2_pending_static_variant",
        reviewLabel: "Pending static variant",
        reviewReason: "Pending static duplicate.",
        suggestedDisposition: "Review pending static duplicate",
        suggestedConfidence: "medium",
        winner: { name: "Winner" },
        rows: [
          {
            id: "source-1",
            name: "Winner",
            actions: [{ action: "approve", route: "/registry/approve" }]
          }
        ]
      }
    ]
  };

  renderAdminRegistryConflicts(reviewEl, payload, {
    onRegistryConflictAction(row, action, card) {
      calls.push({ row, action, card });
    }
  });
  actionButton.click();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].row.id, "source-1");
  assert.equal(calls[0].action.route, "/registry/approve");
  assert.equal(calls[0].card.familyKey, "Studio");
});
