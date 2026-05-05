import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

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

function createReviewElement(safeAutomationButtons = []) {
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll(selector) {
      if (selector === ".admin-registry-conflict-safe-automation-btn") {
        return safeAutomationButtons;
      }
      return [];
    }
  };
}

function payloadWithSafeCard(safeAutomation) {
  return {
    summary: { conflictCount: 1 },
    conflicts: [
      {
        familyKey: "Provider Studio",
        triageBucket: "active_active_likely_duplicate",
        triageLabel: "Active-active",
        triageRisk: "high",
        triageReason: "2 active rows.",
        reviewPriority: 0,
        reviewQueue: "p0_multi_active_provider",
        reviewLabel: "Multiple active providers",
        reviewReason: "2 active provider rows can duplicate fetches.",
        suggestedDisposition: "Review duplicate active provider sources",
        suggestedConfidence: "high",
        safeAutomation,
        winner: { name: "Winner" },
        rows: []
      }
    ]
  };
}

test("registry conflicts renderer shows safe automation controls for eligible cards", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(
    reviewEl,
    payloadWithSafeCard({
      eligible: true,
      action: "auto_demote_same_adapter_provider_alias",
      label: "Auto-demote safe duplicate",
      reason: "Provider Studio has a safe duplicate provider alias.",
      route: "/registry/conflicts/auto-demote-safe",
      targetIds: ["provider-loser"]
    })
  );

  assert.match(reviewEl.innerHTML, /Safe automation available/);
  assert.match(reviewEl.innerHTML, /Auto-demote safe duplicate/);
  assert.match(reviewEl.innerHTML, /Auto-demote safe duplicate · 1/);
  assert.match(reviewEl.innerHTML, /Provider Studio has a safe duplicate provider alias/);
});

test("registry conflicts renderer calls safe automation callback", () => {
  const safeButton = createButton({
    registryConflictSafeAutomationCardIndex: "0",
    registryConflictSafeAutomationAction: "auto_demote_same_adapter_provider_alias",
    registryConflictSafeAutomationIds: "provider-loser"
  });
  const reviewEl = createReviewElement([safeButton]);
  const calls = [];

  renderAdminRegistryConflicts(reviewEl, payloadWithSafeCard({
    eligible: true,
    route: "/registry/conflicts/auto-demote-safe",
    targetIds: ["provider-loser"]
  }), {
    onRegistryConflictSafeAutomation(safeAutomation, card) {
      calls.push({ safeAutomation, card });
    }
  });
  safeButton.click();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].safeAutomation.route, "/registry/conflicts/auto-demote-safe");
  assert.deepEqual(calls[0].safeAutomation.targetIds, ["provider-loser"]);
  assert.equal(calls[0].card.familyKey, "Provider Studio");
});

test("registry conflicts renderer keeps toolbar safe automation action specific", () => {
  const safeButton = createButton({
    registryConflictSafeAutomationCardIndex: "-1",
    registryConflictSafeAutomationAction: "auto_demote_static_normalized_url_alias",
    registryConflictSafeAutomationRoute: "/registry/conflicts/auto-demote-safe",
    registryConflictSafeAutomationIds: "static-loser"
  });
  const reviewEl = createReviewElement([safeButton]);
  const calls = [];

  renderAdminRegistryConflicts(reviewEl, payloadWithSafeCard({
    eligible: true,
    action: "auto_demote_static_normalized_url_alias",
    label: "Auto-demote static URL alias",
    route: "/registry/conflicts/auto-demote-safe",
    targetIds: ["static-loser"]
  }), {
    onRegistryConflictSafeAutomation(safeAutomation, card) {
      calls.push({ safeAutomation, card });
    }
  });
  safeButton.click();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].safeAutomation.action, "auto_demote_static_normalized_url_alias");
  assert.deepEqual(calls[0].safeAutomation.targetIds, ["static-loser"]);
  assert.equal(calls[0].card, null);
});

test("registry conflicts renderer omits safe automation button for ineligible cards", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(
    reviewEl,
    payloadWithSafeCard({
      eligible: false,
      blockedReasons: ["loser_has_positive_evidence"]
    })
  );

  assert.doesNotMatch(reviewEl.innerHTML, /Safe automation available/);
  assert.doesNotMatch(reviewEl.innerHTML, /Apply safe demotions/);
});
