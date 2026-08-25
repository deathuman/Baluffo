import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

function createReviewElement({ searchInputs = [], loadMoreButtons = [] } = {}) {
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll(selector) {
      if (selector === ".admin-registry-conflict-search-input") return searchInputs;
      if (selector === ".admin-registry-conflict-load-more-btn") return loadMoreButtons;
      return [];
    }
  };
}

function createSearchInput() {
  let inputHandler = null;
  return {
    value: "",
    addEventListener(type, handler) {
      if (type === "input") inputHandler = handler;
    },
    type(text) {
      this.value = text;
      if (inputHandler) inputHandler();
    }
  };
}

function card(familyKey, priority, queue) {
  return {
    familyKey,
    triageBucket: "ambiguous_manual_review",
    triageLabel: "Manual review",
    triageRisk: "medium",
    triageReason: "Needs review.",
    reviewPriority: priority,
    reviewQueue: queue,
    reviewLabel: "Review",
    reviewReason: "Needs review.",
    suggestedDisposition: "Review",
    suggestedConfidence: "low",
    safeAutomation: { eligible: false, blockedReasons: [] },
    winner: { name: "Winner" },
    rows: []
  };
}

function payload() {
  return {
    summary: { conflictCount: 3 },
    conflicts: [
      card("Provider Studio", 0, "p0_multi_active_provider"),
      card("Active Studio", 1, "p1_active_provider_static"),
      card("Pending Studio", 2, "p2_pending_static_variant")
    ]
  };
}

test("registry conflicts renderer auto-opens only P0 and P1 groups", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(reviewEl, payload());

  assert.match(reviewEl.innerHTML, /data-registry-conflict-review-queue="p0_multi_active_provider" open\s*>/);
  assert.match(reviewEl.innerHTML, /data-registry-conflict-review-queue="p1_active_provider_static" open\s*>/);
  assert.match(reviewEl.innerHTML, /data-registry-conflict-review-queue="p2_pending_static_variant"\s*>/);
  assert.doesNotMatch(
    reviewEl.innerHTML,
    /data-registry-conflict-review-queue="p2_pending_static_variant"[^>]*open/
  );
});

test("registry conflicts renderer filters cards by search text", () => {
  const searchInput = createSearchInput();
  const reviewEl = createReviewElement({ searchInputs: [searchInput] });

  renderAdminRegistryConflicts(reviewEl, payload());
  searchInput.type("provider");

  assert.equal(reviewEl.dataset.registryConflictSearchQuery, "provider");
  assert.match(reviewEl.innerHTML, /Provider Studio/);
  assert.doesNotMatch(reviewEl.innerHTML, /Active Studio/);
  assert.doesNotMatch(reviewEl.innerHTML, /Pending Studio/);
});

test("registry conflicts renderer shows load-more footer only when more cards exist", () => {
  const loadMoreButton = {
    dataset: {},
    addEventListener(type, handler) {
      if (type === "click") this._click = handler;
    },
    click() {
      this._click?.();
    }
  };
  const reviewEl = createReviewElement({ loadMoreButtons: [loadMoreButton] });
  const pagedPayload = payload();
  pagedPayload.summary.conflictCount = 10;
  let loadMoreCalls = 0;

  renderAdminRegistryConflicts(reviewEl, pagedPayload, {
    onRegistryConflictsLoadMore: () => {
      loadMoreCalls += 1;
    }
  });

  assert.match(reviewEl.innerHTML, /Showing 3 of 10 conflicts/);
  assert.match(reviewEl.innerHTML, /Show 50 more/);

  loadMoreButton.click();
  assert.equal(loadMoreCalls, 1);

  renderAdminRegistryConflicts(reviewEl, payload(), {});
  assert.doesNotMatch(reviewEl.innerHTML, /Show 50 more/);
});
