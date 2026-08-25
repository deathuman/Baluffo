import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

function createSelect(value = "all") {
  let handler = null;
  return {
    value,
    addEventListener(type, h) {
      if (type === "change") handler = h;
    },
    change(next) {
      this.value = next;
      handler?.();
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
    rows: [{ id: `row-${familyKey}`, name: familyKey }]
  };
}

function payload() {
  return {
    summary: { conflictCount: 2 },
    conflicts: [
      card("Alpha Studio", 0, "p0_multi_active_provider"),
      card("Beta Studio", 2, "p2_pending_static_variant")
    ]
  };
}

function withLocation(hash, fn) {
  const previousHistory = globalThis.history;
  const replaceCalls = [];
  globalThis.location = { hash, pathname: "/admin", search: "" };
  globalThis.history = {
    replaceState(_state, _title, url) {
      replaceCalls.push(String(url));
    }
  };
  try {
    return { result: fn(), replaceCalls };
  } finally {
    if (previousHistory === undefined && !Object.prototype.hasOwnProperty.call(globalThis, "history")) {
      delete globalThis.history;
      delete globalThis.location;
    } else {
      globalThis.history = previousHistory;
      delete globalThis.location;
    }
  }
}

test("registry conflict filters seed from URL hash and sync back on change", () => {
  const reviewEl = { dataset: {}, innerHTML: "", querySelectorAll: () => [] };
  const select = createSelect();

  const { replaceCalls } = withLocation("#ops-tab=registry-conflicts&conflict-queue=p0_multi_active_provider", () => {
    reviewEl.querySelectorAll = selector => (selector === ".admin-registry-conflict-review-filter-select" ? [select] : []);
    renderAdminRegistryConflicts(reviewEl, payload());
    assert.equal(reviewEl.dataset.registryConflictReviewFilter, "p0_multi_active_provider");
    assert.doesNotMatch(reviewEl.innerHTML, /Beta Studio/);
    select.change("all");
  });

  assert.equal(reviewEl.dataset.registryConflictReviewFilter, "all");
  assert.ok(replaceCalls.at(-1).includes("ops-tab=registry-conflicts"));
  assert.match(reviewEl.innerHTML, /Beta Studio/);
});

test("adjudication toolbar highlights first check only when never run", () => {
  const fresh = { dataset: {}, innerHTML: "", querySelectorAll: () => [] };
  renderAdminRegistryConflicts(fresh, payload());
  assert.match(fresh.innerHTML, /Recommended first step/);
  assert.match(fresh.innerHTML, /admin-registry-conflict-action-group-start/);

  const checked = { dataset: {}, innerHTML: "", querySelectorAll: () => [] };
  renderAdminRegistryConflicts(checked, {
    ...payload(),
    adjudication: { status: "completed", finishedAt: "2026-08-25T00:00:00Z", demoted: 1 }
  });
  assert.doesNotMatch(checked.innerHTML, /Recommended first step/);
  assert.doesNotMatch(checked.innerHTML, /admin-registry-conflict-action-group-start/);
});
