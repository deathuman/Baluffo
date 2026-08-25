import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminSourcePolicyReview } from "../../../frontend/admin/render/source-policy-review.js";
import { renderDiscoveryCandidateReviewHtml } from "../../../frontend/admin/render/sources.js";

function createReviewEl() {
  const listeners = [];
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll(selector) {
      if (selector.startsWith(".admin-source-policy-select-box")) {
        return this._boxes || [];
      }
      if (selector === ".admin-source-policy-bulk-btn") return this._bulkButtons || [];
      if (selector.includes("filter-btn") || selector.includes("[data-ui=")) return [];
      return [];
    },
    querySelector() {
      return null;
    },
    _listeners: listeners
  };
}

function createCheckbox(staticId, checked = false) {
  const box = {
    dataset: { sourcePolicyStaticId: staticId },
    checked,
    _handler: null,
    addEventListener(type, handler) {
      if (type === "change") this._handler = handler;
    }
  };
  return box;
}

function createBulkButton(action) {
  return {
    dataset: { sourcePolicyBulkAction: action },
    disabled: false,
    hasAttribute() {
      return this.disabled;
    },
    setAttribute(name) {
      if (name === "disabled") this.disabled = true;
    },
    removeAttribute(name) {
      if (name === "disabled") this.disabled = false;
    },
    _click: null,
    addEventListener(type, handler) {
      if (type === "click") this._click = handler;
    }
  };
}

function pairPayload() {
  return {
    recommendations: {
      pairs: [
        {
          staticSourceId: "static:a",
          staticSourceName: "Static A",
          providerSourceId: "prov:a",
          providerSourceName: "Provider A",
          currentRecommendation: "stable_safe_redundant",
          reviewState: "new",
          confidence: 0.9
        },
        {
          staticSourceId: "static:b",
          staticSourceName: "Static B",
          providerSourceId: "prov:b",
          providerSourceName: "Provider B",
          currentRecommendation: "stable_safe_redundant",
          reviewState: "new",
          confidence: 0.8
        }
      ]
    }
  };
}

test("source policy review renders checkboxes, bulk bar, and wires selection", () => {
  const reviewEl = createReviewEl();
  reviewEl.dataset.sourcePolicySelected = JSON.stringify(["static:a"]);
  reviewEl._boxes = [createCheckbox("static:a", true), createCheckbox("static:b", false)];
  reviewEl.querySelectorAll = function (selector) {
    const wantsChecked = selector.includes(":checked");
    if (selector.startsWith(".admin-source-policy-select-box")) {
      return (this._boxes || []).filter(box => !wantsChecked || box.checked);
    }
    if (selector === ".admin-source-policy-bulk-btn") return this._bulkButtons;
    return [];
  };
  reviewEl._bulkButtons = [createBulkButton("acknowledge"), createBulkButton("snooze")];

  let bulkCalls = [];
  renderAdminSourcePolicyReview(reviewEl, pairPayload(), {
    onSourcePolicyBulkAction: (action, rows) => {
      bulkCalls = [action, rows];
    }
  });

  assert.match(reviewEl.innerHTML, /admin-source-policy-select-box/);
  assert.match(reviewEl.innerHTML, /Acknowledge selected/);
  assert.match(reviewEl.innerHTML, /Snooze selected 7d/);
  assert.match(reviewEl.innerHTML, /1 selected/);
  assert.match(reviewEl.innerHTML, /data-source-policy-static-id="static:a"[\s\S]*?checked/);

  reviewEl._boxes[1].checked = true;
  reviewEl._boxes[1]._handler();
  assert.equal(reviewEl.dataset.sourcePolicySelected, JSON.stringify(["static:a", "static:b"]));

  reviewEl._bulkButtons[1]._click();
  assert.equal(bulkCalls[0], "snooze");
  assert.equal(bulkCalls[1].length, 2);
  assert.equal(bulkCalls[1][0].staticSourceId, "static:a");
});

test("discovery candidate lanes show honest counts and a show-more button", () => {
  const rows = Array.from({ length: 14 }, (_, index) => ({
    name: `Candidate ${index}`,
    adapter: "greenhouse"
  }));
  const html = renderDiscoveryCandidateReviewHtml(
    { totalCandidates: 14, topCandidates: rows },
    { showEmpty: true, laneLimits: { "lane-0": 5 }, expandableLanes: true }
  );

  assert.match(html, /showing 5 of 14/);
  assert.match(html, /admin-discovery-lane-more-btn/);
  assert.match(html, /Show 10 more \(9 left\)/);
  assert.match(html, /Candidate 4</);
  assert.doesNotMatch(html, /Candidate 5</);
});
