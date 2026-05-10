import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { renderAdminRegistryConflicts } from "../../../frontend/admin/render/registry-conflicts.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function createReviewElement({ actionButtons = [] } = {}) {
  return {
    dataset: {},
    innerHTML: "",
    querySelectorAll(selector) {
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

test("registry conflicts renderer shows suppressed independent provider board audit", () => {
  const reviewEl = createReviewElement();

  renderAdminRegistryConflicts(reviewEl, {
    summary: { conflictCount: 0 },
    conflicts: [],
    suppressedIndependentProviderBoards: {
      summary: { familyCount: 1, rowCount: 4 },
      families: [
        {
          familyKey: "sony computer entertainment",
          rowCount: 4,
          adapter: "greenhouse",
          sourceIds: [
            "greenhouse:slug:siei",
            "greenhouse:slug:pdi",
            "greenhouse:slug:naughtydog",
            "greenhouse:slug:haven"
          ],
          evidenceReason: "current_fetch_job_identity_overlap_below_threshold"
        }
      ]
    }
  });

  assert.match(reviewEl.innerHTML, /independent provider board family suppressed/i);
  assert.match(reviewEl.innerHTML, /sony computer entertainment/i);
  assert.match(reviewEl.innerHTML, /greenhouse:slug:siei \| greenhouse:slug:pdi/i);
  assert.match(reviewEl.innerHTML, /current fetch job identity overlap below threshold/i);
  assert.doesNotMatch(reviewEl.innerHTML, /admin-registry-conflict-card/);
});

test("registry conflicts renderer keeps row evidence compact and diffs collapsed by default", () => {
  const reviewEl = createReviewElement();
  const payload = {
    summary: { conflictCount: 1 },
    conflicts: [
      {
        familyKey: "Compact Studio",
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
        winner: { name: "Winner", health: "healthy" },
        rows: [
          {
            id: "source-1",
            name: "Winner",
            registryState: "active",
            transitionReason: "approved",
            health: "healthy",
            healthReason: "recent successful fetch",
            jobsFound: 4,
            lastJobsKept: 3,
            lastSuccessfulFetchAt: "2026-05-08T12:00:00+00:00",
            failureCount: 0,
            zeroJobStreak: 0
          }
        ],
        diffs: [
          {
            loserName: "Loser",
            fields: [
              { key: "adapter", label: "Adapter", winnerValue: "greenhouse", loserValue: "static" },
              { key: "url", label: "URL", winnerValue: "https://winner.example/jobs", loserValue: "https://loser.example/jobs" }
            ]
          }
        ]
      }
    ]
  };

  renderAdminRegistryConflicts(reviewEl, payload);

  assert.match(reviewEl.innerHTML, /Jobs found<\/strong> 4/);
  assert.match(reviewEl.innerHTML, /Last jobs kept<\/strong> 3/);
  assert.match(reviewEl.innerHTML, /class="admin-registry-conflict-detail">\s*<summary>Decision details/);
  assert.doesNotMatch(reviewEl.innerHTML, /class="admin-registry-conflict-detail" open/);
  assert.match(reviewEl.innerHTML, /class="admin-registry-conflict-row-details">\s*<summary>More row evidence/);
  assert.match(reviewEl.innerHTML, /<summary>Diffs · 2 fields · Loser vs Winner<\/summary>/);
  assert.doesNotMatch(reviewEl.innerHTML, /class="admin-registry-conflict-diff" open/);
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

test("registry conflicts CSS keeps cards in a bounded scroll list without clipping details", () => {
  const css = fs.readFileSync(path.join(repoRoot, "styles/admin.css"), "utf8");
  const listRule = css.match(/\.admin-registry-conflicts-list\s*\{[\s\S]*?\}/)?.[0] || "";
  const detailsRule = css.match(/\.admin-registry-conflict-detail,\s*\.admin-registry-conflict-row-details,\s*\.admin-registry-conflict-review-group\s*\{[\s\S]*?\}/)?.[0] || "";
  const diffBodyRule = css.match(/\.admin-registry-conflict-diff-body\s*\{[\s\S]*?\}/)?.[0] || "";

  assert.match(listRule, /max-height:\s*clamp\(/);
  assert.match(listRule, /overflow-y:\s*auto/);
  assert.match(listRule, /scrollbar-gutter:\s*stable/);
  assert.doesNotMatch(detailsRule, /overflow:\s*hidden/);
  assert.match(diffBodyRule, /overflow-x:\s*auto/);
});
