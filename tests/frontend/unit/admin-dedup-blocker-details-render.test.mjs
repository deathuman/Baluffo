import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics
} from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

function metricsPayload() {
  return {
    latestRun: {
      sourceCount: 1,
      dedupReviewStateReadWarning: "missing_dedup_review_state_artifact",
      dedupReviewStateSummary: {
        artifactPath: "data/dedup-review-state.json",
        status: "warning",
        readWarning: "missing_dedup_review_state_artifact",
        unresolvedBlockingCount: 2
      },
      dedupEvidence: {
        dedupAuditGate: {
          status: "blocked",
          lifecycleUxReady: false,
          providerStaticDisagreementBlockedCount: 2,
          blockers: ["provider_static_disagreement_needs_review"],
          warnings: ["monitor_review_queue_diagnostics_present"],
          blockerDetails: [
            {
              key: "provider_static_disagreement_needs_review",
              label: "Provider/static disagreements",
              count: 2,
              whyBlocked: "Provider and static rows disagree on URL evidence.",
              nextAction: "Review provider/static cards before lifecycle UX.",
              counts: { blocked: 2, currentRunBlocked: 1, carriedBlocked: 1 },
              examples: [
                {
                  title: "Executive Assistant",
                  company: "Animoca Brands",
                  recommendedReviewAction: "review_provider_static_disagreement",
                  suspectedCause: "provider_static_disagreement",
                  bundleEvidenceOrigin: "current_run"
                }
              ]
            }
          ],
          warningDetails: [
            {
              key: "monitor_review_queue_diagnostics_present",
              label: "Monitor-only review diagnostics",
              count: 3,
              whyBlocked: "These diagnostics are explicitly monitor-only.",
              nextAction: "Use supporting diagnostics for trend monitoring.",
              counts: { "currentRun.unknown": 3 },
              examples: []
            }
          ]
        }
      }
    },
    history: {}
  };
}

test("admin render: Dedup Lists show actionable blocker details", () => {
  const el = makeEl();
  renderAdminOpsDedupLists(el, metricsPayload());

  assert.match(el.innerHTML, /Blocking Issues/i);
  assert.match(el.innerHTML, /Provider\/static disagreements/i);
  assert.match(el.innerHTML, /Why blocked/i);
  assert.match(el.innerHTML, /Provider and static rows disagree on URL evidence/i);
  assert.match(el.innerHTML, /Next action/i);
  assert.match(el.innerHTML, /Review provider\/static cards before lifecycle UX/i);
  assert.match(el.innerHTML, /currentRun unknown 3/i);
  assert.match(el.innerHTML, /Executive Assistant @ Animoca Brands/i);
  assert.match(el.innerHTML, /Review-state file missing\/malformed/i);
  assert.match(el.innerHTML, /restoring old review state alone will not clear current-run blockers/i);
  assert.match(el.innerHTML, /restore or re-review carried rows/i);
});

test("admin render: fetcher metrics use the same Dedup gate summary", () => {
  const el = makeEl();
  renderAdminOpsFetcherMetrics(el, metricsPayload(), null, { includeDedupSection: true });

  assert.match(el.innerHTML, /Blocking Issues/i);
  assert.match(el.innerHTML, /Provider\/static disagreements/i);
  assert.match(el.innerHTML, /Review provider\/static cards before lifecycle UX/i);
  assert.match(el.innerHTML, /Gate metrics/i);
});
