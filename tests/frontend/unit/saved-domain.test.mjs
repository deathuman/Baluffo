import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeCustomJobInput,
  activityTypeLabel,
  formatActivityDetail
} from "../../../frontend/saved/domain.js";

test("saved domain normalizes custom job payload", () => {
  const row = normalizeCustomJobInput({ title: "Dev", company: "Studio", country: "usa" }, { customSourceLabel: "Custom" });
  assert.equal(row.title, "Dev");
  assert.equal(row.company, "Studio");
  assert.equal(row.country, "US");
  assert.equal(row.customSourceLabel, "Custom");
});

test("saved domain maps activity labels/details", () => {
  assert.equal(activityTypeLabel("job_saved"), "Saved");
  assert.equal(activityTypeLabel("outcome_changed"), "Outcome Changed");
  const detail = formatActivityDetail(
    { type: "phase_changed", details: { previousStatus: "bookmark", nextStatus: "applied" } },
    {
      normalizePhase: value => value,
      phaseLabels: { bookmark: "Saved", applied: "Applied" },
      formatPhaseTimestamp: () => ""
    }
  );
  assert.equal(detail, "Saved -> Applied");
  const outcomeDetail = formatActivityDetail(
    { type: "outcome_changed", details: { previousOutcome: "active", nextOutcome: "rejected" } },
    {
      normalizeOutcome: value => value,
      outcomeLabels: { active: "Active", rejected: "Rejected" },
      formatPhaseTimestamp: () => ""
    }
  );
  assert.equal(outcomeDetail, "Active -> Rejected");
});

test("saved domain prefers explicit revert activity details with legacy fallback", () => {
  const phaseOptions = {
    normalizePhase: value => value,
    phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Offer" },
    formatPhaseTimestamp: value => (value ? "Mar 8, 8:00 AM" : "")
  };
  assert.equal(
    formatActivityDetail({
      type: "phase_reverted",
      details: {
        previousPhase: "offer",
        nextPhase: "applied",
        revertedFromPhase: "offer",
        restoredPhase: "bookmark",
        restoredPhaseTimestamp: "2026-03-08T08:00:00.000Z"
      }
    }, phaseOptions),
    "Offer -> Saved (restored Mar 8, 8:00 AM)"
  );
  assert.equal(
    formatActivityDetail({
      type: "phase_reverted",
      details: { previousPhase: "offer", nextPhase: "applied" }
    }, { ...phaseOptions, formatPhaseTimestamp: () => "" }),
    "Offer -> Applied"
  );

  const outcomeOptions = {
    normalizeOutcome: value => value,
    outcomeLabels: { active: "Active", rejected: "Rejected", accepted: "Accepted" },
    formatPhaseTimestamp: value => (value ? "Mar 8, 10:00 AM" : "")
  };
  assert.equal(
    formatActivityDetail({
      type: "outcome_reverted",
      details: {
        previousOutcome: "accepted",
        nextOutcome: "active",
        revertedFromOutcome: "accepted",
        restoredOutcome: "rejected",
        restoredOutcomeTimestamp: "2026-03-08T10:00:00.000Z"
      }
    }, outcomeOptions),
    "Accepted -> Rejected (restored Mar 8, 10:00 AM)"
  );
  assert.equal(
    formatActivityDetail({
      type: "outcome_reverted",
      details: { previousOutcome: "accepted", nextOutcome: "active" }
    }, { ...outcomeOptions, formatPhaseTimestamp: () => "" }),
    "Accepted -> Active"
  );
});
