import test from "node:test";
import assert from "node:assert/strict";
import {
  renderApplicationTrackingControls,
  renderOutcomeControls
} from "../../../frontend/saved/app/tracking-ui.js";

test("saved tracking UI: compact outcome controls use a status chip and terminal menu", () => {
  const labels = {
    active: "Active",
    rejected: "Rejected",
    withdrawn: "Withdrawn",
    ghosted: "Ghosted",
    closed: "Closed",
    accepted: "Accepted"
  };
  const outcomeOptions = ["active", "rejected", "withdrawn", "ghosted", "closed", "accepted"];
  const activeHtml = renderOutcomeControls("job-1", "active", {}, {
    outcomeOptions,
    outcomeLabels: labels,
    canSetOutcome: () => true,
    currentUser: { uid: "u1" }
  });

  assert.match(activeHtml, /outcome-compact/);
  assert.match(activeHtml, /outcome-status-chip active/);
  assert.match(activeHtml, /Set final outcome/);
  assert.doesNotMatch(activeHtml, /outcome-bar/);
  assert.doesNotMatch(activeHtml, /data-outcome-status="active"/);
  ["rejected", "withdrawn", "ghosted", "closed", "accepted"].forEach(status => {
    assert.match(activeHtml, new RegExp(`data-outcome-status="${status}"`));
  });

  const terminalHtml = renderOutcomeControls("job-1", "rejected", {
    rejected: "2026-03-08T10:00:00.000Z"
  }, {
    outcomeOptions,
    outcomeLabels: labels,
    canSetOutcome: (current, next) => current === next || current === "active",
    currentUser: { uid: "u1" }
  });

  assert.match(terminalHtml, /outcome-status-chip terminal/);
  assert.match(terminalHtml, /Rejected/);
  assert.match(terminalHtml, /outcome-status-time/);
  assert.doesNotMatch(terminalHtml, /outcome-status-time">[^<]*:\d{2}:\d{2}/);
  assert.match(terminalHtml, /Change outcome/);
  assert.match(terminalHtml, /data-outcome-status="active"[\s\S]*Reopen as Active/);
  assert.doesNotMatch(terminalHtml, /data-outcome-status="rejected"/);
  assert.match(terminalHtml, /outcome-menu-item[^"]*locked/);
});

test("saved tracking UI: phase timestamps move out of the timeline and into details", () => {
  const html = renderApplicationTrackingControls({
    jobKey: "job-1",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    phaseTimestamps: {
      bookmark: "2026-05-16T19:14:08.000Z"
    }
  }, {
    canTransition: () => true,
    canSetOutcome: () => true,
    currentUser: { uid: "u1" }
  });

  assert.match(html, /data-phase-time="[^"]+"/);
  assert.doesNotMatch(html, /phase-step-time/);
  assert.match(
    html,
    /tracking-current-line[\s\S]*Current phase:[\s\S]*Saved[\s\S]*Entered:[\s\S]*May/
  );
});

test("saved tracking UI: action row exposes next phase and change phase controls", () => {
  const phaseOptions = ["bookmark", "applied", "screening", "assignment"];
  const phaseLabels = {
    bookmark: "Saved",
    applied: "Applied",
    screening: "Screening",
    assignment: "Assignment"
  };
  const activeHtml = renderApplicationTrackingControls({
    jobKey: "job-1",
    pipelinePhase: "screening",
    outcomeStatus: "active",
    activeAt: "2026-05-16T20:21:00.000Z",
    attentionReasons: [
      { key: "reminder_overdue", label: "Overdue reminder" },
      { key: "source_likely_removed", label: "Source likely removed" }
    ],
    primaryAttentionReason: { key: "reminder_overdue", label: "Overdue reminder" },
    phaseTimestamps: {
      screening: "2026-05-16T19:21:00.000Z"
    }
  }, {
    phaseOptions,
    phaseLabels,
    canTransition: (current, next, outcome) => (
      current === "screening" && next === "assignment" && outcome === "active"
    ),
    canSetOutcome: () => true,
    now: new Date("2026-05-16T21:21:00.000Z"),
    currentUser: { uid: "u1" }
  });

  assert.match(activeHtml, /saved-tracking-action-row/);
  assert.match(
    activeHtml,
    /tracking-phase-summary[\s\S]*tracking-current-line[\s\S]*Current phase:[\s\S]*Screening[\s\S]*Entered:[\s\S]*Last activity:[\s\S]*1h ago/
  );
  assert.match(activeHtml, /tracking-attention-chip[\s\S]*data-attention-reason="reminder_overdue"/);
  assert.match(activeHtml, /Needs action:[\s\S]*Overdue reminder/);
  assert.match(activeHtml, /data-tooltip="Needs action: Overdue reminder; Source likely removed"/);
  assert.doesNotMatch(activeHtml, /tracking-entered-summary/);
  assert.match(
    activeHtml,
    /class="btn back-btn phase-next-btn"[\s\S]*data-ui="phase-step-btn"[\s\S]*data-phase="assignment"[\s\S]*Move to Assignment/
  );
  assert.doesNotMatch(activeHtml, /data-tooltip="Move to Assignment\."/);
  assert.doesNotMatch(activeHtml, /data-tooltip="Set phase to Assignment\."/);
  assert.doesNotMatch(activeHtml, /data-tooltip="Choose a different application phase\."/);
  assert.match(activeHtml, /phase-change-menu/);
  assert.match(activeHtml, /phase-menu-item[\s\S]*data-ui="phase-step-btn"/);

  const terminalHtml = renderApplicationTrackingControls({
    jobKey: "job-1",
    pipelinePhase: "screening",
    outcomeStatus: "rejected"
  }, {
    phaseOptions,
    phaseLabels,
    canTransition: () => false,
    canSetOutcome: () => true,
    currentUser: { uid: "u1" }
  });

  assert.doesNotMatch(terminalHtml, /phase-next-btn/);
  assert.match(terminalHtml, /phase-menu-item locked/);
  assert.match(
    terminalHtml,
    /data-tooltip="This phase change requires an override because the job has a final outcome\."/
  );

  const finalPhaseHtml = renderApplicationTrackingControls({
    jobKey: "job-1",
    pipelinePhase: "offer",
    outcomeStatus: "active",
    phaseTimestamps: {
      offer: "2026-05-16T20:52:00.000Z"
    }
  }, {
    phaseOptions: [...phaseOptions, "offer"],
    phaseLabels: { ...phaseLabels, offer: "Offer" },
    canTransition: () => false,
    canSetOutcome: () => true,
    currentUser: { uid: "u1" }
  });

  assert.match(finalPhaseHtml, /tracking-final-indicator[\s\S]*Awaiting outcome/);
  assert.doesNotMatch(finalPhaseHtml, /phase-next-btn/);
  assert.match(finalPhaseHtml, /Set final outcome/);
});
