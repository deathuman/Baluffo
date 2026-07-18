import test from "node:test";
import assert from "node:assert/strict";

import {
  SAVED_FILTER_CLOSED,
  SAVED_FILTER_HAS_ATTACHMENTS,
  SAVED_FILTER_INTERVIEWING,
  SAVED_FILTER_NEEDS_ACTION,
  SAVED_FILTER_AVAILABILITY_ATTENTION,
  SORT_ACTIVITY,
  buildSavedJobViewModel,
  filterSavedJobViews,
  groupSavedJobViews,
  sortSavedJobViews
} from "../../../frontend/saved/app/view-model.js";

const NOW = new Date("2026-04-01T12:00:00.000Z");

function view(job, lifecycleOverlay = null) {
  return buildSavedJobViewModel(job, {
    lifecycleOverlay,
    now: NOW,
    parseIsoDate: value => {
      if (!value) return null;
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    },
    currentUser: { uid: "u1" }
  });
}

test("saved availability attention includes local reports without hiding rows", () => {
  const attention = view({
    jobKey: "job_attention",
    availabilityAttention: {
      events: [{ transitionId: "event_1", alert: true, acknowledgedAt: "" }]
    }
  });
  assert.equal(attention.availabilityAttentionCount, 1);
  assert.equal(attention.needsAction, true);
  assert.deepEqual(filterSavedJobViews([attention], SAVED_FILTER_AVAILABILITY_ATTENTION), [attention]);

  const hidden = view({
    jobKey: "job_hidden",
    availabilityAttention: { hiddenByReport: true, events: [] }
  });
  assert.equal(hidden.reportedUnavailable, true);
  assert.deepEqual(filterSavedJobViews([hidden], "all"), [hidden]);
  assert.deepEqual(filterSavedJobViews([hidden], SAVED_FILTER_AVAILABILITY_ATTENTION), [hidden]);
});

test("saved view model derives tracking, evidence-only needsAction, and buckets", () => {
  const activeRemoved = view({
    jobKey: "job_1",
    applicationStatus: "interview_1",
    reminderAt: "",
    phaseTimestamps: {
      interview_1: "2026-04-01T09:00:00.000Z"
    },
    lastActivityAt: "2026-04-01T10:30:00.000Z",
    attachmentsCount: 0,
    notes: ""
  }, { status: "likely_removed" });
  assert.equal(activeRemoved.pipelinePhase, "interview_1");
  assert.equal(activeRemoved.outcomeStatus, "active");
  assert.equal(activeRemoved.phaseBucket, "interviewing");
  assert.equal(activeRemoved.needsAction, true);
  assert.deepEqual(activeRemoved.needsActionReasons, ["source_likely_removed"]);
  assert.deepEqual(activeRemoved.attentionReasons, [{
    key: "source_likely_removed",
    label: "Source likely removed"
  }]);
  assert.equal(activeRemoved.primaryAttentionReason.label, "Source likely removed");
  assert.equal(activeRemoved.phaseEnteredAt, "2026-04-01T09:00:00.000Z");
  assert.equal(activeRemoved.activeAt, "2026-04-01T10:30:00.000Z");

  const rejectedRemoved = view({
    jobKey: "job_2",
    pipelinePhase: "interview_2",
    outcomeStatus: "rejected",
    notes: "follow up",
    attachmentsCount: 1
  }, { status: "likely_removed" });
  assert.equal(rejectedRemoved.outcomeBucket, "closed");
  assert.equal(rejectedRemoved.needsAction, false);
  assert.equal(rejectedRemoved.hasNotes, true);
  assert.equal(rejectedRemoved.hasAttachments, true);
});

test("saved view model prioritizes reminder action before source lifecycle evidence", () => {
  const overdueAndRemoved = view({
    jobKey: "job_overdue",
    pipelinePhase: "applied",
    outcomeStatus: "active",
    reminderAt: "2026-04-01T10:00:00.000Z"
  }, { status: "likely_removed" });
  assert.equal(overdueAndRemoved.needsAction, true);
  assert.deepEqual(overdueAndRemoved.needsActionReasons, [
    "reminder_overdue",
    "source_likely_removed"
  ]);
  assert.equal(overdueAndRemoved.primaryAttentionReason.label, "Overdue reminder");

  const dueSoon = view({
    jobKey: "job_due",
    pipelinePhase: "screening",
    outcomeStatus: "active",
    reminderAt: "2026-04-01T13:00:00.000Z"
  });
  assert.deepEqual(dueSoon.needsActionReasons, ["reminder_due_soon"]);
  assert.equal(dueSoon.primaryAttentionReason.label, "Reminder due soon");

  const terminalArchivedWithReminder = view({
    jobKey: "job_terminal",
    pipelinePhase: "offer",
    outcomeStatus: "rejected",
    reminderAt: "2026-04-01T13:00:00.000Z"
  }, { status: "archived" });
  assert.deepEqual(terminalArchivedWithReminder.needsActionReasons, ["reminder_due_soon"]);
  assert.equal(terminalArchivedWithReminder.sourceNeedsAction, false);
});

test("saved view model filters and sorts consume derived fields", () => {
  const views = [
    view({
      jobKey: "job_old",
      pipelinePhase: "applied",
      outcomeStatus: "active",
      lastActivityAt: "2026-03-01T12:00:00.000Z"
    }),
    view({
      jobKey: "job_interview",
      pipelinePhase: "interview_1",
      outcomeStatus: "active",
      lastActivityAt: "2026-04-01T10:00:00.000Z"
    }),
    view({
      jobKey: "job_closed",
      pipelinePhase: "offer",
      outcomeStatus: "accepted",
      attachmentsCount: 2,
      lastActivityAt: "2026-04-01T11:00:00.000Z"
    })
  ];

  assert.deepEqual(
    filterSavedJobViews(views, SAVED_FILTER_INTERVIEWING).map(item => item.jobKey),
    ["job_interview"]
  );
  assert.deepEqual(
    filterSavedJobViews(views, SAVED_FILTER_CLOSED).map(item => item.jobKey),
    ["job_closed"]
  );
  assert.deepEqual(
    filterSavedJobViews(views, SAVED_FILTER_HAS_ATTACHMENTS).map(item => item.jobKey),
    ["job_closed"]
  );
  assert.deepEqual(
    filterSavedJobViews([view({
      jobKey: "job_due",
      outcomeStatus: "active",
      reminderAt: "2026-04-01T11:00:00.000Z"
    })], SAVED_FILTER_NEEDS_ACTION).map(item => item.jobKey),
    ["job_due"]
  );
  assert.deepEqual(
    sortSavedJobViews(views, SORT_ACTIVITY).map(item => item.jobKey),
    ["job_closed", "job_interview", "job_old"]
  );
});

test("saved view model grouping keeps flat rows by default", () => {
  const views = [
    view({ jobKey: "job_a", pipelinePhase: "bookmark", outcomeStatus: "active" }),
    view({ jobKey: "job_b", pipelinePhase: "offer", outcomeStatus: "accepted" })
  ];

  assert.deepEqual(groupSavedJobViews(views, "none"), [{
    key: "none",
    label: "",
    count: 2,
    views
  }]);
  assert.deepEqual(groupSavedJobViews(views, "stale").map(group => group.key), ["none"]);
});

test("saved view model groups active rows by stage", () => {
  const views = [
    view({ jobKey: "job_saved", pipelinePhase: "bookmark", outcomeStatus: "active" }),
    view({ jobKey: "job_screen", pipelinePhase: "screening", outcomeStatus: "active" }),
    view({ jobKey: "job_interview", pipelinePhase: "interview_2", outcomeStatus: "active" }),
    view({ jobKey: "job_offer", pipelinePhase: "final", outcomeStatus: "active" })
  ];

  const groups = groupSavedJobViews(views, "stage");

  assert.deepEqual(groups.map(group => [group.key, group.label, group.count]), [
    ["stage_saved", "Saved", 1],
    ["stage_applied", "Applied", 1],
    ["stage_interviewing", "Interviewing", 1],
    ["stage_offer", "Final / Offer", 1]
  ]);
  assert.deepEqual(groups.flatMap(group => group.views.map(item => item.jobKey)), [
    "job_saved",
    "job_screen",
    "job_interview",
    "job_offer"
  ]);
});

test("saved view model groups terminal rows by outcome label", () => {
  const views = [
    view({ jobKey: "job_rejected", pipelinePhase: "interview_2", outcomeStatus: "rejected" }),
    view({ jobKey: "job_withdrawn", pipelinePhase: "applied", outcomeStatus: "withdrawn" }),
    view({ jobKey: "job_ghosted", pipelinePhase: "screening", outcomeStatus: "ghosted" }),
    view({ jobKey: "job_closed", pipelinePhase: "offer", outcomeStatus: "closed" }),
    view({ jobKey: "job_accepted", pipelinePhase: "offer", outcomeStatus: "accepted" })
  ];

  assert.deepEqual(groupSavedJobViews(views, "stage").map(group => [group.label, group.count]), [
    ["Rejected", 1],
    ["Withdrawn", 1],
    ["Ghosted", 1],
    ["Closed", 1],
    ["Accepted", 1]
  ]);
});
