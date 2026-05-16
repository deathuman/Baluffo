import test from "node:test";
import assert from "node:assert/strict";

import {
  SAVED_FILTER_CLOSED,
  SAVED_FILTER_HAS_ATTACHMENTS,
  SAVED_FILTER_INTERVIEWING,
  SAVED_FILTER_NEEDS_ACTION,
  SORT_ACTIVITY,
  buildSavedJobViewModel,
  filterSavedJobViews,
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

test("saved view model derives tracking, evidence-only needsAction, and buckets", () => {
  const activeRemoved = view({
    jobKey: "job_1",
    applicationStatus: "interview_1",
    reminderAt: "",
    attachmentsCount: 0,
    notes: ""
  }, { status: "likely_removed" });
  assert.equal(activeRemoved.pipelinePhase, "interview_1");
  assert.equal(activeRemoved.outcomeStatus, "active");
  assert.equal(activeRemoved.phaseBucket, "interviewing");
  assert.equal(activeRemoved.needsAction, true);
  assert.deepEqual(activeRemoved.needsActionReasons, ["source_likely_removed"]);

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
