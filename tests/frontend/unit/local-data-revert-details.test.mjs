import test from "node:test";
import assert from "node:assert/strict";

import { createSavedJobsDomain } from "../../../frontend/local-data/saved-jobs.js";

function createTrackingDomain(initialRow) {
  let storedRow = { ...initialRow };
  const activityCalls = [];
  const domain = createSavedJobsDomain({
    withStore: async (_storeName, _mode, fn) => {
      const store = {
        get() {
          return {
            result: storedRow,
            onerror: null,
            set onsuccess(handler) {
              handler();
            }
          };
        },
        put(row) {
          storedRow = row;
          return {
            onerror: null,
            set onsuccess(handler) {
              handler();
            }
          };
        }
      };
      await new Promise((resolve, reject) => fn(store, resolve, reject));
    },
    listSavedJobs: async () => [],
    ensureCurrentUser: () => ({ uid: "u1" }),
    notifySavedJobsChanged: async () => {},
    addActivityLog: async (...args) => {
      activityCalls.push(args);
    },
    generateJobKey: input => String(input?.jobKey || "job_1"),
    normalizeSectorValue: value => String(value || "Tech"),
    normalizeCustomSourceLabel: value => String(value || "Personal"),
    sanitizeJobUrl: value => String(value || ""),
    nowIso: () => "2026-03-08T12:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    isClearlyLowerQualityImported: () => false
  });
  return {
    domain,
    activityCalls,
    getStoredRow: () => storedRow
  };
}

test("browser saved-job phase revert activity keeps compatibility fields and adds audit details", async () => {
  const restoredTimestamp = "2026-03-08T08:00:00.000Z";
  const { domain, activityCalls, getStoredRow } = createTrackingDomain({
    pk: "u1::job_1",
    profileId: "u1",
    jobKey: "job_1",
    title: "Role",
    company: "Studio",
    savedAt: restoredTimestamp,
    pipelinePhase: "applied",
    outcomeStatus: "active",
    applicationStatus: "applied",
    phaseTimestamps: {
      bookmark: restoredTimestamp,
      applied: "2026-03-08T09:00:00.000Z"
    },
    outcomeTimestamps: {}
  });

  await domain.updateApplicationTracking(
    "u1",
    "job_1",
    { pipelinePhase: "bookmark" },
    {
      override: true,
      cleanupPhase: "applied",
      preserveTimestamp: restoredTimestamp,
      eventType: "phase_reverted",
      revertedFromPhase: "applied",
      restoredPhase: "bookmark",
      removedPhaseTimestampFor: "applied",
      restoredPhaseTimestamp: restoredTimestamp
    }
  );

  const details = activityCalls[0][3];
  assert.equal(activityCalls[0][1], "phase_reverted");
  assert.equal(details.previousPhase, "applied");
  assert.equal(details.nextPhase, "bookmark");
  assert.equal(details.previousStatus, "applied");
  assert.equal(details.nextStatus, "bookmark");
  assert.equal(details.revertedFromPhase, "applied");
  assert.equal(details.restoredPhase, "bookmark");
  assert.equal(details.removedPhaseTimestampFor, "applied");
  assert.equal(details.restoredPhaseTimestamp, restoredTimestamp);
  assert.equal(getStoredRow().phaseTimestamps.applied, undefined);
});

test("browser saved-job outcome revert activity keeps compatibility fields and adds audit details", async () => {
  const restoredTimestamp = "2026-03-08T10:00:00.000Z";
  const { domain, activityCalls } = createTrackingDomain({
    pk: "u1::job_1",
    profileId: "u1",
    jobKey: "job_1",
    title: "Role",
    company: "Studio",
    savedAt: "2026-03-08T08:00:00.000Z",
    pipelinePhase: "offer",
    outcomeStatus: "accepted",
    applicationStatus: "accepted",
    phaseTimestamps: { offer: "2026-03-08T09:00:00.000Z" },
    outcomeTimestamps: { rejected: restoredTimestamp, accepted: "2026-03-08T11:00:00.000Z" }
  });

  await domain.updateApplicationTracking(
    "u1",
    "job_1",
    { outcomeStatus: "rejected" },
    {
      override: true,
      preserveOutcomeTimestamp: restoredTimestamp,
      eventType: "outcome_reverted",
      revertedFromOutcome: "accepted",
      restoredOutcome: "rejected",
      restoredOutcomeTimestamp: restoredTimestamp
    }
  );

  const details = activityCalls[0][3];
  assert.equal(activityCalls[0][1], "outcome_reverted");
  assert.equal(details.previousOutcome, "accepted");
  assert.equal(details.nextOutcome, "rejected");
  assert.equal(details.previousStatus, "accepted");
  assert.equal(details.nextStatus, "rejected");
  assert.equal(details.revertedFromOutcome, "accepted");
  assert.equal(details.restoredOutcome, "rejected");
  assert.equal(details.restoredOutcomeTimestamp, restoredTimestamp);
});
