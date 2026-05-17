import test from "node:test";
import assert from "node:assert/strict";

import { createSavedMutations } from "../../../frontend/saved/app/runtime/mutations.js";

function createMutationHarness(row) {
  const updateCalls = [];
  const toastCalls = [];
  const mutations = createSavedMutations({
    viewState: {
      currentUser: { uid: "u1" },
      phaseOverrideContext: null,
      trackingOverrideContext: null,
      lastSavedJobsByKey: new Map([[row.jobKey, row]])
    },
    savedPageService: {
      async updateApplicationTracking(...args) {
        updateCalls.push(args);
        return { ok: true };
      }
    },
    normalizePhase: value => String(value || "bookmark"),
    normalizeOutcome: value => String(value || "active"),
    canTransition: () => true,
    canSetOutcome: () => true,
    needsInterviewTimestamp: () => false,
    requestInterviewTimestamp: async () => "",
    phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Offer" },
    outcomeLabels: { active: "Active", rejected: "Rejected", accepted: "Accepted" },
    refreshActivityLog: async () => {},
    renderSavedJobs() {},
    queueActivityPulse() {},
    timelineScopePhase: "phase",
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });
  return { mutations, updateCalls, toastCalls };
}

test("phase revert toast sends explicit audit detail options", async () => {
  const restoredTimestamp = "2026-03-08T08:00:00.000Z";
  const { mutations, updateCalls, toastCalls } = createMutationHarness({
    jobKey: "job_1",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    phaseTimestamps: { bookmark: restoredTimestamp }
  });

  await mutations.updatePhase("job_1", "applied");
  await toastCalls.find(call => call.options.actionLabel === "Revert").options.onAction();

  assert.deepEqual(updateCalls[1], [
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
  ]);
});

test("outcome revert toast sends explicit audit detail options", async () => {
  const restoredTimestamp = "2026-03-08T10:00:00.000Z";
  const { mutations, updateCalls, toastCalls } = createMutationHarness({
    jobKey: "job_1",
    pipelinePhase: "offer",
    outcomeStatus: "rejected",
    outcomeTimestamps: { rejected: restoredTimestamp }
  });

  await mutations.updateOutcome("job_1", "accepted");
  await toastCalls.find(call => call.options.actionLabel === "Revert").options.onAction();

  assert.deepEqual(updateCalls[1], [
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
  ]);
});
