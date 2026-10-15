import test from "node:test";
import assert from "node:assert/strict";

import { createSavedMutations } from "../../../frontend/saved/app/runtime/mutations.js";
import {
  PIPELINE_PHASES,
  clearFuturePhaseTimestamps,
  isPipelinePhaseRewind,
  toApplicationStatusMirror
} from "../../../frontend/local-data/tracking.js";

function createMutationHarness(row, options = {}) {
  const updateCalls = [];
  const toastCalls = [];
  const viewState = {
    currentUser: { uid: "u1" },
    phaseOverrideContext: null,
    trackingOverrideContext: null,
    pendingTrackingByJobKey: new Map(),
    lastSavedJobsByKey: new Map([[row.jobKey, row]])
  };
  const renderCalls = [];
  const mutations = createSavedMutations({
    viewState,
    phaseOptions: PIPELINE_PHASES,
    isPipelinePhaseRewind,
    clearFuturePhaseTimestamps,
    toApplicationStatusMirror,
    savedPageService: {
      async updateApplicationTracking(...args) {
        updateCalls.push(args);
        return options.updateApplicationTracking
          ? options.updateApplicationTracking(...args)
          : { ok: true };
      }
    },
    normalizePhase: value => String(value || "bookmark"),
    normalizeOutcome: value => String(value || "active"),
    canTransition: options.canTransition || (() => true),
    canSetOutcome: options.canSetOutcome || (() => true),
    needsInterviewTimestamp: options.needsInterviewTimestamp || (() => false),
    requestInterviewTimestamp: options.requestInterviewTimestamp || (async () => ""),
    requestTextInputDialog: options.requestTextInputDialog,
    requestConfirmationDialog: options.requestConfirmationDialog,
    phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Offer" },
    outcomeLabels: { active: "Active", rejected: "Rejected", accepted: "Accepted" },
    refreshActivityLog: async () => {},
    renderSavedJobs(jobs) {
      renderCalls.push(jobs);
    },
    queueActivityPulse() {},
    timelineScopePhase: "phase",
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });
  return { mutations, updateCalls, toastCalls, viewState, renderCalls };
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

test("phase tracking is optimistic while saving and reconciles without replacing unrelated canonical fields", async () => {
  let resolvePersistence;
  const persistence = new Promise(resolve => { resolvePersistence = resolve; });
  const row = {
    jobKey: "job_1",
    title: "Original",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: { bookmark: "2026-03-08T08:00:00.000Z" },
    outcomeTimestamps: {}
  };
  const { mutations, viewState, renderCalls } = createMutationHarness(row, {
    updateApplicationTracking: async () => persistence
  });
  const operation = mutations.updatePhase("job_1", "applied");
  await Promise.resolve();

  assert.equal(viewState.pendingTrackingByJobKey.get("job_1")?.state, "pending");
  assert.equal(renderCalls.at(-1)[0].pipelinePhase, "bookmark", "the canonical input remains unchanged");

  const refreshedRow = { ...row, title: "Refreshed title", notes: "new canonical note" };
  viewState.lastSavedJobsByKey.set("job_1", refreshedRow);
  resolvePersistence({ ok: true });
  await operation;

  const reconciled = viewState.lastSavedJobsByKey.get("job_1");
  assert.equal(reconciled.pipelinePhase, "applied");
  assert.equal(reconciled.title, "Refreshed title");
  assert.equal(reconciled.notes, "new canonical note");
  assert.equal(viewState.pendingTrackingByJobKey.has("job_1"), false);
});

test("duplicate phase submissions are ignored while persistence is pending", async () => {
  let resolvePersistence;
  const persistence = new Promise(resolve => { resolvePersistence = resolve; });
  const { mutations, updateCalls, viewState } = createMutationHarness({
    jobKey: "job_1",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  }, {
    updateApplicationTracking: async () => persistence
  });

  const first = mutations.updatePhase("job_1", "applied");
  await Promise.resolve();
  await mutations.updatePhase("job_1", "offer");

  assert.equal(updateCalls.length, 1);
  assert.equal(viewState.pendingTrackingByJobKey.get("job_1")?.phase, "applied");
  resolvePersistence({ ok: true });
  await first;
});

test("duplicate override prompts are ignored and cancellation stays non-optimistic", async () => {
  let resolveReason;
  const reasonPrompt = new Promise(resolve => { resolveReason = resolve; });
  let promptCalls = 0;
  const { mutations, updateCalls, viewState } = createMutationHarness({
    jobKey: "job_1",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  }, {
    canTransition: () => false,
    requestTextInputDialog: async () => {
      promptCalls += 1;
      return reasonPrompt;
    }
  });
  viewState.trackingOverrideContext = {
    kind: "phase",
    jobKey: "job_1",
    phase: "offer"
  };

  const first = mutations.updatePhase("job_1", "offer", { overrideThisTransition: true });
  await Promise.resolve();
  await mutations.updatePhase("job_1", "offer", { overrideThisTransition: true });
  await Promise.resolve();
  assert.equal(promptCalls, 1);
  assert.equal(updateCalls.length, 0);
  assert.equal(viewState.pendingTrackingByJobKey.has("job_1"), false);

  resolveReason(null);
  await first;
  assert.equal(updateCalls.length, 0);
  assert.equal(viewState.lastSavedJobsByKey.get("job_1").pipelinePhase, "bookmark");
  assert.equal(viewState.pendingTrackingByJobKey.has("job_1"), false);
});

test("cancelled interview timestamp prompt leaves tracking unchanged and not pending", async () => {
  const { mutations, updateCalls, viewState } = createMutationHarness({
    jobKey: "job_1",
    pipelinePhase: "screening",
    outcomeStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  }, {
    needsInterviewTimestamp: phase => phase === "interview",
    requestInterviewTimestamp: async () => ""
  });

  await mutations.updatePhase("job_1", "interview");

  assert.equal(updateCalls.length, 0);
  assert.equal(viewState.pendingTrackingByJobKey.has("job_1"), false);
  assert.equal(viewState.lastSavedJobsByKey.get("job_1").pipelinePhase, "screening");
});

test("failed outcome tracking retains canonical outcome and exposes an error state", async () => {
  const row = {
    jobKey: "job_1",
    pipelinePhase: "offer",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  };
  const { viewState, renderCalls } = createMutationHarness(row);
  const mutations = createSavedMutations({
    viewState,
    phaseOptions: PIPELINE_PHASES,
    isPipelinePhaseRewind,
    clearFuturePhaseTimestamps,
    toApplicationStatusMirror,
    savedPageService: {
      async updateApplicationTracking() {
        return { ok: false, error: "Network unavailable" };
      }
    },
    normalizePhase: value => String(value || "bookmark"),
    normalizeOutcome: value => String(value || "active"),
    canTransition: () => true,
    canSetOutcome: () => true,
    needsInterviewTimestamp: () => false,
    requestInterviewTimestamp: async () => "",
    phaseLabels: {},
    outcomeLabels: { active: "Active", rejected: "Rejected" },
    refreshActivityLog: async () => {},
    renderSavedJobs: jobs => renderCalls.push(jobs),
    queueActivityPulse() {},
    timelineScopePhase: "phase",
    showToast() {}
  });

  await mutations.updateOutcome("job_1", "rejected");

  assert.equal(viewState.lastSavedJobsByKey.get("job_1").outcomeStatus, "active");
  assert.equal(viewState.pendingTrackingByJobKey.get("job_1")?.state, "error");
  assert.equal(viewState.pendingTrackingByJobKey.get("job_1")?.error, "Network unavailable");
  assert.equal(renderCalls.at(-1)[0].outcomeStatus, "active");
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
