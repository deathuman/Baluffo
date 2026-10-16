import test from "node:test";
import assert from "node:assert/strict";

import { createSavedMutations } from "../../../frontend/saved/app/runtime/mutations.js";
import {
  PIPELINE_PHASES,
  clearFuturePhaseTimestamps,
  isPipelinePhaseRewind,
  toApplicationStatusMirror
} from "../../../frontend/local-data/tracking.js";

function createDeferred() {
  let resolve;
  const promise = new Promise(res => { resolve = res; });
  return { promise, resolve };
}

function createTrackingHarness(row, options = {}) {
  const updateCalls = [];
  const toastCalls = [];
  const renderCalls = [];
  let activityRefreshCount = 0;
  const viewState = {
    currentUser: { uid: "u1" },
    phaseOverrideContext: null,
    trackingOverrideContext: null,
    pendingTrackingByJobKey: new Map(),
    lastSavedJobsByKey: new Map([[row.jobKey, row]])
  };
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
    phaseLabels: { bookmark: "Saved", applied: "Applied", interview_1: "Interview 1" },
    outcomeLabels: { active: "Active", rejected: "Rejected" },
    async refreshActivityLog() {
      activityRefreshCount += 1;
      if (options.refreshActivityLog) await options.refreshActivityLog();
    },
    renderSavedJobs(jobs) {
      renderCalls.push(jobs);
    },
    queueActivityPulse() {},
    timelineScopePhase: "phase",
    showToast(message, type, toastOptions = {}) {
      toastCalls.push({ message, type, options: toastOptions });
    }
  });
  return {
    mutations, updateCalls, toastCalls, renderCalls, viewState,
    get activityRefreshCount() { return activityRefreshCount; }
  };
}

function switchToSecondProfile(viewState, row = {
  jobKey: "job_1",
  pipelinePhase: "bookmark",
  outcomeStatus: "active",
  applicationStatus: "active",
  phaseTimestamps: {},
  outcomeTimestamps: {}
}) {
  viewState.currentUser = { uid: "u2" };
  viewState.lastSavedJobsByKey = new Map([[row.jobKey, row]]);
  viewState.pendingTrackingByJobKey = new Map();
  viewState.phaseOverrideContext = { kind: "phase", jobKey: row.jobKey, phase: "screening" };
  viewState.trackingOverrideContext = { kind: "outcome", jobKey: row.jobKey, outcomeStatus: "rejected" };
}

test("phase timestamp prompt completion is discarded after a profile switch", async () => {
  const timestampPrompt = createDeferred();
  const row = {
    jobKey: "job_1",
    pipelinePhase: "screening",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  };
  const { mutations, updateCalls, toastCalls, renderCalls, viewState } = createTrackingHarness(row, {
    needsInterviewTimestamp: phase => phase === "interview_1",
    requestInterviewTimestamp: () => timestampPrompt.promise
  });

  const operation = mutations.updatePhase("job_1", "interview_1");
  switchToSecondProfile(viewState);
  timestampPrompt.resolve("2026-04-02T12:00:00.000Z");
  await operation;

  assert.deepEqual(updateCalls, [], "the stale prompt must not start a persistence write");
  assert.equal(viewState.pendingTrackingByJobKey.size, 0);
  assert.equal(viewState.trackingOverrideContext.outcomeStatus, "rejected");
  assert.equal(renderCalls.length, 0, "the stale operation must not render over the new profile");
  assert.deepEqual(toastCalls, []);
});

test("outcome override prompt completion cannot write or clear the next profile context", async () => {
  const reasonPrompt = createDeferred();
  const row = {
    jobKey: "job_1",
    pipelinePhase: "offer",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  };
  const { mutations, updateCalls, toastCalls, renderCalls, viewState } = createTrackingHarness(row, {
    canSetOutcome: () => false,
    requestTextInputDialog: () => reasonPrompt.promise
  });

  await mutations.updateOutcome("job_1", "rejected");
  const priorRenderCount = renderCalls.length;
  const operation = mutations.updateOutcome("job_1", "rejected", { overrideThisTransition: true });
  switchToSecondProfile(viewState);
  reasonPrompt.resolve("user approved override");
  await operation;

  assert.deepEqual(updateCalls, [], "a stale override prompt must not write under another profile");
  assert.equal(viewState.pendingTrackingByJobKey.size, 0);
  assert.equal(viewState.trackingOverrideContext.outcomeStatus, "rejected");
  assert.equal(renderCalls.length, priorRenderCount, "the stale operation must not render over the new profile");
  assert.deepEqual(toastCalls, []);
});

test("phase persistence completion does not reconcile or refresh after a profile switch", async () => {
  const persistence = createDeferred();
  const row = {
    jobKey: "job_1",
    pipelinePhase: "bookmark",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  };
  const { mutations, updateCalls, toastCalls, renderCalls, viewState } = createTrackingHarness(row, {
    updateApplicationTracking: () => persistence.promise
  });

  const operation = mutations.updatePhase("job_1", "applied");
  await Promise.resolve();
  assert.equal(updateCalls[0][0], "u1", "the write stays bound to the initiating profile");
  const priorRenderCount = renderCalls.length;
  switchToSecondProfile(viewState);
  persistence.resolve({ ok: true });
  await operation;

  assert.equal(viewState.pendingTrackingByJobKey.size, 0);
  assert.equal(viewState.lastSavedJobsByKey.get("job_1").pipelinePhase, "bookmark");
  assert.equal(viewState.trackingOverrideContext.outcomeStatus, "rejected");
  assert.equal(renderCalls.length, priorRenderCount, "stale completion must not render");
  assert.deepEqual(toastCalls, []);
});

test("outcome persistence errors are silent after a profile switch", async () => {
  const persistence = createDeferred();
  const row = {
    jobKey: "job_1",
    pipelinePhase: "offer",
    outcomeStatus: "active",
    applicationStatus: "active",
    phaseTimestamps: {},
    outcomeTimestamps: {}
  };
  const { mutations, updateCalls, toastCalls, renderCalls, viewState } = createTrackingHarness(row, {
    updateApplicationTracking: () => persistence.promise
  });

  const operation = mutations.updateOutcome("job_1", "rejected");
  await Promise.resolve();
  assert.equal(updateCalls[0][0], "u1", "the write stays bound to the initiating profile");
  const priorRenderCount = renderCalls.length;
  switchToSecondProfile(viewState);
  persistence.resolve({ ok: false, error: "Old profile write failed" });
  await operation;

  assert.equal(viewState.pendingTrackingByJobKey.size, 0);
  assert.equal(viewState.lastSavedJobsByKey.get("job_1").outcomeStatus, "active");
  assert.equal(viewState.trackingOverrideContext.outcomeStatus, "rejected");
  assert.equal(renderCalls.length, priorRenderCount, "stale failure must not render");
  assert.deepEqual(toastCalls, [], "stale failure must not notify the next profile");
});
