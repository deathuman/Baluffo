import test from "node:test";
import assert from "node:assert/strict";

import { createJobsAuthController } from "../../../frontend/jobs/app/runtime/auth-controller.js";

function createController({ userState, jobsSavedJobsService, applyFiltersAndRender = () => {}, showToast = () => {}, logJobsError = () => {} }) {
  return createJobsAuthController({
    refs: {},
    userState,
    authReadyPoller: { stopPoll() {}, schedulePoll() {} },
    jobsAuthService: {},
    jobsSavedJobsService,
    jobsPageService: { isAvailable: () => true },
    jobsDispatch: { dispatch() {} },
    JOBS_ACTIONS: { SAVE_TOGGLED: "save_toggled" },
    isJobsApiReady: () => true,
    emitDesktopStartupMetric() {},
    showToast,
    logJobsError,
    getAllJobs: () => [{ id: "job-1" }],
    applyFiltersAndRender,
    openJobsCacheDb: async () => null,
    JOBS_SEEN_STORE: "jobs_seen",
    loadSeenJobKeys: async () => new Set(),
    markSeenJob: async () => {},
    buildSeenRowKey: value => String(value || ""),
    getJobKeyForJob: job => String(job?.id || ""),
    toJobSnapshot: job => job,
    sanitizeUrl: value => String(value || "")
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createUserState(savedJobKeys = []) {
  return {
    currentUser: { uid: "user-1" },
    savedJobKeys: new Set(savedJobKeys),
    pendingSavedJobActions: new Map(),
    seenJobKeys: new Set(),
    authStateListenerBound: false
  };
}

test("Jobs save marks pending and ignores duplicate requests for the same job", async () => {
  const request = deferred();
  const userState = createUserState();
  let saveCalls = 0;
  const renderSnapshots = [];
  const controller = createController({
    userState,
    jobsSavedJobsService: {
      saveJobForUser() {
        saveCalls += 1;
        return request.promise;
      }
    },
    applyFiltersAndRender: () => {
      renderSnapshots.push({
        pendingAction: userState.pendingSavedJobActions.get("job-1"),
        isSaved: userState.savedJobKeys.has("job-1")
      });
    }
  });

  const firstSave = controller.toggleSaveJob({ id: "job-1" });
  const duplicateSave = controller.toggleSaveJob({ id: "job-1" });

  assert.equal(saveCalls, 1);
  assert.deepEqual(renderSnapshots[0], { pendingAction: "saving", isSaved: false });
  assert.equal(userState.savedJobKeys.has("job-1"), false);
  assert.equal(userState.pendingSavedJobActions.get("job-1"), "saving");

  request.resolve({ ok: true });
  await Promise.all([firstSave, duplicateSave]);

  assert.equal(userState.savedJobKeys.has("job-1"), true);
  assert.equal(userState.pendingSavedJobActions.has("job-1"), false);
  assert.deepEqual(renderSnapshots.at(-1), { pendingAction: undefined, isSaved: true });
});

test("Jobs save failure clears pending state and preserves the saved state", async () => {
  const request = deferred();
  const userState = createUserState();
  const errors = [];
  const toasts = [];
  const controller = createController({
    userState,
    jobsSavedJobsService: { saveJobForUser: () => request.promise },
    showToast: (...args) => toasts.push(args),
    logJobsError: (...args) => errors.push(args)
  });

  const saving = controller.toggleSaveJob({ id: "job-1" });
  assert.equal(userState.pendingSavedJobActions.get("job-1"), "saving");
  request.reject(new Error("offline"));
  await saving;

  assert.equal(userState.savedJobKeys.has("job-1"), false);
  assert.equal(userState.pendingSavedJobActions.has("job-1"), false);
  assert.equal(errors.length, 1);
  assert.deepEqual(toasts.at(-1), ["Could not update saved jobs right now.", "error"]);
});

test("Jobs remove failure clears pending state and leaves the job saved", async () => {
  const request = deferred();
  const userState = createUserState(["job-1"]);
  const controller = createController({
    userState,
    jobsSavedJobsService: { removeSavedJobForUser: () => request.promise }
  });

  const removing = controller.toggleSaveJob({ id: "job-1" });
  assert.equal(userState.pendingSavedJobActions.get("job-1"), "removing");
  request.resolve({ ok: false, error: "offline" });
  await removing;

  assert.equal(userState.savedJobKeys.has("job-1"), true);
  assert.equal(userState.pendingSavedJobActions.has("job-1"), false);
});
