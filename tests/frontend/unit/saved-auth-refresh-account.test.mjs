import test from "node:test";
import assert from "node:assert/strict";

import { createSavedAuthController } from "../../../frontend/saved/app/runtime/auth-controller.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createHarness(refreshActivityLog) {
  let authListener;
  const activityStatuses = [];
  const viewState = {
    currentUser: null,
    unsubscribeSavedJobs() {},
    expandedJobKey: null,
    jobDetailTabByKey: new Map(),
    cachedActivityEntries: [],
    lastSavedJobsByKey: new Map(),
    savedWorkspaceStatsReady: false,
    savedLifecycleOverlayByJobKey: new Map(),
    savedLifecycleOverlayRequestId: 0,
    selectedJobKey: "",
    phaseOverrideContext: null,
    trackingOverrideContext: null,
    pendingTrackingByJobKey: new Map(),
    loadedAttachmentJobKeys: new Set(),
    loadingAttachmentJobKeys: new Set(),
    timelineScope: "all",
    lastActivityPulse: null,
    savedAuthListenerBound: false
  };
  const noop = () => {};
  const controller = createSavedAuthController({
    refs: {},
    viewState,
    savedPageService: { isAvailable: () => true },
    savedAuthService: {
      onAuthStateChanged(callback) {
        authListener = callback;
        return noop;
      }
    },
    savedAuthReadyPoller: { schedulePoll: noop, stopPoll: noop },
    isSavedApiReady: () => true,
    savedDispatch: { dispatch: noop },
    SAVED_ACTIONS: { AUTH_CHANGED: "auth_changed" },
    clearNoteSaveQueues: noop,
    setActivityPanelOpen: noop,
    setCustomJobPanelOpen: noop,
    setCustomJobAvailability: noop,
    updateTimelineScopeButtons: noop,
    renderWorkspaceStats: noop,
    emitSavedStartupMetric: noop,
    setSourceStatus: noop,
    setActivityStatus: value => activityStatuses.push(value),
    renderAuthRequired: noop,
    renderTimeline: noop,
    markSavedFirstInteractive: noop,
    setSavedFilter: noop,
    defaultSavedFilter: "all",
    setSavedSort: noop,
    defaultSavedSort: "updated",
    renderSelectedJobHint: noop,
    setBackupButtonsEnabled: noop,
    setSavedFilterBarVisible: noop,
    setSavedSortBarVisible: noop,
    loadTimelinePreferences: () => ({ scope: "all" }),
    subscribeToSavedJobs: noop,
    refreshActivityLog,
    timelineScopeAll: "all",
    showToast: noop
  });
  controller.initSavedJobsPage();
  return {
    onAuthStateChanged: user => authListener(user),
    viewState,
    activityStatuses
  };
}

async function flushMicrotasks() {
  await Promise.resolve();
  await Promise.resolve();
}

test("auth controller suppresses stale activity refresh errors after profile switch", async () => {
  const firstRefresh = deferred();
  const refreshStarted = deferred();
  let refreshCount = 0;
  const loggedErrors = [];
  const originalError = console.error;
  console.error = (...args) => loggedErrors.push(args);
  const harness = createHarness(() => {
    refreshCount += 1;
    if (refreshCount === 1) {
      refreshStarted.resolve();
      return firstRefresh.promise;
    }
    return Promise.resolve();
  });

  try {
    harness.onAuthStateChanged({ uid: "user-1" });
    await refreshStarted.promise;
    harness.onAuthStateChanged({ uid: "user-2" });
    firstRefresh.reject(new Error("activity refresh failed"));
    await flushMicrotasks();
  } finally {
    console.error = originalError;
  }

  assert.equal(harness.viewState.currentUser.uid, "user-2");
  assert.equal(refreshCount, 2);
  assert.equal(loggedErrors.length, 0);
  assert.equal(harness.activityStatuses.includes("Could not load activity."), false);
});

test("auth controller reports activity refresh errors for the active profile", async () => {
  const refreshError = new Error("activity refresh failed");
  const loggedErrors = [];
  const originalError = console.error;
  console.error = (...args) => loggedErrors.push(args);
  const harness = createHarness(async () => {
    throw refreshError;
  });

  try {
    harness.onAuthStateChanged({ uid: "user-1" });
    await flushMicrotasks();
  } finally {
    console.error = originalError;
  }

  assert.equal(harness.viewState.currentUser.uid, "user-1");
  assert.deepEqual(loggedErrors, [["Failed to load activity:", refreshError]]);
  assert.equal(harness.activityStatuses.includes("Could not load activity."), true);
});
