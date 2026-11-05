import test from "node:test";
import assert from "node:assert/strict";

import { createSavedAuthController } from "../../../frontend/saved/app/runtime/auth-controller.js";
import { savedPageService } from "../../../frontend/saved/services.js";
import { createButton } from "./helpers/saved-runtime-helpers.mjs";

function createAuthHarness({
  isAvailable = () => false,
  isSavedApiReady = () => false,
  onAuthStateChanged = () => {},
  finishSavedWorkspaceBoot = () => {}
} = {}) {
  const prefetched = [];
  const refs = {
    signInBtnEl: createButton(),
    signOutBtnEl: createButton()
  };
  const viewState = {
    currentUser: null,
    unsubscribeSavedJobs() {},
    jobDetailTabByKey: new Map(),
    cachedActivityEntries: [],
    lastSavedJobsByKey: new Map(),
    selectedJobKey: "",
    timelineScope: "all"
  };
  const controller = createSavedAuthController({
    refs,
    viewState,
    savedPageService: {
      isAvailable,
      prefetchSavedJobs(uid) {
        prefetched.push(uid);
      }
    },
    savedAuthService: {
      onAuthStateChanged
    },
    savedAuthReadyPoller: {
      schedulePoll() {},
      stopPoll() {}
    },
    isSavedApiReady,
    savedDispatch: { dispatch() {} },
    SAVED_ACTIONS: { AUTH_CHANGED: "auth_changed" },
    clearNoteSaveQueues() {},
    setActivityPanelOpen() {},
    setCustomJobPanelOpen() {},
    setCustomJobAvailability() {},
    setSavedGroupBarVisible() {},
    updateTimelineScopeButtons() {},
    renderWorkspaceStats() {},
    emitSavedStartupMetric() {},
    setSourceStatus() {},
    setActivityStatus() {},
    renderAuthRequired() {},
    renderTimeline() {},
    markSavedFirstInteractive() {},
    setSavedFilter() {},
    defaultSavedFilter: "all",
    setSavedSort() {},
    defaultSavedSort: "updated",
    renderSelectedJobHint() {},
    setBackupButtonsEnabled() {},
    setSavedFilterBarVisible() {},
    setSavedSortBarVisible() {},
    loadTimelinePreferences: () => ({ scope: "all" }),
    subscribeToSavedJobs() {},
    refreshActivityLog: async () => {},
    timelineScopeAll: "all",
    showToast() {},
    finishSavedWorkspaceBoot
  });
  return { controller, prefetched };
}

function withWindow(storageValue, callback) {
  const originalWindow = globalThis.window;
  globalThis.window = {
    localStorage: {
      getItem(key) {
        return key === "baluffo_current_profile_id" ? storageValue : "";
      }
    }
  };
  try {
    return callback();
  } finally {
    globalThis.window = originalWindow;
  }
}

test("saved auth controller clears the startup workspace reservation for guests", () => {
  let finishCount = 0;
  withWindow("", () => {
    const { controller } = createAuthHarness({
      isAvailable: () => true,
      isSavedApiReady: () => true,
      onAuthStateChanged(callback) {
        callback(null);
        return () => {};
      },
      finishSavedWorkspaceBoot() {
        finishCount += 1;
      }
    });

    controller.initSavedJobsPage();
  });

  assert.equal(finishCount, 1);
});

test("saved auth controller prefetches the persisted hint while auth restores", () => {
  withWindow("local_hint", () => {
    const { controller, prefetched } = createAuthHarness();

    controller.initSavedJobsPage();

    assert.deepEqual(prefetched, ["local_hint"]);
  });
});

test("saved page service waits for the local data API before prefetching", async () => {
  const originalWindow = globalThis.window;
  const prefetched = [];
  globalThis.window = {};
  try {
    const pending = savedPageService.prefetchSavedJobs("local_hint");
    setTimeout(() => {
      globalThis.window.JobAppLocalData = {
        prefetchSavedJobs(uid) {
          prefetched.push(uid);
          return Promise.resolve([{ jobKey: "job_1" }]);
        }
      };
    }, 10);

    assert.deepEqual(await pending, [{ jobKey: "job_1" }]);
    assert.deepEqual(prefetched, ["local_hint"]);
  } finally {
    globalThis.window = originalWindow;
  }
});
