import test from "node:test";
import assert from "node:assert/strict";

import { createSavedActivityController } from "../../../frontend/saved/app/runtime/activity-controller.js";
import { createSavedAuthController } from "../../../frontend/saved/app/runtime/auth-controller.js";
import { createSavedCustomJobController } from "../../../frontend/saved/app/runtime/custom-job-controller.js";
import {
  createButton,
  createElement
} from "./helpers/saved-runtime-helpers.mjs";

test("saved activity controller falls back selected scope when no job is selected", () => {
  const allScopeBtn = createButton({ dataset: { timelineScope: "all" } });
  const selectedScopeBtn = createButton({ dataset: { timelineScope: "selected" } });
  const dom = {
    activityScopeBtnEls: [allScopeBtn, selectedScopeBtn],
    activityPanelEl: createElement(),
    historyPanelToggleBtnEl: createButton(),
    activitySelectedJobEl: createElement(),
    savedMetricTotalEl: createElement(),
    savedMetricRemindersEl: createElement(),
    savedMetricActivityEl: createElement(),
    activityPanelStatusEl: createElement(),
    activityPanelBodyEl: createElement()
  };
  const viewState = {
    activityPanelOpen: false,
    timelineScope: "all",
    selectedJobKey: "",
    currentUser: null,
    lastSavedJobsByKey: new Map(),
    cachedActivityEntries: [],
    lastActivityPulse: null
  };
  const controller = createSavedActivityController({
    dom,
    viewState,
    savedPageService: {
      isAvailable: () => true,
      listActivityForUser: async () => ({ ok: true, data: [] })
    },
    setActivityStatus: () => {},
    timelinePrefPrefix: "test_saved_timeline",
    timelineScopeAll: "all",
    activityHighlightMs: 20,
    renderActivityEntryHtml: () => "<div></div>",
    getReminderMeta: () => ({ isSoon: false }),
    loadSavedTimelinePreferences: () => ({ visible: false, scope: "all" }),
    persistSavedTimelinePreferences: () => {},
    activityTypeLabel: () => "activity",
    formatActivityDetail: () => "detail",
    formatPhaseTimestamp: () => ""
  });

  controller.setTimelineScope("selected");

  assert.equal(viewState.timelineScope, "all");
  assert.equal(allScopeBtn.classList.contains("active"), true);
  assert.equal(selectedScopeBtn.disabled, true);
});

test("saved custom job controller resets form chrome when closing the panel", () => {
  let resetCount = 0;
  const dom = {
    customJobFormEl: {
      reset() {
        resetCount += 1;
      }
    },
    customJobWorkTypeEl: createElement({ value: "remote" }),
    customJobContractTypeEl: createElement({ value: "contract" }),
    customJobSectorEl: createElement({ value: "game" }),
    customJobReminderEl: createElement({ value: "2026-04-21T10:00" }),
    customJobPanelEl: createElement(),
    addCustomJobBtnEl: createButton(),
    customJobPanelTitleEl: createElement(),
    customJobPanelHintEl: createElement(),
    customJobSaveBtnEl: createButton(),
    customJobLinkEl: createElement({ value: "" }),
    customJobLinkWarningEl: createElement()
  };
  const viewState = {
    customJobMode: "edit",
    customJobTargetKey: "job-1",
    customJobPanelOpen: false,
    lastSavedJobsByKey: new Map(),
    currentUser: null
  };
  const controller = createSavedCustomJobController({
    dom,
    viewState,
    savedPageService: {
      isAvailable: () => false
    },
    normalizeCustomJobInput: values => values,
    toDatetimeLocalValue: value => String(value || ""),
    savedDispatch: { dispatch() {} },
    savedActions: { CUSTOM_JOB_MUTATED: "custom_job_mutated" },
    queueActivityPulse() {},
    timelineScopeAll: "all",
    refreshActivityLog: async () => {}
  });

  controller.setCustomJobPanelOpen(true);
  controller.setCustomJobPanelOpen(false);

  assert.equal(resetCount, 1);
  assert.equal(viewState.customJobMode, "create");
  assert.equal(viewState.customJobTargetKey, "");
  assert.equal(dom.customJobPanelEl.classList.contains("hidden"), true);
  assert.equal(dom.customJobPanelEl.attributes["aria-hidden"], "true");
  assert.equal(dom.addCustomJobBtnEl.textContent, "+ Add Custom Job");
  assert.equal(dom.customJobPanelTitleEl.textContent, "Add Custom Job");
  assert.equal(dom.customJobSaveBtnEl.textContent, "Save Custom Job");
});

test("saved auth controller delays the initial guest render while desktop auth restores", () => {
  const originalWindow = globalThis.window;
  let pendingTimer = null;
  globalThis.window = {
    setTimeout(fn) {
      pendingTimer = fn;
      return 1;
    },
    clearTimeout() {
      pendingTimer = null;
    },
    localStorage: {
      getItem(key) {
        return key === "baluffo_current_profile_id" ? "local_packaged_smoke_user" : "";
      }
    }
  };

  let authListener = null;
  let sourceStatus = "";
  let activityStatus = "";
  let authRequired = "";
  let subscribedUid = "";

  const refs = {
    savedAuthStatusEl: createElement(),
    savedAuthStatusHintEl: createElement(),
    savedAuthAvatarEl: createElement(),
    signInBtnEl: createButton(),
    signOutBtnEl: createButton(),
    jobsPageBtnEl: createButton(),
    addCustomJobBtnEl: createButton()
  };
  const viewState = {
    currentUser: null,
    unsubscribeSavedJobs() {},
    expandedJobKey: null,
    phaseOverrideArmedGlobal: false,
    jobDetailTabByKey: new Map(),
    cachedActivityEntries: [],
    lastSavedJobsByKey: new Map(),
    selectedJobKey: "",
    timelineScope: "all",
    lastActivityPulse: null,
    savedAuthListenerBound: false
  };

  try {
    const controller = createSavedAuthController({
      refs,
      viewState,
      savedPageService: {
        isAvailable: () => true
      },
      savedAuthService: {
        onAuthStateChanged(callback) {
          authListener = callback;
          callback(null);
          return () => {};
        }
      },
      savedAuthReadyPoller: {
        schedulePoll() {},
        stopPoll() {}
      },
      isSavedApiReady: () => true,
      savedDispatch: { dispatch() {} },
      SAVED_ACTIONS: { AUTH_CHANGED: "auth_changed" },
      clearNoteSaveQueues() {},
      setActivityPanelOpen() {},
      setCustomJobPanelOpen() {},
      setCustomJobAvailability() {},
      updateTimelineScopeButtons() {},
      renderWorkspaceStats() {},
      emitSavedStartupMetric() {},
      setSourceStatus(value) {
        sourceStatus = String(value || "");
      },
      setActivityStatus(value) {
        activityStatus = String(value || "");
      },
      renderAuthRequired(value) {
        authRequired = String(value || "");
      },
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
      subscribeToSavedJobs(uid) {
        subscribedUid = uid;
      },
      refreshActivityLog: async () => {},
      timelineScopeAll: "all",
      showToast() {}
    });

    controller.initSavedJobsPage();

    assert.equal(refs.savedAuthStatusEl.textContent, "Restoring profile...");
    assert.equal(sourceStatus, "Restoring your saved jobs...");
    assert.equal(activityStatus, "Restoring activity...");
    assert.equal(authRequired, "Restoring your local profile. Please wait...");
    assert.equal(typeof authListener, "function");
    assert.equal(typeof pendingTimer, "function");
    assert.equal(subscribedUid, "");

    authListener({
      uid: "local_packaged_smoke_user",
      displayName: "Packaged Smoke User"
    });

    assert.equal(pendingTimer, null);
    assert.match(String(refs.savedAuthStatusEl.textContent || ""), /Packaged Smoke User/);
    assert.equal(sourceStatus, "Loading your saved jobs...");
    assert.equal(activityStatus, "Loading activity...");
    assert.equal(subscribedUid, "local_packaged_smoke_user");
  } finally {
    globalThis.window = originalWindow;
  }
});
