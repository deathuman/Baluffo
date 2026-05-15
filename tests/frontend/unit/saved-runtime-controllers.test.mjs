import test from "node:test";
import assert from "node:assert/strict";

import { createSavedAuthController } from "../../../frontend/saved/app/runtime/auth-controller.js";
import { createSavedBoot } from "../../../frontend/saved/app/runtime/boot.js";
import { createSavedCustomJobController } from "../../../frontend/saved/app/runtime/custom-job-controller.js";
import {
  createButton,
  createElement
} from "./helpers/saved-runtime-helpers.mjs";

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
  const perfCalls = [];
  const firstRenderCalls = [];

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
      markSavedStep(name, payload = {}) {
        perfCalls.push({ type: "mark", name, payload });
      },
      measureSavedStep(name, startMark, endMark, payload = {}) {
        perfCalls.push({ type: "measure", name, startMark, endMark, payload });
      },
      markSavedFirstRender(stage, rowCount = 0) {
        firstRenderCalls.push({ stage, rowCount });
      },
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
    assert.deepEqual(perfCalls.map(item => `${item.type}:${item.name}`), [
      "mark:saved_auth_init_start",
      "mark:saved_auth_init_end",
      "measure:saved_auth_init"
    ]);
    assert.deepEqual(firstRenderCalls, [{ stage: "auth_restoring", rowCount: 0 }]);

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

test("saved auth controller marks waiting init path", () => {
  const perfCalls = [];
  const firstRenderCalls = [];
  const refs = {
    signInBtnEl: createButton(),
    signOutBtnEl: createButton()
  };

  const controller = createSavedAuthController({
    refs,
    viewState: {
      currentUser: null,
      unsubscribeSavedJobs() {},
      jobDetailTabByKey: new Map(),
      cachedActivityEntries: [],
      lastSavedJobsByKey: new Map(),
      selectedJobKey: "",
      timelineScope: "all"
    },
    savedPageService: {
      isAvailable: () => false
    },
    savedAuthService: {
      onAuthStateChanged() {}
    },
    savedAuthReadyPoller: {
      schedulePoll() {},
      stopPoll() {}
    },
    isSavedApiReady: () => false,
    savedDispatch: { dispatch() {} },
    SAVED_ACTIONS: { AUTH_CHANGED: "auth_changed" },
    clearNoteSaveQueues() {},
    setActivityPanelOpen() {},
    setCustomJobPanelOpen() {},
    setCustomJobAvailability() {},
    updateTimelineScopeButtons() {},
    renderWorkspaceStats() {},
    emitSavedStartupMetric() {},
    markSavedStep(name, payload = {}) {
      perfCalls.push({ type: "mark", name, payload });
    },
    measureSavedStep(name, startMark, endMark, payload = {}) {
      perfCalls.push({ type: "measure", name, startMark, endMark, payload });
    },
    markSavedFirstRender(stage, rowCount = 0) {
      firstRenderCalls.push({ stage, rowCount });
    },
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
    showToast() {}
  });

  controller.initSavedJobsPage();

  assert.deepEqual(perfCalls.map(item => `${item.type}:${item.name}`), [
    "mark:saved_auth_init_start",
    "mark:saved_auth_init_end",
    "measure:saved_auth_init"
  ]);
  assert.deepEqual(perfCalls.at(-2).payload, { waiting: true });
  assert.deepEqual(firstRenderCalls, [{ stage: "auth_waiting", rowCount: 0 }]);
});

test("saved boot marks boot and DOM cache milestones", () => {
  const originalWindow = globalThis.window;
  const perfCalls = [];
  globalThis.window = {
    setInterval() {
      return 1;
    },
    clearInterval() {}
  };
  const dom = {};
  const viewState = {};
  const element = createButton({
    addEventListener() {},
    removeEventListener() {}
  });

  try {
    const boot = createSavedBoot({
      adminBridgeBase: "http://127.0.0.1:8877",
      startupMetrics: {
        emit(event, payload = {}) {
          perfCalls.push({ type: "metric", name: event, payload });
        }
      },
      dom,
      viewState,
      noteSaveState: {
        inFlight: new Map(),
        pendingValues: new Map(),
        lastInteractionAt: 0
      },
      savedPageService: {
        subscribeSavedJobs() {
          return () => {};
        }
      },
      savedAuthController: {
        initSavedJobsPage() {}
      },
      applySavedAdminBridgeState() {},
      cssEscape: value => value,
      setSelectedJobKey() {},
      removeSavedJob() {},
      updatePhase() {},
      toggleDetailsForJob() {},
      openCustomJobEditor() {},
      setJobDetailsTab() {},
      applyJobDetailsTab() {},
      refreshActivityLog: async () => {},
      renderSavedJobs() {},
      loadSavedLifecycleOverlay: async () => new Map(),
      queueNotesSave() {},
      flushNotesSave() {},
      uploadAttachments() {},
      getLastJobsUrl() {},
      navigateDesktopPage() {},
      defaultSavedFilter: "all",
      defaultSavedSort: "updated",
      timelineScopeAll: "all",
      setCustomJobPanelOpen() {},
      createCustomJob() {},
      updateCustomJobWarning() {},
      setSavedFilter() {},
      setSavedSort() {},
      setActivityPanelOpen() {},
      setTimelineScope() {},
      renderTimeline() {},
      renderWorkspaceStats() {},
      renderSelectedJobHint() {},
      renderAuthRequired() {},
      setSourceStatus() {},
      isDesktopRuntimeMode: () => false,
      documentObject: {
        querySelector() {
          return element;
        },
        querySelectorAll() {
          return [];
        }
      }
    });

    boot.bootSavedPage();
    viewState.adminBridgeWatcher?.stopAdminBridgeButtonWatch();
  } finally {
    globalThis.window = originalWindow;
  }

  assert.deepEqual(
    perfCalls
      .filter(item => item.type === "metric")
      .map(item => item.name),
    [
      "saved_boot_start",
      "saved_dom_cache_start",
      "saved_dom_cache_end",
      "saved_dom_cache",
      "saved_boot_end",
      "saved_boot"
    ]
  );
});
