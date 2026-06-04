import test from "node:test";
import assert from "node:assert/strict";

import { createSavedActivityController } from "../../../frontend/saved/app/runtime/activity-controller.js";
import { createSavedMutations } from "../../../frontend/saved/app/runtime/mutations.js";
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

test("saved activity controller hides workspace stats until profile rows load", () => {
  const savedWorkspaceStripEl = createElement({ hidden: true });
  const dom = {
    activityScopeBtnEls: [],
    activityPanelEl: createElement(),
    historyPanelToggleBtnEl: createButton(),
    activitySelectedJobEl: createElement(),
    savedWorkspaceStripEl,
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
    savedWorkspaceStatsReady: false,
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

  controller.renderWorkspaceStats();

  assert.equal(savedWorkspaceStripEl.hidden, true);
  assert.equal(savedWorkspaceStripEl.classList.contains("hidden"), true);
  assert.equal(savedWorkspaceStripEl.attributes["aria-hidden"], "true");

  viewState.currentUser = { uid: "u1" };
  controller.renderWorkspaceStats([]);

  assert.equal(savedWorkspaceStripEl.hidden, false);
  assert.equal(savedWorkspaceStripEl.classList.contains("hidden"), false);
  assert.equal(savedWorkspaceStripEl.attributes["aria-hidden"], "false");
  assert.equal(dom.savedMetricTotalEl.textContent, "0");
});

test("saved activity controller opens to selected scope when a job is selected", () => {
  const allScopeBtn = createButton({ dataset: { timelineScope: "all" } });
  const selectedScopeBtn = createButton({ dataset: { timelineScope: "selected" } });
  const historyPanelToggleBtnEl = createButton();
  const dom = {
    activityScopeBtnEls: [allScopeBtn, selectedScopeBtn],
    activityPanelEl: createElement(),
    historyPanelToggleBtnEl,
    activitySelectedJobEl: createElement(),
    savedMetricTotalEl: createElement(),
    savedMetricRemindersEl: createElement(),
    savedMetricActivityEl: createElement(),
    activityRecentBadgeEl: createElement(),
    activityPanelStatusEl: createElement(),
    activityPanelBodyEl: createElement()
  };
  const viewState = {
    activityPanelOpen: false,
    timelineScope: "all",
    selectedJobKey: "job_1",
    currentUser: { uid: "u1" },
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

  controller.setActivityPanelOpen(true);

  assert.equal(viewState.activityPanelOpen, true);
  assert.equal(viewState.timelineScope, "selected");
  assert.equal(selectedScopeBtn.classList.contains("active"), true);
  assert.equal(historyPanelToggleBtnEl.attributes["aria-expanded"], "true");
});

test("saved mutations confirm before removing a saved job", async () => {
  const confirmationCalls = [];
  const removeCalls = [];
  const toastCalls = [];
  const viewState = {
    currentUser: { uid: "u1" },
    phaseOverrideContext: { jobKey: "job_1" },
    trackingOverrideContext: { jobKey: "job_1" },
    lastSavedJobsByKey: new Map([
      ["job_1", {
        jobKey: "job_1",
        title: "Gameplay Engineer",
        company: "Studio"
      }]
    ])
  };
  const mutations = createSavedMutations({
    viewState,
    savedPageService: {
      async removeSavedJobForUser(...args) {
        removeCalls.push(args);
        return { ok: true };
      }
    },
    async requestConfirmationDialog(params) {
      confirmationCalls.push(params);
      return true;
    },
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });

  await mutations.removeSavedJob("job_1");

  assert.equal(confirmationCalls.length, 1);
  assert.equal(confirmationCalls[0].title, "Remove saved job?");
  assert.equal(confirmationCalls[0].confirmLabel, "Remove job");
  assert.match(confirmationCalls[0].description, /Gameplay Engineer at Studio/);
  assert.deepEqual(removeCalls, [["u1", "job_1"]]);
  assert.equal(viewState.phaseOverrideContext, null);
  assert.equal(viewState.trackingOverrideContext, null);
  assert.equal(toastCalls[0].message, "Removed saved job.");
  assert.equal(toastCalls[0].type, "success");
  assert.equal(toastCalls[0].options.actionLabel, "Undo");
});

test("saved mutations do not remove a saved job when confirmation is cancelled", async () => {
  const confirmationCalls = [];
  const removeCalls = [];
  const toastCalls = [];
  const viewState = {
    currentUser: { uid: "u1" },
    phaseOverrideContext: { jobKey: "job_1" },
    trackingOverrideContext: { jobKey: "job_1" },
    lastSavedJobsByKey: new Map([
      ["job_1", {
        jobKey: "job_1",
        title: "Gameplay Engineer",
        company: "Studio"
      }]
    ])
  };
  const mutations = createSavedMutations({
    viewState,
    savedPageService: {
      async removeSavedJobForUser(...args) {
        removeCalls.push(args);
        return { ok: true };
      }
    },
    async requestConfirmationDialog(params) {
      confirmationCalls.push(params);
      return false;
    },
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });

  await mutations.removeSavedJob("job_1");

  assert.equal(confirmationCalls.length, 1);
  assert.deepEqual(removeCalls, []);
  assert.deepEqual(toastCalls, []);
  assert.deepEqual(viewState.phaseOverrideContext, { jobKey: "job_1" });
  assert.deepEqual(viewState.trackingOverrideContext, { jobKey: "job_1" });
});

test("saved mutations require contextual override before persisting locked phase", async () => {
  const updateCalls = [];
  const confirmationCalls = [];
  const renderCalls = [];
  const toastCalls = [];
  const viewState = {
    currentUser: { uid: "u1" },
    phaseOverrideContext: null,
    lastSavedJobsByKey: new Map([
      ["job_1", {
        jobKey: "job_1",
        applicationStatus: "bookmark",
        phaseTimestamps: {}
      }]
    ])
  };
  const mutations = createSavedMutations({
    viewState,
    savedPageService: {
      async updateApplicationStatus(uid, jobKey, phase, options) {
        updateCalls.push({ uid, jobKey, phase, options });
        return { ok: true };
      }
    },
    normalizePhase: value => String(value || "bookmark"),
    normalizeOutcome: value => (String(value || "").trim() === "rejected" ? "rejected" : "active"),
    canTransition: () => false,
    canSetOutcome: () => true,
    async requestConfirmationDialog(params) {
      confirmationCalls.push(params);
      return true;
    },
    needsInterviewTimestamp: () => false,
    requestInterviewTimestamp: async () => "",
    phaseLabels: { bookmark: "Saved", offer: "Final Round" },
    outcomeLabels: { active: "Active" },
    refreshActivityLog: async () => {},
    renderSavedJobs(rows) {
      renderCalls.push(rows.length);
    },
    queueActivityPulse() {},
    timelineScopePhase: "phase",
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });

  await mutations.updatePhase("job_1", "offer");

  assert.deepEqual(viewState.phaseOverrideContext, {
    kind: "phase",
    jobKey: "job_1",
    phase: "offer",
    fromPhase: "bookmark",
    fromOutcome: "active"
  });
  assert.deepEqual(viewState.trackingOverrideContext, viewState.phaseOverrideContext);
  assert.equal(updateCalls.length, 0);
  assert.equal(confirmationCalls.length, 0);
  assert.deepEqual(renderCalls, [1]);

  await mutations.updatePhase("job_1", "offer", { overrideThisTransition: true });

  assert.equal(confirmationCalls.length, 1);
  assert.equal(updateCalls.length, 1);
  assert.deepEqual(updateCalls[0], {
    uid: "u1",
    jobKey: "job_1",
    phase: "offer",
    options: { override: true, overrideReason: "" }
  });
  assert.equal(viewState.phaseOverrideContext, null);
  assert.equal(viewState.trackingOverrideContext, null);
  assert.equal(toastCalls.at(-1).options.actionLabel, "Revert");
});
