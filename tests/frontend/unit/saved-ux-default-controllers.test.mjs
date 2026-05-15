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
    canTransition: () => false,
    async requestConfirmationDialog(params) {
      confirmationCalls.push(params);
      return true;
    },
    needsInterviewTimestamp: () => false,
    requestInterviewTimestamp: async () => "",
    phaseLabels: { bookmark: "Saved", offer: "Final Round" },
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
    jobKey: "job_1",
    phase: "offer",
    fromPhase: "bookmark"
  });
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
    options: { override: true }
  });
  assert.equal(viewState.phaseOverrideContext, null);
  assert.equal(toastCalls.at(-1).options.actionLabel, "Revert");
});
