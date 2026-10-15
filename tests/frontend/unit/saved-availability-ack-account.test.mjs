import test from "node:test";
import assert from "node:assert/strict";

import { createSavedBoot } from "../../../frontend/saved/app/runtime/boot.js";
import { createButton, createElement } from "./helpers/saved-runtime-helpers.mjs";

function deferred() {
  let resolve;
  const promise = new Promise(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createHarness({ currentUser, acknowledgeAvailabilityAttention }) {
  let listClick;
  let acknowledgeAllClick;
  const savedJobsListEl = createElement({
    addEventListener(name, listener) {
      if (name === "click") listClick = listener;
    }
  });
  const acknowledgeAllButton = createButton({
    addEventListener(name, listener) {
      if (name === "click") acknowledgeAllClick = listener;
    }
  });
  const job = {
    jobKey: "job-1",
    availabilityAttention: {
      events: [{ transitionId: "transition-1", alert: true, acknowledgedAt: "" }]
    }
  };
  const viewState = { currentUser, lastSavedJobsByKey: new Map([[job.jobKey, job]]) };
  const toasts = [];
  let refreshCount = 0;
  const boot = createSavedBoot({
    startupMetrics: null,
    adminBridgeBase: "",
    documentObject: {
      querySelector(selector) {
        if (selector === '[data-ui="saved-jobs-list"]') return savedJobsListEl;
        if (selector === '[data-ui="saved-availability-attention-ack-all-btn"]') return acknowledgeAllButton;
        return null;
      },
      querySelectorAll() { return []; }
    },
    dom: {},
    viewState,
    noteSaveState: { inFlight: new Map(), pendingValues: new Map(), lastInteractionAt: 0 },
    savedPageService: {
      acknowledgeAvailabilityAttention,
      async getAvailabilityAttention() {
        refreshCount += 1;
        return { ok: true, data: { count: 0, events: [] } };
      }
    },
    savedAuthController: { initSavedJobsPage() {} },
    cssEscape: value => value,
    setSelectedJobKey() {}, removeSavedJob() {}, updatePhase() {}, updateOutcome() {},
    toggleDetailsForJob() {}, openCustomJobEditor() {}, setJobDetailsTab() {}, applyJobDetailsTab() {},
    refreshActivityLog: async () => {}, renderSavedJobs() {},
    loadSavedLifecycleOverlay: async () => new Map(), queueNotesSave() {}, flushNotesSave() {},
    uploadAttachments() {}, getLastJobsUrl() {}, navigateDesktopPage() {},
    defaultSavedFilter: "all", defaultSavedSort: "updated", defaultSavedGroup: "none",
    timelineScopeAll: "all", setCustomJobPanelOpen() {}, createCustomJob() {}, updateCustomJobWarning() {},
    setSavedFilter() {}, setSavedSort() {}, setSavedGroup() {}, persistSavedListPreferences() {},
    setActivityPanelOpen() {}, setTimelineScope() {}, renderTimeline() {}, renderWorkspaceStats() {},
    renderSelectedJobHint() {}, renderAuthRequired() {}, setSourceStatus() {},
    isDesktopRuntimeMode: () => false,
    showToast(message, type) { toasts.push({ message, type }); }
  });
  boot.bootSavedPage();
  return {
    viewState, toasts,
    get refreshCount() { return refreshCount; },
    acknowledge(action) {
      if (action === "all") return acknowledgeAllClick();
      const originalElement = globalThis.Element;
      class TestElement {
        closest(selector) { return selector === "[data-ui='saved-availability-attention-btn']" ? this : null; }
      }
      globalThis.Element = TestElement;
      try {
        const target = new TestElement();
        target.dataset = { jobKey: "job-1" };
        listClick({ target });
      } finally {
        globalThis.Element = originalElement;
      }
    }
  };
}

const flushHandlers = () => new Promise(resolve => setImmediate(resolve));

for (const mode of ["job", "all"]) {
  test(`Saved suppresses ${mode} acknowledgement after an account switch`, async () => {
    const response = deferred();
    const calls = [];
    const harness = createHarness({
      currentUser: { uid: "user-1" },
      acknowledgeAvailabilityAttention(...args) { calls.push(args); return response.promise; }
    });
    harness.acknowledge(mode);
    harness.viewState.currentUser = { uid: "user-2" };
    response.resolve({ ok: true });
    await flushHandlers();
    assert.deepEqual(calls, [["user-1", mode === "all" ? { allCurrent: true } : { transitionId: "transition-1" }]]);
    assert.deepEqual(harness.toasts, []);
    assert.equal(harness.refreshCount, 0);
  });

  test(`Saved keeps same-profile ${mode} acknowledgement feedback and refresh`, async () => {
    const calls = [];
    const harness = createHarness({
      currentUser: { uid: "user-1" },
      async acknowledgeAvailabilityAttention(...args) { calls.push(args); return { ok: true }; }
    });
    harness.acknowledge(mode);
    await flushHandlers();
    assert.deepEqual(calls, [["user-1", mode === "all" ? { allCurrent: true } : { transitionId: "transition-1" }]]);
    assert.equal(harness.toasts.length, 1);
    assert.equal(harness.refreshCount, 1);
  });
}
