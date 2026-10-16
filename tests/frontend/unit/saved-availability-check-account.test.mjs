import test from "node:test";
import assert from "node:assert/strict";

import { createSavedBoot } from "../../../frontend/saved/app/runtime/boot.js";
import { createElement } from "./helpers/saved-runtime-helpers.mjs";

function deferred() {
  let resolve;
  const promise = new Promise(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createClassList() {
  const values = new Set();
  return {
    add(...tokens) { tokens.forEach(token => values.add(token)); },
    remove(...tokens) { tokens.forEach(token => values.delete(token)); },
    contains(token) { return values.has(token); }
  };
}

function createToastElement() {
  return {
    classList: createClassList(),
    children: [],
    appendChild(child) { this.children.push(child); },
    remove() { this.removed = true; }
  };
}

async function withBrowserGlobals(callback) {
  const previousDocument = globalThis.document;
  const hadDocument = Object.prototype.hasOwnProperty.call(globalThis, "document");
  const previousElement = globalThis.Element;
  const hadElement = Object.prototype.hasOwnProperty.call(globalThis, "Element");
  const previousAnimationFrame = globalThis.requestAnimationFrame;
  const hadAnimationFrame = Object.prototype.hasOwnProperty.call(globalThis, "requestAnimationFrame");
  const previousSetTimeout = globalThis.setTimeout;
  const previousClearTimeout = globalThis.clearTimeout;
  const toastElements = [];
  let nextTimerId = 0;
  const fakeTimers = new Map();

  globalThis.document = {
    body: {
      appendChild(element) { toastElements.push(element); }
    },
    createElement: createToastElement
  };
  globalThis.requestAnimationFrame = callbackFn => callbackFn();
  globalThis.setTimeout = (callbackFn, delay, ...args) => {
    if (delay === 750) return previousSetTimeout(callbackFn, 0, ...args);
    const timerId = ++nextTimerId;
    fakeTimers.set(timerId, { callbackFn, delay, args });
    return timerId;
  };
  globalThis.clearTimeout = timerId => fakeTimers.delete(timerId);

  class TestElement {
    closest(selector) {
      return selector === "[data-ui='saved-check-availability-btn']" ? this : null;
    }
  }
  globalThis.Element = TestElement;

  try {
    return await callback({ toastElements, TestElement });
  } finally {
    globalThis.setTimeout = previousSetTimeout;
    globalThis.clearTimeout = previousClearTimeout;
    if (hadDocument) globalThis.document = previousDocument;
    else delete globalThis.document;
    if (hadElement) globalThis.Element = previousElement;
    else delete globalThis.Element;
    if (hadAnimationFrame) globalThis.requestAnimationFrame = previousAnimationFrame;
    else delete globalThis.requestAnimationFrame;
  }
}

function createHarness({ checkJobAvailability, getJobAvailabilityCheckStatus, loadSavedLifecycleOverlay }) {
  let listClick;
  const savedJobsListEl = createElement({
    addEventListener(name, listener) {
      if (name === "click") listClick = listener;
    }
  });
  const originalOverlay = new Map([["previous", { status: "active" }]]);
  const viewState = {
    currentUser: { uid: "user-1" },
    lastSavedJobsByKey: new Map([[
      "job-1",
      { jobKey: "job-1", availabilityId: "availability-1", jobLink: "https://example.com/job" }
    ]]),
    savedLifecycleOverlayByJobKey: originalOverlay
  };
  const renders = [];
  const overlayLoads = [];
  const boot = createSavedBoot({
    startupMetrics: null,
    adminBridgeBase: "",
    documentObject: {
      querySelector(selector) {
        return selector.includes("saved-jobs-list") ? savedJobsListEl : null;
      },
      querySelectorAll() { return []; }
    },
    dom: {},
    viewState,
    noteSaveState: { inFlight: new Map(), pendingValues: new Map(), lastInteractionAt: 0 },
    savedPageService: { checkJobAvailability, getJobAvailabilityCheckStatus },
    canManageAvailability: () => true,
    savedAuthController: { initSavedJobsPage() {} },
    cssEscape: value => value,
    setSelectedJobKey() {}, removeSavedJob() {}, updatePhase() {}, updateOutcome() {},
    toggleDetailsForJob() {}, openCustomJobEditor() {}, setJobDetailsTab() {}, applyJobDetailsTab() {},
    refreshActivityLog: async () => {},
    renderSavedJobs(jobs) { renders.push(jobs); },
    loadSavedLifecycleOverlay() {
      overlayLoads.push(true);
      return loadSavedLifecycleOverlay();
    },
    queueNotesSave() {}, flushNotesSave() {}, uploadAttachments() {}, getLastJobsUrl() {},
    navigateDesktopPage() {}, defaultSavedFilter: "all", defaultSavedSort: "updated",
    defaultSavedGroup: "none", timelineScopeAll: "all", setCustomJobPanelOpen() {},
    createCustomJob() {}, updateCustomJobWarning() {}, setSavedFilter() {}, setSavedSort() {},
    setSavedGroup() {}, persistSavedListPreferences() {}, setActivityPanelOpen() {},
    setTimelineScope() {}, renderTimeline() {}, renderWorkspaceStats() {}, renderSelectedJobHint() {},
    renderAuthRequired() {}, setSourceStatus() {}, isDesktopRuntimeMode: () => false
  });
  boot.bootSavedPage();

  return {
    viewState,
    originalOverlay,
    renders,
    overlayLoads,
    clickAvailabilityCheck() {
      const target = new globalThis.Element();
      target.dataset = { availabilityId: "availability-1" };
      listClick({ target });
    }
  };
}

const flushAsyncHandlers = () => new Promise(resolve => setImmediate(resolve));

test("Saved dismisses the availability progress toast and suppresses stale-profile feedback", () => withBrowserGlobals(async ({ toastElements }) => {
  const request = deferred();
  const checkStarted = deferred();
  const harness = createHarness({
    checkJobAvailability() {
      checkStarted.resolve();
      return request.promise;
    },
    async getJobAvailabilityCheckStatus() {
      return { ok: false, error: "Old profile check failed." };
    },
    async loadSavedLifecycleOverlay() { return new Map(); }
  });

  harness.clickAvailabilityCheck();
  await checkStarted.promise;
  assert.equal(toastElements.length, 1);
  assert.equal(toastElements[0].classList.contains("visible"), true);

  harness.viewState.currentUser = { uid: "user-2" };
  request.resolve({ ok: false, error: "Old profile check failed." });
  await flushAsyncHandlers();

  assert.equal(toastElements.length, 1, "stale failure must not display a result toast");
  assert.equal(toastElements[0].classList.contains("visible"), false, "progress toast must be dismissed");
  assert.equal(harness.overlayLoads.length, 0);
  assert.deepEqual(harness.renders, []);
  assert.equal(harness.viewState.savedLifecycleOverlayByJobKey, harness.originalOverlay);
}));

test("Saved ignores availability overlay results after a profile switch", () => withBrowserGlobals(async ({ toastElements }) => {
  const overlayRequest = deferred();
  const overlayStarted = deferred();
  const harness = createHarness({
    async checkJobAvailability() { return { ok: true, data: { runId: "run-1" } }; },
    async getJobAvailabilityCheckStatus() {
      return {
        ok: true,
        data: {
          status: "succeeded",
          result: { classification: "direct_live", confidence: "definitive", applied: true }
        }
      };
    },
    loadSavedLifecycleOverlay() {
      overlayStarted.resolve();
      return overlayRequest.promise;
    }
  });

  harness.clickAvailabilityCheck();
  await overlayStarted.promise;
  assert.equal(harness.overlayLoads.length, 1);
  assert.equal(toastElements.length, 2, "same-profile completion keeps its verdict toast");
  assert.equal(toastElements[0].classList.contains("visible"), false, "progress toast is dismissed after completion");

  harness.viewState.currentUser = { uid: "user-2" };
  const staleOverlay = new Map([["job:job-1", { status: "closed" }]]);
  overlayRequest.resolve(staleOverlay);
  await flushAsyncHandlers();

  assert.equal(harness.viewState.savedLifecycleOverlayByJobKey, harness.originalOverlay);
  assert.deepEqual(harness.renders, []);
  assert.equal(toastElements.length, 2, "profile switch must not add stale feedback");
}));
