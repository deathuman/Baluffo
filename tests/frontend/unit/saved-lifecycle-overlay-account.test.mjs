import test from "node:test";
import assert from "node:assert/strict";

import { createSavedBoot } from "../../../frontend/saved/app/runtime/boot.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createHarness() {
  const subscriptions = [];
  const overlayRequests = [];
  const renders = [];
  const viewState = {
    currentUser: { uid: "user-1" },
    lastSavedJobsByKey: new Map(),
    savedLifecycleOverlayByJobKey: new Map(),
    savedLifecycleOverlayRequestId: 0
  };
  const boot = createSavedBoot({
    startupMetrics: null,
    dom: {},
    viewState,
    noteSaveState: {
      inFlight: new Map(),
      pendingValues: new Map(),
      lastInteractionAt: 0
    },
    savedPageService: {
      subscribeSavedJobs(uid, onJobs) {
        subscriptions.push({ uid, onJobs });
        return () => {};
      },
      async getAvailabilityAttention() {
        return { ok: true, data: { count: 0, events: [] } };
      }
    },
    setSourceStatus() {},
    renderWorkspaceStats() {},
    renderSelectedJobHint() {},
    renderTimeline() {},
    renderSavedJobs(jobs) { renders.push(jobs); },
    markSavedFirstRender() {},
    refreshActivityLog: async () => {},
    loadSavedLifecycleOverlay() {
      const request = deferred();
      overlayRequests.push(request);
      return request.promise;
    }
  });
  return { boot, viewState, subscriptions, overlayRequests, renders };
}

async function flushAsyncHandlers() {
  await new Promise(resolve => setImmediate(resolve));
}

function emitJobs(subscription, uid) {
  subscription.onJobs([{ jobKey: `${uid}-job` }]);
}

function withDocument(callback) {
  const hadDocument = Object.prototype.hasOwnProperty.call(globalThis, "document");
  const previousDocument = globalThis.document;
  globalThis.document = { activeElement: null };
  return Promise.resolve()
    .then(callback)
    .finally(() => {
      if (hadDocument) globalThis.document = previousDocument;
      else delete globalThis.document;
    });
}

test("Saved ignores a previous profile lifecycle overlay completion", () => withDocument(async () => {
  const harness = createHarness();
  const { boot, viewState, subscriptions, overlayRequests, renders } = harness;

  boot.subscribeToSavedJobs("user-1");
  emitJobs(subscriptions[0], "user-1");

  viewState.currentUser = { uid: "user-2" };
  // Model a reset request counter so the account guard is independently exercised.
  viewState.savedLifecycleOverlayRequestId = 0;
  boot.subscribeToSavedJobs("user-2");
  emitJobs(subscriptions[1], "user-2");

  const currentOverlay = new Map([["job:user-2-job", { status: "active" }]]);
  overlayRequests[1].resolve(currentOverlay);
  await flushAsyncHandlers();
  const renderCountAfterCurrentProfile = renders.length;

  overlayRequests[0].resolve(new Map([["job:user-1-job", { status: "removed" }]]));
  await flushAsyncHandlers();

  assert.equal(viewState.savedLifecycleOverlayByJobKey, currentOverlay);
  assert.equal(renders.length, renderCountAfterCurrentProfile);
}));

test("Saved ignores a previous profile lifecycle overlay failure", () => withDocument(async () => {
  const harness = createHarness();
  const { boot, viewState, subscriptions, overlayRequests } = harness;

  boot.subscribeToSavedJobs("user-1");
  emitJobs(subscriptions[0], "user-1");

  viewState.currentUser = { uid: "user-2" };
  // Model a reset request counter so the account guard is independently exercised.
  viewState.savedLifecycleOverlayRequestId = 0;
  boot.subscribeToSavedJobs("user-2");
  emitJobs(subscriptions[1], "user-2");

  const currentOverlay = new Map([["job:user-2-job", { status: "active" }]]);
  overlayRequests[1].resolve(currentOverlay);
  await flushAsyncHandlers();
  overlayRequests[0].reject(new Error("stale lifecycle overlay failed"));
  await flushAsyncHandlers();

  assert.equal(viewState.savedLifecycleOverlayByJobKey, currentOverlay);
}));
