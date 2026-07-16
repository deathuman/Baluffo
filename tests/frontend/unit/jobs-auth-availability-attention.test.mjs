import test from "node:test";
import assert from "node:assert/strict";

import { createJobsAuthController } from "../../../frontend/jobs/app/runtime/auth-controller.js";
import { createElement } from "./helpers/saved-runtime-helpers.mjs";

test("signed-in Jobs navigation shows Saved availability attention", async () => {
  let authStateChanged = null;
  const savedJobsBtn = createElement();
  const controller = createJobsAuthController({
    refs: {
      authSignInBtn: createElement(),
      authSignOutBtn: createElement(),
      savedJobsBtn,
      authStatus: createElement(),
      authStatusHint: createElement(),
      authAvatar: createElement(),
      guestNoticeEl: createElement({ hidden: true })
    },
    userState: {
      currentUser: null,
      savedJobKeys: new Set(),
      seenJobKeys: new Set(),
      authStateListenerBound: false
    },
    authReadyPoller: { stopPoll() {}, schedulePoll() {} },
    jobsAuthService: {
      onAuthStateChanged(callback) { authStateChanged = callback; }
    },
    jobsSavedJobsService: {
      async getSavedJobKeys() { return { data: [] }; },
      async getAvailabilityAttention() { return { data: { count: 2 } }; }
    },
    jobsPageService: { isAvailable: () => true },
    jobsDispatch: { dispatch() {} },
    JOBS_ACTIONS: { AUTH_CHANGED: "auth_changed" },
    isJobsApiReady: () => true,
    emitDesktopStartupMetric() {},
    showToast() {},
    logJobsError() {},
    getAllJobs: () => [],
    applyFiltersAndRender() {},
    loadSeenJobKeys: async () => new Set()
  });

  controller.initAuth();
  await authStateChanged({ uid: "user-1", displayName: "User" });

  assert.equal(savedJobsBtn.textContent, "Saved Jobs (2)");
  assert.equal(savedJobsBtn.classList.contains("needs-attention"), true);
  assert.match(savedJobsBtn.attributes["aria-label"], /2 availability updates/);
});
