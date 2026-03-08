import test from "node:test";
import assert from "node:assert/strict";
import { createJobsDispatcher, JOBS_ACTIONS } from "../../../frontend/jobs/actions.js";
import { createSavedDispatcher, SAVED_ACTIONS } from "../../../frontend/saved/actions.js";
import { createAdminDispatcher, ADMIN_ACTIONS } from "../../../frontend/admin/actions.js";

test("jobs dispatcher tracks refresh and save actions", () => {
  const store = createJobsDispatcher();
  store.dispatch({ type: JOBS_ACTIONS.REFRESH_REQUESTED });
  store.dispatch({ type: JOBS_ACTIONS.REFRESH_COMPLETED, payload: { finishedAt: "2026-03-08T10:00:00Z" } });
  store.dispatch({ type: JOBS_ACTIONS.SAVE_TOGGLED, payload: { jobKey: "job_abc" } });
  const state = store.getState();
  assert.equal(state.refreshing, false);
  assert.equal(state.lastRefreshAt, "2026-03-08T10:00:00Z");
  assert.equal(state.lastSaveToggleJobKey, "job_abc");
});

test("saved dispatcher tracks notes lifecycle", () => {
  const store = createSavedDispatcher();
  store.dispatch({ type: SAVED_ACTIONS.NOTES_QUEUED, payload: { jobKey: "job_1" } });
  store.dispatch({ type: SAVED_ACTIONS.NOTES_SAVE_FAILED, payload: { jobKey: "job_1", error: "boom" } });
  store.dispatch({ type: SAVED_ACTIONS.CUSTOM_JOB_MUTATED, payload: { at: "2026-03-08T10:10:00Z" } });
  store.dispatch({ type: SAVED_ACTIONS.ATTACHMENT_MUTATED, payload: { jobKey: "job_1" } });
  const state = store.getState();
  assert.equal(state.pendingNotesCount, 0);
  assert.equal(state.lastNotesError, "boom");
  assert.equal(state.lastCustomMutationAt, "2026-03-08T10:10:00Z");
  assert.equal(state.lastAttachmentJobKey, "job_1");
});

test("admin dispatcher tracks unlock and refresh events", () => {
  const store = createAdminDispatcher();
  store.dispatch({ type: ADMIN_ACTIONS.UNLOCKED });
  store.dispatch({ type: ADMIN_ACTIONS.OVERVIEW_REFRESHED, payload: { at: "2026-03-08T10:00:00Z" } });
  store.dispatch({ type: ADMIN_ACTIONS.DISCOVERY_REFRESHED, payload: { at: "2026-03-08T10:01:00Z" } });
  store.dispatch({ type: ADMIN_ACTIONS.OPS_REFRESHED, payload: { at: "2026-03-08T10:02:00Z" } });
  store.dispatch({ type: ADMIN_ACTIONS.LOCKED });
  const state = store.getState();
  assert.equal(state.isUnlocked, false);
  assert.equal(state.lastOverviewAt, "2026-03-08T10:00:00Z");
  assert.equal(state.lastDiscoveryAt, "2026-03-08T10:01:00Z");
  assert.equal(state.lastOpsAt, "2026-03-08T10:02:00Z");
});

test("jobs dispatcher tracks failed refresh", () => {
  const store = createJobsDispatcher();
  store.dispatch({ type: JOBS_ACTIONS.REFRESH_REQUESTED });
  store.dispatch({ type: JOBS_ACTIONS.REFRESH_FAILED, payload: { error: "network" } });
  const state = store.getState();
  assert.equal(state.refreshing, false);
  assert.equal(state.refreshError, "network");
});

test("jobs dispatcher tracks auth and filters", () => {
  const store = createJobsDispatcher();
  store.dispatch({ type: JOBS_ACTIONS.AUTH_CHANGED, payload: { uid: "u42" } });
  store.dispatch({ type: JOBS_ACTIONS.FILTERS_CHANGED, payload: { signature: "work=remote" } });
  const state = store.getState();
  assert.equal(state.authUserId, "u42");
  assert.equal(state.lastFilterHash, "work=remote");
});

test("saved dispatcher tracks auth and notes success", () => {
  const store = createSavedDispatcher();
  store.dispatch({ type: SAVED_ACTIONS.AUTH_CHANGED, payload: { uid: "u7" } });
  store.dispatch({ type: SAVED_ACTIONS.NOTES_QUEUED, payload: { jobKey: "job_x" } });
  store.dispatch({ type: SAVED_ACTIONS.NOTES_SAVED, payload: { jobKey: "job_x" } });
  const state = store.getState();
  assert.equal(state.authUserId, "u7");
  assert.equal(state.pendingNotesCount, 0);
  assert.equal(state.lastNotesError, "");
});
