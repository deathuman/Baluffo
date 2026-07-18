import test from "node:test";
import assert from "node:assert/strict";

import {
  createSavedBoot,
  runSavedAvailabilityReport
} from "../../../frontend/saved/app/runtime/boot.js";
import { createButton, createElement } from "./helpers/saved-runtime-helpers.mjs";

test("Saved renders the current availability-attention summary", async () => {
  const banner = createElement();
  const count = createElement();
  const filter = createButton({ dataset: { savedFilter: "availability_attention" } });
  const boot = createSavedBoot({
    startupMetrics: null,
    dom: {
      availabilityAttentionBannerEl: banner,
      availabilityAttentionCountEl: count,
      savedCustomFilterBtnEls: [filter]
    },
    viewState: { currentUser: { uid: "user-1" } },
    savedPageService: {
      async getAvailabilityAttention(uid) {
        assert.equal(uid, "user-1");
        return { ok: true, data: { count: 2, events: [{ transitionId: "t-1" }] } };
      }
    }
  });

  await boot.refreshAvailabilityAttention();

  assert.equal(banner.classList.contains("hidden"), false);
  assert.equal(count.textContent, "2 availability updates need attention.");
  assert.equal(filter.textContent, "Availability attention (2)");
});

test("Saved availability report confirms the named job and supports Undo", async () => {
  const row = {
    jobKey: "job-1",
    title: "Gameplay Engineer",
    company: "Studio",
    availabilityId: "availability-1",
    jobLink: "https://example.com/jobs/1",
    availabilityAttention: { events: [] }
  };
  const confirmations = [];
  const toasts = [];
  const calls = [];
  const deps = {
    canManageAvailability: () => true,
    viewState: { currentUser: { uid: "user-1" }, lastSavedJobsByKey: new Map([[row.jobKey, row]]) },
    requestConfirmationDialog: async options => {
      confirmations.push(options);
      return true;
    },
    savedPageService: {
      async manageAvailabilityReport(uid, jobKey, action) {
        calls.push({ uid, jobKey, action });
        return { ok: true, data: { queuedForCheck: true } };
      }
    },
    renderSavedJobs() {},
    showToast(message, type, options) {
      toasts.push({ message, type, options });
    }
  };

  await runSavedAvailabilityReport(deps, async () => {}, row.jobKey, "report");

  assert.match(confirmations[0].description, /Gameplay Engineer at Studio/);
  assert.match(confirmations[0].description, /independent availability check will be queued/i);
  assert.equal(calls.length, 1);
  assert.equal(row.availabilityAttention.hiddenByReport, true);
  assert.equal(toasts[0].options.actionLabel, "Undo");

  await toasts[0].options.onAction();
  assert.deepEqual(calls.map(call => call.action), ["report", "clear"]);
  assert.equal(row.availabilityAttention.hiddenByReport, false);
  assert.equal(toasts.at(-1).message, "Unavailable report cleared.");
});

test("Saved availability report cancellation does not mutate the job", async () => {
  const row = { jobKey: "job-2", title: "Artist", company: "Studio" };
  let serviceCalls = 0;
  let confirmation = null;
  await runSavedAvailabilityReport({
    canManageAvailability: () => true,
    viewState: { currentUser: { uid: "user-1" }, lastSavedJobsByKey: new Map([[row.jobKey, row]]) },
    requestConfirmationDialog: async options => {
      confirmation = options;
      return false;
    },
    savedPageService: {
      async manageAvailabilityReport() {
        serviceCalls += 1;
        return { ok: true, data: {} };
      }
    },
    renderSavedJobs() {},
    showToast() {}
  }, async () => {}, row.jobKey, "report");

  assert.equal(serviceCalls, 0);
  assert.doesNotMatch(confirmation.description, /availability check will be queued/i);
  assert.equal(row.availabilityAttention, undefined);
});

test("Saved availability report aborts if the profile changes during confirmation", async () => {
  const row = { jobKey: "job-3", title: "Artist", company: "Studio" };
  const viewState = {
    currentUser: { uid: "user-1" },
    lastSavedJobsByKey: new Map([[row.jobKey, row]])
  };
  const calls = [];
  const toasts = [];

  await runSavedAvailabilityReport({
    canManageAvailability: () => true,
    viewState,
    requestConfirmationDialog: async () => {
      viewState.currentUser = { uid: "user-2" };
      return true;
    },
    savedPageService: {
      async manageAvailabilityReport(...args) {
        calls.push(args);
        return { ok: true, data: {} };
      }
    },
    renderSavedJobs() {},
    showToast(message, type) {
      toasts.push({ message, type });
    }
  }, async () => {}, row.jobKey, "report");

  assert.deepEqual(calls, []);
  assert.deepEqual(toasts, [{
    message: "Profile changed; unavailable report was not updated.",
    type: "info"
  }]);
  assert.equal(row.availabilityAttention, undefined);
});

test("Saved availability Undo stays bound to the originating profile", async () => {
  const originalRow = {
    jobKey: "job-4",
    title: "Engineer",
    company: "Studio",
    availabilityAttention: {}
  };
  const otherRow = {
    jobKey: "job-4",
    title: "Other profile job",
    availabilityAttention: { hiddenByReport: true }
  };
  const viewState = {
    currentUser: { uid: "user-1" },
    lastSavedJobsByKey: new Map([[originalRow.jobKey, originalRow]])
  };
  const calls = [];
  const toasts = [];

  await runSavedAvailabilityReport({
    canManageAvailability: () => true,
    viewState,
    requestConfirmationDialog: async () => true,
    savedPageService: {
      async manageAvailabilityReport(uid, jobKey, action) {
        calls.push({ uid, jobKey, action });
        return { ok: true, data: {} };
      }
    },
    renderSavedJobs() {},
    showToast(message, type, options) {
      toasts.push({ message, type, options });
    }
  }, async () => {}, originalRow.jobKey, "report");

  viewState.currentUser = { uid: "user-2" };
  viewState.lastSavedJobsByKey = new Map([[otherRow.jobKey, otherRow]]);
  await toasts[0].options.onAction();

  assert.deepEqual(calls, [
    { uid: "user-1", jobKey: "job-4", action: "report" },
    { uid: "user-1", jobKey: "job-4", action: "clear" }
  ]);
  assert.equal(otherRow.availabilityAttention.hiddenByReport, true);
});
