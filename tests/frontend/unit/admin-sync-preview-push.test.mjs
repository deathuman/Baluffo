import test from "node:test";
import assert from "node:assert/strict";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";

test("admin sync preview push posts dryRun and reports the preview outcome", async () => {
  const toasts = [];
  const posts = [];
  const state = { syncConfigDirty: false };
  const controller = createAdminSyncController({
    state,
    refs: {},
    getBridge: async () => ({}),
    postBridge: async (path, payload) => {
      posts.push({ path, payload });
      return { ok: true, dryRun: true, wouldChange: true, counts: { active: 12, pending: 3 } };
    },
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast(message, level) { toasts.push({ message, level }); },
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  await controller.previewPushSources();

  assert.deepEqual(posts, [{ path: "/sync/push", payload: { dryRun: true } }]);
  assert.ok(
    toasts.some(
      ({ message, level }) => level === "success" && /changes would be written/i.test(message)
    )
  );
  assert.match(toasts.find(({ level }) => level === "success").message, /12 active, 3 pending/);
});

test("admin sync preview push reports a failed preview instead of a success toast", async () => {
  const toasts = [];
  const controller = createAdminSyncController({
    state: { syncConfigDirty: false },
    refs: {},
    getBridge: async () => ({}),
    postBridge: async () => ({ ok: false, error: "remote unavailable" }),
    isSyncBusy: () => false,
    setBusyFlag() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    showToast(message, level) { toasts.push({ message, level }); },
    toLocalTime: value => value.toISOString(),
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    escapeHtml: value => String(value)
  });

  await controller.previewPushSources();

  assert.ok(
    toasts.some(
      ({ message, level }) => level === "error" && /remote unavailable/.test(message)
    )
  );
  assert.ok(!toasts.some(({ level }) => level === "success"));
});
