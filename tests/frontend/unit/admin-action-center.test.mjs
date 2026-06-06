import test from "node:test";
import assert from "node:assert/strict";
import { createActionCenterController } from "../../../frontend/admin/app/action-center.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

test("action center renders remote sync conflict as reviewable warning", async () => {
  const refs = {
    actionCenterItemsEl: createElement(),
    actionCenterCopyBtnEl: createElement()
  };
  const calls = [];
  const controller = createActionCenterController({
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/health?view=ready") {
        return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0 } };
      }
      if (path === "/sync/status?view=summary") {
        return {
          config: { enabled: true, ready: true, state: "remote_conflict" },
          runtime: {
            lastAction: "push",
            lastResult: "error",
            lastError: "is at a8f0ae858e0e7c8ecafe671bf9825f6e7328dd97 but expected db2c4166cf428892f165629d27933ce492d346d1"
          }
        };
      }
      return null;
    },
    postBridge: async () => ({}),
    showToast() {},
    logAdminError() {}
  });

  await controller.pollActionCenter({ includeStorage: false });

  assert.deepEqual(calls, ["/ops/health?view=ready", "/sync/status?view=summary"]);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Sync needs attention/);
  assert.match(refs.actionCenterItemsEl.innerHTML, /Sync conflict needs review; data refresh can continue/);
  assert.match(refs.actionCenterItemsEl.innerHTML, /data-preset="sync_pull"/);
});
