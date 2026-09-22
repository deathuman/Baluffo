// Shared Action Center controller fixture. Extracted so the render-behaviour suite
// and the route/startup-lane suite drive the controller from identical payloads.
import { createActionCenterController } from "../../../../frontend/admin/app/action-center.js";
import { createElement } from "./admin-controller-test-helpers.mjs";

export function cleanHealthPayload() {
  return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0 } };
}

export function cleanSyncPayload() {
  return {
    config: { enabled: true, ready: true, state: "ready" },
    runtime: { lastAction: "pull", lastResult: "ok", lastError: "" }
  };
}

export function cleanStoragePayload() {
  return { ok: true, storage: { healthy: true, diagnostics: [] } };
}

export function createActionCenterFixture({
  getBridge,
  onSyncStatus,
  shouldDeferStorageHealth,
  shouldDeferCoreSignals,
  enqueueStartupTask,
  showToast,
  includeHeaderRefs = true
} = {}) {
  const refs = {
    // `startPolling` binds a delegated click listener on the items container, so the
    // stub needs one. Harmless for the poll-only tests, which never call bindEvents.
    actionCenterItemsEl: createElement({ addEventListener() {} }),
    actionCenterCopyBtnEl: createElement({ addEventListener() {} })
  };
  // The header chip and stamp are optional at runtime; the default fixture supplies
  // them so their wiring is exercised, and `includeHeaderRefs: false` covers the
  // legacy two-ref shape that must keep working.
  if (includeHeaderRefs) {
    refs.actionCenterStatusChipEl = createElement();
    refs.actionCenterCheckedAtEl = createElement();
  }
  const calls = [];
  const toasts = [];
  const controller = createActionCenterController({
    refs,
    getBridge: getBridge || (async path => {
      calls.push(path);
      if (path === "/ops/fetch-kpis?view=summary") {
        return cleanHealthPayload();
      }
      if (path === "/sync/status?view=summary") {
        return cleanSyncPayload();
      }
      if (path === "/ops/storage-health") {
        return cleanStoragePayload();
      }
      return null;
    }),
    postBridge: async () => ({}),
    showToast: showToast || ((message, tone) => toasts.push([message, tone])),
    logAdminError() {},
    onSyncStatus,
    shouldDeferCoreSignals,
    shouldDeferStorageHealth,
    enqueueStartupTask
  });
  return { refs, calls, toasts, controller };
}
