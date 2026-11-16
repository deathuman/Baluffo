/**
 * Shared fixtures for the Runtime Database Storage reclaim tests.
 *
 * A payload is what the bridge returns, never what the panel owns, so these helpers
 * build route-shaped objects and a controller wired to them. Kept in one place so a
 * new case cannot quietly drift from the payload the bridge actually sends.
 */

import { createStorageReclaimController } from "../../../../frontend/admin/app/storage-reclaim-panel.js";
import { createElement } from "./admin-controller-test-helpers.mjs";

export const DAY_SECONDS = 24 * 60 * 60;
export const MB = 1024 * 1024;

function verdict(overrides = {}) {
  return {
    verdict: "bloat",
    blockers: [],
    measuredStagingBytes: 900 * MB,
    measuredStagingAgeS: 2 * DAY_SECONDS,
    measuredStagingStale: false,
    lastRehearsalGateAdequate: true,
    ...overrides
  };
}

export function payload(reclaim = {}, verdictOverrides = {}) {
  return {
    storageReclaim: {
      scope: "inline",
      status: "idle",
      phase: "idle",
      runId: "",
      mode: "",
      verdict: verdict(verdictOverrides),
      ...reclaim
    }
  };
}

export function refs() {
  return {
    adminStorageReclaimStatusEl: createElement(),
    adminStorageReclaimDialogEl: createElement(),
    adminStorageReclaimHealthEl: createElement({ hidden: true }),
    adminStorageReclaimRehearseBtnEl: createElement(),
    adminStorageReclaimRunBtnEl: createElement(),
    adminStorageReclaimStopBtnEl: createElement({ hidden: true }),
    adminStorageReclaimModalEl: createElement({ hidden: true }),
    adminStorageReclaimModalBodyEl: createElement(),
    adminStorageReclaimModalStopBtnEl: createElement(),
    adminStorageReclaimModalHideBtnEl: createElement()
  };
}

export function controller(target, { get, post } = {}, options = {}) {
  const calls = { get: [], post: [] };
  const instance = createStorageReclaimController({
    refs: target,
    getBridge: async url => {
      calls.get.push(url);
      return get ? get(url) : payload();
    },
    postBridge: async (url, body) => {
      calls.post.push({ url, body });
      return post ? post(url, body) : payload({ status: "running", runId: "run-1", mode: "reclaim" });
    },
    formatBytes: value => `${Math.round(value / MB)} MB`,
    ...options
  });
  return { instance, calls };
}

export const settle = () => new Promise(resolve => setImmediate(resolve));

/**
 * A timer the test fires by hand. The panel's follow-up read is a timer, so a
 * case has to drive it rather than wait three real seconds per read.
 */
export function fakeTimers() {
  const pending = [];
  return {
    pending,
    setTimer: callback => {
      const timer = { callback };
      pending.push(timer);
      return timer;
    },
    clearTimer: timer => {
      const index = pending.indexOf(timer);
      if (index >= 0) pending.splice(index, 1);
    },
    fire: async () => {
      const next = pending.shift();
      if (!next) return false;
      next.callback();
      await settle();
      await settle();
      return true;
    }
  };
}
