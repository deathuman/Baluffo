import test from "node:test";
import assert from "node:assert/strict";
import { scheduleAdminRender } from "../../../frontend/admin/app/render-scheduler.js";

function restoreGlobal(name, descriptor) {
  if (descriptor) {
    Object.defineProperty(globalThis, name, descriptor);
  } else {
    delete globalThis[name];
  }
}

test("admin render scheduler uses requestIdleCallback when available", () => {
  const originalIdle = Object.getOwnPropertyDescriptor(globalThis, "requestIdleCallback");
  const originalCancel = Object.getOwnPropertyDescriptor(globalThis, "cancelIdleCallback");
  const calls = [];
  try {
    Object.defineProperty(globalThis, "requestIdleCallback", {
      configurable: true,
      value(callback, options) {
        calls.push({ kind: "idle", options });
        callback();
        return 7;
      }
    });
    Object.defineProperty(globalThis, "cancelIdleCallback", {
      configurable: true,
      value(id) {
        calls.push({ kind: "cancel", id });
      }
    });
    let ran = false;

    const cancel = scheduleAdminRender(() => {
      ran = true;
    }, { timeoutMs: 123 });
    cancel();

    assert.equal(ran, true);
    assert.deepEqual(calls, [
      { kind: "idle", options: { timeout: 123 } },
      { kind: "cancel", id: 7 }
    ]);
  } finally {
    restoreGlobal("requestIdleCallback", originalIdle);
    restoreGlobal("cancelIdleCallback", originalCancel);
  }
});

test("admin render scheduler falls back to timeout scheduling", () => {
  const originalIdle = Object.getOwnPropertyDescriptor(globalThis, "requestIdleCallback");
  const originalSetTimeout = Object.getOwnPropertyDescriptor(globalThis, "setTimeout");
  const originalClearTimeout = Object.getOwnPropertyDescriptor(globalThis, "clearTimeout");
  const calls = [];
  try {
    delete globalThis.requestIdleCallback;
    Object.defineProperty(globalThis, "setTimeout", {
      configurable: true,
      value(callback, delayMs) {
        calls.push({ kind: "timeout", delayMs });
        callback();
        return 9;
      }
    });
    Object.defineProperty(globalThis, "clearTimeout", {
      configurable: true,
      value(id) {
        calls.push({ kind: "clear", id });
      }
    });
    let ran = false;

    const cancel = scheduleAdminRender(() => {
      ran = true;
    }, { fallbackDelayMs: 75 });
    cancel();

    assert.equal(ran, true);
    assert.deepEqual(calls, [
      { kind: "timeout", delayMs: 75 },
      { kind: "clear", id: 9 }
    ]);
  } finally {
    restoreGlobal("requestIdleCallback", originalIdle);
    restoreGlobal("setTimeout", originalSetTimeout);
    restoreGlobal("clearTimeout", originalClearTimeout);
  }
});
