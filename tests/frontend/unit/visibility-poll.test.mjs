import test from "node:test";
import assert from "node:assert/strict";
import { createVisibilityPausedInterval } from "../../../frontend/shared/visibility-poll.js";

function createFakeWindow() {
  const intervals = new Map();
  const listeners = [];
  let nextId = 1;
  const fakeWindow = {
    document: { hidden: false },
    setInterval: (cb) => {
      const id = nextId++;
      intervals.set(id, cb);
      return id;
    },
    clearInterval: (id) => intervals.delete(id),
    addEventListener: (type, fn) => {
      if (type === "visibilitychange") listeners.push(fn);
    }
  };
  return {
    fakeWindow,
    intervals,
    fireVisibility: () => listeners.forEach((fn) => fn())
  };
}

test("interval starts when visible and clears on stop()", () => {
  const { fakeWindow, intervals } = createFakeWindow();
  const controller = createVisibilityPausedInterval(() => {}, 1000, fakeWindow);
  assert.equal(intervals.size, 1);
  controller.stop();
  assert.equal(intervals.size, 0);
});

test("interval pauses when hidden and resumes when visible", () => {
  const { fakeWindow, intervals, fireVisibility } = createFakeWindow();
  createVisibilityPausedInterval(() => {}, 1000, fakeWindow);
  assert.equal(intervals.size, 1);
  fakeWindow.document.hidden = true;
  fireVisibility();
  assert.equal(intervals.size, 0);
  fakeWindow.document.hidden = false;
  fireVisibility();
  assert.equal(intervals.size, 1);
});

test("stop() is permanent; visibility change does not resume", () => {
  const { fakeWindow, intervals, fireVisibility } = createFakeWindow();
  const controller = createVisibilityPausedInterval(() => {}, 1000, fakeWindow);
  controller.stop();
  assert.equal(intervals.size, 0);
  fakeWindow.document.hidden = false;
  fireVisibility();
  assert.equal(intervals.size, 0);
});

test("does not start while hidden at creation, then resumes on visible", () => {
  const { fakeWindow, intervals, fireVisibility } = createFakeWindow();
  fakeWindow.document.hidden = true;
  createVisibilityPausedInterval(() => {}, 1000, fakeWindow);
  assert.equal(intervals.size, 0);
  fakeWindow.document.hidden = false;
  fireVisibility();
  assert.equal(intervals.size, 1);
});
