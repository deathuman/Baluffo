import test from "node:test";
import assert from "node:assert/strict";
import {
  clearNoteSaveQueues,
  flushNotesSave,
  queueNotesSave
} from "../../../frontend/saved/app/notes.js";

function createNoteSaveDeps({ updateJobNotes, flushNotesSaveImpl } = {}) {
  const state = {
    inFlight: new Map(),
    pendingValues: new Map(),
    latestValues: new Map(),
    saveStatusByJobKey: new Map(),
    generation: 0,
    timers: new Map(),
    lastInteractionAt: 0
  };
  const saveStates = [];
  const failed = [];
  const saved = [];
  const deps = {
    noteSaveState: state,
    currentUser: { uid: "user-1" },
    getCurrentUser: () => deps.currentUser,
    updateJobNotes,
    getPreviousNoteLength: () => undefined,
    setNoteSaveState: (jobKey, saveState) => saveStates.push({ jobKey, saveState }),
    dispatchSaved: jobKey => saved.push(jobKey),
    dispatchFailed: (jobKey, error) => failed.push({ jobKey, error }),
    queueActivityPulse() {},
    timelineScopeNotes: "notes",
    flushNotesSave: flushNotesSaveImpl || (() => {})
  };
  return { deps, state, saveStates, failed, saved };
}

function waitFor(predicate, message) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + 1000;
    const check = () => {
      if (predicate()) {
        resolve();
      } else if (Date.now() >= deadline) {
        reject(new Error(message));
      } else {
        setTimeout(check, 0);
      }
    };
    check();
  });
}

test("failed notes save stays errored without automatically resubmitting", async () => {
  const automaticFlushes = [];
  let updateCalls = 0;
  const { deps, state, saveStates, failed } = createNoteSaveDeps({
    updateJobNotes: async () => {
      updateCalls += 1;
      return { ok: false, error: "offline" };
    },
    flushNotesSaveImpl: (...args) => automaticFlushes.push(args)
  });
  state.pendingValues.set("job-1", "Draft note");

  try {
    await flushNotesSave("job-1", undefined, deps);
    await new Promise(resolve => setTimeout(resolve, 10));

    assert.equal(updateCalls, 1);
    assert.deepEqual(automaticFlushes, []);
    assert.equal(state.pendingValues.get("job-1"), "Draft note");
    assert.equal(saveStates.at(-1).saveState, "error");
    assert.deepEqual(failed, [{ jobKey: "job-1", error: "offline" }]);
  } finally {
    clearNoteSaveQueues(state);
  }
});

test("a later notes edit retries after a failed save", async () => {
  let updateCalls = 0;
  const savedValues = [];
  const { deps, state, saveStates, failed, saved } = createNoteSaveDeps({
    updateJobNotes: async (_uid, _jobKey, value) => {
      updateCalls += 1;
      if (updateCalls === 1) return { ok: false, error: "offline" };
      savedValues.push(value);
      return { ok: true };
    }
  });
  deps.flushNotesSave = (jobKey, value) => flushNotesSave(jobKey, value, deps);
  state.pendingValues.set("job-1", "Draft note");

  try {
    await flushNotesSave("job-1", undefined, deps);
    assert.equal(saveStates.at(-1).saveState, "error");

    queueNotesSave("job-1", "Edited note", {
      noteSaveState: state,
      noteAutosaveMs: 0,
      dispatchQueued() {},
      setNoteSaveState: deps.setNoteSaveState,
      flushNotesSave: deps.flushNotesSave
    });
    await waitFor(() => updateCalls === 2 && !state.pendingValues.has("job-1"), "edited note retry did not settle");

    assert.deepEqual(savedValues, ["Edited note"]);
    assert.equal(saveStates.at(-1).saveState, "saved");
    assert.deepEqual(failed, [{ jobKey: "job-1", error: "offline" }]);
    assert.deepEqual(saved, ["job-1"]);
  } finally {
    clearNoteSaveQueues(state);
  }
});

test("a newer note edited during a save is flushed after the current save", async () => {
  let resolveFirstSave;
  const savedValues = [];
  const { deps, state, saved } = createNoteSaveDeps({
    updateJobNotes: async (_uid, _jobKey, value) => {
      savedValues.push(value);
      if (savedValues.length === 1) {
        return new Promise(resolve => {
          resolveFirstSave = resolve;
        });
      }
      return { ok: true };
    }
  });
  deps.flushNotesSave = (jobKey, value) => flushNotesSave(jobKey, value, deps);

  queueNotesSave("job-1", "First note", {
    noteSaveState: state,
    noteAutosaveMs: 60_000,
    dispatchQueued() {},
    setNoteSaveState: deps.setNoteSaveState,
    flushNotesSave: deps.flushNotesSave
  });
  const firstFlush = flushNotesSave("job-1", undefined, deps);

  try {
    await waitFor(() => typeof resolveFirstSave === "function", "first save did not start");
    queueNotesSave("job-1", "Newer note", {
      noteSaveState: state,
      noteAutosaveMs: 60_000,
      dispatchQueued() {},
      setNoteSaveState: deps.setNoteSaveState,
      flushNotesSave: deps.flushNotesSave
    });
    resolveFirstSave({ ok: true });
    await firstFlush;
    await waitFor(() => savedValues.length === 2, "newer note was not flushed");
    await waitFor(() => state.pendingValues.size === 0, "newer note save did not settle");

    assert.deepEqual(savedValues, ["First note", "Newer note"]);
    assert.deepEqual(saved, ["job-1"]);
    assert.equal(state.pendingValues.has("job-1"), false);
  } finally {
    clearNoteSaveQueues(state);
  }
});
