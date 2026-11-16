/**
 * The Runtime Database Storage reclaim panel's follow-up read: while a run is
 * live the panel reads the route again on its own, so a run the bridge has
 * already finished collapses its modal instead of waiting for a manual Refresh.
 *
 * The read is bounded and self-cancelling, which is what these cases pin: a
 * terminal status clears the timer, repeated payloads keep one timer rather than
 * one per render, a run that outlives the window stops being followed, and a run
 * that starts later on the same page gets a window of its own.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { controller, fakeTimers, payload, refs } from "./helpers/admin-storage-reclaim-fixtures.mjs";

test("a live run's own follow-up read closes the modal when the run finishes", async () => {
  const target = refs();
  const clock = fakeTimers();
  const { instance, calls } = controller(
    target,
    {
      // The bridge had already finished; only the panel's own read learns that.
      get: () =>
        payload({ scope: "modal", status: "complete", phase: "complete", runId: "run-12", mode: "reclaim" })
    },
    clock
  );

  instance.accept(
    payload({ scope: "modal", status: "running", phase: "verifying", runId: "run-12", mode: "reclaim" })
  );
  assert.equal(target.adminStorageReclaimModalEl.hidden, false);
  assert.equal(clock.pending.length, 1);

  await clock.fire();

  // A plain read: only Refresh asks for the verdict to be recomputed.
  assert.deepEqual(calls.get, ["/ops/storage-reclaim"]);
  assert.equal(target.adminStorageReclaimModalEl.hidden, true);
  assert.equal(target.adminStorageReclaimModalBodyEl.innerHTML, "");
  assert.equal(target.adminStorageReclaimStatusEl.textContent, "Reclaim complete");
  assert.equal(target.adminStorageReclaimStopBtnEl.hidden, true);
  // Self-cancelling: a settled run schedules nothing more.
  assert.equal(clock.pending.length, 0);
});

test("a still-running read keeps the follow-up going", async () => {
  const target = refs();
  const clock = fakeTimers();
  const { instance, calls } = controller(
    target,
    {
      get: () =>
        payload({
          scope: "modal",
          status: "running",
          phase: "deleting-backup",
          runId: "run-13",
          mode: "reclaim"
        })
    },
    clock
  );

  instance.accept(
    payload({ scope: "modal", status: "running", phase: "copying", runId: "run-13", mode: "reclaim" })
  );
  await clock.fire();
  await clock.fire();

  assert.equal(calls.get.length, 2);
  assert.match(target.adminStorageReclaimStatusEl.textContent, /deleting the old database/);
  assert.equal(clock.pending.length, 1);
});

test("a run that outlives its window stops being followed and says so", async () => {
  const target = refs();
  const clock = fakeTimers();
  // Modal scope, like a container: the dialog is behind the modal, so both have
  // to carry the stopped line.
  const running = runId =>
    payload({ scope: "modal", status: "running", phase: "copying", runId, mode: "reclaim" });
  let tick = 0;
  const { instance } = controller(
    target,
    { get: () => running("run-20") },
    { ...clock, now: () => tick, liveReadMaxMs: 1000 }
  );

  instance.accept(running("run-20"));
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /The panel follows it while it runs/);
  assert.doesNotMatch(target.adminStorageReclaimModalBodyEl.innerHTML, /Live updates stopped/);
  assert.equal(clock.pending.length, 1);

  tick = 5000; // the run outlives its window
  await clock.fire();

  // It says what is true: no more live updates, and Refresh is the way on.
  assert.equal(clock.pending.length, 0);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /Live updates stopped for this run/);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /<strong>Refresh<\/strong> resumes them/);
  assert.doesNotMatch(target.adminStorageReclaimDialogEl.innerHTML, /follows it while it runs/);
  // The container modal is the whole panel there, and the way back is Hide + Refresh.
  assert.match(target.adminStorageReclaimModalBodyEl.innerHTML, /Live updates stopped for this run/);
  assert.match(
    target.adminStorageReclaimModalBodyEl.innerHTML,
    /Hide this dialog and <strong>Refresh<\/strong> to resume them/
  );

  // Refresh reopens the window instead of leaving the stopped line behind.
  await instance.refresh();
  assert.equal(clock.pending.length, 1);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /The panel follows it while it runs/);
  assert.doesNotMatch(target.adminStorageReclaimModalBodyEl.innerHTML, /Live updates stopped/);
});

test("a settled run clears the pending timer", () => {
  const target = refs();
  const clock = fakeTimers();
  const { instance } = controller(target, {}, clock);

  instance.accept(payload({ status: "running", phase: "copying", runId: "run-15", mode: "reclaim" }));
  assert.equal(clock.pending.length, 1);

  // The run ends before the timer fires: nothing is left scheduled.
  instance.accept(payload({ status: "complete", phase: "complete", runId: "run-15", mode: "reclaim" }));

  assert.equal(clock.pending.length, 0);
});

test("payload after payload keeps a single timer", () => {
  const target = refs();
  const clock = fakeTimers();
  const { instance, calls } = controller(target, {}, clock);
  const running = payload({ status: "running", phase: "copying", runId: "run-16", mode: "reclaim" });

  instance.accept(running);
  instance.accept(running);
  instance.accept(running);

  assert.equal(clock.pending.length, 1);
  assert.equal(calls.get.length, 0);
});

test("a later run gets its own follow-up window", async () => {
  const target = refs();
  const clock = fakeTimers();
  const running = runId => payload({ status: "running", phase: "copying", runId, mode: "reclaim" });
  let tick = 0;
  const { instance } = controller(
    target,
    { get: () => running("run-17") },
    { ...clock, now: () => tick, liveReadMaxMs: 1000 }
  );

  instance.accept(running("run-17"));
  assert.equal(clock.pending.length, 1);

  tick = 5000; // this run has outlived its window
  await clock.fire();
  assert.equal(clock.pending.length, 0);

  // A run that begins later is still followed: the closed window was not shared.
  instance.accept(running("run-18"));
  assert.equal(clock.pending.length, 1);
});
