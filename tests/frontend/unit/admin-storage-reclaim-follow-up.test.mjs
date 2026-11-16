/**
 * The Runtime Database Storage reclaim panel's follow-up read: while a run is
 * live the panel reads the route again on its own, so a run the bridge has
 * already finished collapses its modal instead of waiting for a manual Refresh.
 *
 * The read is self-cancelling, which is what these cases pin: a terminal status
 * clears the timer, repeated payloads keep one timer rather than one per render,
 * and the follow-up keeps going for as long as the run is live.
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

test("a run that never settles keeps being followed, however long it runs", async () => {
  const target = refs();
  const clock = fakeTimers();
  // Modal scope, like a container: the dialog is behind the modal, so the modal
  // is the only place the operator can watch the run.
  const running = runId =>
    payload({ scope: "modal", status: "running", phase: "copying", runId, mode: "reclaim" });
  const { instance, calls } = controller(target, { get: () => running("run-20") }, clock);

  instance.accept(running("run-20"));
  await clock.fire();
  await clock.fire();
  await clock.fire();

  // No window: neither presentation promises anything it is not doing, and the
  // reads keep coming while the run is live.
  assert.equal(calls.get.length, 3);
  assert.equal(clock.pending.length, 1);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /follows it while the bridge answers/);
  assert.doesNotMatch(target.adminStorageReclaimDialogEl.innerHTML, /stopped/i);
  assert.match(target.adminStorageReclaimModalBodyEl.innerHTML, /copying the database/);
  assert.doesNotMatch(target.adminStorageReclaimModalBodyEl.innerHTML, /stopped/i);
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

test("a bridge that stops answering ends the follow-up and keeps the last status", async () => {
  const target = refs();
  const clock = fakeTimers();
  const running = payload({
    scope: "modal",
    status: "running",
    phase: "copying",
    runId: "run-30",
    mode: "reclaim"
  });
  let down = true;
  const { instance, calls } = controller(
    target,
    {
      get: () => {
        if (down) throw new Error("connection refused");
        return payload({
          scope: "modal",
          status: "running",
          phase: "verifying",
          runId: "run-30",
          mode: "reclaim"
        });
      }
    },
    clock
  );

  instance.accept(running);
  assert.match(target.adminStorageReclaimStatusEl.textContent, /copying the database/);

  // Four failures are a blip the panel keeps trying through.
  for (let attempt = 0; attempt < 4; attempt += 1) await clock.fire();
  assert.equal(clock.pending.length, 1);

  // The fifth ends it: no timer, and the status the bridge last gave is kept.
  await clock.fire();
  assert.equal(clock.pending.length, 0);
  assert.equal(calls.get.length, 5);
  assert.match(target.adminStorageReclaimStatusEl.textContent, /copying the database/);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /Reclaim status unavailable/);
  assert.equal(target.adminStorageReclaimModalEl.hidden, false);

  // A read that answers again resumes the follow-up.
  down = false;
  await instance.refresh();
  assert.equal(clock.pending.length, 1);
  assert.doesNotMatch(target.adminStorageReclaimDialogEl.innerHTML, /status unavailable/);
  assert.match(target.adminStorageReclaimStatusEl.textContent, /verifying the copy/);
});

test("a run renders and settles normally while its reads succeed", async () => {
  const target = refs();
  const clock = fakeTimers();
  const running = phase =>
    payload({ scope: "modal", status: "running", phase, runId: "run-31", mode: "reclaim" });
  const complete = payload({
    scope: "modal",
    status: "complete",
    phase: "complete",
    runId: "run-31",
    mode: "reclaim"
  });
  // The accept below is the click's own payload; these answer the follow-up reads.
  const answers = [() => running("verifying"), () => complete];
  const { instance, calls } = controller(target, { get: () => answers.shift()() }, clock);

  instance.accept(running("copying"));
  assert.match(target.adminStorageReclaimModalBodyEl.innerHTML, /copying the database/);

  await clock.fire();
  assert.match(target.adminStorageReclaimStatusEl.textContent, /verifying the copy/);
  assert.equal(clock.pending.length, 1);

  // The read that sees the end closes the modal, and nothing is left armed.
  await clock.fire();
  assert.equal(target.adminStorageReclaimStatusEl.textContent, "Reclaim complete");
  assert.equal(target.adminStorageReclaimModalEl.hidden, true);
  assert.equal(clock.pending.length, 0);
  assert.equal(calls.get.length, 2);
});

test("a run that begins after the last one settled is followed too", async () => {
  const target = refs();
  const clock = fakeTimers();
  const running = runId => payload({ status: "running", phase: "copying", runId, mode: "reclaim" });
  const done = runId => payload({ status: "complete", phase: "complete", runId, mode: "reclaim" });
  const { instance, calls } = controller(target, { get: () => running("run-18") }, clock);

  instance.accept(running("run-17"));
  assert.equal(clock.pending.length, 1);

  instance.accept(done("run-17"));
  assert.equal(clock.pending.length, 0);

  // Settling one run must not leave the panel deaf to the next one.
  instance.accept(running("run-18"));
  assert.equal(clock.pending.length, 1);
  await clock.fire();
  assert.equal(calls.get.length, 1);
});
