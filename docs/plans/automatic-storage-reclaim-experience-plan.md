# Automatic Reclaim: Experience and Test Detail

> - **Class:** companion slice detail for
>   [`automatic-storage-reclaim-plan.md`](automatic-storage-reclaim-plan.md)
> - **Trigger:** implementing or reviewing the reclaim dialog, the inline desktop panel,
>   or the reclaim test suite, without re-reading the whole parent plan
> - **Verified against:** `560ed78f` (tree inspection only; no reclaim code exists)
> - **Status:** Planned — not started. Every slice here is unimplemented.

The parent plan carries the decisions and their rationale; this doc carries the
mechanics and the per-file case list those decisions imply, because at ~1,300 words they
were half the parent's budget and none of its judgement. Where the two disagree, the
parent wins and this file is the bug. Sections here mirror parent §6 and §8.

## A. Presentation mechanics

Parent §6 states why the container blocks and desktop does not; this section is the
behaviour those decisions require in the shipped Admin surface.

In a container, the modal is one native `<dialog>` in `admin.html`, opened with
`showModal()`:

- `::backdrop` dims the app; the document behind is inert, so clicks, Tab and scroll
  cannot reach it.
- `state.adminBusyState.storageReclaim` keeps existing `shouldDefer()` guards and
  mutating polls off; the bridge hold covers other tabs and the Jobs page.
- **Stop** renders only in the cancellable phases listed in the parent plan's §3. During `installing`,
  `verifying` and `deleting-backup` the dialog's `cancel` event (ESC) and backdrop
  clicks are `preventDefault()`ed and the body states that it cannot be interrupted.
- `aria-live="polite"` phase line, focus moves to the primary action on open and
  returns to the storage panel on close.
- Recovery: `GET /ops/storage-reclaim` on Admin init reopens the modal for a run
  started in another tab or before a reload.
- Exits: `complete` (reclaimed and freed bytes, Close) and
  `failed`/`superseded`/`interrupted`/`cancelled` (message, Close).

**Slow copy.** The copy scales with the file, so on a slow disk it can run for tens of
minutes. The modal shows real progress, never a bare spinner:

- The phase line plus `tablesDone / tablesTotal` and the staging size as it grows
  (`progress.stagingBytes`, emitted per table by
  `rebuild_current_feed_database`).
- **ETA from staged bytes**: the last two poll samples (`Delta stagedBytes / Delta ms`)
  extrapolated to the target — `currentGenerationEstimatedBytes` plus non-feed
  allocation, both already verdict inputs, so no extra scan.
- Shown only with trustworthy inputs: verdict `live`, `countsKnown: true`, ≥10 s
  elapsed, staging ≥10 % of target. Otherwise `Copying - 7 of 31 tables`, no number — an
  invented estimate is worse than none.
- A sample rate that collapses below 5 % of the previous sample suppresses the ETA
  until it recovers, so a stalled disk reads as slow rather than as
  "2 minutes remaining" forever.
- `installing` and `deleting-backup` show no ETA: they are bounded, uninterruptible
  steps and are labelled as such.
- **A bridge that disappears mid-run neither closes the dialog nor reads as a failure.**
  It shows "Waiting for the bridge…" and keeps polling until the bridge answers; the
  outcome is settled by the restart rules below, which is why the dialog never decides
  from its own last sample.

**Closing the tab is safe because the job is backend-owned, and the modal says so.**
The guarantee is structural, not a UI convention: the run is a bridge thread keyed by
data directory with no request-scoped state, every transition is journaled before it is
reported, and the install is a single atomic rename under the write and read locks — so
abandoning the page can neither fail, cancel, nor half-finish the run, and the only
thing the page owns is a poll timer. The dialog states it plainly — "You can close this
tab; the operation keeps running and this panel shows the outcome when you return" —
and `GET /ops/storage-reclaim` on Admin init reopens the modal at the current phase.
The dialog therefore sends **no** cancel on `pagehide`, `beforeunload`, or unmount;
only the explicit **Stop** cancels. The block stops *mutations*, not observation. On
desktop the page
sends no cancel either, but the bridge it talks to exits with the app, so closing the
window ends the run as `cancelled` (shutdown's forced cancel) or `interrupted` (killed
process) instead of `complete`; the live database is unchanged either way, and the
panel reports it on the next launch.

**When a restart leaves the run interrupted, not running.** After a bridge restart
`StorageRebuildJob` rehydrates only terminal journal statuses; `running`, `staged` and
`backup-created` are settled by `_reconcile_crash_state`, which runs on the next
`start()`. A fresh job therefore reports `idle` — so the reclaim endpoint, not the
modal's polling loop, decides the outcome. Every status read checks three things: the
journal phase, whether a job thread exists in this process, and whether the swap
completed. A modal left open across the restart shows the waiting line, then closes on
the first settled read — it never spins on a run that no longer exists and never
invents a new `runId`.

| Journal after restart | Reclaim status reports | Modal |
|---|---|---|
| `running` / `staged` (pre-swap) | `interrupted` | Closes: "Interrupted before install. The live database is unchanged and nothing was deleted." The staging file is removed by the next start's §3 step 2 (journal token, not the one-hour age cutoff). |
| `backup-created`, post-swap | `complete` | Closes: "Rebuild finished; the previous copy is retained. Use **Delete old database** in the panel." No delete resumes, per §3. |
| terminal (`complete`, `failed`, `cancelled`, `superseded`) | that status | Closes with that message. |

The modal never polls a run that cannot progress: `interrupted` is terminal, and a
second `GET` returns the same settled state instead of a new `runId`.

## B. Test list

Each bullet is a set of cases, not a file. Assertions are on observable outcomes - a
refusal code, a retained file, a settled journal status - never on a call having happened.

- **Policy**: ratio matrix, 512 MiB floor, cooldown from a journal `finishedAt`,
  every blocker, and the case that matters most — a database with history *inside*
  the retention window reports `ok`.
- **Orchestrator**: phase walk against a fake rebuild job; cancel during `copying`
  never reaches a delete; delete receives the completed run's `backupBytes`;
  `backup-exists` refuses; `superseded` stops without retry; a restart leaves a
  complete rebuild with the copy on disk and **no** delete; a `no-current-generation`
  database yields `ok` with that blocker and no ratio.
- **Content parity**: a tampered staging copy fails the copy-time digest and never
  installs; a live database whose saved-jobs content drifts after install fails the
  delete-time digest and keeps the retained copy; a legitimate `task_runs` write
  between install and delete does **not** fail, because that table is excluded.
- **Dialog**: opens on a live run; Stop hidden in `installing`/`verifying`/
  `deleting-backup`; ESC blocked there; single-flight; reload recovery; never opens
  for `backup-exists`; the ETA is suppressed when `countsKnown` is false or the verdict
  is cached/stalled and shown when both samples are clean; a restart-interrupted run
  closes with the unchanged-database message and issues no further polls; a
  post-swap `complete` offers no delete action; closing the tab or firing `pagehide`
  sends no cancel and the run still reaches `complete`; a bridge that vanishes mid-run
  shows the waiting line, keeps polling, and closes on the settled outcome.
- **Full disk**: a killed run's staging file is removed without waiting an hour when
  the journal names no active run, and kept when the token matches a journal that says
  `running`; a staging sidecar whose file is gone is removed; the measured gate starts
  a copy the heuristic would refuse; the automatic trigger never reaches step 5 and
  declines with `free-space`; a refusal carries `shortfallBytes` and the artifacts it
  removed; no step deletes the live database or its `-wal`/`-shm`.
- **Dry run**: builds and validates staging, reports `stagingBytes` and `durationMs`,
  leaves `backupExists` and journal `status` unchanged, and removes its staging file on
  success, failure and cancel.
- **Hold**: a hold whose job thread dies without a terminal phase is released by the
  next task-start read, which returns 200 instead of `409 storage-reclaim-running`,
  settles the run `interrupted` and records `releasedBy: "watchdog"`; a silent live hold
  expires at 60 s, a hold past `RECLAIM_HOLD_MAX_S` is released even while alive, and a
  healthy run releases on its terminal phase idempotently.
- **Presentation**: with `desktop_mode` set the trigger runs without taking the hold,
  `GET` reports `scope: "inline"`, no dialog opens even for a live run, and the panel's
  existing Stop cancels by `runId`; without it `GET` reports `scope: "modal"`, the
  dialog opens, and task starts hit `409 storage-reclaim-running`. The verdict, checks
  and cooldown are identical in both.
- **Guardrails**: route inventory, gateway budget, `ruff`, plan lifecycle tripwires,
  markdown link check.

## C. UI files these slices touch

| Where | Pinned by |
|---|---|
| `admin.html`, new `frontend/admin/app/storage-reclaim-dialog.js`, wiring in `frontend/admin/app/storage-breakdown.js` (beside `acceptRebuild` / `scheduleRebuildPoll`), `frontend/shared/ui/tokens/admin.js`, `styles/admin.css` | `tests/frontend/unit/admin-storage-reclaim-dialog.test.mjs` |

Backend slices stay in the parent plan's §7, because the orchestrator, hold and route
files are needed to read the decisions there.
