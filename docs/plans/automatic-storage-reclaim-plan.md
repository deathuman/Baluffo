# Automatic Bloat Reclaim Plan

> - **Class:** enhance-stable
> - **Trigger:** a runtime database whose file is mostly feed history the retention
>   policy would already delete — the case where the bounded prune needs weeks
>   (measured ~7.3k rows/min) and only a rebuild returns the space
> - **Status:** In progress. The four preview fields (§1) are implemented in
  `cleanup_preview` (pinned by `tests/storage/test_job_runtime_preview.py`), the
  verdict leaf is `src/storage/reclaim_policy.py` (pinned by
  `tests/storage/test_reclaim_policy.py`), and `GET /ops/storage-reclaim` serves
  the verdict, live status and journal through the 60 s cache
  (`src/bridge/storage_reclaim.py`, pinned by
  `tests/bridge/test_storage_reclaim.py`). The journaled `StorageReclaimJob` and
  the start/cancel POSTs are implemented in the same module, composing the
  rebuild's public start/cancel/delete entry points; `active-task` is read from
  the bridge's task hold. The reclaim hold (§3) is implemented in
  `src/bridge/storage_health.py` (expiry, heartbeat, stall, hard maximum, read-path
  watchdog, shutdown release) with the `409 storage-reclaim-running` guard on
  task-start routes; the run takes it on container scope and releases it in the
  same `finally` that clears its thread. Content parity (§3) is implemented in
  `src/storage/current_feed_rebuild.py`: the immutable-table allowlist is hashed
  on both sides of the copy, `copyDigests` is journaled, and the installed
  database is re-hashed before the delete, failing as `non-feed-mismatch` with the
  table's name. A write that races the copy settles `superseded`, so only a copy defect
  is reported as a mismatch. The rehearsal (§9) is implemented:
  `POST {dryRun: true}` starts a background read-only copy on the same single-flight
  and hold, stops runtime retention as the rebuild does, is capped at
  `RECLAIM_MAX_RUN_S`, reports staging/reclaimed
  bytes, duration, per-table counts, validation and content digests under the
  journal's `result`, unlinks its staging file and scratch
  `<db>.reclaim-rehearsal.json` in a `finally`, and pulses the hold while
  retention and validation run. Triggers (§4) are implemented in
  `src/bridge/storage_reclaim_triggers.py`: the closeout transition that releases
  the compaction hold asks once, startup schedules one check `60` s after
  maintenance resumes, both go through `auto: true` so the verdict, every safety
  blocker and the cooldown still apply, and a running retention pass defers them
  into one coalesced re-check that waits for it (capped at
  `RECLAIM_TRIGGER_RECHECK_MAX_S`, two hours, not the run's own ceiling).
  The full-disk order (§3) is implemented in
  `src/bridge/storage_reclaim_space.py`: a copy removes a staging file no live run
  claims, starts on the measured gate when the heuristic is short, and reports the
  relief and both gates. Step 5 stays the panel's **Delete old database** click.
  Presentation (§6) is partly implemented: the inline desktop panel and the container modal
  (Stop, Hide, status text, exits, `aria-live`) ship in
  `frontend/admin/app/storage-reclaim-panel.js` and
  `frontend/admin/render/storage-reclaim-dialog.js`, a `role="dialog"` element toggled
  with `hidden` rather than a native `<dialog>`; its focus, inert background and the
  slow-copy ETA are not.

## Scope

One automatic operation that decides a database is bloated, rebuilds it around the
published feed, verifies the result, and deletes the retained pre-rebuild copy —
without a second manual click.

It **composes** the shipped current-feed rebuild (`StorageRebuildJob`) instead of
replacing it. The manual **Rebuild current feed** / **Roll back** / **Delete old
database** controls keep working unchanged and remain the fallback whenever this
operation is disabled or blocked.

Not in scope: pruning `source_runs` or event history; changing retention policy;
changing what a rebuild copies; desktop packaging; anything on the Jobs page.

## 1. Bloat verdict (read-only)

```
historicalBytes = bounded payload sample of every non-current jobs/job_sources row
reclaimable     = historicalBytes + freelistBytes
ratio           = reclaimable / databaseBytes

bloat  when  ratio >= BALUFFO_RECLAIM_MIN_RATIO_PCT / 100
        and  reclaimable >= RECLAIM_ABSOLUTE_FLOOR (512 MiB, constant)
```

"Why" is built in, not measured: this repo's retention deletes every generation
except the current one (`cleanup_preview(keep_generations=1)`), so any non-current
generation in the feed tables is space the product has already declared disposable.

**The verdict does not subtract payload from `tableSizes`.** `tableSizes` counts
allocated pages, including indexes and the current generation; the preview fields
count payload bytes. Subtracting one from the other would count current-feed index
and page overhead as reclaimable and could call bloat on a database whose history
is still inside the retention window. `historicalBytes` is instead measured
directly and is deliberately conservative: it ignores index and page overhead, and a
generation with no known row count contributes only its sampled bytes, so the
structural error always runs low. The one place it is an estimate rather than a strict
bound is a generation whose count is known, projected from its sample mean — a sample
unrepresentative of that generation can read high, which is why the rehearsal in §9
prints the numbers before auto mode is switched on. `freelistBytes` is the
exact freelist page count and `databaseBytes` the file size.

`cleanup_preview` is capped at `DEFAULT_GENERATION_DELETE_CAP = 4`, so its candidate
list cannot describe a weeks-old backlog. It therefore gains **four** fields. The
current method samples only the capped candidates, so the first two are new bounded
samples rather than free fields:

- `currentGenerationEstimatedBytes` — bounded payload sample of the generation being
  kept (also the ETA target);
- `historicalGenerationEstimatedBytes` — bounded payload sample across the
  generations outside the keep window (every non-current generation at the
  default `keep_generations=1`, so a keep-two preview cannot count a generation
  retention still protects);
- `historicalGenerationCount` — number of non-current generations seen by that sample;
- `estimatedReclaimableBytes` — `historicalGenerationEstimatedBytes + freelistBytes`,
  the conservative lower bound used above.

`candidateGenerationCount` and `capReached` stay as **evidence only**; neither gates
the decision, because the cap makes the count misleading on exactly the databases
this operation exists for.

Payload: `verdict` (`bloat` / `ok`), `ratio`, `reclaimable`, `databaseBytes`,
`freelistBytes`, `currentGenerationEstimatedBytes`,
`historicalGenerationEstimatedBytes`, `historicalGenerationCount`,
`estimatedReclaimableBytes`, `reclaimableBasis` (`payload-sample+freelist`),
`candidateGenerationCount`, `capReached`, `blockers[]`, `cooldownRemainingHours`.

Blockers, each stated rather than silently skipped: `no-current-generation`,
`active-task`, `compaction-running`, `backup-exists`, `free-space`, `cooldown`,
`auto-disabled`, `store-unavailable`, `sample-unavailable`.

`free-space` is a refusal with a way out, not a dead end: it names the shortfall and
follows the ordered resolution in §3, which re-asks the gate against a measured staging
size before it suggests giving up the rollback copy.

`no-current-generation` is required, not defensive. With no published feed there is
no protected generation to define "history": every feed row would count as
reclaimable, so the ratio is meaningless rather than conservative — measured at
**15.25 %** on an empty 236 KiB dev database with zero history. Only the 512 MiB
floor kept that from being a verdict; a first-run or reset database must report
`verdict: "ok"` with this blocker.

Computed on demand (Admin init and after a fetch closeout), never on the 5 s
storage-health poll: the read does bounded row counts and payload sampling. The
route caches the verdict for `RECLAIM_VERDICT_TTL_S = 60` so modal polling does not
repeat those samples; `?refresh=1` forces a fresh read.

## 2. Configuration

| Knob | Default | Behavior it changes |
|---|---|---|
| `BALUFFO_RECLAIM_AUTO_ENABLED` | unset (self-arming) | Kill switch for automatic starts. Unset, one rehearsal runs and the journal records the decision and its evidence as `autoArm`; auto starts once it is adequate and the anchor fresh. `0` blocks even the rehearsal, `1` forces auto on, unrecognised values are off; manual controls are unchanged. |
| `BALUFFO_RECLAIM_MIN_RATIO_PCT` | `10` | How much of the file must be reclaimable before it counts as bloat. |
| `BALUFFO_RECLAIM_MIN_INTERVAL_HOURS` | `168` | Anti-thrash for the **automatic** trigger only: 168 h after a **completed** reclaim, one hour after a failed, cancelled, interrupted or superseded one, so a transient failure cannot silence auto mode for a week; `auto: false` passes both (§5). |

No other knobs. Constants in the policy leaf: the 512 MiB floor,
`RECLAIM_VERDICT_TTL_S = 60`, and reuse of the existing free-space gate
`rebuild_required_free_bytes()`.

## 3. The operation

`StorageReclaimJob` (`src/bridge/storage_reclaim.py`) owns one run per data dir and
drives the existing rebuild job by composition:

```
evaluating → rebuild(queued … installing) → verifying → deleting-backup → complete
                                           ↘ failed / superseded / cancelled / interrupted
```

| Phase | Cancellable | Notes |
|---|---|---|
| evaluating | yes | verdict recomputed under the hold |
| stopping-retention, checking-space, copying | yes | `POST /ops/storage-reclaim/cancel` delegates to the existing cancel with the rebuild `runId` |
| installing | no | atomic swap in `StorageRebuildJob._run` |
| verifying | no | `quick_check`, foreign keys, counts, feed hashes, non-feed content digests |
| deleting-backup | no | the existing background delete worker validates, byte-guards, then unlinks; reclaim does not retry it |

- The delete calls `start_delete_backup(confirm=True,
  expected_backup_bytes=<backupBytes from the completed rebuild snapshot>)` and polls the
  storage-rebuild status until that run is terminal, because the request thread must not
  hold a multi-second validation and unlink. The byte guard stays intact; it protects
  against deleting a different file, not against an absent-minded click.
- `backup-exists` at start is refused, exactly as the rebuild refuses it, and
  surfaces as a blocker. A retained copy is a panel condition, never a modal.
- **The delete is gated on content parity, not row counts.** The shipped rebuild
  compares per-table **row counts** for the non-feed tables and content-hashes only
  `jobs` / `job_sources` (`validate_feed_generation`), so today a copy bug inside
  saved jobs, notes, or the source registry passes every check and the only recovery
  would be the copy this operation deletes. Reclaim therefore adds two checks at two
  different moments:
  1. **Copy fidelity, at copy time** — per-table content digests of the data-of-record
     tables computed on the source and on the staging copy inside the same copy pass,
     compared before install and stored in the journal. This is the check that can
     catch a copy bug, and it is the one the rebuild is missing.
  2. **Installed-DB integrity, before the delete** — the same digests recomputed on
     the live database and compared with the journal. Only **immutable data-of-record
     tables** are hashed (`saved_jobs`, notes, source-registry rows, tombstones,
     availability ledgers, and an explicit allowlist in the leaf). Append-only runtime
     tables (`task_runs`, `task_events`, `source_runs`, `sync_runs`) and
     `job_feed_state` are excluded: they change legitimately, and hashing them would
     turn every delete into a false `non-feed-mismatch`.
  A mismatch at either point is a blocker (`non-feed-mismatch`), keeps the retained
  copy, and names the table in the modal. The allowlist is a constant, not a knob.
- No retry on `superseded`: a write landed mid-copy, so retrying in a loop would
  burn full database copies. The cooldown applies and the next window tries once.
- Journal: a dedicated `<db>.reclaim.json`, not a nested record in
  `<db>.rebuild.json`. The rebuild leaf rewrites its own journal wholesale when
  the copy starts (`write_rebuild_journal`), so a nested reclaim record would be
  erased at exactly the wrong moment. The reclaim journal carries
  `{schemaVersion, runId, rebuildRunId, mode, status, phase, startedAt, finishedAt,
  auto, expectedBackupBytes, copyDigests}`; `finishedAt` dates the cooldown, whose
  length depends on `status`: the full window for `complete`, one hour otherwise.
  It is written at every transition, read at startup, and **never** resumed into a
  delete. The rebuild journal remains the source of rebuild phase and crash
  recovery. A reclaim journal write is best-effort: memory settles the live run,
  and the next start reconciles.
- `set_storage_reclaim_hold()` lives beside `set_storage_compaction_hold()` in
  `src/bridge/storage_health.py`; task-start routes return
  `409 {reason: "storage-reclaim-running"}` while it is held. Without this the
  operation's promise is false: a fetch started from the Jobs page makes the
  install `superseded` and burns a full copy. The hold is **container-only** (§4): on
  desktop there is nothing to hold, because a lost race costs a seconds-long copy.

### Hold TTL and watchdog

The hold is a boolean today (`_COMPACTION_HOLD`), and a boolean cannot expire: a crashed
run would block every fetch start until the bridge restarted. Reclaim stores
`{runId, heldAt, heartbeatAt}` and derives `expiresAt` from `heldAt` on every read.

- **Release condition**: the job writes a terminal phase (`complete`, `failed`,
  `superseded`, `interrupted`, `cancelled`) and releases in the same `finally` that
  clears its thread. Never on a phase transition, never from the page — closing the tab
  releases nothing (§6).
- **Watchdog**: when the hold's `runId` has no terminal journal status and its thread is
  not alive, the read settles the run `interrupted` and releases. The check lives on the
  read path, so it works with no job running to notice.
- **Stall**: `RECLAIM_HOLD_HEARTBEAT_TIMEOUT_S = 60`, refreshed by each progress event;
  a silent hold with a live thread is released the same way.
- **Maximum: `RECLAIM_HOLD_MAX_S = 28800` (8 h)** from `heldAt`, hard and not renewable.
  That exceeds the copy envelope (minutes to hours) plus validation, and caps a stuck
  run at eight hours of blocked starts. `RECLAIM_MAX_RUN_S` is the same value, given to
  each long wait rather than to the run as one clock; reaching it stops the copy (up to
  `RECLAIM_STOP_WAIT_S` = 60 s) before settling, adopting a copy that finished.
  Constants, not knobs.
- **Shutdown** releases unconditionally; the next start reconciles from the journal.

Task-start replies carry `releasedBy: terminal | watchdog | ttl | shutdown`, so a normal
release is distinguishable from a rescue.

**Restart policy.** A pending delete does **not** resume after a
bridge restart. The unlink is the only irreversible step, and a journal is evidence,
not intent. Nothing is lost: the journal reads `complete` with the copy on disk, so
the storage panel renders **Delete old database** with the reviewed byte count, and
that route re-runs the same feed validation before unlinking. Existing reconciliation is unchanged
(`_reconcile_missing_backup`, `_reconcile_crash_state`) and no new terminal state is
introduced.

### Full disk: the ordered resolution

The deadlock: freeing space needs space — retention only fills the freelist, compaction
refuses at `skipped-no-space` when free < live + 64 MiB (`baluffo_store.py`), and
`VACUUM` needs a temp copy. Two levers remain: removing a file that is not the live
database, and checking whether the gate asks for more than the copy will use. Every step
re-measures; the order stops at the first that satisfies the gate.

| # | Step | Frees | Automatic |
|---|---|---|---|
| 1 | Measure `freeBytes`, heuristic `requiredBytes` (`rebuild_required_free_bytes`) and the measured gate (1.25 × the last measured copy + 64 MiB) | — | yes |
| 2 | Remove staging (`<db>.<token>.rebuild.tmp`) no live copy has claimed — not the one-hour cutoff, which blocks a retry after a kill | ≤ one partial copy | yes |
| 3 | Remove orphaned `<staging>-wal`/`-shm`, `<journal>.tmp`, and `<db>.reclaim-rehearsal.json`/`.tmp` when no rehearsal thread owns them; the shipped glob covers `*.rebuild.tmp` only | MBs | yes |
| 4 | Start when `freeBytes` ≥ the measured gate: the 1 % heuristic is probe-avoidance, not physics — 2.32 GB asked for a 0.5–1.5 GB copy | 0 (re-asks) | yes |
| 5 | Delete the retained `<db>.pre-rebuild`, distinct inode, byte-guarded, after showing the bytes and the rollback it costs | ~old size | **click** |
| 6 | Refuse with `free-space`: `freeBytes`, `requiredBytes`, `measuredRequiredBytes`, `shortfallBytes`, `relievedBytes`, artifacts removed | — | — |

- **Step 4 before step 5, and step 2 before step 5**: never ask for the rollback copy
  while a cheaper lever or a re-ask might do.
- **Step 5 is never automatic.** The trigger stops at 4 and declines with `free-space`;
  the panel renders **Delete old database** beside the shortfall.
- **Fail closed only when unmeasurable.** `freeBytes <= 0` is a failed `stat`
  (`free_disk_bytes`), not a small disk; a small measured number is step 4's case. An
  ENOSPC mid-copy already raises, unlinks its staging and journals `failed`, leaving the
  live database untouched.
- **An unwritable journal is not fatal**: both journal writes are best-effort, the run
  settles in memory, and the next start reconciles.

Never: delete generation rows (a freelist page is not a filesystem byte), use compaction
or `VACUUM` as relief, touch the live database or its `-wal`/`-shm`, or delete a path this
codebase does not name.

**Implemented as `src/bridge/storage_reclaim_space.py`.** Ownership is a live
registry, not a file age: a copy claims its staging and scratch paths while its
thread exists, so a killed run's partial copy stays reclaimable. `space_for_copy`
runs in the rebuild's `checking-space`, so the gate covers every copy; reclaim also
relieves and journals `space`, and its verdict counts `relievableBytes`. The gate is
`1.25 × the last measured copy + 64 MiB`, journaled as `measuredStagingBytes` until a
run measures one: a payload sample under-read real copies by 2.4-4.5× and the bounded
table breakdown is truncated on this schema. A rehearsal seeds it, and so does a
completed real run; only a rehearsal reports `gateAdequate` against the gate it started
on, exposed as `lastRehearsalGateAdequate`, and the verdict marks the anchor stale
after 30 days. `.pre-rebuild` is never a candidate.

## 4. Triggers

1. After a fetch closeout transitions the task ledger to no active task. That is
   the seam `admin_task_lifecycle.finish_run/fail_run/cancel_run` already owns when
   it releases the compaction hold. If retention still runs, the pre-flight
   refuses and one delayed re-check runs once it settles.
2. At startup, one check ~60 s after `resume_storage_maintenance` returns, keeping
   the first paint off a multi-GB copy; the same re-check waits out the startup
   retention pass.
3. Every three hours, an idle probe reads free space and the database file's size
   and asks for the verdict only within 1.5× the copy gate (measured, or the 1 %
   heuristic before one exists): a disk filling while nothing fetches is otherwise
   never checked. The startup check and the idle beat rehearse once when the anchor
   is stale, at most daily, skipped while a run is in flight or the copy does not
   fit.

All three run in the bridge, so a container with no Admin tab open still reclaims;
cooldown is the only guard against a repeat, and the probe starts nothing by itself.
Each runs the §3 pre-flight first (steps 1–4) and none can reach step 5, so a full
volume declines instead of spending the rollback copy on its own initiative.

**Same triggers, two presentations — decided.** All three run on container and
desktop alike. `desktop_mode` (`--desktop-mode` / `BALUFFO_DESKTOP_MODE`, already on
every health payload as `deps.desktop_mode`) selects the presentation and the
concurrency guard, not whether the operation is allowed.

- **Container**: the blocking modal of §6 plus `set_storage_reclaim_hold()`. The copy
  runs for minutes to hours on a hundreds-of-gigabytes store; a fetch mid-copy would
  burn it.
- **Desktop**: the same operation, inline. No dialog is constructed; the panel
  renders the phases from the same `GET`, and **Stop** cancels by `runId`. No hold is
  taken: a personal store is tens to hundreds of megabytes, so the copy is seconds and
  losing a race costs `superseded`, not a burned multi-minute copy. The 512 MiB floor makes it rare.

Two reasons the desktop modal is wrong rather than merely unnecessary. **The shell:**
`admin.html` is embedded in the desktop app
(`startup_probe_policy.EMBEDDED_PAGE_PROBES`, chromium-app mode), so the dialog blocks
Jobs and Saved for seconds of work. **The lifetime:** closing the window cancels the
run, so the container's "closing the tab is safe" promise would be false here, and a
modal is wrong for work that cannot outlive it.

Everything else is shared: the verdict, the §3 parity checks, the retained-copy
rules, the full-disk order, and the cooldown. The manual **Rebuild current feed**,
**Roll back** and **Delete old database** controls stay as the fallback.

## 5. Routes

| Route | Shape |
|---|---|
| `GET /ops/storage-reclaim` | verdict, live status, `scope` (`modal` in a container, `inline` on desktop), journal; read-only except it may settle a dead hold/run, never starts a copy, deletes a file, or changes a healthy journal. Verdict is cached for `RECLAIM_VERDICT_TTL_S`; `?refresh=1` recomputes. |
| `POST /ops/storage-reclaim` | `202` started; `{dryRun: true}` starts a background rehearsal and returns its `runId`; `200 {started: false, reason}` per blocker. `cooldown` and `auto-disabled` refuse an automatic start (`auto: true`) only — an explicit `auto: false` start overrides both and records `triggerOverride: true`. |
| `POST /ops/storage-reclaim/cancel` | cancels a rehearsal event, or delegates to the rebuild cancel when a rebuild owns the copy; `409` on stale `runId` |

The cooldown is a trigger guard, not a safety gate: every other blocker
(`no-current-generation`, `active-task`, `compaction-running`, `backup-exists`,
`free-space`, `store-unavailable`, `sample-unavailable`) refuses both an automatic and
an explicit start, while `cooldown` and `auto-disabled` refuse only the automatic one. The
verdict keeps reporting them so the reason an automatic start did not happen is
visible. GET is not a mutation endpoint; the watchdog may write a terminal settlement
for a dead run, but a caller can never start or stop work with it.

Register in `tools/repo_health/bridge_route_inventory.py` and add the two POSTs to
the gateway's heavy-route budget in `src/container_gateway.py`. POST returns
immediately; the long work stays on the job thread.

## 6. Presentation: modal in the container, inline on desktop

**The blocking dialog is container-only.** A container reclaim copies 0.5-1.5 GB on a
slow disk, so a second mutation mid-copy must not start: the dialog blocks the app and the bridge
hold blocks other tabs.
Desktop is the opposite — short copy, bridge exits with the app — so the run renders
inline from the same payload (`scope: "inline"`), cancelled by **Stop**.

**Shipped versus planned.** The modal ships as a `role="dialog"` element toggled with
`hidden` behind a fixed scrim, with Stop, Hide, statuses and exits, and ESC, scrim clicks
and a dialog's `cancel` are refused while a run is live; `showModal()`, an inert
background, focus return, the ETA and per-table progress are planned (experience plan §A).

Three decisions follow from backend ownership:

- **Closing the tab is safe.** The run is a bridge thread journaled before it is
  reported and installed by one atomic rename, so abandoning the page can neither fail,
  cancel, nor half-finish it. Only **Stop** cancels.
- **The endpoint settles the outcome, not the modal.** A restart leaves the job reporting
  `idle`, so the modal's last sample cannot tell a landed swap from an interrupted one.
- **No invented ETA.** Numbers only from samples it trusts; a stalled disk reads as
  slow, not "2 minutes remaining" forever.
- **Reads follow a live run; nothing else polls.** The panel reads the route on a 3 s
  timer while a run is live, one read at a time, stopping after five failed reads;
  Refresh re-reads with `?refresh=1`; **Rehearse**, **Run** and **Stop** call the same
  routes.

Focus, recovery and slow-copy mechanics are in
[`automatic-storage-reclaim-experience-plan.md`](automatic-storage-reclaim-experience-plan.md).

## 7. Files

| Slice | Where it lands | Pinned by |
|---|---|---|
| Verdict leaf | new `src/storage/reclaim_policy.py`, reading the preview fields plus `file_sizes()`; no `tableSizes` subtraction | `tests/storage/test_reclaim_policy.py` |
| Four preview fields | `src/storage/job_runtime.py::cleanup_preview` (`currentGenerationEstimatedBytes`, `historicalGenerationEstimatedBytes`, `historicalGenerationCount`, `estimatedReclaimableBytes`) | `tests/storage/test_job_runtime_preview.py` |
| Non-feed content digests | `src/storage/current_feed_rebuild.py` (digest at copy time, allowlist constant, journal field) and the delete-time recheck beside `validate_feed_generation` | `tests/storage/test_current_feed_rebuild.py` |
| Orchestrator + journal | new `src/bridge/storage_reclaim.py`, composing `StorageRebuildJob.start/cancel` and `delete_backup`; dedicated `<db>.reclaim.json` | `tests/bridge/test_storage_reclaim.py` |
| Hold | `src/bridge/storage_health.py`; checks in the task-start handlers | `tests/bridge/test_storage_reclaim.py` |
| Routes | `src/bridge/routes/get_ops_diagnostics.py`, `src/bridge/routes/post_routes_admin.py` | `tests/bridge/test_admin_post_route_capability.py` |
| Triggers | task closeout in `src/bridge/admin_task_lifecycle.py` and startup in `src/bridge/bootstrap.py` | `tests/bridge/test_storage_reclaim.py` |
| Gateway | `src/container_gateway.py::_proxy_timeout_for` | `tests/test_container_gateway_schedule.py` |
| Docs | this plan; `storage-contract.md` on landing; `admin-bridge-api.md` for the routes | repo guardrails |

## 8. Tests

Three properties decide whether this feature is safe, and the suite must fail on each
one independently: the verdict never calling bloat on a database whose history sits
inside the retention window; no cancel, unmount or page event reaching a container run;
and a hold released when its job thread dies. The per-file cases for those, and for the
orchestrator, parity, full-disk and dry-run behaviour, are listed in
[`automatic-storage-reclaim-experience-plan.md`](automatic-storage-reclaim-experience-plan.md)
beside the guarantee each one pins.

Tests assert outcomes, not calls: the delete tests check the retained copy is gone and
the live database still validates, not that a function ran.

## 9. Verification

1. Focused pytest for the policy and orchestrator files above.
2. `node --test tests/frontend/unit/admin-storage-reclaim-dialog.test.mjs` and the
   existing `admin-storage-rebuild-controller.test.mjs`, which must stay green —
   the manual path is not allowed to change.
3. `python -m tools.repo_health` for route inventory, docs links and plan tripwires.
4. One live run against a **copy** of a bloated `data/` directory, printing the
   verdict and blockers before anything is deleted, then a full run with the modal
   observed.

### Dry run: the rehearsal that replaces the assumptions

The numbers this plan relies on are assumed, not measured: a 0.5–1.5 GB post-rebuild
size, a 2.32 GB free-space requirement derived from the 1 % heuristic, and a copy
duration inferred from the source size.

`POST /ops/storage-reclaim` accepts `{dryRun: true}` and starts a **background
rehearsal** on the same single-flight as reclaim, taking the same container hold:
`202` plus its own `runId`, with `GET`/cancel using the same routes and
`mode: "rehearsal"` in the status. It runs the real copy against the **live** database,
read-only: `rebuild_current_feed_database` into a staging file, then feed validation
and the content digests on that staging copy. It reports `stagingBytes`,
`reclaimedBytes` (`sourceDatabaseBytes - stagingBytes`), `durationMs`, per-table
counts, the validation result, and `freeBytes` / `requiredFreeBytes`; then it unlinks
staging in a `finally`. It never links a backup, never installs, and never deletes.

The copy helper always journals, so the rehearsal passes a scratch
path, `<db>.reclaim-rehearsal.json`, and unlinks it in the same `finally`.
It never writes the real `<db>.rebuild.json`; the real `<db>.reclaim.json` records
this run's `mode: "rehearsal"` status and result, never a rebuild status. A synchronous
request would exceed the Admin request budget on a multi-GB copy, so this is a job.
Gates match a real run: the §3 pre-flight, no active task, retention stopped.

The hold is refreshed by a pulse every `RECLAIM_HOLD_PULSE_S` for the whole
blocking stretch. Validation is a few monolithic SQLite calls with no progress of its
own, and a pass longer than the stall timeout would otherwise release the hold
mid-run. The pulse only beats the hold the run already took: no phase, no renewal of
the hard maximum.

That is what makes auto mode honest: a host arms only from a rehearsal whose report
shows staging size, reclaimed bytes and duration, and the decision is journaled with
that evidence. Without it, auto reclaim spends an assumption.

## 10. Is retention keeping up?

Reclaim is a shortcut for a retention backlog, so this section tells a temporary
backlog from a structural one. Counters ship in `/ops/storage-health` under
`runtimeRetention`; `docs/storage-contract.md` defines them. Two frame the reading:
`truncated` (surfaced as `completionState: "follow-up-scheduled"`) is draining, not
stalled; `nothing-to-prune` beside a multi-GB feed table is the historical failure,
where source-run retention saw nothing while payloads accumulated.

`totalGenerationCount` answers the question, because `keep_generations=1` means a
caught-up pass starts at zero candidates:

- **Rising with `truncated: true`** — legacy drain: ≤16 generations/day against a
  measured 139M-row backlog, so weeks are expected; reclaim buys time, not an outcome.
- **Rising with `truncated: false`** — staged faster than removed: a changed schedule, a
  second writer, or generations the cleanup cannot see.
- **Flat above 1 with `deletedGenerationCount: 0`** — candidates found and kept.

**Recurrence threshold.** Reclaim is recurring when a completed run is followed by
another `bloat` verdict at the first allowed re-run — 512 MiB and 10 % again inside the
7-day cooldown, roughly **one superseded generation per week not removed** (both sizes
0.5–1.5 GB). Then every cooldown buys a copy; the fix is retention or the schedule.

## 11. Settled decisions

Recorded so implementation does not relitigate them:

- **Seven knobs → three.** The generation-count gate read a capped list (max 4) and
  blind to real backlog; the absolute-bytes knob became the 512 MiB constant;
  the freelist ratio merged into `reclaimable`, covering dropped generations and free
  pages together.
- **The verdict is a conservative payload lower bound, not `tableSizes` minus the
  current generation.** Mixing allocated-page bytes with payload bytes would count
  current-feed indexes as reclaimable; the bounded all-history sample can only
  understate history, never manufacture a false bloat verdict.
- **Reclaim has its own journal.** The rebuild leaf rewrites `<db>.rebuild.json`
  wholesale at copy start, so nesting the reclaim record there would erase it just
  as the run begins. `<db>.reclaim.json` also makes the latest non-rehearsal
  `finishedAt` the cooldown source and keeps a rehearsal out of rebuild recovery.
- **The rehearsal is a background job with a scratch journal.** The copy helper
  always writes a journal, so reusing the real path would clobber recovery or block an
  HTTP thread for a multi-GB copy.
- **Cooldown and auto-disabled gate only automatic starts, per §5.** Manual controls
  stay available when auto mode is off; an explicit start overrides both trigger
  guards, never a safety blocker.
- **The age check was dropped.** Candidates carry no timestamp
  (`_generations_before_current` orders by `feed_generation`); adding one means new SQL
  over the largest table to prove what retention asserts.
- **`needs-review` was dropped.** It duplicated journal `complete` plus **Delete old
  database**, whose route re-validates the feed — a terminal state and a resume test
  for no new capability.
- **No resume after restart**, per §3.
- **Delete requires content parity, per §3.** Row counts alone would let the
  irreversible step lean on the same weakness the retained copy exists to cover. Two
  checks at two moments (copy fidelity, then immutable-table integrity), because
  hashing mutable runtime tables would produce daily false mismatches.
- **Full disk resolves in a fixed order, per §3**: orphans, then the measured gate,
  then — by click only — the retained copy. Retention and compaction need the space
  they return.
- **`no-current-generation` is a blocker, per §1**, measured rather than imagined: a
  database with no published feed produced a 15 % ratio from nothing.
- **The modal and the hold are container-only; the operation is not, per §4 and §6.**
  Desktop runs it inline and unheld: same copy, seconds not minutes.
- **Auto-start arms itself on measurement, per §9.** A host with no recorded arm
  rehearses once; the journal records the decision and its evidence, and the env var
  stays a kill switch, not the enablement step.

## 12. Stop conditions

Stop and ask instead of improvising when:

- the verdict is `bloat` but a blocker persists across two windows;
- the run reports `superseded` twice, or `failed` at any phase;
- the retained copy is missing after a completed rebuild, or rollback is unavailable;
- a non-feed digest mismatches at either checkpoint, or verification hashes/counts
  differ from the journal;
- the modal cannot render the active run on load (stale route or build);
- a rehearsal leaves its scratch journal or staging file behind, or the reclaim
  journal and rebuild journal disagree about the active `rebuildRunId`.
