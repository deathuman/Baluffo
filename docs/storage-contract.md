# Storage Contract

> - **Status:** Active
> - **Use this when:** implementing or validating runtime SQLite/WAL storage, storage authority migration, compatibility exports, evidence archives, or source-sync sharding
> - **Canonical for:** target storage authority boundaries, SQLite connection and transaction discipline, migration safety, export and rollback behavior, and hot-path size budgets
> - **Not canonical for:** current endpoint payload fields, current JSON artifact schemas, source-sync v2 schema details, or Jobs frontend row fields
> - **Then inspect:** [`sync-contract.md`](sync-contract.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md), [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), and archived rollout history in [`archive/runtime-storage-and-sync-architecture-plan.md`](archive/runtime-storage-and-sync-architecture-plan.md) only when historical provenance is needed
> - **Last updated:** 2026-11-10

This document defines the current runtime storage contract. The archived rollout plan records sequencing and closeout evidence; this active contract owns the invariants future code must preserve.

## Scope

Runtime storage covers bridge-owned hot state, terminal compatibility exports, registry/source-sync payloads, and filesystem-backed evidence archives.
Desktop packaging resolves this under the desktop data root; container deployments resolve it under `/data`.

The target shape is:

```text
SQLite/WAL local runtime database for hot state
+ compact compatibility JSON exports
+ registry-only bounded journal recovery
+ sharded GitHub source-sync snapshots
+ bounded filesystem-backed evidence archives
```

The Jobs frontend boot path remains static JSON plus IndexedDB. Desktop local user data remains JSON-backed through the existing local-data contract. BaluffoSync remains source-registry sync, not a job feed and not a remote database.

Availability runtime artifacts remain bounded filesystem compatibility/evidence state:

- `jobs-lifecycle-state.json` owns exact identity aliases, verification counters, closure origin, and transition IDs.
- `jobs-availability-history.json` is a compact 30-day UI projection, loaded only when history filters need it.
- `jobs-availability-priority.json` schema v2 is bridge-written input containing only exact identity, canonical public link, priority, and `canonical` / `custom_saved` scope.
- `jobs-availability-sweep-plan.json` is a bounded rotation plan; safe per-domain and total limits may defer work and report degraded coverage.
- `jobs-availability-shadow-results.json` retains at most 500 compact classifications and never stores HTML, headers, authenticated state, or raw errors.
- `jobs-availability-direct-checkpoints.json` privately retains at most 20,000 compact latest-check rows keyed by exact `availabilityId`. It advances saved-first/oldest-first rotation across scheduled runs and is not a public feed, lifecycle authority, or user-owned status store.
- `jobs-availability-identity-quarantine.json` schema v2 privately retains at most 2,000 contaminated legacy identities or unresolved exact-identity candidate groups for 30 days; schema-v1 reads remain supported. Rows contain only compact lifecycle evidence, hashed source-alias evidence, bounded counts, exact replacement IDs, and canonical URL fingerprints. Top-level row/truncation counts expose retention pressure. It is excluded from public feeds/history/bundles and must return 404 from static serving.
- `local-user-data/jobs-custom-availability-state.json` is the private bridge-owned lifecycle ledger for monitored custom Saved URLs; it never publishes into canonical feeds, history, reconciliation, or public coverage.
- Saved attention/reports remain profile-owned inside `local-user-data`; `systemActivityAt` is separate from user/application activity sorting.

## Authority Split

| Category | Target authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` after M3 cutover | `/ops/task-state?view=summary` hot-path projection; `/ops/task-state` full diagnostic projection | JSON lifecycle files remain compatibility exports and rollback fallback. |
| Live task events | SQLite `task_events` after M3 cutover | `/ops/task-live/<taskType>` | Recent bounded event windows only. |
| Sync runs/history | SQLite `sync_runs` after M3 cutover | Existing history/task summaries | Includes sync size and shard metrics. |
| Fetch source progress | SQLite `source_runs` after M4 cutover | `jobs-fetch-tasks.json` compatibility while needed | SQLite keeps compact source rows; full source details are archive-owned and hydrated for the full fetch-report view. Older terminal runs are pruned by bounded, repeating runtime retention. |
| Jobs feed | SQLite `jobs` and `job_sources` server-side after M5 cutover | `jobs-unified-light.json` permanent export | Frontend continues static JSON plus IndexedDB. |
| Full fetch/dedup/source evidence | Filesystem archive plus JSON manifest | Lazy detail/export APIs | Not SQLite. Enforce retention budgets. |
| Source registry | SQLite `source_registry_rows`, `source_registry_tombstones`, and `source_registry_state` after M6 cutover | Active/pending/rejected/tombstone JSON exports plus sharded source-sync export | Route payloads remain shape-compatible; rollback returns reads to JSON while retaining SQLite for diagnosis. |
| Bridge diagnostics | Bounded JSONL or SQLite table | Support artifact | Diagnostics are not lifecycle authority. |
| Desktop local user data | Existing JSON files | `LOCAL_DATA_RUNTIME_METHODS` | No SQLite migration. |

## SQLite Runtime Store

The database lives under the configured data directory on the same volume as runtime artifacts, for example:

```text
data/baluffo-runtime.db
```

In the container runtime, this is `/data/baluffo-runtime.db`. It must not live under `_out/`, versioned app folders, or Docker image layers.

The Admin quick storage check must stay bounded on large databases: current-generation
`job_sources` and `jobs` counts come from `job_feed_state`, stored row bytes are sampled
with sequential rowid windows, and the quick path must never group `job_sources` or `jobs`.
Full per-generation source-row and allocation counts belong to the explicit exact diagnostic.

SQLite uses Python stdlib `sqlite3`. No new Python or Node dependency is allowed for the runtime store. Packaged builds must keep `sqlite3` in the main PyInstaller hidden imports.

Connection initialization must apply and verify:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;
```

All writes use `BEGIN IMMEDIATE`:

```python
connection.execute("BEGIN IMMEDIATE")
try:
    # writes
    connection.commit()
except Exception:
    connection.rollback()
    raise
```

Store rules:

- Keep one bridge/runtime write-owner abstraction. Narrow helpers must not open ad hoc SQLite write connections.
- Retry `SQLITE_BUSY` with bounded exponential backoff and a total deadline.
- Bound the wait for the in-process write lock too
  (`BALUFFO_STORAGE_WRITE_LOCK_TIMEOUT_MS`, default `300000`). `busy_timeout`
  covers SQLite's own contention; it does not cover a holder inside this process
  (a VACUUM, or a callback that blocked), and an unbounded wait there parks every
  later writer with no error and a task that never leaves `running`. On timeout
  the write raises `BaluffoStoreError`, marks the store unhealthy, and journals a
  `sqliteSlowWrite` row with `operation == "write_lock_timeout"`.
- Bulk writes default to 500 rows per transaction.
- Migrations are idempotent and tracked in a migration table.
- Startup verifies schema version, WAL mode, foreign keys, and `quick_check`.
- Backups use SQLite backup APIs, then validate the restored database with `quick_check`.
- WAL checkpointing is required only at controlled points: after large terminal writes, explicit maintenance, backup preparation, and clean shutdown. Do not checkpoint on hot heartbeats.
- Background maintenance (`baluffo-sqlite-wal-maintenance`,
  `baluffo-sqlite-compaction-maintenance`) takes the store write lock for its whole
  run, so it must never land on a startup write path.

## Maintenance Scheduling Rules

WAL checkpoint and VACUUM scheduling is threshold-driven and asynchronous:

- `write()` schedules a checkpoint when the WAL exceeds
  `wal_checkpoint_threshold_bytes` and a VACUUM when the freelist exceeds both
  `vacuum_freelist_threshold_bytes` and the effective freelist ratio. Background
  WAL scheduling is single-in-flight, and PASSIVE scheduling has a
  `wal_checkpoint_cooldown_s` (60 seconds) guard: a checkpoint can leave the WAL
  file at the journal-size limit even after SQLite has checkpointed its frames,
  so the cooldown prevents every later write from scheduling another PASSIVE
  checkpoint. A WAL that crosses `wal_truncate_threshold_bytes` schedules
  TRUNCATE regardless of the cooldown. Explicit close and maintenance checkpoints
  still run directly.
- The effective ratio is size-aware: below `vacuum_large_database_bytes`
  (512 MiB) it is `vacuum_freelist_ratio` (0.20), at or above it is the larger
  `vacuum_large_database_ratio` (0.35). A VACUUM rewrites the whole file, so its
  lock hold grows with the database while the space it reclaims does not; a
  multi-gigabyte database must therefore be more free before a rebuild is worth
  the lock. The applied ratio is reported in the maintenance record and both
  configured ratios are in `health()["compaction"]`
  (`freelistRatio`, `largeDatabaseFreelistRatio`).
- **A running task holds the rebuild outright.** The bridge holds background
  VACUUMs while any row in the task lifecycle ledger is active: `AdminTaskLifecycle`
  applies the hold before a `start_run` write and re-derives it after every
  transition (`start_run`, `finish_run`, `fail_run`, `cancel_run`, `orphan_run`),
  and `admin_task_runtime.on_bridge_started` seeds it from the ledger before the
  startup steps, so a task that was already running when the bridge started is
  covered. That seed is the first startup step (`compaction_hold_seed`); its
  `desktop_bridge_startup_step_finished` row carries `seededFromActiveTask`,
  `held`, `activeTaskTypes`, and `activeRunIds`, so a launch-time held rebuild is
  visible in the startup journal as well as in `/ops/storage-health`. While held,
  a due rebuild records `compaction.status ==
  "held-active-task"` and runs no VACUUM — it does not wait for the idle window
  either, because the release re-schedules it. The hold exists because write
  silence cannot tell a finished task from a slow source: a fetch that pauses
  between two sources looks idle. Holding before the start write matters because
  that write itself can cross the freelist threshold.
- **A VACUUM only starts once the write path is quiet.** This is the second gate,
  and it still applies after the last task finishes and the hold is released: the
  release schedules the rebuild, and the rebuild then waits out the window. The
  worker waits for `vacuum_idle_s` (30 s) without a write attempt, checking every
  `vacuum_idle_poll_s` (5 s) and giving up after `vacuum_idle_max_wait_s`
  (600 s). The required window is size-aware like the ratio: databases at or above
  `vacuum_large_database_bytes` must be quiet for `vacuum_large_database_idle_s`
  (120 s) instead, because a bursty fetch can leave a 30 s gap between sources
  while it is still writing. Past the same size boundary the rebuild is the
  compact-copy rewrite described below, not an in-place VACUUM. Giving up records
  `compaction.status == "skipped-busy"` with `idleForS`, the applied window
  (`effectiveIdleGateS`, alongside the configured `idleGateS`), and the sizes that
  asked for it, and runs no VACUUM; the next write schedules it again, so a busy
  session keeps its writes unblocked and a quiet one still reclaims. Reads do not
  reset the idle clock — a page that polls the bridge must not be able to postpone
  compaction forever. WAL checkpointing is not gated: a checkpoint holds the write
  lock only for its own duration.
- A **deferred** store (`BaluffoStore(defer_maintenance=True)`, or
  `set_storage_maintenance_deferred(True)`) still records the decision but starts
  no maintenance thread. The record is
  `walMaintenance.status == "deferred"` / `compaction.status == "deferred"` with
  the sizes that triggered it, so the skipped work stays visible in
  `/ops/storage-health`.
- The bridge defers maintenance from `admin_bridge.main()` until
  `on_bridge_serving` finishes the post-bind startup steps, then calls
  `storage_health.resume_storage_maintenance()`. Each store runs its deferred work
  at most once per process, on one background thread after
  `DEFAULT_POST_SERVE_MAINTENANCE_GRACE_S` (5 s), so the first page load is not
  queued behind a VACUUM. Later writes schedule maintenance normally.
- A store that is asked to resume with nothing over threshold records
  `"not-needed"`.
- **Runtime retention repeats after serving.** Each store runs a bounded
  `source_runs` retention pass through the post-serve maintenance thread, then
  schedules the next pass (`DEFAULT_RUNTIME_RETENTION_INTERVAL_S`, 6 hours). A
  pass that reports `truncated` is followed up after
  `DEFAULT_RUNTIME_RETENTION_FOLLOWUP_S` (30 seconds) until the backlog drains,
  so a long-running container keeps cleaning instead of stopping after one
  startup pass. The newest `BALUFFO_RUNTIME_RETENTION_KEEP_RUNS` fetch runs
  (default `30`) and all active runs are retained; every older terminal run is
  deleted in batches, including a recent run beyond the count cap, so growth is
  bounded even when fetches arrive faster than the age cutoff. The age cutoff
  `BALUFFO_RUNTIME_RETENTION_DAYS` (default `30`) still governs old
  `task_events` and terminal `sync_runs`, and deletion is capped at
  `BALUFFO_RUNTIME_RETENTION_MAX_RUNS` per pass (default `200`), with separate
  bounded row caps
  (`BALUFFO_RUNTIME_RETENTION_MAX_EVENT_ROWS` / `_MAX_SYNC_RUNS`).
  `BALUFFO_RUNTIME_RETENTION_COMPACT_ROWS` bounds the archive-payload
  compaction pass. The status is reported live in `health()["runtimeRetention"]`:
  `status`, `phase`, `stage`, `elapsedMs`, `processedRunCount`/`totalRunCount`,
  deletion counters, `truncated`, `timerScheduled`, and `nextRunInS`. A pass held
  by an active task reports `held-active-task` and is re-scheduled when the hold
  is released.
- **Jobs feed generations are pruned by the same post-serve pass.** The current
  published generation is always kept; up to four older `jobs`/`job_sources`
  generations are deleted per bounded pass, and a pass that hits the cap keeps
  the 30-second follow-up scheduled until the backlog drains. This is separate
  from source-run retention: without it, every successful feed publish leaves a
  full historical copy in SQLite. Each generation is removed in bounded
  `job_sources` and `jobs` batches, one transaction per batch, so a large
  generation does not hold the write lock for the whole cleanup. Deletes default
  to 1,000 rows per transaction; `job_sources` selects rowids first, so a job with
  many source rows cannot make one batch larger than the row cap. The cleanup
  runs under `BaluffoStore.bulk_delete_mode()`: the writer uses an 80 MiB
  `wal_autocheckpoint`, while the normal 64 MiB background WAL scheduler and
  compaction scheduler are suppressed. One TRUNCATE checkpoint runs on exit, so
  the WAL cannot remain at the 64 MiB trigger. Each cleanup delete transaction
  temporarily sets `secure_delete=FAST` and restores the writer's prior value
  before releasing the lock; unrelated runtime writes therefore keep the normal
  policy. FAST still overwrites deleted cell headers but leaves more of the old
  public job payload recoverable than `secure_delete=ON`; foreign keys and
  `synchronous=NORMAL` remain enabled. Progress and
  completion report `deletedGenerationCount`/`deletedRowCount` under
  `jobGenerations`, with the generation count repeated at the top level; a new
  pass starts with an empty `jobGenerations` and `truncated=false`. Compaction
  starts only after the generation backlog is drained, because a rebuild during
  another deletion batch only skips as busy.
- **Full source details are written once.** The fetch closeout writes the
  per-source `details` to `artifacts/fetch/<run>/source-details.json.gz` and
  stores only the compact row in SQLite. The full `/ops/fetch-report` view
  rehydrates details from that archive; summary/live views stay compact. A
  retention compaction pass strips legacy duplicated `details` from SQLite only
  when the row's `evidenceRefs` names a `sourceDetailsArchive`, so it cannot
  discard the only copy. Compaction starts only after the retention backlog is
  drained, avoiding a repeated `skipped-busy` loop. A drained pass retries a due
  rebuild even when it deleted nothing, so a rebuild previously skipped for free
  space or a busy writer is not stranded until the next fetch.
- `close()` still checkpoints passively, because shutdown is a controlled point.
- **Post-retention shrink uses a compact copy, not an in-place VACUUM.** Freeing
  pages is not shrinking the file: after a retention pass deletes rows, the
  database keeps its size until it is rebuilt. Above
  `vacuum_large_database_bytes` the store therefore rewrites it with `VACUUM INTO`
  into a sibling staging file while writers keep running, validates the copy with
  `quick_check`, and swaps it in under the write and read locks. The step is
  bounded twice: it refuses to start when the free space cannot hold the live
  bytes plus a 64 MiB margin (`status == "skipped-no-space"` with
  `freeBytes`/`requiredBytes`), and a `compaction_budget_s`
  (`BALUFFO_STORAGE_COMPACTION_BUDGET_S`, default 900 s; `0` disables the deadline)
  timer interrupts the copy (`status == "budget-exceeded"`). A copy that a committed write superseded is
  discarded instead of installed (`status == "superseded"`), because installing
  it would drop that write. The record carries `method == "vacuum-into"`,
  `beforeBytes`, `afterBytes`, `reclaimedBytes`, and `budgetMs`; the original file
  stays in place until the copy validates, so a bounded-out or failed run costs
  the staging file only.

### Measured generation-cleanup levers (2026-11-12)

These measurements close the current cache-size, index-drop, delete-policy, and
checkpoint-scheduling questions. They are **subset** measurements, not a full
211 GB run: 110 MB / 108 MB current-data copies, 20,000 `job_sources` rows and
about 40,000 `jobs`, the same delete SQL, a 300-second budget, and live retention
competing for the same disk. All runs reported `busyErrors=0`.

| Experiment | Rows deleted | Rows/minute | Maximum write-lock hold | Result |
|---|---:|---:|---:|---|
| 32 MiB cache (first subset) | 17,750 | 3,479.3 | 21,130 ms | Baseline; 71 batches |
| 64 MiB cache (same subset) | 17,750 | 3,392.6 | 20,321 ms | **No improvement; do not tune cache size for cleanup** |
| Indexed 32 MiB (matched rebuilt subset) | 20,000 | 4,436.6 | 16,238 ms | Baseline; source rows drained |
| Dropped `idx_job_sources_generation_job` | 14,000 | 2,753.6 | 24,669 ms | **Regression; hit the 300 s cap** |
| 5 s table-size sampler during cleanup | 14,250 | 2,829.9 | 29,667 ms | 61 samples, 13.7 s sampling |
| Same cleanup, table-size sampling skipped | 15,000 | 2,936.4 | 20,479 ms | +3.8% rows/minute; max hold noisy |
| Current 250-row shape, 32 MiB cache | 8,500 | 3,872.1 | 16,282 ms | Baseline for the new experiments; 34 batches |
| `secure_delete=OFF`, 250-row, auto 1,000 pages | 10,500 | 5,093.5 | 14,463 ms | +31.5%; not enough |
| `secure_delete=FAST`, 1,000-row, deferred checkpoint | 20,000 | 326,175 | 365 ms | Cleanup 0.76 s + 2.9 s final checkpoint |
| `secure_delete=FAST`, per-transaction, 1,000-row, deferred | 20,000 | 130,435-417,536 (median 309,516) | 178-1,298 ms (median 465) | 3 confirmation runs; WAL exactly 85,848,472 B each |
| `secure_delete=OFF`, 1,000-row, deferred checkpoint | 20,000 | 417,827 | 190 ms | Cleanup 0.57 s + 2.3 s final checkpoint |
| `secure_delete=ON`, 1,000-row, deferred checkpoint | 20,000 | 99,420 | 607 ms | Cleanup 1.03 s + 11.0 s final checkpoint |
| `secure_delete=OFF`, 1,000-row, 80 MiB auto-checkpoint | 20,000 | 169,276 | 4,180 ms | Bounded WAL; 4.8 s + 2.3 s final checkpoint |
| `secure_delete=OFF`, 1,000-row, 64 MiB auto-checkpoint | 20,000 | 70,526 | 7,845 ms | WAL held at threshold; one auto + one final checkpoint |

Rows/minute for the new rows includes the final checkpoint; the raw delete loop
was faster than the table suggests. The decisive lever is not cache size or the
index: it is checkpoint scheduling and `secure_delete`. The 250-row shape spends
much of its time in repeated 4 MiB WAL checkpoints, and `secure_delete=FAST`
avoids both the per-cell full overwrite and the much slower final checkpoint that
`secure_delete=ON` causes. A bounded 80 MiB auto-checkpoint (`20,000` pages) keeps
the WAL from growing without the 64 MiB re-checkpoint loop; suppressing the
store's separate 64 MiB background scheduler during the bulk delete is required,
or the two schedulers contend.

The same `BaluffoStore.bulk_delete_mode()` path was exercised on a local copy of
the subset before the per-transaction secure-delete refinement: it deleted
**59,433 rows** (39,433 `jobs` plus 20,000 `job_sources`) in **1.646 s** including
the final TRUNCATE checkpoint; the WAL was zero bytes afterwards. That run is not
directly
comparable to the source-only host rows above, and it is not a full 211 GB
confirmation, but it verifies the actual store/context path rather than only the
raw SQL harness.

A follow-up confirmation on a freshly rebuilt subset applied `secure_delete=FAST`
inside each delete transaction, matching the shipped `BaluffoStore.write()` path,
instead of for the whole connection. Three transaction-scoped runs kept the WAL
exactly at **85,848,472 bytes** every time, the same as the earlier connection-wide
run. Delete-loop throughput ranged from 0.570 s to 1.666 s (720,178-2,105,408
rows/minute; median 1,398,290), maximum hold ranged from 178 to 1,298 ms (median
465 ms), and inclusive rows/minute with the final checkpoint ranged from 130,435
to 417,536 (median 309,516). The earlier single run was 0.756 s, 1,588,118
rows/minute, 365 ms maximum hold, and 326,175 rows/minute inclusive. The final
TRUNCATE checkpoint is the noisy component: it took 2.211-8.342 s across these
runs, and a paired connection-scoped run also took 8.381 s, so the variance is
host I/O rather than per-transaction scoping. The scoped run does not add a
measurable regression.

The post-drop plan still uses an index, but the larger primary-key autoindex
(`sqlite_autoindex_job_sources_1`), not the narrower generation/job index. The
earlier `EXPLAIN` claim that the dedicated index was redundant was incomplete:
the plan shape changed but the ordered delete became materially slower. Do not
retry the cache-size or index-drop levers unless the delete SQL, batch size,
schema, or underlying storage changes. The 1,000-row batch is safe here because
the measured maximum hold stayed under one second; do not raise it further
without re-measuring the hold.

The table-size sampler is a different result. On the 108 MB subset it completed
in 224 ms on average (13.7 s across 61 samples); skipping it during retention
raised throughput **3.8%** and cut the maximum hold from **29.7 s to 20.5 s**,
while p95 and mean hold moved only about **5%** and **4%**. The live 211 GB store
currently reports a **partial** sample taking **1.0–1.5 s every 5 s**, so the
subset pair understates the live read cost, but this was a single pair under
live disk contention. Serving cached or omitted table sizes while retention runs
is directionally supported; a full-size confirmation is still needed and needs
the live retention backlog drained first.

Why this exists: a 1.44 GB runtime database with a 1.16 GB freelist and a 392 MB
WAL started a VACUUM and a TRUNCATE checkpoint the moment the desktop bridge
opened the store. Every later startup write waited behind that lock (~14 s
measured), which pushed desktop launches to the 25 s readiness deadline. Deferring
the first rebuild until after the bridge serves moved that cost off the launch
path; the idle gate and the size-aware ratio keep the released rebuild from
landing on top of a running fetch or sync, which writes continuously and would
otherwise queue behind tens of seconds of VACUUM on the same multi-gigabyte
database.

`GET /ops/storage-health` exposes the storage health payload. It must include migration version, WAL mode, foreign-key state, last write error, busy count/rate, `quick_check` status, and current authority mode per surface. `walMaintenance.passiveCheckpoint` carries process-lifetime counters for successful PASSIVE checkpoints only: `count`, `waitMsTotal`, `holdMsTotal`, `waitMsMax`, `holdMsMax`, `waitMsLast`, and `holdMsLast`. A return with `busy != 0` is a retryable attempt and is excluded, as are TRUNCATE attempts; the counters reset when the bridge process restarts. `walMaintenance.passiveCheckpointBusyRetries` separately counts PASSIVE attempts that were actually retried after a busy result; a terminal busy attempt with no remaining retry is not counted, and this counter does not alter the successful-only timings.

The read is bounded, because a diagnostic must answer while maintenance holds the write lock: the writer-only pragmas use a short lock timeout and fall back to the values captured while the writer was idle (`walModeSource`/`foreignKeysSource` = `cached-busy`, `healthLockBusy: true`), and the payload is cached per data dir for a few seconds (`healthSource`, `healthCacheAgeMs`) with `healthTiming` reporting the cost. Table-size sampling installs a SQLite progress handler and reads column lengths instead of materializing large payloads, so one oversized row cannot hold the single-flight read past its budget; an interrupted sample is returned as `tableSizes.status == "partial"` rather than as an unavailable panel. Partial table-size snapshots use a short cache and are retried on the next health poll. A read that cannot run at all reports `healthStalled: true` and `healthSource: "unavailable"` while leaving `healthy` at its last known value: a diagnostic that did not run is not evidence of corruption and must not raise the Admin storage alert. It also reports `walMaintenance` and `compaction` status records (including `deferred`, `skipped-busy`, `held-active-task`, and `postServeRun`, plus the gate settings `idleGateS`/`idleMaxWaitS`, `largeDatabaseIdleS`, `heldByActiveTask`, and `lastWriteAgoS`) so skipped, held, or failed maintenance is diagnosable without the bridge log. Admin renders `tableSizes` in a Runtime Database Storage panel beside Stored Profiles Overview; it reads the route once after startup by joining the serial startup bridge lane, so the read waits behind the bootstrap heavy loads instead of racing them or waiting for the Action Center's next poll; it refreshes on an explicit Refresh click and defers while the active-work gate is held. While an explicit maintenance pass is running and the gate is clear, the panel adds one bounded status poll: 3 s for the first two minutes, 15 s after that, stopping when the pass ends or after 30 minutes. It never schedules that poll while active work holds the gate.

The unbounded exact follow-up is a separate, explicit background job, not part of the health route: `POST /ops/storage-diagnostic` starts one read-only worker per data dir, `GET /ops/storage-diagnostic` reports its phase/progress/result, and `POST /ops/storage-diagnostic/cancel` asks it to stop through SQLite's progress handler. When `dbstat` is present it reports exact per-table and per-index allocated bytes; when it is absent the result says `tableSizes.status == "unavailable"` and never substitutes the bounded rowid sample. Independently of allocation support, it groups `job_sources` by `feed_generation` and reports exact row counts, payload JSON bytes (`length(CAST(payload_json AS BLOB))`), and stored-text bytes. The job measures aggregates only; it does not return row payloads or become an arbitrary SQL surface. A completed job is held in process memory for Admin until the bridge restarts or a new run replaces it. Admin can save the completed result as a JSON file for an incident. The export is generated client-side from the completed `storageDiagnostic` payload: it adds only `kind: "baluffo.storage-diagnostic"`, `schemaVersion`, and `exportedAt`, preserves the exact table and generation totals unchanged, and does not trigger another scan or add a bridge route.

The same route accepts `mode: "quick"` for a bounded first look. It reads page counters and current feed state, reads the current `jobs` count from `job_feed_state`, and samples stored row bytes for the current and older generations; it never groups `job_sources`, groups `jobs`, or runs `dbstat`. The row-byte estimate measures every table's current-generation rows through the generation-leading index first, then spends only the remaining budget on whole-table sequential rowid windows, so a multi-hundred-gigabyte store answers without a full scan and a slow older-row sample cannot hide another table's current figures. It excludes indexes and is marked partial when a sample deadline expires. `rowBytesProbe.currentComplete` and `totalComplete` state whether every table's current measurement and full current/older measurement completed; a partial current sum may be present while either flag is false, and `currentShare` is omitted until the total comparison is complete. The quick result is aggregate-only, read-only, cancellable through the same progress handler, and held in the same in-process job. It is a diagnosis, not a cleanup authorization: old generations are removed by the post-serve retention pass after the current generation is established.

Admin also exposes that cleanup as an explicit **Run maintenance** action in the Runtime Database Storage panel. `POST /ops/storage-maintenance` starts one forced, bounded pass through the same scheduler, keeps the current generation, and refuses to schedule while an active task holds compaction. The route only hands the pass to the background worker; it does not wait, and a pass that hits a cap leaves the normal 30-second follow-up in place. The response reports `started`, `alreadyRunning`, and the current `runtimeRetention`/`compaction` records so an operator can tell a new pass from a held or already-running one. While the pass runs, `runtimeRetention` carries `phase`, `stage`, `elapsedMs`, `processedRunCount`/`totalRunCount`, and deletion counters; those fields advance before the pass completes. Admin polls `/ops/storage-health` for them and stops when `running` is false.

The payload also carries `writeLock`, refreshed from the live store even when
the rest is cached or stalled: `held`, `threadName`, `threadId`,
`threadNativeId`, `operation`, `depth`, `acquiredAt`, and `heldMs`. Admin's
Runtime Database Storage panel shows the holder, operation, and held duration,
with explicit fallbacks for "No write lock held" and an unavailable field.

## Migration Safety

Milestone 1 is skeleton-only. It must not shadow-write lifecycle, sync, source-run, or jobs data.

Each later authority migration follows this surface-by-surface sequence:

1. JSON remains read authority.
2. Writes are mirrored into SQLite.
3. SQLite projection is compared with the existing JSON/API compatibility shape.
4. Cutover state advances only after the required consecutive matching packaged runs.
5. Reads switch to SQLite while compatibility exports continue.

Cutover and rollback state must be persisted, not just held in memory.

Rollback triggers include failed migration, failed `quick_check` or integrity check, repeated busy timeout, or projection mismatch. On rollback, reads stay on or return to the JSON path, the store is marked unhealthy, and the SQLite file is retained for diagnosis. Runtime code must not auto-delete the database.

Milestone 3 cutover is complete for `taskRuns`, `taskEvents`, and `syncRuns`: new stores seed those authority modes as `sqlite`, while projection mismatches or SQLite read/write failures persistently roll the affected surface back to `json`.

Every lifecycle save mirrors the bounded `taskRuns` projection into SQLite in **one transaction** (`TaskRuntimeStore.upsert_task_runs`), not one commit per row. The projection is bounded by `task_row_limit` (240), so per-row commits cost a full WAL commit each — ~90 ms apiece on a mature runtime database, which made a single lifecycle transition take ~15 s and held the bridge's `startup_sync_schedule` startup step for that long. The batched upsert keeps the same rows and the same canceled-is-terminal guard (in the `ON CONFLICT` update predicate) with one commit; the parity comparison still runs against the JSON projection afterwards.

Milestone 4 cutover is complete for `sourceRuns`: new stores seed `sourceRuns=sqlite`, terminal bridge-started fetch reports mirror source rows into `source_runs`, and source-run read/write/parity failures persistently roll the surface back to `json`.

Milestone 5 cutover is complete for `jobsFeed`: new stores seed `jobsFeed=sqlite`, bridge-managed terminal fetch closeout and direct availability reconciliation publish canonical rows through generation-scoped SQLite tables, then regenerate the private full JSON plus public light/startup projections. Pipeline finalization, bootstrap promotion/rollback, and bridge reconciliation share a re-entrant cross-process data-directory lock. Finalization performs its pre-compute lifecycle refresh/direct-live merge without the lock and holds it only for the authoritative feed/lifecycle commit; dedupe, lifecycle computation, quality audits, report construction, and non-feed artifacts run outside it. Writers wait through contention, reload lifecycle authority after acquiring the lock, and direct transitions recheck evidence freshness before mutation. Direct reconciliation restores the exact previous generation and both plain/gzip physical projections on lifecycle/export failure. Jobs-feed failures retain SQLite generations for diagnosis.

Milestone 6 cutover is complete for `sourceRegistry`: new stores seed `sourceRegistry=sqlite`, registry active/pending/rejected rows and tombstones publish through generation-scoped SQLite tables, and successful authoritative publishes regenerate compatibility JSON/gzip exports. Source-registry read/write/parity/export failures, missing published generations, or direct JSON drift persistently roll the surface back to `json` while retaining SQLite generations and diagnostics.

## Compatibility Exports

Supported projections and temporary private JSON handoffs remain generated until their owning surfaces are separately retired.

Rules:

- Runtime evidence files are canonical JSON or gzip JSON artifacts, not journal-overlay artifacts.
- Registry journaling is registry-only and bounded.
- `jobs-unified-light.json` is the sole supported public Jobs feed; `jobs-unified-startup.json` is its bounded same-generation boot cache.
- Do not add cache hashes inside `jobs-unified-light.json` rows. Use sidecar metadata or bridge metadata.
- `jobs-unified.json` is deprecated/internal and must not be served or consumed by frontend code. It remains temporarily for pipeline-to-bridge ingestion, direct CLI output, and JSON fallback rollback. Delete it only after pipeline generations reach SQLite without a public-path handoff, direct CLI has an explicit alternative output, and rollback uses retained SQLite generations.
- CSV generation, publication, launcher handling, and report size/output fields are removed; there is no on-demand replacement.
- After M6, `source-registry-active.json`, `source-registry-pending.json`, `source-registry-rejected.json`, and `source-registry-tombstones.json` remain compatibility/debug exports. When `sourceRegistry=sqlite`, bridge-owned registry routes publish SQLite first, then regenerate those exports; direct CLI JSON writes are treated as JSON fallback/drift and trigger rollback rather than silently overwriting SQLite.
- Full fetch/dedup/source evidence moves to filesystem-backed archives with a JSON manifest, not SQLite.
- After M4, bridge-started terminal `jobs-fetch-report.json` is a compact compatibility/debug export with lean source rows and `sourceRuns.sourceDetailsArchive` refs; direct CLI or old full reports remain valid JSON fallback.

## Source Sync

The source-sync write contract is a committed v3 manifest plus immutable shard payloads. The v2 monolith remains a read fallback only when no trusted committed v3 manifest exists. The committed manifest must not point at shards before every referenced shard exists and validates.

Push protocol contract:

1. Read the last committed manifest if present.
2. Build shards from the current registry projection.
3. Push only changed immutable shard paths.
4. Validate pushed shards by hash and read-back where needed.
5. Update the committed manifest only after all referenced shards validate.
6. Leave the old committed manifest untouched if shard push fails.
7. Garbage-collect old unreferenced shard objects only after a successful commit, with path-prefix validation and a per-run deletion cap.
8. Report garbage-collection warnings after a successful commit without rolling the commit back.

Pull protocol contract:

- Read only the committed v3 manifest.
- Validate schema, content hash, shard list, and per-shard SHA-256.
- Fall back to v2 monolithic `source-sync.json` only when v3 is absent.

Sync result, task-summary, timing-history, and `storageMetrics` payloads keep legacy size fields for compatibility and add authoritative v3 pressure fields: `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `shardsReadBytes`, `totalShardBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes`.

Source sync must not include jobs, fetch reports, local source-policy review state, or evidence archives.

## Evidence Archives

Evidence archives are filesystem-backed and tracked by:

```text
data/evidence-archive-manifest.json
```

Default policy:

- total archive budget: 500 MiB
- per-run compressed debug archive warning: 25 MiB
- default retention window: 90 days
- never evict current active-run evidence
- evict unpinned oldest first, then largest non-pinned archives if still over budget

Compatibility exports such as `jobs-unified-light.json` are not debug archives.

After M4, terminal source `details` for bridge-started fetches are written to gzip archives and referenced from both `source_runs.evidence_ref_json` (surfaced as `evidenceRefs` in source-run JSON rows) and the compact fetch report. `/ops/fetch-report` hydrates details from SQLite/archive while `sourceRuns=sqlite`; rollback to JSON leaves the compact or full report available for diagnosis.

## Size Budgets

Hot-path payload growth must be impossible by test.

| Payload | Suggested budget |
|---|---:|
| single task row | 32 KiB |
| task history row | 64 KiB |
| live task summary | 256 KiB |
| compact fetch report | 1 MiB |
| `/ops/task-state?view=summary` response | 256 KiB |
| `/ops/fetch-report?view=summary` and `/ops/fetch-report?view=live` response | 64 KiB |
| `/registry/conflicts?view=summary` response | 256 KiB |
| `/ops/task-live/fetch` response | 1 MiB unless paginated |
| per sync shard | 5-10 MiB |
| compressed debug archive | warning at 25 MiB |

Summary-route budgets cover compute time as well as response size. Admin startup and active/final-state routes must avoid full registry normalization, JSON parity checks, work-item expansion, full fetch-report parsing, and full conflict-card/evidence construction. This applies while `sourceRegistry=sqlite` and after rollback to `sourceRegistry=json`; JSON-mode summaries may use lightweight artifact counts/fingerprints instead of exact normalized registry state. `/registry/conflicts?view=summary` may return `summaryStatus: "pending"` when exact conflict counts are not already cached.

## Benchmark Contract

For every milestone that changes runtime storage authority:

- Capture a JSON-baseline run before migration.
- Capture an equivalent shadow or authoritative run with the same source set and comparable artifact sizes.
- Full discovery/fetch wall-clock time must not regress by more than 5% without an explicit acceptance note.
- Admin live progress route latency must not regress by more than 10% without a root-cause note.
- Large artifact rewrites must decrease for the migrated surface or the tradeoff must be documented.
- `storageMetrics` must show equal or lower hot artifact bytes, registry journal bytes, and source-sync pressure for migrated surfaces unless a migration note explains otherwise.
- Compatibility exports must remain byte-contract compatible where existing frontend or docs require them.
