# Storage Contract

> - **Status:** Active
> - **Use this when:** implementing or validating runtime SQLite/WAL storage, storage authority migration, compatibility exports, evidence archives, or source-sync sharding
> - **Canonical for:** target storage authority boundaries, SQLite connection and transaction discipline, migration safety, export and rollback behavior, and hot-path size budgets
> - **Not canonical for:** current endpoint payload fields, current JSON artifact schemas, source-sync v2 schema details, or Jobs frontend row fields
> - **Then inspect:** [`sync-contract.md`](sync-contract.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md), [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), and archived rollout history in [`archive/runtime-storage-and-sync-architecture-plan.md`](archive/runtime-storage-and-sync-architecture-plan.md) only when historical provenance is needed
> - **Last updated:** 2026-08-18

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
| Fetch source progress | SQLite `source_runs` after M4 cutover | `jobs-fetch-tasks.json` compatibility while needed | Live UI keeps compact current progress; terminal source details hydrate from SQLite/archive. |
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
  `vacuum_freelist_threshold_bytes` and the effective freelist ratio.
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
  while it is still writing. Giving up records
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
- `close()` still checkpoints passively, because shutdown is a controlled point.

Why this exists: a 1.44 GB runtime database with a 1.16 GB freelist and a 392 MB
WAL started a VACUUM and a TRUNCATE checkpoint the moment the desktop bridge
opened the store. Every later startup write waited behind that lock (~14 s
measured), which pushed desktop launches to the 25 s readiness deadline. Deferring
the first rebuild until after the bridge serves moved that cost off the launch
path; the idle gate and the size-aware ratio keep the released rebuild from
landing on top of a running fetch or sync, which writes continuously and would
otherwise queue behind tens of seconds of VACUUM on the same multi-gigabyte
database.

`GET /ops/storage-health` exposes the storage health payload. It must include migration version, WAL mode, foreign-key state, last write error, busy count/rate, `quick_check` status, and current authority mode per surface. It also reports `walMaintenance` and `compaction` status records (including `deferred`, `skipped-busy`, `held-active-task`, and `postServeRun`, plus the gate settings `idleGateS`/`idleMaxWaitS`, `largeDatabaseIdleS`, `heldByActiveTask`, and `lastWriteAgoS`) so skipped, held, or failed maintenance is diagnosable without the bridge log.

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

Milestone 5 cutover is complete for `jobsFeed`: new stores seed `jobsFeed=sqlite`, bridge-managed terminal fetch closeout and direct availability reconciliation publish canonical rows through generation-scoped SQLite tables, then regenerate the private full JSON plus public light/startup projections. Pipeline finalization, bootstrap promotion/rollback, and bridge reconciliation share a re-entrant cross-process data-directory lock. Writers wait through contention, reload lifecycle authority after acquiring the lock, and direct transitions recheck evidence freshness before mutation. Direct reconciliation restores the exact previous generation and both plain/gzip physical projections on lifecycle/export failure. Jobs-feed failures retain SQLite generations for diagnosis.

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
