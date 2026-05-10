# Runtime Storage and Source Sync Architecture Plan

> - **Status:** Proposed
> - **Use this when:** reducing runtime artifact bloat, planning SQLite/WAL storage, changing live task/report persistence, or replacing monolithic source-sync snapshots
> - **Canonical for:** long-term storage direction, source-sync sharding target, hot-path payload budgets, and migration sequencing
> - **Not canonical for:** current endpoint response fields, current source-sync snapshot schema, or existing fetch report compatibility requirements
> - **Then inspect:** [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`admin-bridge-api.md`](../admin-bridge-api.md), [`fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`sync-contract.md`](../sync-contract.md), and [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md)
> - **Last updated:** 2026-05-08

## Verdict

The long-term target should be:

```text
SQLite/WAL local runtime database
+ compact compatibility JSON exports
+ sharded GitHub source-sync snapshots
+ bounded debug/evidence archives
```

Large JSON artifacts should become outputs and diagnostics, not the storage model for live runtime authority.

## Validated Baseline

Current `main` already has important groundwork:

- `src/source_sync.py` uses a 100 MiB source-sync hard cap and a 5 MiB warning threshold.
- `src/source_sync_snapshot.py` calculates size from the serialized snapshot payload, fingerprints meaningful content, skips no-op pushes, validates duplicate source identities, retries after remote conflicts, and fails terminally with `snapshot_too_large` when the hard cap is exceeded.
- `src/bridge/sync_service.py` logs `sync_push_snapshot_size_warning` with `sizeBytes` and `maxSnapshotSizeBytes`; history still needs the same byte-budget details in its returned summary.
- `src/source_registry_io.py` already uses lean active/pending registry rows, gzip-backed registry storage, separate `source-registry-metadata.json.gz`, JSON journals, atomic writes, and mtime-aware journal overlay behavior.
- Admin task lifecycle authority is `data/admin-task-lifecycle.json`, not `jobs-lifecycle-state.json`. `jobs-lifecycle-state.json` is job lifecycle state for fetched job rows.
- Fetch, discovery, sync, and pipeline lifecycle identity is runId-owned, but the ledger is still JSON-backed and still writes frequent lifecycle/progress updates through file serialization.
- Runtime evidence files remain large enough to affect both reliability and performance. A 2026-05-08 packaged run measured:
  - sync snapshot payload: about 22.7 MiB
  - `jobs-fetch-report.json`: about 55.5 MiB
  - `jobs-fetch-tasks.json`: about 8.6 MiB
  - `source-registry-active.jsonl`: about 17.7 MiB
  - `source-registry-metadata.json.gz`: about 12.5 MiB compressed, 21.9 MiB uncompressed

The 100 MiB cap fixes the immediate 5 MiB failure, but it should be treated as an emergency guard, not as a healthy operating target.

## Risk Answers

### Would the app get stuck again at 100 MiB?

It should fail terminally instead of stalling if lifecycle projection stays correct. The pipeline must mark the sync child and parent pipeline terminal with the `snapshot_too_large` failure.

However, a hard-cap failure would still block sync. The durable fix is sharding and changed-shard push, so one growing JSON payload cannot take down sync.

### How do we prevent artifacts from becoming bloated?

Separate hot state from evidence:

- Hot state should be compact, indexed, and bounded.
- Evidence should be lazy-loaded, compressed, retained by policy, and excluded from live progress paths.
- Compatibility JSON should be exports, not live authority.

### How do we reduce disk writes and sizes?

Stop rewriting giant reports for live progress. Use small summary writes, append-only events, database rows, and terminal-only exports.

### Can artifact size affect discovery/fetch speed?

Yes. Repeated JSON serialization, gzip compression, atomic replace, large route reads, antivirus scanning, and Windows filesystem contention can delay fetch/discovery finalization and bridge responsiveness. The plan should add write-size and write-duration instrumentation before and during migration.

## Authority Split

Target authority model:

| Category | Authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` | tiny `/ops/task-state` JSON projection | lifecycle rows stay compact |
| Live task events | SQLite `task_events` or bounded JSONL during transition | `/ops/task-live/<taskType>` | recent window only |
| Fetch source progress | SQLite `source_runs` | small live summary | no full report rewrite |
| Jobs feed | SQLite `jobs` and `job_sources` | `jobs-unified-light.json` after terminal run | frontend can migrate to paginated bridge queries |
| Full dedup/source evidence | compressed archive or DB detail rows | lazy API/export only | not in lifecycle/history rows |
| Source registry | SQLite-backed rows or staged registry service | sharded source-sync export | source-sync remains source-registry only |
| Bridge diagnostics | bounded JSONL or SQLite table | retained support artifact | not lifecycle authority |

## SQLite Store Target

Introduce a small storage layer rather than a broad rewrite:

```text
src/storage/
  baluffo_store.py
  migrations/
    001_initial.sql
    002_task_events.sql
    003_fetch_source_runs.sql
    004_jobs_feed.sql
  runtime_evidence.py
  artifact_manifest.py
```

Use:

```text
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
```

Implementation constraints:

- Keep one write-owner abstraction in the bridge/runtime process. Do not let narrow helpers open ad hoc write connections.
- Use short transactions and bulk inserts for source/fetch rows.
- Treat migrations as compatibility work: old JSON must remain readable until cutover is validated.
- Add a store health endpoint or diagnostic payload for migration version, WAL mode, and last write error.
- Locate the database under the configured data directory on the same volume as existing runtime artifacts. Do not place it under `_out/` build artifacts.
- Warm up the connection and initialize WAL at bridge startup before a user launches discovery/fetch.
- Keep an explicit retry policy for transient Windows file locking and antivirus contention in addition to `busy_timeout`.
- Checkpoint WAL after large terminal writes and after clean shutdown when doing so will not block the UI.
- Run `PRAGMA quick_check` or `PRAGMA integrity_check` as a bounded health operation on startup cadence and before backups.
- Provide a backup/restore flow that can copy a consistent SQLite database and its WAL state without corrupting local user data.

## Migration Safety Model

Each milestone must define:

- **Read fallback:** which JSON path remains readable if SQLite initialization or migration fails.
- **Write mode:** shadow-write, dual-write, or SQLite-authoritative.
- **Cutover gate:** the exact validation required before JSON stops being the authority for that surface.
- **Rollback trigger:** the health, mismatch, or runtime error that reverts reads to the JSON path.
- **Parity evidence:** how many consecutive runs must produce equivalent compatibility exports.

Default rules:

- New SQLite-backed surfaces start in shadow-write mode.
- Cutover requires at least three consecutive successful packaged runs where SQLite projections match the existing JSON/API contract for the migrated surface.
- Any failed migration, failed integrity check, repeated `database is locked` timeout, or projection mismatch keeps or restores JSON authority for that milestone.
- Rollback must not delete the SQLite file automatically. Keep it for diagnosis, mark the store unhealthy, and continue through the legacy JSON path.
- Compatibility exports remain generated until the owning frontend/API route no longer consumes them and a separate compatibility decision removes them.

## Initial Schema Shape

```sql
CREATE TABLE sources (
  id TEXT PRIMARY KEY,
  canonical_key TEXT UNIQUE NOT NULL,
  url TEXT,
  name TEXT,
  adapter TEXT,
  studio TEXT,
  registry_state TEXT NOT NULL,
  source_type TEXT,
  state_changed_at TEXT,
  state_changed_by TEXT,
  pending_reason TEXT
);

CREATE TABLE source_health (
  source_id TEXT PRIMARY KEY REFERENCES sources(id),
  health TEXT,
  health_reason TEXT,
  last_successful_fetch_at TEXT,
  last_seen_in_fetch_at TEXT,
  last_jobs_kept INTEGER DEFAULT 0,
  failure_count INTEGER DEFAULT 0,
  zero_job_streak INTEGER DEFAULT 0,
  browser_fallback_count INTEGER DEFAULT 0
);

CREATE TABLE source_runs (
  run_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  status TEXT,
  fetched_count INTEGER,
  kept_count INTEGER,
  dropped_count INTEGER,
  duration_ms INTEGER,
  error TEXT,
  started_at TEXT,
  finished_at TEXT,
  evidence_ref TEXT,
  PRIMARY KEY (run_id, source_id)
);

CREATE TABLE jobs (
  job_key TEXT PRIMARY KEY,
  dedup_key TEXT,
  provider_job_id TEXT,
  canonical_url TEXT,
  title TEXT,
  company TEXT,
  city TEXT,
  country TEXT,
  work_type TEXT,
  contract_type TEXT,
  profession TEXT,
  status TEXT,
  first_seen_at TEXT,
  last_seen_at TEXT,
  removed_at TEXT,
  quality_score REAL
);

CREATE TABLE job_sources (
  job_key TEXT REFERENCES jobs(job_key),
  source_id TEXT REFERENCES sources(id),
  source_job_id TEXT,
  job_url TEXT,
  last_seen_at TEXT,
  evidence_json TEXT,
  PRIMARY KEY (job_key, source_id, source_job_id)
);

CREATE TABLE task_runs (
  run_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  parent_run_id TEXT,
  parent_task_type TEXT,
  status TEXT NOT NULL,
  stage TEXT,
  owner_kind TEXT,
  owner_pid INTEGER,
  started_at TEXT,
  heartbeat_at TEXT,
  finished_at TEXT,
  terminal_reason TEXT,
  progress_json TEXT,
  summary_json TEXT
);

CREATE TABLE task_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT,
  task_type TEXT,
  ts TEXT,
  level TEXT,
  event TEXT,
  phase_key TEXT,
  work_item_id TEXT,
  message TEXT,
  fields_json TEXT
);

CREATE TABLE sync_runs (
  run_id TEXT PRIMARY KEY,
  action TEXT,
  remote_repo TEXT,
  remote_branch TEXT,
  remote_path TEXT,
  base_sha TEXT,
  new_sha TEXT,
  pushed INTEGER,
  no_op INTEGER,
  size_bytes INTEGER,
  max_snapshot_size_bytes INTEGER,
  active_count INTEGER,
  pending_count INTEGER,
  conflict_count INTEGER,
  started_at TEXT,
  finished_at TEXT,
  error TEXT
);

CREATE TABLE artifact_manifest (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT,
  kind TEXT,
  path TEXT,
  size_bytes INTEGER,
  sha256 TEXT,
  created_at TEXT,
  retention_class TEXT
);
```

This schema is intentionally high level. Each milestone should tighten columns only when migrating that authority surface.

## Source Sync Target

BaluffoSync should remain a source-registry sync repo, not a job feed and not a remote database.

Replace the monolithic snapshot:

```text
baluffo/source-sync.json
```

with:

```text
baluffo/source-sync/manifest.json
baluffo/source-sync/active/00.json.gz
baluffo/source-sync/active/01.json.gz
baluffo/source-sync/pending/00.json.gz
baluffo/source-sync/metadata/00.json.gz
```

Manifest example:

```json
{
  "schemaVersion": 3,
  "generatedAt": "2026-05-08T12:00:00+02:00",
  "contentHash": "...",
  "activeCount": 2102,
  "pendingCount": 315,
  "shards": [
    {
      "bucket": "active",
      "key": "00",
      "path": "baluffo/source-sync/active/00.json.gz",
      "rowCount": 148,
      "sizeBytes": 421322,
      "sha256": "..."
    }
  ]
}
```

Rules:

- Shard by stable source identity hash prefix.
- Push changed shards first, then commit the manifest last.
- Enforce a per-shard cap, initially 5-10 MiB.
- If a shard exceeds budget, split by longer hash prefix.
- Keep source sync to active/pending/core metadata and source health. Do not sync jobs or fetch reports.
- Keep schema-v2 monolithic sync readable during migration.
- Acknowledge GitHub's content API still replaces a whole shard; sharding reduces blast radius and cap pressure, but the real size control remains keeping sync scope narrow.
- Track changed-shard count, bytes pushed, and manifest size in sync history.

This removes the next-cap failure mode: one growing source snapshot can no longer make the entire sync push impossible.

## Artifact Changes

### `data/jobs-fetch-report.json`

Target: compact terminal summary and artifact references.

```json
{
  "runId": "fetch_...",
  "startedAt": "...",
  "finishedAt": "...",
  "summary": {
    "sourceCount": 2102,
    "resolvedSources": 2102,
    "outputCount": 34879,
    "failedSources": 315,
    "mergedCount": 1234
  },
  "artifactRefs": {
    "sourceRuns": "db:source_runs?runId=fetch_...",
    "dedupEvidence": "artifacts/fetch/fetch_.../dedup-evidence.json.gz"
  }
}
```

### `data/jobs-fetch-tasks.json`

Move authority to `source_runs` and `task_events`. Keep a small compatibility file only while existing Admin surfaces need it.

### `data/jobs-unified.json` and `data/jobs-unified-light.json`

Generate after successful fetch completion. Do not rewrite them during live progress. Long term, Jobs page should read paginated bridge results from SQLite, with `jobs-unified-light.json` as a compatibility export.

### Heavy Evidence

Move these out of hot reports:

- `dedupEvidence`
- `sourceBundle`
- collision samples beyond capped previews
- provider/static disagreement examples
- slow source rows
- failure buckets and large audit samples

Keep compact counters in hot summaries. Store full evidence in DB detail rows or compressed archive files referenced by `artifact_manifest`.

## Evidence Archive Retention

Retained evidence should be useful but bounded:

- Default evidence archive budget: 500 MiB total under the configured data directory.
- Per-run debug archive warning: 25 MiB compressed.
- Default retention window: keep terminal evidence for 90 days unless pinned by an operator/debug flag.
- Eviction order: unpinned oldest evidence first, then largest non-pinned archive when total budget is still exceeded.
- Never evict current active-run evidence.
- Record every archive in `artifact_manifest` with `size_bytes`, `sha256`, `retention_class`, and `created_at`.
- Compatibility exports such as `jobs-unified-light.json` are not debug archives and require their own compatibility lifecycle before removal.

## Migration Roadmap

### Milestone 0 - Immediate correctness and observability closeout

1. Add `sizeBytes` and `maxSnapshotSizeBytes` to sync task summary/history, not only bridge event logs.
2. Split runtime evidence readers from registry JSON readers. Runtime evidence files should not inherit registry journal overlay semantics unless explicitly opted in.
3. Keep existing mtime behavior that prevents older stale journals from shadowing newer JSON, but add runtime evidence tests for packaged fetch/report paths.
4. Add write-size/write-duration instrumentation for the largest artifacts: fetch report, fetch tasks, unified jobs, lifecycle ledgers, and sync snapshots.
5. Verify packaged fetch completes and terminalizes when source-level failures occur.

### Milestone 0.5 - Registry journal containment

The registry JSONL journal size is already a present-tense issue. A recent local `source-registry-active.jsonl` was about 17.7 MiB even though journal compaction is meant to keep churn bounded.

Before SQLite migration:

1. Add per-journal byte and row-count telemetry for registry journals.
2. Compact when a registry journal exceeds 1 MiB or a configured row threshold, not only on normal write cadence.
3. Rotate or truncate journal files after successful compaction into gzip-backed base storage.
4. Add a startup maintenance pass that compacts oversized registry journals before the bridge begins heavy discovery/fetch work.
5. Add tests proving a large stale registry journal cannot grow unbounded across repeated writes.
6. Keep runtime evidence files out of registry journal compaction unless they explicitly opt into that storage mode.

### Milestone 1 - Storage contract and SQLite skeleton

1. Add `docs/storage-contract.md`.
2. Add `src/storage/baluffo_store.py` and migrations.
3. Add tests proving migrations are idempotent, WAL is enabled, and read-only compatibility projections work.
4. Define authority explicitly:
   - SQLite: runtime authority.
   - JSON: compatibility/export/debug.
   - BaluffoSync: remote source-registry snapshot only.

### Milestone 2 - Move task runs/events first

1. Move `admin-task-lifecycle.json` authority into `task_runs`.
2. Move recent live events into `task_events`.
3. Move sync timing/history into `sync_runs`.
4. Keep `/ops/task-state`, `/ops/history`, and `/ops/task-live/<taskType>` payloads stable through compatibility projections.

This gives the largest live-progress reliability win with the lowest user-facing risk.

### Milestone 3 - Move per-source fetch details

1. Move `jobs-fetch-tasks.json` rows into `source_runs`.
2. Move `report.sources[]` detail rows into `source_runs` plus evidence references.
3. Serve Admin source progress/details through paginated bridge APIs.
4. Stop loading a full fetch report to answer current progress.

### Milestone 4 - Move jobs feed authority

1. Move `jobs-unified.json` authority into `jobs` and `job_sources`.
2. Store source bundles as normalized `job_sources` rows or compressed evidence refs.
3. Generate `jobs-unified-light.json` only as a terminal compatibility export.
4. Add paginated bridge APIs for Jobs page reads before switching the frontend:
   - list route with `limit`, `cursor`, sort, search, and filter parameters
   - detail route for one job, including lazy evidence and source bundle expansion
   - aggregate/count route for filter chips and quick stats
5. Keep `jobs-unified-light.json` as the default frontend boot path during a compatibility window.
6. Switch the Jobs page behind a feature flag or runtime capability check only after route parity tests and frontend interaction tests pass.
7. Cut over fully after at least three successful full fetches where SQLite route results match compatibility export counts, filters, saved-job identity, and lifecycle labels.

### Milestone 5 - Shard source sync

1. Project source registry rows into canonical active/pending/core metadata.
2. Write manifest plus shards.
3. Push changed shards only.
4. Commit manifest last.
5. Keep monolithic schema-v2 read support until all installed apps can read schema-v3 sharded sync.

## Size Budgets

Add tests that fail when hot payloads exceed budgets:

| Payload | Suggested budget |
|---|---:|
| single task row | 32 KiB |
| task history row | 64 KiB |
| live task summary | 256 KiB |
| compact fetch report | 1 MiB |
| `/ops/task-state` response | 256 KiB |
| `/ops/task-live/fetch` response | 1 MiB unless paginated |
| per sync shard | 5-10 MiB |
| final compressed debug archive | warning at 25 MiB |

The exact numbers can move. The invariant is that hot-path payload growth must be impossible by test.

## Performance Measurements To Add

The discovery and fetch sanity benchmark payloads do not yet include a complete `storageMetrics` object. Milestone 0 should add write-size/write-duration instrumentation and then preserve median/min/max values for these metrics across repeated perf CI summaries.

Before large migrations, collect and compare:

- serialization duration per artifact
- compressed and uncompressed byte size
- atomic replace duration
- route read/parse duration for `/ops/task-state`, `/ops/task-live/fetch`, `/ops/fetch-report`, and Jobs page boot
- number of writes per run by artifact
- largest retained artifact per run
- hot artifact budget warnings
- registry JSONL journal bytes
- source-sync snapshot size and hard-cap headroom

These measurements should prove whether JSON rewrite pressure is materially slowing discovery/fetch and should identify the first table/artifact to migrate.

## Benchmark Contract

Use the existing performance/benchmark workflow as the baseline source. For every milestone that changes runtime storage authority:

- Capture a JSON-baseline run before the migration.
- Capture an equivalent SQLite-shadow or SQLite-authoritative run with the same source set and comparable artifact sizes.
- Full discovery/fetch wall-clock time must not regress by more than 5% without an explicit acceptance note.
- Admin live progress route latency should improve or remain within baseline noise; any route that regresses by more than 10% needs a root-cause note.
- The number of large artifact rewrites per run should decrease for the migrated surface.
- Once Milestone 0 adds storage instrumentation, benchmark `storageMetrics` must show equal or lower hot artifact bytes, registry journal bytes, and source-sync snapshot pressure for migrated surfaces unless a migration note explains the tradeoff.
- Compatibility exports must remain byte-contract compatible where the existing frontend or docs require them.

## Acceptance Criteria

- Active tasks cannot stall because a child task reached a terminal state.
- Sync cap failures terminalize with explicit `snapshot_too_large` evidence.
- Admin live progress reads compact current state, not full terminal reports.
- JSON exports are compatibility/debug artifacts, not live runtime authority.
- Source sync can grow by adding shards without raising a global snapshot cap.
- Full fetch/discovery evidence remains available through lazy detail APIs or compressed archives.
- Hot-path payload budgets are enforced in tests.
- Each milestone has a documented rollback path and cutover gate.
- SQLite health checks and backup/restore flows are validated before any user data authority moves.
- Registry journal size is bounded before broader storage migration begins.
