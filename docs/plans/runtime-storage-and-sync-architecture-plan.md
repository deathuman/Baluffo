# Runtime Storage and Source Sync Architecture Plan

> - **Status:** Proposed (revised 2026-05-11 after deep codebase assessment)
> - **Use this when:** reducing runtime artifact bloat, planning SQLite/WAL storage, changing live task/report persistence, or replacing monolithic source-sync snapshots
> - **Canonical for:** long-term storage direction, source-sync sharding target, hot-path payload budgets, migration sequencing, SQLite connection/transaction discipline, and write-coordination design
> - **Not canonical for:** current endpoint response fields, current source-sync snapshot schema, or existing fetch report compatibility requirements
> - **Then inspect:** [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`admin-bridge-api.md`](../admin-bridge-api.md), [`fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`sync-contract.md`](../sync-contract.md), [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md), and [`LOCAL_SETUP.md`](../LOCAL_SETUP.md)
> - **Last updated:** 2026-05-11

## Verdict

The long-term target should be:

```text
SQLite/WAL local runtime database (hot state only)
+ compact compatibility JSON exports (kept permanently for jobs feed)
+ sharded GitHub source-sync snapshots (highest reliability priority)
+ bounded filesystem-backed evidence archives (independent of SQLite)
```

Large JSON artifacts should become outputs and diagnostics, not the storage model for live runtime authority.

The jobs feed (`jobs-unified-light.json`) remains a JSON export permanently — the static-file + IndexedDB frontend pattern is optimal for the current use case and migrating to paginated bridge APIs is a separate decision gated on measured page-load performance, not part of this storage migration.

The sharded source sync is moved ahead of SQLite data migration because it fixes the single most critical reliability failure mode (monolithic snapshot exceeding size cap making sync impossible) and can be implemented entirely within the existing JSON-backed registry system.

## Design Decisions

These decisions are locked in after a full codebase assessment on 2026-05-11. Each was validated against the actual build system, subprocess model, threading architecture, frontend data-loading patterns, and existing performance infrastructure.

| Decision | Rationale | Source verification |
|----------|-----------|---------------------|
| SQLite is safe to add (stdlib, no dependency) | Python `sqlite3` is stdlib. PyInstaller 6.19.0 bundles it via hidden import. | `scripts/build_portable_exe.py:43-80` — proven hidden import mechanism |
| Bridge is already the sole write-owner for hot state | Discovery and fetch run as subprocesses that write only JSON evidence files. The bridge's threads are the only writers of registry, lifecycle, and sync history. | `src/bridge/pipeline_service.py:848-856`, `src/bridge/task_launch_api.py:61-146`, `src/bridge/discovery_service.py:391-398` |
| Sharded sync comes before SQLite data migration | Fixes the most critical reliability failure (monolithic cap blocking all sync) without depending on any SQLite infrastructure. | `src/source_sync_snapshot.py:793-800` — hard size rejection, no sharding fallback |
| Jobs feed stays JSON-exported permanently | Frontend loads entire file via `fetch()` + IndexedDB cache. Paginated APIs would add latency and break this pattern without corresponding benefit. | `frontend/jobs/app/sources.js:10-22`, `frontend/jobs/app/feed.js:288` |
| Evidence archives are filesystem + JSON manifest, not SQLite | The `artifact_manifest` SQLite table adds complexity without benefit over a simple JSON manifest. Archive retention is a filesystem concern. | n/a — simplification decision |
| Desktop `local_data_store` stays JSON-backed | User data is small, portable (backup/restore), and has a stable JS runtime contract. No benefit from SQLite migration. | `src/local_data_store.py`, `DATA_CONTRACT.md:317-409` |
| `BEGIN IMMEDIATE` for all write transactions | Prevents deadlocks in the multi-threaded bridge where two `DEFERRED` transactions can deadlock on write upgrade. | `src/bridge/server/httpd.py:19` — `ThreadingHTTPServer` model |
| `busy_timeout=30000` (30s) with retry, not 5000 | Multiple threads (HTTP handlers, sync worker, pipeline worker, discovery watcher) may contend. 5s is insufficient for bulk inserts. | Codebase threading analysis: 6 concurrent thread types in bridge |

## Validated Baseline

Current `main` already has important groundwork:

- `src/source_sync.py` uses a 100 MiB source-sync hard cap and a 5 MiB warning threshold.
- `src/source_sync_snapshot.py` calculates size from the serialized snapshot payload, fingerprints meaningful content, skips no-op pushes, validates duplicate source identities, retries after remote conflicts, and fails terminally with `snapshot_too_large` when the hard cap is exceeded.
- `src/bridge/sync_service.py` logs `sync_push_snapshot_size_warning` with `sizeBytes` and `maxSnapshotSizeBytes`; history still needs the same byte-budget details in its returned summary.
- `src/source_registry_io.py` already uses lean active/pending registry rows, gzip-backed registry storage, separate `source-registry-metadata.json.gz`, JSONL journals, atomic writes, and mtime-aware journal overlay behavior.
- Admin task lifecycle authority is `data/admin-task-lifecycle.json`, not `jobs-lifecycle-state.json`. `jobs-lifecycle-state.json` is job lifecycle state for fetched job rows.
- Fetch, discovery, sync, and pipeline lifecycle identity is runId-owned, but the ledger is still JSON-backed and still writes frequent lifecycle/progress updates through file serialization.
- Runtime evidence files remain large enough to affect both reliability and performance. A 2026-05-08 packaged run measured:
  - sync snapshot payload: about 22.7 MiB
  - `jobs-fetch-report.json`: about 55.5 MiB
  - `jobs-fetch-tasks.json`: about 8.6 MiB
  - `source-registry-active.jsonl`: about 17.7 MiB
  - `source-registry-metadata.json.gz`: about 12.5 MiB compressed, 21.9 MiB uncompressed

The 100 MiB cap fixes the immediate 5 MiB failure, but it should be treated as an emergency guard, not as a healthy operating target.

### Registry Journal Root Cause

The 17.7 MiB `source-registry-active.jsonl` is **not** a configuration issue — it is a fundamental design problem:

1. **Journals store full payloads.** Every journal record in `_append_json_journal_record` (line 648 of `source_registry_io.py`) is a complete JSON array of all active sources. A single record inherently exceeds the 1 MiB compaction threshold.
2. **Compaction cannot shrink below one record.** `_compact_json_journal_if_needed` (line 533) truncates the journal to a single record. If that record IS 17.7 MiB, compaction does nothing useful.
3. **Compaction uses `_WRITE_POLICY_BEST_EFFORT`.** If `os.replace` fails (antivirus lock, disk contention, permissions), the failure is silently swallowed. The journal retains all accumulated records and grows unboundedly on subsequent writes. This is proven in the test at `test_source_registry_seed_runtime.py:357-389`.
4. **No startup maintenance pass exists.** Journals are only compacted during writes to `save_json_atomic`. There is no startup pass to clean up accumulated records from prior sessions where compaction silently failed.

Milestone 0.5 must address the root cause (full-payload journal records and silent failure swallowing) before any SQLite work begins.

## Risk Answers

### Would the app get stuck again at 100 MiB?

It should fail terminally instead of stalling if lifecycle projection stays correct. The pipeline must mark the sync child and parent pipeline terminal with the `snapshot_too_large` failure.

However, a hard-cap failure would still block sync. The durable fix is sharding and changed-shard push, so one growing JSON payload cannot take down sync. Sharded sync is now Milestone 2 (immediately after SQLite skeleton) to land this fix as early as possible.

### How do we prevent artifacts from becoming bloated?

Separate hot state from evidence:

- Hot state should be compact, indexed, and bounded.
- Evidence should be lazy-loaded, compressed, retained by policy, and excluded from live progress paths.
- Compatibility JSON should be exports, not live authority.

### How do we reduce disk writes and sizes?

Stop rewriting giant reports for live progress. Use small summary writes, append-only events, database rows, and terminal-only exports.

### Can artifact size affect discovery/fetch speed?

Yes. Repeated JSON serialization, gzip compression, atomic replace, large route reads, antivirus scanning, and Windows filesystem contention can delay fetch/discovery finalization and bridge responsiveness. Milestone 0 must add write-size and write-duration instrumentation before any migration begins so the actual impact is measurable.

## Authority Split

Target authority model:

| Category | Authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` | tiny `/ops/task-state` JSON projection | lifecycle rows stay compact |
| Live task events | SQLite `task_events` or bounded JSONL during transition | `/ops/task-live/<taskType>` | recent window only |
| Fetch source progress | SQLite `source_runs` | small live summary | no full report rewrite |
| Jobs feed | SQLite `jobs` and `job_sources` (server-side authority only) | `jobs-unified-light.json` after terminal run (permanent export) | frontend stays on static JSON + IndexedDB; paginated bridge APIs are a separate decision gated on performance data |
| Full dedup/source evidence | compressed filesystem archive | lazy API/export only | not in lifecycle/history rows |
| Source registry | SQLite-backed rows or staged registry service | sharded source-sync export | source-sync remains source-registry only |
| Bridge diagnostics | bounded JSONL or SQLite table | retained support artifact | not lifecycle authority |
| Desktop local user data | JSON files under `data/local-user-data/` | existing `LOCAL_DATA_RUNTIME_METHODS` contract | stays JSON-backed; portable backup/restore format v2 is stable |

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
```

Note: `artifact_manifest.py` is removed from this list. Evidence archive tracking uses a filesystem-backed JSON manifest (see Evidence Archive Retention), not a SQLite table.

Use:

```text
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;
```

### Transaction Discipline

All write transactions must use `BEGIN IMMEDIATE`. SQLite's default `DEFERRED` transaction mode can cause deadlocks when two threads start `DEFERRED` transactions, read, and then both try to upgrade to write locks. Read transactions can remain `DEFERRED` — they do not block each other in WAL mode.

```python
# Required pattern for all write operations:
connection.execute("BEGIN IMMEDIATE")
try:
    # ... writes ...
    connection.commit()
except Exception:
    connection.rollback()
    raise
```

### Write-Coordination Design

Map existing locks to SQLite access patterns:

| Existing lock | SQLite behavior | Notes |
|---|---|---|
| `OPS_STATE_LOCK` (threading.RLock) | Serializes `task_runs`/`task_events` writes; reads are lock-free in WAL mode | Lock stays as Python mutex for non-DB state |
| `SYNC_STATE_LOCK` | Serializes `sync_runs` writes | Same pattern |
| `PIPELINE_STATE_LOCK` | Stays in-memory only | Pipeline status is transient, not persisted to SQLite |
| Registry writes | Serialized by SQLite's implicit write lock; reader threads use deferred transactions | Large bulk writes batched into 500-row transactions |
| `_REGISTRY_SERVICE_LOCK` | Stays for singleton creation; write serialization handled by SQLite | No change needed |

### Batch Size Constraints

- Maximum transaction duration budget: 100ms for hot-path writes (task heartbeats, event appends)
- Bulk insert batch size: 500 rows per transaction (source runs, job rows)
- Bulk operations (e.g., inserting all source runs after fetch completion) must be partitioned into batches with commit between batches to prevent long write locks from blocking readers

### Implementation Constraints

- Keep one write-owner abstraction in the bridge/runtime process. Do not let narrow helpers open ad hoc write connections.
- Use `BEGIN IMMEDIATE` transactions and bulk inserts for source/fetch rows.
- Treat migrations as compatibility work: old JSON must remain readable until cutover is validated.
- Add a store health endpoint or diagnostic payload for migration version, WAL mode, last write error, and `SQLITE_BUSY` occurrence rate.
- Locate the database under the configured data directory on the same volume as existing runtime artifacts. Do not place it under `_out/` build artifacts.
- Warm up the connection and initialize WAL at bridge startup before a user launches discovery/fetch.
- Keep an explicit retry policy for transient Windows file locking and antivirus contention in addition to `busy_timeout`. Retry with exponential backoff (base 10ms, max 5s, up to 10 attempts) on `SQLITE_BUSY`.
- Checkpoint WAL after large terminal writes and after clean shutdown when doing so will not block the UI. Checkpointing must use a REQUIRED policy (retry with backoff) — never silently skip checkpointing. If checkpointing fails after N retries, log a bridge error and mark the store unhealthy.
- Run `PRAGMA quick_check` or `PRAGMA integrity_check` as a bounded health operation on startup cadence and before backups.
- Provide a backup/restore flow that can copy a consistent SQLite database and its WAL state without corrupting local user data.
- Add `"sqlite3"` to `MAIN_RUNTIME_HIDDEN_IMPORTS` in `scripts/build_portable_exe.py` before merging any SQLite code. PyInstaller 6.19.0 with pyinstaller-hooks-contrib handles this automatically when the import is declared.

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
CREATE INDEX idx_source_runs_run_id ON source_runs(run_id);
CREATE INDEX idx_source_runs_status ON source_runs(status);

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
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_dedup_key ON jobs(dedup_key);

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
CREATE INDEX idx_task_runs_status ON task_runs(status);
CREATE INDEX idx_task_runs_type_status ON task_runs(task_type, status);
CREATE INDEX idx_task_runs_parent ON task_runs(parent_run_id);

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
CREATE INDEX idx_task_events_run_id ON task_events(run_id);

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
CREATE INDEX idx_sync_runs_started ON sync_runs(started_at);
```

This schema is intentionally high level. Each milestone should tighten columns only when migrating that authority surface.

The `artifact_manifest` table from the original proposal is removed. Evidence archive tracking uses a filesystem-backed JSON manifest (see Evidence Archive Retention).

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
  "phase": "proposed",
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

### Two-Phase Manifest Commit

GitHub has no transactional API across multiple file PUTs. To prevent inconsistency where shards are pushed but the manifest push fails (leaving readers with a stale manifest that doesn't reference the new shards), use a two-phase pattern:

1. **Phase 1 — Propose:** Push the manifest first with `"phase": "proposed"` and a `schemaVersion` bump. This manifest references the new shard set but marks them as not yet committed.

2. **Phase 2 — Push shards:** Push each changed shard. Validate each pushed shard's SHA-256 against the manifest entry. If a shard push fails, retry or abort.

3. **Phase 3 — Commit:** Push the manifest again with `"phase": "committed"` (or remove the `phase` field). This is the canonical state readers use.

4. **Pull behavior:** Readers only trust manifests with `phase` absent or `"committed"`. A `"proposed"` manifest is ignored — readers fall back to the last committed state.

5. **Abort:** If shard pushes fail irrecoverably after phase 1, push a new manifest with `"phase": "aborted"` referencing the previously-known good shard set, then push that manifest again as committed.

Rules:

- Shard by stable source identity hash prefix.
- Push changed shards only (compare SHA-256 against last committed manifest).
- Enforce a per-shard cap, initially 5-10 MiB.
- If a shard exceeds budget, split by longer hash prefix.
- Keep source sync to active/pending/core metadata and source health. Do not sync jobs or fetch reports.
- Keep schema-v2 monolithic sync readable during migration.
- Acknowledge GitHub's content API still replaces a whole shard; sharding reduces blast radius and cap pressure, but the real size control remains keeping sync scope narrow.
- Track changed-shard count, bytes pushed, manifest size, and shard-level SHA-256 in sync history.

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

Generate after successful fetch completion. Do not rewrite them during live progress.

These remain the **permanent** frontend boot path. The frontend loads them via `fetch()` + IndexedDB caching, which is optimal for the current use case. Server-side authority moves to SQLite; the JSON export is the canonical read path for the frontend indefinitely. Paginated bridge APIs for the Jobs page are a separate decision gated on measured page-load performance at realistic dataset sizes.

### Heavy Evidence

Move these out of hot reports:

- `dedupEvidence`
- `sourceBundle`
- collision samples beyond capped previews
- provider/static disagreement examples
- slow source rows
- failure buckets and large audit samples

Keep compact counters in hot summaries. Store full evidence in compressed archive files under the configured data directory, referenced by a filesystem-backed JSON manifest (see Evidence Archive Retention).

## Evidence Archive Retention

Evidence archives are a filesystem concern backed by a JSON manifest, independent of the SQLite migration.

Retained evidence should be useful but bounded:

- Default evidence archive budget: 500 MiB total under the configured data directory.
- Per-run debug archive warning: 25 MiB compressed.
- Default retention window: keep terminal evidence for 90 days unless pinned by an operator/debug flag.
- Eviction order: unpinned oldest evidence first, then largest non-pinned archive when total budget is still exceeded.
- Never evict current active-run evidence.

Archive tracking uses a JSON manifest file (not a SQLite table):

```json
{
  "schemaVersion": 1,
  "archives": [
    {
      "runId": "fetch_20260508_120000",
      "kind": "dedup-evidence",
      "path": "artifacts/fetch/fetch_20260508_120000/dedup-evidence.json.gz",
      "sizeBytes": 1048576,
      "sha256": "...",
      "createdAt": "2026-05-08T12:00:00+02:00",
      "retentionClass": "default",
      "pinned": false
    }
  ]
}
```

- The manifest lives at `data/evidence-archive-manifest.json`.
- A cleanup function runs at bridge startup and after each fetch completion to enforce the budget and retention window.
- Compatibility exports such as `jobs-unified-light.json` are not debug archives and require their own compatibility lifecycle before removal.

## Migration Roadmap

### Milestone 0 — Immediate correctness and observability closeout

All work in this milestone must complete before any SQLite code is written. The instrumentation added here provides the performance baseline needed to validate (or reject) the SQLite migration.

1. Add `sizeBytes` and `maxSnapshotSizeBytes` to sync task summary/history return dict and timing records, not only bridge event logs.
2. Split runtime evidence readers from registry JSON readers. Runtime evidence files should not inherit registry journal overlay semantics unless explicitly opted in.
3. Keep existing mtime behavior that prevents older stale journals from shadowing newer JSON, but add runtime evidence tests for packaged fetch/report paths.
4. Add write-size/write-duration instrumentation for the largest artifacts: fetch report, fetch tasks, unified jobs, lifecycle ledgers, and sync snapshots. Include serialization duration, compressed/uncompressed byte size, and atomic replace duration per artifact. Preserve median/min/max values in `storageMetrics` across repeated perf CI summaries.
5. Verify packaged fetch completes and terminalizes when source-level failures occur.

**Gate before proceeding to M0.5:** Collect `storageMetrics` from at least 3 full packaged fetches with realistic source counts. If JSON serialization + atomic replace accounts for less than 5% of total fetch wall-clock time and all route latencies are within budget, reassess whether the full SQLite migration is justified versus lighter alternatives (incremental JSON writes, better gzip, async file I/O).

### Milestone 0.5 — Registry journal repair

The registry journal design has a fundamental flaw: journals store full payloads, so compaction can never reduce the file below the size of one record. Combined with BEST_EFFORT failure swallowing on `os.replace`, this enables unbounded journal growth.

Before SQLite migration:

1. Add per-journal byte and row-count telemetry for registry journals to detect growth before it becomes a problem.
2. **Fix the compaction root cause:** change journal records from full payloads to deltas (only changed rows since last canonical write). If delta tracking is too complex, eliminate the registry journal entirely for lean registries — the gzip-backed canonical files already provide crash safety via atomic writes.
3. Replace `_WRITE_POLICY_BEST_EFFORT` on journal compaction with `_WRITE_POLICY_REQUIRED`. If `os.replace` fails persistently, log a bridge error rather than silently accumulating journal records.
4. Add a startup maintenance pass that compacts, repairs, or truncates oversized registry journals before the bridge begins heavy discovery/fetch work.
5. Add tests proving a large stale registry journal cannot grow unbounded across repeated writes, including the scenario where `os.replace` repeatedly fails.
6. Keep runtime evidence files out of registry journal compaction unless they explicitly opt into that storage mode.

### Milestone 1 — Storage contract and SQLite skeleton

1. Add `"sqlite3"` to `MAIN_RUNTIME_HIDDEN_IMPORTS` in `scripts/build_portable_exe.py` and update the corresponding test assertion in `tests/test_build_portable_exe.py`.
2. Add `docs/storage-contract.md`.
3. Add `src/storage/baluffo_store.py` and migrations with indexes, `BEGIN IMMEDIATE` write discipline, batch size constraints, and retry policy.
4. Add tests proving migrations are idempotent, WAL is enabled, `BEGIN IMMEDIATE` prevents deadlocks, batch inserts work with 500-row partitions, and read-only compatibility projections work.
5. Define authority explicitly:
   - SQLite: runtime authority (task lifecycle, source runs, sync history, registry).
   - JSON: compatibility/export/debug. Jobs feed stays JSON-exported permanently.
   - BaluffoSync: remote source-registry snapshot only.
   - Filesystem + JSON manifest: evidence archives.

### Milestone 2 — Shard source sync (moved ahead of SQLite data migration)

This milestone was Milestone 5 in the original plan. It is moved here because it fixes the most critical reliability failure mode and can be implemented entirely within the existing JSON-backed registry system — no SQLite dependency.

1. Project source registry rows into canonical active/pending/core metadata.
2. Implement sharding by stable source identity hash prefix.
3. Write manifest plus shards with the two-phase commit pattern (propose → push shards → commit).
4. Push changed shards only (compare SHA-256 against last committed manifest).
5. Enforce per-shard cap, initially 5-10 MiB.
6. Keep monolithic schema-v2 read support until all installed apps can read schema-v3 sharded sync.
7. Track changed-shard count, bytes pushed, manifest size, and shard-level SHA-256 in sync history.

### Milestone 3 — Move task runs/events first (formerly Milestone 2)

1. Move `admin-task-lifecycle.json` authority into `task_runs`.
2. Move recent live events into `task_events`.
3. Move sync timing/history into `sync_runs`.
4. Keep `/ops/task-state`, `/ops/history`, and `/ops/task-live/<taskType>` payloads stable through compatibility projections.

This gives the largest live-progress reliability win with the lowest user-facing risk.

### Milestone 4 — Move per-source fetch details (formerly Milestone 3)

1. Move `jobs-fetch-tasks.json` rows into `source_runs`.
2. Move `report.sources[]` detail rows into `source_runs` plus evidence references.
3. Serve Admin source progress/details through paginated bridge APIs.
4. Stop loading a full fetch report to answer current progress.

### Milestone 5 — Move jobs feed server-side authority (formerly Milestone 4, rescoped)

1. Move `jobs-unified.json` authority into `jobs` and `job_sources` (server-side only).
2. Store source bundles as normalized `job_sources` rows or compressed evidence refs.
3. Generate `jobs-unified-light.json` as a terminal compatibility export — **permanently**. This remains the frontend's canonical data source.
4. Keep the existing frontend static-JSON + IndexedDB pattern. Do not build paginated bridge APIs for the Jobs page as part of this milestone.
5. **Future (separate decision):** Paginated bridge APIs for the Jobs page should only be pursued if measured page-load performance shows JSON parsing above 500ms at realistic dataset sizes. If built, route design should include:
   - list route with `limit`, `cursor`, sort, search, and filter parameters
   - detail route for one job, including lazy evidence and source bundle expansion
   - aggregate/count route for filter chips and quick stats

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

The discovery and fetch sanity benchmark payloads do not yet include a complete `storageMetrics` object. Milestone 0 must add write-size/write-duration instrumentation and then preserve median/min/max values for these metrics across repeated perf CI summaries.

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
- JSON exports are compatibility/debug artifacts, not live runtime authority (except jobs feed which stays JSON-exported permanently).
- Source sync can grow by adding shards without raising a global snapshot cap.
- Full fetch/discovery evidence remains available through lazy detail APIs or compressed filesystem archives.
- Hot-path payload budgets are enforced in tests.
- Each milestone has a documented rollback path and cutover gate.
- SQLite health checks and backup/restore flows are validated before any user data authority moves.
- Registry journal size is bounded and cannot grow unboundedly before broader storage migration begins.
- All SQLite write transactions use `BEGIN IMMEDIATE`; `SQLITE_BUSY` is handled with exponential backoff retry.
- WAL checkpointing uses REQUIRED policy and never silently fails.
- `sqlite3` is declared as a hidden import in the PyInstaller build configuration before any SQLite code lands on `main`.
