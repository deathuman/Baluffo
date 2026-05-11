# Runtime Storage and Source Sync Architecture Plan

> - **Status:** Proposed (revised 2026-05-11 after validation loop)
> - **Use this when:** reducing runtime artifact bloat, planning SQLite/WAL storage, changing live task/report persistence, or replacing monolithic source-sync snapshots
> - **Canonical for:** long-term storage direction, journal-scope policy, source-sync sharding target, storage metrics gate, hot-path payload budgets, migration sequencing, SQLite connection/transaction discipline, and rollback expectations
> - **Not canonical for:** current endpoint response fields, current source-sync snapshot schema, or existing fetch report compatibility requirements
> - **Then inspect:** [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`admin-bridge-api.md`](../admin-bridge-api.md), [`fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`sync-contract.md`](../sync-contract.md), [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md), and [`LOCAL_SETUP.md`](../LOCAL_SETUP.md)
> - **Last updated:** 2026-05-11

## Verdict

The durable target is:

```text
SQLite/WAL local runtime database for hot state
+ compact compatibility JSON exports
+ registry-only bounded journal recovery
+ sharded GitHub source-sync snapshots
+ bounded filesystem-backed evidence archives
```

Large JSON artifacts should become exports, evidence, and diagnostics, not live runtime authority. The migration must still be evidence-gated: Milestone 0 adds storage metrics, Milestone 0.5 fixes registry journal growth, and only then should the project decide whether the full SQLite migration is still justified.

The jobs feed remains JSON-exported permanently. The current static-file plus IndexedDB frontend pattern is the canonical frontend boot path. Paginated Jobs bridge APIs are a separate future decision gated on measured page-load parsing cost, not part of this migration.

## Validated Current State

The 2026-05-11 validation found that several lifecycle closeout items are already implemented and should not be re-planned as fresh work:

- `load_runtime_evidence()` exists and runtime evidence reads for fetch, discovery, sync, route helpers, and pipeline paths use it.
- `load_json_array()` has mtime parity with `load_json_object()` and guards runtime evidence filenames.
- `save_json_atomic()` now writes existing runtime evidence filenames directly to canonical JSON without `.jsonl` journal append or compaction.
- Runtime evidence no-op checks inspect canonical JSON directly, so a stale newer adjacent journal does not force unnecessary rewrites or affect equality.
- `_append_json_journal_record()` rejects runtime evidence filenames with a clear error.
- Bridge startup quarantines stale `.jsonl` siblings for current runtime evidence filenames before lifecycle cleanup and startup sync scheduling.
- `source-discovery-candidates.json` is classified as array-shaped runtime evidence and uses canonical-only `load_runtime_evidence_array()` reads.
- JSON journaling is registry opt-in: generic JSON writes use canonical atomic writes, and journal append/overlay is restricted to explicit registry artifacts.
- `/ops/task-state` enriches fetch, discovery, and sync active rows from shared live projections.
- Route-level coverage exists for source-level `failedSources > 0` terminalizing as completed/succeeded when the report has no task-level error.
- Frontend task-run view-model coverage exists for non-stale recent progress.
- Generic object journal-overlay coverage now uses `source-approval-state.json` instead of `jobs-fetch-report.json`, so fetch reports remain runtime evidence in tests too.
- Sync push byte-budget fields (`sizeBytes`, `maxSnapshotSizeBytes`, `sizeWarning`) propagate through service results, timing records, task summaries, run history, no-op pushes, and `snapshot_too_large` failure metadata.
- Storage metrics foundation exists: JSON/gzip writes emit serialization, byte-size, and replace-duration diagnostics, registry journals expose byte/row telemetry, source-sync push sizing records snapshot pressure, `/ops/storage-metrics` exposes the combined diagnostic payload, and sanity benchmark payloads plus repeated `perf_ci.py` summaries preserve storage metric min/median/max.
- Registry journal maintenance foundation exists: append-time hard caps rewrite oversized registry journals instead of appending, oversized startup journals are compacted before lifecycle cleanup/startup sync, and post-write journal compaction uses REQUIRED policy instead of silent BEST_EFFORT replacement.
- Registry journals now write schema-v2 delta records for array and object payloads, including `rowIds` for exact registry array order reconstruction and content hashes for base/current validation. Legacy schema-v1 full-payload records remain readable during transition.
- Targeted validation passed: lifecycle/storage-adjacent Python tests, journal/source-sync/build tests, the narrow frontend task-run view-model test, focused sync-size propagation tests, and focused storage-metrics route/module/benchmark tests.

The remaining risks are not those old lifecycle read-path gaps. The open architecture risks are:

- Storage metrics still need real local discovery/fetch sanity evidence before using them for the M0/M0.5 SQLite go/no-go decision.
- The previous "push proposed manifest first" sharded-sync design can hide the last committed manifest from readers and is not acceptable.

## Strategy Corrections

This plan supersedes older versions of the runtime storage roadmap.

- **Journal scope:** Journaling is now registry opt-in. Runtime evidence, lifecycle ledgers, run history, bridge diagnostics, and compatibility exports do not inherit adjacent `.jsonl` overlay semantics.
- **Runtime evidence writes:** The writer hardening slice is complete for existing runtime evidence filenames: canonical writes skip journaling, no-op checks ignore stale journals, private journal append rejects those paths, startup cleanup quarantines stale runtime journals, and discovery candidates use canonical-only array reads.
- **Metrics gate:** Always fix registry journaling first. Then collect `storageMetrics` from at least three realistic packaged fetches and decide whether full SQLite remains justified. If JSON serialization plus atomic replace is below 5% of wall clock and route latencies are within budget, prefer lighter alternatives over broad SQLite migration.
- **Source-sync atomicity:** Do not overwrite the committed manifest with a `"proposed"` manifest. Push immutable, content-addressed or generation-scoped shards first; update the committed manifest only after every referenced shard exists and validates.
- **v2 compatibility:** A v3 reader fallback to monolithic v2 only helps upgraded clients. During transition, either dual-write v2 while under cap or explicitly accept that v2-only clients stop receiving updates.
- **SQLite sequencing:** Milestone 1 is skeleton-only. Do not shadow-write lifecycle, sync, source-run, or jobs data in M1; those migrations belong to M3-M5.
- **Metric module placement:** Storage metrics must live in a leaf module that can be imported by registry IO and bridge code without importing composition-root modules.
- **Jobs export contract:** Do not add cache hashes inside `jobs-unified-light.json` rows. Use a sidecar metadata file or bridge metadata to avoid changing frontend row shape/order.

## Target Authority Split

| Category | Target authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` after M3 cutover | `/ops/task-state` JSON projection | Until cutover, `admin-task-lifecycle.json` remains authority. |
| Live task events | SQLite `task_events` after M3 cutover | `/ops/task-live/<taskType>` | Recent bounded window only. |
| Sync runs/history | SQLite `sync_runs` after M3 cutover | Existing history/task summaries | Sync size metrics are present; shard metrics land in M2 before migration. |
| Fetch source progress | SQLite `source_runs` after M4 cutover | `jobs-fetch-tasks.json` compatibility while needed | Active progress needs a streaming/live path; terminal-only bulk insert is not enough for live UI. |
| Jobs feed | SQLite `jobs` and `job_sources` server-side after M5 cutover | `jobs-unified-light.json` permanent export | Frontend continues static JSON plus IndexedDB. |
| Full fetch/dedup/source evidence | Filesystem archive plus JSON manifest | Lazy detail/export APIs | Not SQLite; enforce retention budget. |
| Source registry | SQLite-backed rows or staged registry service after later cutover | Sharded source-sync export | Registry journal repair lands before SQLite. |
| Bridge diagnostics | Bounded JSONL or SQLite table | Support artifact | Not lifecycle authority. |
| Desktop local user data | Existing JSON files | `LOCAL_DATA_RUNTIME_METHODS` contract | No SQLite migration. |

## Non-Negotiable Design Rules

- Repo docs and source stay canonical over external memory.
- No new Python or Node dependencies are required for SQLite; use stdlib `sqlite3`.
- Add `"sqlite3"` to PyInstaller hidden imports before any SQLite code lands.
- Keep one bridge/runtime write-owner abstraction. Narrow helpers must not open ad hoc SQLite write connections.
- All SQLite writes use `BEGIN IMMEDIATE`.
- All SQLite bulk writes use bounded transactions, default 500 rows per transaction.
- Use SQLite backup APIs for consistent backups rather than raw file copying while the database is live.
- WAL checkpointing is REQUIRED only at controlled points: after large terminal writes, explicit maintenance, backup preparation, and clean shutdown. Do not checkpoint on hot heartbeats.
- Failed SQLite migration, failed quick/integrity check, repeated busy timeout, or projection mismatch leaves reads on the JSON path and marks the store unhealthy. Do not auto-delete the SQLite file.
- Compatibility exports remain generated until the owning frontend/API surface is separately retired.

## Registry Journal Policy

### Current Problem

The registry journal design is unsafe for large registries:

1. Journal records store full payloads.
2. A compacted journal with one full registry payload can still be tens of MiB.
3. BEST_EFFORT compaction can silently fail and leave every old record in place.
4. Startup does not repair oversized journals before heavy work.

### Required Target

Journaling must become an explicit registry recovery mechanism, not the default behavior of `save_json_atomic()`.

- `save_json_atomic()` writes generic JSON atomically without journal append.
- Registry entrypoints opt into registry journaling through a registry-specific path.
- Runtime evidence filenames must never be journaled. If a private journal append helper receives a runtime evidence path, it should raise a clear error. New runtime evidence artifacts must be added to `_RUNTIME_EVIDENCE_FILE_NAMES` before using public JSON save helpers.
- Startup maintenance quarantines stale runtime `.jsonl` artifacts next to runtime evidence files.
- Registry journal readers overlay canonical JSON only for explicit registry artifacts.

### Delta Journal Constraints

If delta journals are implemented, they must satisfy all of these conditions:

- Delta journals apply only to registry array rows keyed by stable source `id`.
- Object payloads and non-registry arrays do not use delta journals.
- Each delta record includes schema version, canonical base hash, changed rows, removed ids, resulting row count, timestamp, and record hash.
- If the canonical hash does not match the delta base, the reader must ignore the delta chain and use canonical JSON or a validated full snapshot fallback.
- If canonical JSON is absent/corrupt, delta reconstruction is valid only when the journal contains an explicit full baseline record.
- Before appending, enforce a hard journal cap. If the journal is already over cap and required compaction/rewrite fails, abort the append instead of growing the file.
- Compaction/rewrite uses REQUIRED write policy and emits a bridge error event on persistent failure.

If those requirements make delta journals too complex, eliminate registry journals for lean gzip-backed registries and rely on atomic canonical writes plus startup validation.

## Storage Metrics

Milestone 0 must add a leaf metrics module, for example `src/storage_metrics.py`, that can be imported by registry IO, jobs pipeline, and bridge code without importing bridge composition roots.

Metrics must include:

- per artifact serialization duration
- compressed and uncompressed byte size
- atomic replace duration
- write count
- no-op write count
- registry `.jsonl` journal bytes and row count
- source-sync snapshot/shard bytes and cap headroom
- route read/parse latency for `/ops/task-state`, `/ops/task-live/fetch`, `/ops/fetch-report`, and Jobs page boot where measurable

The collector must support subprocess writers. Pipeline subprocesses should append metrics to a sidecar JSONL or equivalent simple artifact that the bridge can aggregate. Metrics writes must not recursively call the instrumented JSON writer.

Expose:

- `GET /ops/storage-metrics`
- perf CI median/min/max trend summaries
- benchmark output for discovery/fetch sanity runs

## SQLite Store Target

Add a small storage layer:

```text
src/storage/
  __init__.py
  baluffo_store.py
  migrations/
    001_initial.sql
    002_task_events.sql
    003_fetch_source_runs.sql
    004_jobs_feed.sql
  runtime_evidence.py
```

Use:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;
```

Write transaction pattern:

```python
connection.execute("BEGIN IMMEDIATE")
try:
    # writes
    connection.commit()
except Exception:
    connection.rollback()
    raise
```

Store implementation requirements:

- migration table tracks applied migrations and schema version
- migrations are idempotent
- connection initialization verifies WAL mode and foreign keys
- retry `SQLITE_BUSY` with exponential backoff and a total deadline
- health payload includes migration version, WAL mode, last write error, busy count/rate, quick_check status, and current authority mode per surface
- backup uses SQLite backup API and validates restored DB with quick_check
- per-surface cutover/rollback state is persisted, not just in memory
- batch writes default to 500 rows per transaction

The database lives under the configured data directory on the same volume as runtime artifacts, for example `data/baluffo-runtime.db`. It must not live under `_out/`.

## Source Sync Target

BaluffoSync remains a source-registry sync repo, not a job feed and not a remote database.

Replace the monolithic v2 payload:

```text
baluffo/source-sync.json
```

with a committed manifest pointer plus immutable shard payloads:

```text
baluffo/source-sync/manifest.json
baluffo/source-sync/shards/<generation-or-content-hash>/active/00.json.gz
baluffo/source-sync/shards/<generation-or-content-hash>/pending/00.json.gz
baluffo/source-sync/shards/<generation-or-content-hash>/metadata/00.json.gz
```

Committed manifest example:

```json
{
  "schemaVersion": 3,
  "generatedAt": "2026-05-11T12:00:00+02:00",
  "contentHash": "...",
  "activeCount": 2102,
  "pendingCount": 315,
  "generation": "20260511T100000Z-abc123",
  "shards": [
    {
      "bucket": "active",
      "key": "00",
      "path": "baluffo/source-sync/shards/20260511T100000Z-abc123/active/00.json.gz",
      "rowCount": 148,
      "sizeBytes": 421322,
      "sha256": "..."
    }
  ]
}
```

Push protocol:

1. Read the last committed manifest if present.
2. Build the new shard set from the current registry projection.
3. Compare shard SHA-256 values against the last committed manifest and push only changed immutable shard paths.
4. Validate every pushed shard by returned SHA and by read-back when needed.
5. Push `manifest.json` only after every referenced shard exists and validates.
6. If shard push fails, leave the old committed manifest untouched.
7. Run bounded garbage collection for old unreferenced shard generations after a successful commit.

Pull protocol:

- Read only committed `manifest.json`.
- Validate schema, content hash, shard list, and per-shard SHA-256.
- If v3 manifest is absent, fall back to v2 monolithic `source-sync.json`.
- During transition, either dual-write v2 while under cap or document that v2-only clients will not receive v3-only updates.

Rules:

- shard by stable source identity hash prefix
- enforce 5-10 MiB per shard
- split oversized shards by longer hash prefix
- sync active/pending/core metadata and source health only
- do not sync jobs, fetch reports, local source-policy review state, or evidence archives
- track shard count, changed shard count, pushed bytes, manifest bytes, shard cap, schema version, and shard hashes in sync history

## Evidence Archive Retention

Evidence archives are filesystem-backed with a JSON manifest, independent of SQLite.

Manifest path:

```text
data/evidence-archive-manifest.json
```

Default policy:

- total archive budget: 500 MiB
- per-run compressed debug archive warning: 25 MiB
- default retention window: 90 days
- never evict current active-run evidence
- evict unpinned oldest first, then largest non-pinned archives if still over budget

Manifest shape:

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

Compatibility exports such as `jobs-unified-light.json` are not debug archives and require their own compatibility lifecycle before removal.

## Migration Roadmap

### Milestone 0R - Reconcile Current State

Purpose: remove stale plan assumptions before implementing new storage code.

- Mark already-implemented lifecycle read-path items as verification-only.
- Verify current tests for `load_runtime_evidence`, load array mtime guard, discovery/sync enrichment, failedSources route terminalization, and frontend progress convergence.
- Audit residual `SyncHistoryDeps.task_state_path` and `task_running_from_state` usage. Remove legacy liveness dependence only where compatibility tests prove it is safe.
- Keep `admin-task-state.json` and `admin-run-history.json` as explicit compatibility/migration artifacts until their owning compatibility surfaces are retired.

Gate: targeted lifecycle/storage tests pass and the implementation plan no longer lists completed lifecycle items as future work.

### Milestone 0A - Journal Scope and Runtime Evidence Write Hardening

Purpose: stop non-registry artifacts from inheriting registry journal semantics.

- **Done:** Add runtime evidence write exclusion for the current `_RUNTIME_EVIDENCE_FILE_NAMES` set: `jobs-fetch-report.json`, `jobs-fetch-tasks.json`, `source-discovery-candidates.json`, `source-discovery-report.json`, and `sync-live-task.json`.
- **Done:** Make runtime evidence no-op checks compare canonical JSON directly, ignoring stale adjacent journals.
- **Done:** Reject runtime evidence filenames in `_append_json_journal_record()`.
- **Done:** Add tests proving runtime evidence writes do not create journals, stale runtime journals do not affect canonical no-op checks, and private journal append rejects evidence paths.
- **Done:** Move generic object journal-overlay tests to `source-approval-state.json` so runtime evidence files are not treated as registry-like journal fixtures.
- **Done:** Add bridge startup quarantine for stale `.jsonl` artifacts adjacent to current runtime evidence filenames, with startup wrapper coverage.
- **Done:** Classify `source-discovery-candidates.json` as array-shaped runtime evidence, add `load_runtime_evidence_array()`, route discovery/bridge readers through it, and test stale journal resistance.
- **Done:** Make journaling registry opt-in rather than default for all remaining non-runtime JSON artifacts.
- **Done:** Keep registry journal overlay only for explicit registry artifacts after the broader opt-in change.

Gate: complete. No runtime evidence file can be journaled through public JSON save helpers, and non-registry JSON artifacts use canonical atomic writes without journal overlay.

### Milestone 0B - Sync Size Propagation

Purpose: make sync byte-budget data available beyond bridge logs.

- **Done:** Return `sizeBytes`, `maxSnapshotSizeBytes`, and `sizeWarning` from sync push service results.
- **Done:** Include those fields in sync timing records.
- **Done:** Propagate them into sync task run summaries/history rows.
- **Done:** Preserve size metadata on `snapshot_too_large` errors so failed sync tasks retain byte-budget evidence.
- **Done:** Add tests for no-op, warning, and over-cap paths.

Gate: complete. Sync history and timing records expose the same byte-budget information as bridge warning logs, and over-cap failures carry the same fields into task summaries.

### Milestone 0C - Storage Metrics

Purpose: create the evidence gate for broader migration.

- **Done:** Add a leaf `src/storage_metrics.py` module importable by registry IO, source-sync, bridge routes, benchmarks, and scripts without importing bridge composition roots.
- **Done:** Instrument JSON/gzip writes with serialization duration, compressed/uncompressed bytes, atomic replace duration, write count, and failed-write count.
- **Done:** Aggregate metrics across subprocess writers through `data/storage-metrics.jsonl` while retaining an in-memory fallback; the metrics writer uses plain JSONL append and does not call instrumented JSON save helpers.
- **Done:** Expose `/ops/storage-metrics` with storage write metrics, registry journal telemetry, source-sync snapshot pressure, and existing route timing counters.
- **Done:** Include benchmark `storageMetrics` in discovery/fetch sanity payloads and preserve min/median/max in repeated `perf_ci.py` summaries/trend records.
- **Done:** Include `registryJsonlJournalBytes` and row counts before journal repair so the repair can be measured.

Gate: implementation is unit-validated and metrics writes do not recurse through instrumented JSON paths. Before the M0/M0.5 go/no-go decision, collect at least one local discovery/fetch sanity run and confirm the emitted `storageMetrics` payload is populated.

### Milestone 0.5 - Registry Journal Repair

Purpose: make registry journal growth provably bounded before SQLite work.

- **Done:** Add per-journal byte and row-count telemetry through `storageMetrics.registryJsonlJournals`.
- **Done:** Replace BEST_EFFORT compaction with REQUIRED rewrite/repair policy for registry journal compaction.
- **Done:** Add append-time hard-cap behavior that rewrites a registry journal to the latest delta/full-payload-compatible record instead of appending when the append would exceed the cap.
- **Done:** Add startup maintenance that compacts oversized registry journals before lifecycle cleanup and startup sync scheduling, with bridge diagnostics for compacted/error counts.
- **Done:** Implement registry-only schema-v2 delta journals for registry arrays and tombstone objects. Array deltas include `rowIds` so changed/removed rows cannot lose ordering semantics.
- **Done:** Add tests for delta correctness, canonical hash mismatch, corrupt canonical fallback, compaction failure, startup repair, hard-cap append refusal, and legacy full-payload v1 read compatibility.

Gate: repeated registry writes cannot grow any journal unboundedly, even when replace fails.

### M0/M0.5 Gate - Measure Before SQLite

Run at least three realistic packaged fetches after M0A-M0C and M0.5.

Local preflight evidence collected on 2026-05-11 with `npm run perf:ci:median`:

- Discovery quick benchmark: 3 runs, median 2017ms, storage write count median 51, serialization median total 4ms, atomic/write median total 18ms, registry journal bytes 31700.
- Fetch smoke benchmark: 3 runs, median 6906ms, storage write count median 39, atomic/write median total 501ms, compressed bytes median 2036676, uncompressed bytes median 26888413.
- This is local sanity evidence only. It confirms discovery and fetch benchmark payloads now carry populated `storageMetrics`, but it does not replace the required packaged fetch gate.
- Packaged smoke runtime snapshots capture `/ops/storage-metrics` as `storage-metrics.json`, so the packaged fetch gate can preserve the same storage evidence without manual API scraping.

Proceed to SQLite skeleton only if the evidence still supports it:

- JSON serialization plus atomic replace is a material cost, or large artifact rewrites are a reliability problem even if wall-clock cost is low.
- Admin live route latency remains within budget.
- Registry journal bytes are bounded after repair.
- Source-sync snapshot pressure is understood.

If JSON serialization plus atomic replace is below 5% of fetch wall clock and route latency is healthy, reassess lighter alternatives before implementing broad SQLite migration.

### Milestone 1 - Storage Contract and SQLite Skeleton

Purpose: land SQLite infrastructure without changing data authority.

- **Done:** Add `"sqlite3"` to `MAIN_RUNTIME_HIDDEN_IMPORTS` and test it.
- Add `docs/storage-contract.md`.
- Add `src/storage/baluffo_store.py`, migrations, health state, backup/restore, and migration runner.
- Validate WAL, `BEGIN IMMEDIATE`, busy retry deadline, batch insert partitioning, quick_check, backup/restore, and checkpoint failure handling.
- Do not shadow-write task, sync, source-run, or jobs data in this milestone.

M1.1 implementation note: the packaged runtime now includes `sqlite3` in the main PyInstaller hidden imports, with a packaging guard test. This does not start the SQLite authority migration and does not satisfy the packaged fetch evidence gate by itself.

Gate: SQLite health endpoint works, migration tests pass, packaged build can import `sqlite3`, and no runtime authority has moved.

### Milestone 2 - Sharded Source Sync

Purpose: remove the monolithic source-sync cap failure mode before data authority migration.

- Implement source registry projection to active/pending/core metadata and source health.
- Implement stable hash-prefix sharding with per-shard caps and overflow splitting.
- Implement immutable shard push followed by committed manifest update.
- Push changed shards only.
- Validate pull with v3 sharded sync and v2 fallback.
- Define and test transition policy: dual-write v2 while under cap, or document v3-only updates for upgraded clients.
- Add shard metrics to sync summaries and timing records.
- Add shard garbage collection for unreferenced old generations.

Gate: unchanged sync push is a no-op, large registry sync does not hit the global monolith cap, partial shard push leaves old committed state readable, and v2 fallback behavior is explicit.

### Milestone 3 - Move Task Runs, Events, and Sync Runs

Purpose: migrate the lowest-risk live runtime authority first.

- Add task run APIs: upsert, heartbeat, terminalize, current runs, recent runs.
- Add task event append/query APIs with bounded recent windows.
- Add sync run APIs including size and shard metrics.
- Shadow-write SQLite alongside existing JSON.
- Compare SQLite projections to JSON/API compatibility shapes after writes.
- Persist cutover and rollback state per surface.
- After three consecutive packaged runs with parity, switch reads to SQLite while continuing JSON compatibility exports.

Gate: `/ops/task-state`, `/ops/history`, and `/ops/task-live/<taskType>` match pre-migration shapes; rollback to JSON is tested.

### Milestone 4 - Move Per-Source Fetch Details and Evidence Archives

Purpose: stop loading full terminal reports for Admin source details.

- Decide and implement one live-progress approach:
  - terminal-only source-run bulk insert plus existing compact live evidence, or
  - streaming source-run writes from the pipeline through a bridge-owned IPC/API path.
- Add `source_runs` bulk insert/query APIs with 500-row batches.
- Move terminal source details from fetch reports into `source_runs` plus evidence refs.
- Add evidence archive manifest and retention enforcement.
- Compact `jobs-fetch-report.json` to summary plus artifact refs after compatibility tests are updated.
- Keep `jobs-fetch-tasks.json` compatibility until dependent Admin surfaces no longer need it.

Gate: active progress remains correct, terminal source detail queries do not require full report loads, evidence archives stay under budget, and JSON fallback works.

### Milestone 5 - Move Jobs Feed Server-Side Authority

Purpose: move server-side job authority while keeping the frontend's static JSON contract.

- Add jobs and job_sources APIs with idempotent batched upserts.
- Store normalized job source rows rather than large nested bundles where practical.
- Generate `jobs-unified-light.json` and `jobs-unified.json` as terminal compatibility exports.
- Keep frontend unchanged: `fetch()` plus IndexedDB from static JSON.
- If an export hash is needed, write it to sidecar metadata or route metadata, not inside job rows.
- Document future paginated Jobs API design in `docs/storage-contract.md` only if measurements show JSON parse cost above the threshold.

Gate: three packaged runs with job parity, compatibility exports match row contract, and frontend loads from exported JSON.

## Size Budgets

Tests should enforce hot payload budgets:

| Payload | Suggested budget |
|---|---:|
| single task row | 32 KiB |
| task history row | 64 KiB |
| live task summary | 256 KiB |
| compact fetch report | 1 MiB |
| `/ops/task-state` response | 256 KiB |
| `/ops/task-live/fetch` response | 1 MiB unless paginated |
| per sync shard | 5-10 MiB |
| compressed debug archive | warning at 25 MiB |

The exact numbers can move. The invariant is that hot-path payload growth must be impossible by test.

## Benchmark Contract

For every milestone that changes runtime storage authority:

- Capture a JSON-baseline run before migration.
- Capture an equivalent shadow or authoritative run with the same source set and comparable artifact sizes.
- Full discovery/fetch wall-clock time must not regress by more than 5% without an explicit acceptance note.
- Admin live progress route latency must not regress by more than 10% without a root-cause note.
- Large artifact rewrites must decrease for the migrated surface or the tradeoff must be documented.
- `storageMetrics` must show equal or lower hot artifact bytes, registry journal bytes, and source-sync pressure for migrated surfaces unless a migration note explains otherwise.
- Compatibility exports must remain byte-contract compatible where existing frontend or docs require them.

## Rollback Paths

| Milestone | Rollback |
|---|---|
| M0R | No data change; revert doc/test expectation changes. |
| M0A | Re-enable old JSON journaling only for non-runtime compatibility artifacts if needed; runtime evidence still reads and writes canonical files. |
| M0B | Remove additive sync size fields from summaries; bridge log warnings remain. |
| M0C | Disable metrics collection; no authority change. |
| M0.5 | Keep legacy v1 journal reader if needed; repair can fall back to canonical JSON. |
| M1 | Delete or ignore SQLite file; no authority migrated. |
| M2 | Keep old committed manifest or v2 monolith; partial shard pushes are unreferenced and ignored. |
| M3 | Reads revert to `admin-task-lifecycle.json` and compatibility history JSON. SQLite retained for diagnosis. |
| M4 | Reads revert to `jobs-fetch-tasks.json` and full fetch report. SQLite/evidence archives retained for diagnosis. |
| M5 | Reads/exports revert to existing jobs JSON pipeline output. SQLite retained for diagnosis. |

## Acceptance Criteria

- Runtime evidence is never shadowed by stale `.jsonl` journals.
- Existing runtime evidence writes do not create `.jsonl` journals, and stale adjacent journals do not affect canonical runtime evidence no-op checks.
- `source-discovery-candidates.json` uses canonical-only array reads and cannot be shadowed by adjacent journals.
- Bridge startup quarantines stale runtime evidence `.jsonl` siblings for the current runtime evidence filename set.
- Non-registry JSON artifacts are not journaled by `save_json_atomic()`, and stale adjacent journals do not overlay their canonical JSON.
- Registry journal size is bounded and cannot grow unboundedly across repeated writes or replace failures.
- Sync size metrics are present in logs, timing, summaries, and history; shard metrics are added with the sharded sync milestone.
- Storage metrics prove whether SQLite migration is still justified after journal repair.
- Sync cap failures terminalize with explicit `snapshot_too_large` evidence.
- Sharded source sync can grow by adding shards without overwriting committed state before shards exist.
- Admin live progress reads compact current state, not full terminal reports.
- JSON exports are compatibility/debug artifacts, except the jobs feed export which remains the permanent frontend boot path.
- Full fetch/discovery evidence remains available through lazy detail APIs or compressed filesystem archives.
- Hot-path payload budgets are enforced in tests.
- SQLite health checks and backup/restore flows are validated before any user data authority moves.
- All SQLite write transactions use `BEGIN IMMEDIATE`; busy contention has bounded retry behavior.
- WAL checkpoint failures are logged and mark the store unhealthy at controlled checkpoint points.
