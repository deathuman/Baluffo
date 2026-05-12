# Runtime Storage and Source Sync Architecture Plan

> - **Status:** Completed (M0-M6 closed on 2026-05-12)
> - **Use this when:** reducing runtime artifact bloat, planning SQLite/WAL storage, changing live task/report persistence, or replacing monolithic source-sync snapshots
> - **Canonical for:** long-term storage direction, journal-scope policy, source-sync sharding target, storage metrics gate, hot-path payload budgets, migration sequencing, SQLite connection/transaction discipline, and rollback expectations
> - **Not canonical for:** current endpoint response fields, current source-sync snapshot schema, or existing fetch report compatibility requirements
> - **Then inspect:** [`../storage-contract.md`](../storage-contract.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`admin-bridge-api.md`](../admin-bridge-api.md), [`fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`sync-contract.md`](../sync-contract.md), [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md), and [`LOCAL_SETUP.md`](../LOCAL_SETUP.md)
> - **Last updated:** 2026-05-12

## Verdict

The durable target is:

```text
SQLite/WAL local runtime database for hot state
+ compact compatibility JSON exports
+ registry-only bounded journal recovery
+ sharded GitHub source-sync snapshots
+ bounded filesystem-backed evidence archives
```

Large JSON artifacts should be exports, evidence, and diagnostics, not live runtime authority. The migration was evidence-gated: Milestone 0 added storage metrics, Milestone 0.5 fixed registry journal growth, M1-M5 moved task/source/jobs runtime authority, and M6 closed the remaining source-registry and packaged-evidence gaps.

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

The remaining-risk audit is closed as part of M6:

- The M0/M0.5 packaged fetch evidence gate is reconciled by `test:frontend:packaged:fetch-evidence`, which writes deterministic post-fetch evidence artifacts and passed three consecutive times on the final portable build.
- Source registry authority is in scope for this plan: `sourceRegistry=sqlite` is the new-store default after migration `008`, with compatibility JSON/tombstone exports and JSON rollback on storage/parity/export failure.
- The acceptance matrix below maps every closeout criterion to implementation evidence, tests, packaged evidence, or the explicit optional real-network corroboration note.

## Strategy Corrections

This plan supersedes older versions of the runtime storage roadmap.

- **Journal scope:** Journaling is now registry opt-in. Runtime evidence, lifecycle ledgers, run history, bridge diagnostics, and compatibility exports do not inherit adjacent `.jsonl` overlay semantics.
- **Runtime evidence writes:** The writer hardening slice is complete for existing runtime evidence filenames: canonical writes skip journaling, no-op checks ignore stale journals, private journal append rejects those paths, startup cleanup quarantines stale runtime journals, and discovery candidates use canonical-only array reads.
- **Metrics gate:** Always fix registry journaling first. Then collect `storageMetrics` from at least three realistic packaged fetches and decide whether full SQLite remains justified. If JSON serialization plus atomic replace is below 5% of wall clock and route latencies are within budget, prefer lighter alternatives over broad SQLite migration.
- **Source-sync atomicity:** Do not overwrite the committed manifest with a `"proposed"` manifest. Push immutable, content-addressed or generation-scoped shards first; update the committed manifest only after every referenced shard exists and validates.
- **v2 compatibility:** The cutover policy is v3-only writes. Upgraded clients keep monolithic v2 as a read fallback only when no trusted committed v3 manifest exists; v2-only clients stop receiving updates after cutover.
- **SQLite sequencing:** Milestone 1 is skeleton-only. Do not shadow-write lifecycle, sync, source-run, or jobs data in M1; those migrations belong to M3-M5.
- **Metric module placement:** Storage metrics must live in a leaf module that can be imported by registry IO and bridge code without importing composition-root modules.
- **Jobs export contract:** Do not add cache hashes inside `jobs-unified-light.json` rows. Use a sidecar metadata file or bridge metadata to avoid changing frontend row shape/order.

## Target Authority Split

| Category | Target authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` after M3 cutover | `/ops/task-state` JSON projection | Until cutover, `admin-task-lifecycle.json` remains authority. |
| Live task events | SQLite `task_events` after M3 cutover | `/ops/task-live/<taskType>` | Recent bounded window only. |
| Sync runs/history | SQLite `sync_runs` after M3 cutover | Existing history/task summaries | Sync size and shard metrics are present before migration. |
| Fetch source progress | SQLite `source_runs` after M4 cutover | `jobs-fetch-tasks.json` compatibility while needed | Active progress needs a streaming/live path; terminal-only bulk insert is not enough for live UI. |
| Jobs feed | SQLite `jobs` and `job_sources` server-side after M5 cutover | `jobs-unified-light.json` permanent export | Frontend continues static JSON plus IndexedDB. |
| Full fetch/dedup/source evidence | Filesystem archive plus JSON manifest | Lazy detail/export APIs | Not SQLite; enforce retention budget. |
| Source registry | SQLite `source_registry_rows`, `source_registry_tombstones`, and `source_registry_state` after M6 cutover | Active/pending/rejected/tombstone JSON exports plus sharded source-sync export | Registry journals remain bounded compatibility/recovery artifacts; route payloads stay shape-compatible. |
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
    005_task_sync_runtime.sql
    006_source_run_runtime.sql
    007_jobs_feed_runtime.sql
    008_source_registry_runtime.sql
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
baluffo/source-sync/shards/active/<key>/<sha256>.json.gz
baluffo/source-sync/shards/pending/<key>/<sha256>.json.gz
```

Committed manifest example:

```json
{
  "schemaVersion": 3,
  "generatedAt": "2026-05-11T12:00:00+02:00",
  "shardCount": 1,
  "totalRowCount": 2102,
  "totalSizeBytes": 421322,
  "shardCapBytes": 10485760,
  "shards": [
    {
      "bucket": "active",
      "key": "00",
      "path": "baluffo/source-sync/shards/active/00/<sha256>.json.gz",
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
7. Run bounded garbage collection for old unreferenced shard objects after a successful commit.

Pull protocol:

- Read only committed `manifest.json`.
- Validate schema, content hash, shard list, and per-shard SHA-256.
- If v3 manifest is absent, fall back to v2 monolithic `source-sync.json`.
- Write policy is v3-only. The v2 monolith is a read fallback only when no trusted committed v3 manifest exists; v2-only clients do not receive v3-only updates after cutover.

Rules:

- shard by stable source identity hash prefix
- enforce a 10 MiB default per-shard cap
- split oversized shards by longer hash prefix
- sync active/pending/core metadata and source health only
- do not sync jobs, fetch reports, local source-policy review state, or evidence archives
- track shard count, changed shard count, pushed bytes, manifest bytes, shard cap, schema version, and shard hashes in sync results, task summaries, timing history, and storage metrics

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
- Packaged smoke runtime snapshots capture `/ops/storage-health` as `storage-health.json`, so M1 gate runs can verify packaged `sqlite3` import, migration resource loading, WAL health, and JSON-backed authority modes.

Proceed to SQLite skeleton only if the evidence still supports it:

- JSON serialization plus atomic replace is a material cost, or large artifact rewrites are a reliability problem even if wall-clock cost is low.
- Admin live route latency remains within budget.
- Registry journal bytes are bounded after repair.
- Source-sync snapshot pressure is understood.

If JSON serialization plus atomic replace is below 5% of fetch wall clock and route latency is healthy, reassess lighter alternatives before implementing broad SQLite migration.

### Milestone 1 - Storage Contract and SQLite Skeleton

Purpose: land SQLite infrastructure without changing data authority.

- **Done:** Add `"sqlite3"` to `MAIN_RUNTIME_HIDDEN_IMPORTS` and test it.
- **Done:** Add `docs/storage-contract.md`.
- **Done:** Add `src/storage/baluffo_store.py`, migrations, health state, backup/restore, and migration runner.
- **Done:** Validate WAL, `BEGIN IMMEDIATE`, busy retry deadline, batch insert partitioning, quick_check, backup/restore, and checkpoint failure handling at the storage-layer test level.
- **Done:** Add a bridge storage health endpoint backed by `BaluffoStore.health()`.
- Do not shadow-write task, sync, source-run, or jobs data in this milestone.

M1.1 implementation note: the packaged runtime now includes `sqlite3` in the main PyInstaller hidden imports, with a packaging guard test. This does not start the SQLite authority migration and does not satisfy the packaged fetch evidence gate by itself.

M1.2 implementation note: `docs/storage-contract.md` now captures target authority boundaries, SQLite/WAL discipline, migration safety, compatibility exports, source-sync push/pull contract, evidence archive retention, size budgets, and benchmark expectations. This is a contract/documentation slice only; storage implementation still remains gated.

M1.3 implementation note: the SQLite storage package now contains the core store class, idempotent SQL migrations, JSON-authority default state, WAL/foreign-key/quick-check startup validation, bounded `BEGIN IMMEDIATE` write retry, batch execution, required checkpoint handling, and SQLite backup/restore round-trip coverage. Portable packaging now collects `src.storage` data files so SQL migration resources are available once the store is imported in packaged runtime. No bridge route reads or runtime authority moved in this slice.

M1.4 implementation note: `GET /ops/storage-health` now returns the cached runtime store health payload, including migration version, WAL mode, foreign-key state, quick_check status, busy counters, last write error, and per-surface authority modes. The endpoint initializes the SQLite skeleton if needed but leaves every authority mode JSON-backed.

M1.5 implementation note: packaged smoke runtime snapshots now preserve `/ops/storage-health` as `storage-health.json` next to `/ops/storage-metrics`. A packaged gate run can therefore prove `sqlite3` imports in the packaged runtime and that migration SQL resources initialize the SQLite skeleton without moving authority.

M1.6 implementation note: the ship bundle runtime closure now includes `src/storage_metrics.py` and `src/storage`, and packaging tests verify the SQLite store module plus migration SQL resources are present in versioned packaged app roots. This closes the resource-copy side of the packaged import gate before any runtime authority moves.

M1.7 implementation note: ship bundle import validation now runs a storage probe from the versioned packaged app root. The probe imports stdlib `sqlite3`, initializes `BaluffoStore` in a temporary data directory, and verifies migration version `004`, WAL mode, `quick_check`, and JSON-backed authority modes. Missing migration resources now fail bundle validation before a packaged runtime is shipped.

M1.8 validation note: `npm run build:portable-exe` passed on 2026-05-12, producing `dist/baluffo-portable/Baluffo.exe`, `dist/baluffo-portable/BaluffoUpdater.exe`, `dist/baluffo-portable-0.1.33.zip`, and the `_out/latest/build/portable` mirror. The build exercised the ship-bundle import/storage validation and PyInstaller processed the stdlib `sqlite3` hook. `npm run test:frontend:packaged:orchestrated` then passed against `_out/latest/build/portable/Baluffo.exe`; the packaged smoke snapshot at `.tmp/packaged-desktop-smoke/20260511-222732-930580-930582400/storage-health.json` reported `ok=true`, migration version `004`, WAL mode `wal`, `quick_check=ok`, and JSON-backed authority modes for task runs, task events, sync runs, source runs, jobs feed, and source registry.

Gate: SQLite health endpoint works, migration tests pass, packaged build can import `sqlite3`, and no runtime authority has moved.

### Milestone 2 - Sharded Source Sync

Purpose: remove the monolithic source-sync cap failure mode before data authority migration.

- **Done:** Implement source registry projection to active/pending/core metadata and source health.
- **Done:** Implement stable hash-prefix sharding with per-shard caps and overflow splitting.
- **Done:** Implement immutable shard push followed by committed manifest update.
- **Done:** Push changed shards only and read back changed shard payloads before committing the manifest.
- **Done:** Validate pull with v3 sharded sync and v2 fallback when no trusted v3 manifest exists.
- **Done:** Cut live push over to v3-only writes; the v2 monolith is read fallback only and is not updated by push.
- **Done:** Add shard metrics to sync results, task summaries, timing records, and storage metrics.
- **Done:** Add bounded shard garbage collection for unreferenced objects after successful manifest commit.

Gate: complete. Unchanged committed-v3 sync push is a no-op, matching v2-only remote state creates the first v3 manifest, large registry sync no longer fails on aggregate monolith size, partial shard push leaves old committed state readable, v2 fallback behavior is explicit, and GC failures are reported as warnings after successful commits.

M2.1 implementation note: `src/source_sync_shard.py` now provides deterministic local shard construction without changing remote sync writes. `shard_key()` hashes stable source identity prefixes; `build_shards()` groups rows into gzip JSON schema-v3 shard payloads, computes path-ready manifest metadata, enforces a caller-supplied per-shard byte cap, recursively splits oversized shards by longer hash prefixes, and raises a clear error for a single row that cannot fit under the cap. The ship bundle now carries the sharding leaf module. Focused tests cover deterministic metadata/payloads, canonical row ordering, overflow splitting, oversize errors, and remote-path validation.

M2.2 implementation note: the sharding leaf now builds and validates committed schema-v3 manifests, derives `baluffo/source-sync/manifest.json` from the existing v2 snapshot path, reads only trusted committed manifests, and refuses to push uncommitted/proposed manifests. The helper writes target the manifest path separately from the existing v2 monolith and do not change `push_sources_snapshot()` yet. Focused tests cover manifest totals, path validation, proposed-manifest distrust, committed manifest read/write requests, and decoded PUT payloads.

M2.3 implementation note: the sharding leaf now compares candidate shards against the last trusted committed manifest by remote path plus shard SHA-256, so a shard is skipped only when the exact referenced path already exists with the same payload hash. It also has immutable shard PUT helpers that validate payload SHA-256 before upload, omit an overwrite `sha`, and return pushed-byte/row metadata. Focused tests cover changed-shard detection, untrusted manifest fallback, immutable shard payload uploads, SHA mismatch rejection, and pushing only missing or changed shards.

M2.4 implementation note: the sharding leaf now has v3 read helpers that fetch each committed manifest shard, verify base64 content, compressed byte size, SHA-256, gzip JSON schema, bucket/key, and row count before returning active/pending rows. `read_sharded_snapshot()` returns `None` when no committed manifest is available, preserving the existing v2 monolithic pull fallback boundary until the live pull path is deliberately wired. Focused tests cover single-shard reads, hash mismatch rejection, absent-manifest fallback, and merging active plus pending shards from a committed manifest.

M2.5 prep note: shard bundles now use content-addressed remote paths (`bucket/key/sha256.json.gz`) before push metrics are computed. This closes the immutable-PUT loophole where a changed shard could reuse an existing hash-prefix path and require overwrite semantics. `build_sharded_snapshot_bundle()` emits the committed manifest candidate, all shard objects, changed-shard list, manifest size, shard cap, pushed-byte estimate, total shard bytes, and per-shard hashes. Focused tests cover stable no-op paths across regenerated manifests, changed-shard metrics, content-addressed paths, and invalid snapshot row rejection.

M2.6 implementation note: sync pull now prefers a trusted committed v3 manifest and validates every referenced shard before merging, while a missing/untrusted manifest still falls back to the existing v2 monolithic `source-sync.json` reader. The v3 path is deliberately pull-only for this slice so push conflict handling keeps using the v2 monolith SHA until sharded push orchestration is wired separately. Focused tests cover v2 fallback after manifest 404, committed v3 pull without touching the monolith, manifest SHA propagation, and preservation of the existing v2 unexpected-key warning contract.

M2.7 prep note: the sharding leaf now exposes `push_sharded_snapshot()` as the single orchestration boundary for the later live push cutover. It builds the content-addressed bundle, no-ops when the committed manifest already references identical shard hashes, pushes changed immutable shard objects first, and only then updates the committed manifest. Focused tests verify shard-before-manifest write order and unchanged-shard no-op behavior without changing the live v2 monolith push path yet.

M2.8 implementation note: v3 shard writes now use the committed manifest SHA for manifest updates, treat existing content-addressed shard paths as idempotent only after reading back and validating SHA/size/payload, verify changed shards by read-back before manifest commit, and map manifest 409 conflicts to the existing sync conflict path.

M2.9 implementation note: live `push_sources_snapshot()` now reads remote state with v3 preference, keeps v2 as read fallback only when no trusted v3 manifest exists, writes through `push_sharded_snapshot()`, and no longer writes `baluffo/source-sync.json`. Matching v2-only remote content still creates the first v3 manifest, while matching committed-v3 remote content remains the no-op path. `snapshot_too_large` now represents unsplittable shard/row failures rather than aggregate monolith size.

M2.10 implementation note: v3 shard metrics now flow through source-sync results, `SyncService`, task summaries, timing history, and `storageMetrics`. Legacy `sizeBytes`, `maxSnapshotSizeBytes`, and `sizeWarning` remain present for compatibility, while `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes` describe v3 pressure.

M2.11 closeout note: committed v3 manifest writes now run bounded post-commit garbage collection under `baluffo/source-sync/shards/`. GC deletes only path-validated `.json.gz` shard files not referenced by the current committed manifest, applies a per-run deletion cap, and reports warnings after successful commits without rolling back the manifest.

### Milestone 3 - Move Task Runs, Events, and Sync Runs

Purpose: migrate the lowest-risk live runtime authority first.

- **Done:** Add task run APIs: upsert, heartbeat, terminalize, current runs, recent runs.
- **Done:** Add task event append/query APIs with bounded recent windows.
- **Done:** Add sync run APIs including size and shard metrics.
- **Done:** Shadow-write SQLite alongside existing JSON.
- **Done:** Compare SQLite projections to JSON/API compatibility shapes after writes.
- **Done:** Persist cutover and rollback state per surface.
- **Done:** Switch reads to SQLite while continuing JSON compatibility exports.

Gate: complete. `/ops/task-state`, `/ops/history`, and `/ops/task-live/<taskType>` preserve their route shapes, `taskRuns`/`taskEvents`/`syncRuns` seed as SQLite authority on new stores, generated JSON compatibility exports remain in place, and parity/read failures roll affected surfaces back to JSON while retaining SQLite for diagnosis.

M3.1 implementation note: migration `005_task_sync_runtime.sql` extends `task_runs`, `task_events`, and `sync_runs` for current lifecycle fields, bounded event lookup, sync action/duration, snapshot format, and shard hashes. `TaskRuntimeStore` owns task run, event, and sync-run SQLite APIs without importing bridge composition roots.

M3.2 implementation note: `AdminTaskLifecycle` mirrors lifecycle JSON writes into SQLite when `taskRuns` is in shadow or SQLite mode, compares JSON route rows to SQLite projections, reports diagnostics through `/ops/storage-health`, and rolls `taskRuns` back to JSON on write/parity failures.

M3.3 implementation note: sync worker progress and final messages mirror into `task_events`, and final sync task summaries mirror into `sync_runs` with v3 shard metrics. Existing `sync-live-task.json`, timing history, and lifecycle JSON exports continue to be written.

M3.4 implementation note: task run reads switch to SQLite behind the persisted `taskRuns=sqlite` mode, while shadow mode keeps JSON reads and records parity diagnostics. `/ops/task-live/<taskType>` uses SQLite bounded event windows when `taskEvents=sqlite` has matching events.

M3.5 closeout note: new stores now seed `taskRuns`, `taskEvents`, and `syncRuns` as SQLite authority. If an upgraded runtime has JSON rows that are not present in SQLite, route reads roll the affected surface back to JSON instead of dropping visible task state.

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

Gate: complete. Active fetch progress remains on compact live evidence, terminal source details are queryable from SQLite without loading a full report, bridge-started terminal reports compact bulky `details` into gzip evidence archives, and read/write/parity failures roll `sourceRuns` back to JSON while retaining SQLite/archive diagnostics.

M4.1 implementation note: migration `006_source_run_runtime.sql` extends `source_runs` for source identity, adapter/fetch strategy/studio fields, error and low-confidence counts, evidence refs, schema version, and update timestamps. `SourceRuntimeStore` owns 500-row source-run bulk upsert/query/summary, and `EvidenceArchiveStore` owns gzip JSON writes, SHA-256/size tracking, path-prefix validation, and retention enforcement.

M4.2 implementation note: terminal bridge fetch lifecycle closeout mirrors normalized `jobs-fetch-report.json` source rows into SQLite when `sourceRuns` is shadow or SQLite, records parity diagnostics through `/ops/storage-health`, and rolls `sourceRuns` back to JSON on write/parity failure. New stores seeded `sourceRuns=shadow` only during the shadow slice.

M4.3 implementation note: `/ops/fetch-report` hydrates terminal `sources` from SQLite when `sourceRuns=sqlite`, keeps `?view=live` compact, and `/ops/fetch-report/sources` adds bounded source-row querying with JSON fallback.

M4.4 implementation note: after successful SQLite mirroring in authoritative mode, bridge-started terminal fetch reports move bulky source `details` into gzip evidence archives, write `sourceRuns.sourceDetailsArchive` refs into the compact report, and keep lean source rows for static/browser fallback.

M4.5 closeout note: new stores now seed `sourceRuns=sqlite`. Migration/package expectations are at schema version `006`, packaged jobs/admin smoke exercises a tiny source-run fetch through the real closeout path, and compact report hydration is validated through both `/ops/fetch-report` and `/ops/fetch-report/sources`.

### Milestone 5 - Move Jobs Feed Server-Side Authority

Purpose: move server-side job authority while keeping the frontend's static JSON contract.

- **Done:** Add generation-scoped `jobs` and `job_sources` APIs with idempotent batched upserts.
- **Done:** Store normalized source-bundle rows in `job_sources` while reconstructing exact canonical rows for export.
- **Done:** Publish a jobs-feed generation only after all rows insert and parity passes.
- **Done:** Generate `jobs-unified-light.json`, `jobs-unified.json`, and `jobs-unified.csv` as terminal compatibility exports from SQLite when `jobsFeed=sqlite`.
- **Done:** Keep frontend unchanged: `fetch()` plus IndexedDB from static JSON.
- **Done:** Keep export hashes out of job rows; storage-health diagnostics carry parity evidence.
- **Done:** Leave future paginated Jobs API design out of scope because M5 keeps static export boot.

Gate: complete. New stores seed `jobsFeed=sqlite`, terminal bridge-managed fetch closeout writes a published SQLite generation before regenerating compatibility exports, direct CLI output remains JSON fallback, packaged jobs-pipeline smoke proves SQLite parity plus static JSON loading, and rollback returns `jobsFeed` to JSON on storage/parity/export failure.

M5.1 implementation note: migration `007_jobs_feed_runtime.sql` rebuilds `jobs` and `job_sources` as generation-scoped tables and adds `job_feed_state` as the published-generation pointer. `JobRuntimeStore` owns staged generation writes, publish-time hash/count checks, current-row reconstruction, summary reads, and bounded old-generation cleanup.

M5.2 implementation note: bridge terminal fetch closeout mirrors `jobs-unified.json` into SQLite when `jobsFeed` is shadow or SQLite, compares the staged generation to the JSON export, publishes only after parity passes, and rolls `jobsFeed` back to JSON on write/parity failure.

M5.3 implementation note: when `jobsFeed=sqlite`, authoritative closeout regenerates `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` from the current SQLite generation using the existing output field lists and gzip-aware writers. Export failures roll the surface back to JSON.

M5.4 implementation note: packaged source-runs smoke now writes a deterministic jobs row, forces `jobsFeed=sqlite` inside the isolated smoke data directory, and proves storage-health parity plus static `jobs-unified-light.json` serving without frontend changes.

M5.5 closeout note: new stores now seed `jobsFeed=sqlite`. Migration/package expectations are at schema version `007`, and compatibility exports remain the permanent Jobs frontend boot path.

### Milestone 6 - Source Registry SQLite Authority and Evidence Closeout

Purpose: close this architecture plan end to end by adding the missing source-registry SQLite authority surface, proving deterministic packaged fetch evidence after the M5 cutover, and recording the final acceptance matrix.

- **Done:** Add migration `008_source_registry_runtime.sql` plus `SourceRegistryRuntimeStore` with generation-scoped active/pending/rejected rows, generation-scoped tombstones, a published `source_registry_state` pointer, parity hashes, and bounded old-generation cleanup.
- **Done:** Mirror normalized registry state and tombstones into SQLite in shadow/sqlite modes, compare JSON and SQLite projections, expose diagnostics through `/ops/storage-health`, and roll `sourceRegistry` back to JSON on write/parity failure.
- **Done:** Cut registry reads, summaries, tombstone load/save, POST mutation flows, and sync-service registry persistence to SQLite when `sourceRegistry=sqlite`; regenerate compatibility JSON/tombstone exports after successful authoritative publishes.
- **Done:** Add deterministic packaged fetch evidence smoke command `npm run test:frontend:packaged:fetch-evidence`. The smoke writes post-fetch `storage-health`, `storage-metrics`, fetch report, source-details query, registry summary, static jobs-feed sample, and `m6-fetch-evidence-summary.json` artifacts under the smoke output directory.
- **Done:** Keep the optional real-network corroboration path documented but non-blocking: `python src/packaged_desktop_smoke.py --node-smoke-script tests/frontend/packaged-desktop-smoke.fetch-evidence.mjs --fetch-evidence-mode real --playwright-timeout 600`.
- **Done:** Update package/schema expectations to migration `008`; new stores seed `sourceRegistry=sqlite` along with the M3-M5 SQLite authority modes.

Gate: complete. Three consecutive deterministic packaged fetch evidence passes ran against the final portable build, and the final gate passed: `cmd /c npm run test:py`, `cmd /c npm run lint:precommit`, `cmd /c npm run test:frontend:packaged`, and `cmd /c npm run test:frontend:packaged:jobs-pipeline`. Optional real-network evidence remains a local corroboration command, not a completion blocker.

M6.1 implementation note: migration `008_source_registry_runtime.sql` adds generation-scoped source-registry rows/tombstones and `source_registry_state`. `SourceRegistryRuntimeStore` owns staged generation writes, publish, replace, current-state reads, summaries, parity hashing, and bounded cleanup without importing bridge composition roots.

M6.2 implementation note: `RegistryService` mirrors JSON-authority writes into SQLite in shadow/sqlite modes and persists `sourceRegistry=json` on write/parity failure so route payloads remain JSON-compatible while retaining SQLite rows for diagnosis.

M6.3 implementation note: `/registry/active`, `/registry/pending`, `/registry/rejected`, `/registry/summary`, tombstone helpers, registry POST mutations, and sync-service persistence read/publish through SQLite when `sourceRegistry=sqlite`. Compatibility exports continue for active/pending/rejected/tombstones, and direct JSON drift triggers rollback instead of silent overwrite.

M6.4 implementation note: the packaged fetch-evidence smoke proves `sourceRuns=sqlite`, `jobsFeed=sqlite`, `sourceRegistry=sqlite`, migration `008`, passing source/job/registry diagnostics, bounded registry journal metrics, compact fetch-report hydration, `/ops/fetch-report/sources` SQLite details, and static `jobs-unified-light.json` serving.

M6.5 closeout note: the plan is completed. M6 made source registry a first-class SQLite authority surface inside this plan, recorded deterministic packaged evidence, and kept real-network evidence optional because the required gate must be repeatable without external network availability.

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
| M6 | `sourceRegistry` rolls back to JSON on storage, busy-timeout, missing-generation, parity, or compatibility-export failure. Deterministic packaged evidence failure blocks plan completion until fixed; SQLite rows remain for diagnosis. |

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
- M6 closeout maps each criterion above to concrete evidence and removes or links any remaining out-of-scope future work before the plan status becomes completed.

## M6 Acceptance Matrix

| Criterion | Evidence |
|---|---|
| Runtime evidence is never shadowed by stale `.jsonl` journals. | M0A writer hardening plus focused runtime-evidence tests in `npm run test:py`. |
| Existing runtime evidence writes do not create `.jsonl` journals, and stale adjacent journals do not affect canonical runtime evidence no-op checks. | M0A tests and storage-metrics/runtime-evidence coverage in `npm run test:py`. |
| `source-discovery-candidates.json` uses canonical-only array reads and cannot be shadowed by adjacent journals. | M0A discovery-candidate coverage in `npm run test:py`. |
| Bridge startup quarantines stale runtime evidence `.jsonl` siblings for the current runtime evidence filename set. | M0A startup cleanup coverage in `npm run test:py`. |
| Non-registry JSON artifacts are not journaled by `save_json_atomic()`, and stale adjacent journals do not overlay their canonical JSON. | M0A/M0.5 registry-journal tests plus `lint:precommit` import/contract policy. |
| Registry journal size is bounded and cannot grow unboundedly across repeated writes or replace failures. | M0.5 bounded journal implementation; M6 packaged fetch evidence asserts bounded registry journal metrics. |
| Sync size metrics are present in logs, timing, summaries, and history; shard metrics are added with the sharded sync milestone. | M0B/M2.10 sync propagation tests and bridge sync tests in `npm run test:py`. |
| Storage metrics prove whether SQLite migration is still justified after journal repair. | M0C metrics plus M6 deterministic packaged evidence artifacts: `storage-metrics.post-fetch.json` and `m6-fetch-evidence-summary.json`. |
| Sync cap failures terminalize with explicit `snapshot_too_large` evidence. | M2 sharded push/source-sync tests in `npm run test:py`. |
| Sharded source sync can grow by adding shards without overwriting committed state before shards exist. | M2.8-M2.11 shard IO/push tests and v3-only contract docs. |
| Admin live progress reads compact current state, not full terminal reports. | M3/M4 task runtime and source-run route tests plus packaged jobs-pipeline smoke. |
| JSON exports are compatibility/debug artifacts, except the jobs feed export which remains the permanent frontend boot path. | M4/M5 closeout notes, storage contract, and packaged jobs/feed static-serving assertions. |
| Full fetch/discovery evidence remains available through lazy detail APIs or compressed filesystem archives. | M4 evidence archive tests and M6 `/ops/fetch-report/sources` packaged evidence artifact. |
| Hot-path payload budgets are enforced in tests. | Storage/source-run/task route tests and packaged fetch-evidence assertions for compact fetch report hydration. |
| SQLite health checks and backup/restore flows are validated before any user data authority moves. | M1 storage skeleton tests, migration/package expectations through `008`, and `/ops/storage-health` packaged evidence. |
| All SQLite write transactions use `BEGIN IMMEDIATE`; busy contention has bounded retry behavior. | `BaluffoStore` tests, storage package tests, and `lint:precommit` contract checks. |
| WAL checkpoint failures are logged and mark the store unhealthy at controlled checkpoint points. | Storage health/store tests and `/ops/storage-health` diagnostics contract. |
| Source registry SQLite authority is no longer dangling future scope. | M6.1-M6.3 implementation, `sourceRegistry=sqlite` default, registry route tests, and packaged fetch evidence asserting `sourceRegistry=sqlite`. |
| Final deterministic packaged evidence and gates passed. | `build:portable-exe`; `test:frontend:packaged:fetch-evidence` x3; `test:py`; `lint:precommit`; `test:frontend:packaged`; `test:frontend:packaged:jobs-pipeline`. Optional real-network evidence is documented but not required. |
