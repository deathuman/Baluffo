# Storage Contract

> - **Status:** Active
> - **Use this when:** implementing or validating runtime SQLite/WAL storage, storage authority migration, compatibility exports, evidence archives, or source-sync sharding
> - **Canonical for:** target storage authority boundaries, SQLite connection and transaction discipline, migration safety, export and rollback behavior, and hot-path size budgets
> - **Not canonical for:** current endpoint payload fields, current JSON artifact schemas, source-sync v2 schema details, or Jobs frontend row fields
> - **Then inspect:** [`sync-contract.md`](sync-contract.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md), [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), and archived rollout history in [`archive/runtime-storage-and-sync-architecture-plan.md`](archive/runtime-storage-and-sync-architecture-plan.md) only when historical provenance is needed
> - **Last updated:** 2026-05-12

This document defines the current runtime storage contract. The archived rollout plan records sequencing and closeout evidence; this active contract owns the invariants future code must preserve.

## Scope

Runtime storage covers bridge-owned hot state, terminal compatibility exports, registry/source-sync payloads, and filesystem-backed evidence archives.

The target shape is:

```text
SQLite/WAL local runtime database for hot state
+ compact compatibility JSON exports
+ registry-only bounded journal recovery
+ sharded GitHub source-sync snapshots
+ bounded filesystem-backed evidence archives
```

The Jobs frontend boot path remains static JSON plus IndexedDB. Desktop local user data remains JSON-backed through the existing local-data contract. BaluffoSync remains source-registry sync, not a job feed and not a remote database.

## Authority Split

| Category | Target authority | Compatibility/export surface | Notes |
|---|---|---|---|
| Current task liveness | SQLite `task_runs` after M3 cutover | `/ops/task-state` JSON projection | JSON lifecycle files remain compatibility exports and rollback fallback. |
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

It must not live under `_out/`.

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

`GET /ops/storage-health` exposes the storage health payload. It must include migration version, WAL mode, foreign-key state, last write error, busy count/rate, `quick_check` status, and current authority mode per surface.

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

Milestone 4 cutover is complete for `sourceRuns`: new stores seed `sourceRuns=sqlite`, terminal bridge-started fetch reports mirror source rows into `source_runs`, and source-run read/write/parity failures persistently roll the surface back to `json`.

Milestone 5 cutover is complete for `jobsFeed`: new stores seed `jobsFeed=sqlite`, bridge-managed terminal fetch closeout mirrors canonical jobs rows into generation-scoped SQLite tables, and successful authoritative closeout regenerates `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` as compatibility exports. Jobs-feed read/write/parity/export failures persistently roll the surface back to `json` while retaining SQLite generations for diagnosis.

Milestone 6 cutover is complete for `sourceRegistry`: new stores seed `sourceRegistry=sqlite`, registry active/pending/rejected rows and tombstones publish through generation-scoped SQLite tables, and successful authoritative publishes regenerate compatibility JSON/gzip exports. Source-registry read/write/parity/export failures, missing published generations, or direct JSON drift persistently roll the surface back to `json` while retaining SQLite generations and diagnostics.

## Compatibility Exports

Compatibility JSON exports remain generated until the owning frontend/API surface is separately retired.

Rules:

- Runtime evidence files are canonical JSON or gzip JSON artifacts, not journal-overlay artifacts.
- Registry journaling is registry-only and bounded.
- `jobs-unified-light.json` remains the permanent Jobs frontend boot export.
- Do not add cache hashes inside `jobs-unified-light.json` rows. Use sidecar metadata or bridge metadata.
- After M5, `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` are generated from the published SQLite jobs-feed generation during bridge-managed terminal fetch closeout when `jobsFeed=sqlite`; direct CLI outputs remain JSON fallback until bridge postprocessing runs.
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

Sync result, task-summary, timing-history, and `storageMetrics` payloads keep legacy size fields for compatibility and add authoritative v3 pressure fields: `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes`.

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

After M4, terminal source `details` for bridge-started fetches are written to gzip archives and referenced from both `source_runs.evidence_refs` and the compact fetch report. `/ops/fetch-report` and `/ops/fetch-report/sources` hydrate details from SQLite/archive while `sourceRuns=sqlite`; rollback to JSON leaves the compact or full report available for diagnosis.

## Size Budgets

Hot-path payload growth must be impossible by test.

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

## Benchmark Contract

For every milestone that changes runtime storage authority:

- Capture a JSON-baseline run before migration.
- Capture an equivalent shadow or authoritative run with the same source set and comparable artifact sizes.
- Full discovery/fetch wall-clock time must not regress by more than 5% without an explicit acceptance note.
- Admin live progress route latency must not regress by more than 10% without a root-cause note.
- Large artifact rewrites must decrease for the migrated surface or the tradeoff must be documented.
- `storageMetrics` must show equal or lower hot artifact bytes, registry journal bytes, and source-sync pressure for migrated surfaces unless a migration note explains otherwise.
- Compatibility exports must remain byte-contract compatible where existing frontend or docs require them.
