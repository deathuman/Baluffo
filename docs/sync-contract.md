# Source Sync Contract

This document tracks the source-sync assumptions that should stay stable across runtime and governance changes.

## Canonical snapshot shape

The canonical remote snapshot is intentionally narrow:

- `schemaVersion`
- `generatedAt`
- `source`
- `active`
- `pending`

That shape is enforced by the source-sync schema validator and should remain the normal apply/input contract for production writes.

## Sharded Sync (schema v3)

Source-sync writes are v3-only. A write commits a schema-v3 manifest plus immutable gzip shard files:

```text
baluffo/source-sync/manifest.json
baluffo/source-sync/shards/active/<key>/<sha256>.json.gz
baluffo/source-sync/shards/pending/<key>/<sha256>.json.gz
```

Key details:

- **Schema version:** 3
- **Shard key:** stable source identity hash prefix; the default generation prefix is 1 hex character, with 1-character recursive splits only when a shard exceeds the per-shard cap
- **Per-shard cap:** 10 MiB by default; split by longer hash prefix if exceeded
- **Push protocol:** push changed shards, verify them, then update committed `manifest.json`
- **Changed-shard detection:** Compare SHA-256 against last committed manifest
- **Write compatibility:** do not update the v2 monolith
- **Read compatibility:** fall back to v2 `source-sync.json` only when no trusted committed v3 manifest exists
- **Garbage collection:** after a successful manifest commit, delete a bounded number of unreferenced shard files under `baluffo/source-sync/shards/`; GC warnings are reported after success and do not roll back the commit
- **Metrics:** sync results, task summaries, timing history, and storage metrics include additive shard fields: `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `shardsReadBytes`, `totalShardBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes`
- **Live progress:** sharded pulls emit additive `/ops/task-live/sync` progress while reading shards, and sharded pushes emit progress while uploading/verifying shards, committing the manifest, and pruning old shards. These heartbeat counters are display-only and do not change the read/write protocol or committed snapshot contract.
- **Pull no-op:** successful pulls persist the last committed manifest SHA in runtime sync state. If a later pull sees the same committed manifest SHA, it skips shard downloads and reports `skipped: true` with `skipReason: remote_manifest_unchanged`.
- **Runtime history:** after the runtime-storage M3 cutover, sync task events and final sync-run rows are mirrored into SQLite `task_events` and `sync_runs`; compatibility JSON timing/live-task exports remain present.

Current storage and runtime authority behavior is canonical in [`storage-contract.md`](storage-contract.md). The completed rollout history is archived in [`archive/runtime-storage-and-sync-architecture-plan.md`](archive/runtime-storage-and-sync-architecture-plan.md).

## GitHub API versioning

Source-sync requests currently send `X-GitHub-Api-Version: 2022-11-28`.

Keep the version in a module constant and monitor the GitHub API changelog for deprecation notices before changing it.

## Snapshot Size Guard

Source-sync keeps a write-time size guard to catch runaway payloads before pushing to GitHub. The default ceiling is 100 MB, with warnings above 5 MB. Normal production snapshots may exceed the old 5 MB historical rejection limit; that is not a lifecycle failure by itself.

When the limit is exceeded, sync must fail with `snapshot_too_large` and the pipeline must terminalize the sync child and parent pipeline instead of leaving either row active.

## Repo-local guidance

Use this doc for snapshot contract notes, API-version changes, and other release-path reminders that are part of the source-sync contract surface but not full operator runbook material.
