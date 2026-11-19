# Source Sync Contract

This document tracks the source-sync assumptions that should stay stable across runtime and governance changes.

## Canonical snapshot shape

The canonical remote snapshot is intentionally narrow:

- `schemaVersion`
- `generatedAt`
- `source`
- `active`
- `pending`
- `rejected` (valid top-level key of the remote snapshot read/merge shape — the validator accepts it and legacy monoliths may carry it — but v3 writes never shard or push rejected rows; rejected stays local-only)

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
- **Push protocol:** one atomic Git Data API commit per push. Changed shards and `manifest.json` are uploaded as blobs, assembled into a tree on top of the current branch tree, committed together, and only then is the branch ref fast-forwarded. Readers resolve the manifest through the branch ref, so a manifest can never reference shards from an unfinished push.
- **Push verification:** each shard's SHA-256 is checked against its payload before upload, each created blob's git object id is checked against the uploaded bytes, and the new tree is read back and matched against the expected path/blob pairs before the commit is created. A truncated tree listing fails the push instead of committing an unverified generation. The ref update is never forced, so a writer that read an older branch head gets `remote_conflict` instead of overwriting the newer generation. An incremental push also re-reads the committed manifest SHA it started from before writing.
- **Changed-shard detection:** Compare SHA-256 against last committed manifest
- **Write compatibility:** do not update the v2 monolith
- **Read compatibility:** fall back to v2 `source-sync.json` only when no trusted committed v3 manifest exists
- **Garbage collection:** after a successful generation commit, delete a bounded number of unreferenced shard files under `baluffo/source-sync/shards/`. GC retains the current manifest's shards plus the last three manifest-changing commits, read through the Contents API at each commit ref, so a pull that read an older manifest can still read its shards during a concurrent push. The shard listing itself comes from the branch tree, not the Contents API, whose 1,000-entry directory cap would hide the tail of a busy shard directory; a truncated tree listing deletes nothing and reports a warning. If the retention history cannot be established, GC fails closed and deletes nothing. GC warnings are reported after success and do not roll back the commit.
- **Metrics:** sync results, task summaries, and timing history include additive shard fields: `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `shardsReadBytes`, `totalShardBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes`. Pull timing history additionally includes `shardReadDurationMs`, the wall duration of the sharded read phase, separate from merge. The push result's `gc` object also reports `retainGenerations`, `retainedGenerationCount`, and `retainedShardCount` alongside the existing delete counts and warnings.
- **Storage metrics:** `record_source_sync_snapshot` records size pressure (`sizeBytes`, `maxSnapshotSizeBytes`, `sizeWarning`, `wouldChange`) and only the storage-relevant shard subset `snapshotFormat`, `shardCount`, `changedShardCount`, `shardsPushedBytes`, `manifestSizeBytes`, `shardCapBytes`, and `shardHashes`; it does not record `shardsReadBytes`, `totalShardBytes`, or `shardReadDurationMs`.
- **Live progress:** sharded pulls emit additive `/ops/task-live/sync` progress while reading shards, and sharded pushes emit progress while uploading/verifying shards, committing the manifest, and pruning old shards. These heartbeat counters are display-only and do not change the read/write protocol or committed snapshot contract.
- **Pull no-op:** successful pulls persist the last committed manifest SHA in runtime sync state. If a later pull sees the same committed manifest SHA, it skips shard downloads and reports `skipped: true` with `skipReason: remote_manifest_unchanged`.
- **Push fast path:** successful pushes persist the committed manifest SHA and a local registry revision (active, pending and rejected rows plus tombstones) in runtime sync state. When both still match, a push reads only the committed manifest (one GET) and reports `skipped: true` with `skipReason: remote_and_local_unchanged` instead of downloading every shard. Row counts on that path come from the manifest's per-shard `rowCount`, so a preview still reports the remote generation when the local registry lacks rows another client merged earlier. The same path backs the Admin Preview Push action, which calls `POST /sync/push` with `{"dryRun": true}` and never writes.
- **Sync routes:** `POST /sync/pull` and `POST /sync/push` are async-first. The default body starts the tracked task (`/tasks/run-sync-pull` / `/tasks/run-sync-push` behave the same) and returns `{started, task, action}`, so the request thread never holds a remote transfer; a sync task already running answers 409. `{"inline": true}` keeps the synchronous run for scripted callers such as the packaged sync rehearsal. The push `dryRun` preview stays synchronous because it returns the comparison.
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
