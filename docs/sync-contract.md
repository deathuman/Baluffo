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

## Planned: Sharded Sync Migration (schema v3)

The monolithic `baluffo/source-sync.json` will be replaced with sharded gzip files:

```text
baluffo/source-sync/manifest.json
baluffo/source-sync/active/00.json.gz
baluffo/source-sync/pending/00.json.gz
baluffo/source-sync/metadata/00.json.gz
```

Key details:

- **Schema version:** 3
- **Shard key:** stable source identity hash prefix
- **Per-shard cap:** 5-10 MiB; split by longer hash prefix if exceeded
- **Push protocol:** Two-phase manifest commit (propose → push changed shards → commit)
- **Changed-shard detection:** Compare SHA-256 against last committed manifest
- **Backwards compatibility:** Schema v2 monolithic read support kept during migration

Full design in [`docs/plans/runtime-storage-and-sync-architecture-plan.md`](plans/runtime-storage-and-sync-architecture-plan.md).

## GitHub API versioning

Source-sync requests currently send `X-GitHub-Api-Version: 2022-11-28`.

Keep the version in a module constant and monitor the GitHub API changelog for deprecation notices before changing it.

## Snapshot Size Guard

Source-sync keeps a write-time size guard to catch runaway payloads before pushing to GitHub. The default ceiling is 100 MB, with warnings above 5 MB. Normal production snapshots may exceed the old 5 MB historical rejection limit; that is not a lifecycle failure by itself.

When the limit is exceeded, sync must fail with `snapshot_too_large` and the pipeline must terminalize the sync child and parent pipeline instead of leaving either row active.

## Repo-local guidance

Use this doc for snapshot contract notes, API-version changes, and other release-path reminders that are part of the source-sync contract surface but not full operator runbook material.
