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

## GitHub API versioning

Source-sync requests currently send `X-GitHub-Api-Version: 2022-11-28`.

Keep the version in a module constant and monitor the GitHub API changelog for deprecation notices before changing it.

## Snapshot Size Guard

Source-sync keeps a write-time size guard to catch runaway payloads before pushing to GitHub. The default ceiling is 100 MB, with warnings above 5 MB. Normal production snapshots may exceed the old 5 MB historical rejection limit; that is not a lifecycle failure by itself.

When the limit is exceeded, sync must fail with `snapshot_too_large` and the pipeline must terminalize the sync child and parent pipeline instead of leaving either row active.

## Repo-local guidance

Use this doc for snapshot contract notes, API-version changes, and other release-path reminders that are part of the source-sync contract surface but not full operator runbook material.
