## [0.3.063] - 2026-11-15

### Changed

- Automatic database storage reclaim now arms itself instead of waiting to be switched on. After upgrading, a host that never set `BALUFFO_RECLAIM_AUTO_ENABLED` runs one rehearsal — a validated copy that is measured and then discarded, never installed — and, when that measurement shows the free-space margin is adequate, automatic shrinking may start on its own once a run is safe: the database reads as bloated, the copy fits in free space, and no task or compaction is running. The Runtime Database Storage panel now says why automatic starts are allowed, and Rehearse, Run and Stop work as before. To opt out, set `BALUFFO_RECLAIM_AUTO_ENABLED=0` and restart the bridge: that blocks the arming rehearsal and every automatic start, while the manual controls keep working.

### Fixed

- Container images now record the commit and build time they were built from, so `/ops/health` reports the identity of the running image instead of the placeholder `unknown`; a local build without a stamp reports empty strings rather than a value nobody can check.
- The one-time delete of a retained pre-rebuild database now runs as a background storage-rebuild job. Admin starts it, can leave the page open, and polls the existing storage-rebuild status until validation and unlink finish; a duplicate start reuses the active run. Validation failure is terminal and keeps the backup. If the bridge restarts after the file was unlinked but before the delete journal was written, the next status read records the completed deletion without touching the live database or any other file.

### Notes

- The background-delete and reconciliation changes keep the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the delete remains explicitly confirmed and feed-validated.
- Self-arming changes only when automatic reclaim may start, never what it does: a run still validates the database before installing anything, keeps the retained copy, and refuses every safety blocker. A host with `BALUFFO_RECLAIM_AUTO_ENABLED=0` behaves exactly as before, and every host can still rehearse or run by hand.
