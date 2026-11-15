## [0.3.063] - 2026-11-15

### Fixed

- Container images now record the commit and build time they were built from, so `/ops/health` reports the identity of the running image instead of the placeholder `unknown`; a local build without a stamp reports empty strings rather than a value nobody can check.
- The one-time delete of a retained pre-rebuild database now runs as a background storage-rebuild job. Admin starts it, can leave the page open, and polls the existing storage-rebuild status until validation and unlink finish; a duplicate start reuses the active run. Validation failure is terminal and keeps the backup. If the bridge restarts after the file was unlinked but before the delete journal was written, the next status read records the completed deletion without touching the live database or any other file.

### Notes

- The background-delete and reconciliation changes keep the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the delete remains explicitly confirmed and feed-validated.
