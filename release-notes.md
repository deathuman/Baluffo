## [0.3.061] - 2026-11-14

### Added

- Admin can rebuild the runtime database around the current published feed in one action. The rebuild keeps every non-feed table — including saved jobs, notes, tasks, history, and source registry data — and copies only the current `jobs` / `job_sources` generation. It shows live progress, validated job/source counts and hashes, and reclaimed space, and retains the previous database for rollback until you explicitly delete it.
- Admin now has a read-only **Preview cleanup** step. It shows the protected current version, the saved versions that maintenance would remove, bounded job/source row and payload-byte estimates, reusable database pages, and free disk space without deleting anything or scheduling work.
- A bounded feed-cleanup pass can be stopped from Admin at its next safe batch or phase boundary. The stop control is tied to the active pass, so a stale or missing run identity cannot cancel a newer pass. An in-flight compaction is not interruptible and is labelled separately.
- Storage maintenance progress now reports when a capped pass queued a follow-up pass, when a failed pass queued a retry, and whether the current pass is fully complete. Saved-version progress separates versions processed from versions deleted.
- A tracked generation-cleanup profiler builds a disposable, versioned SQLite fixture and attributes delete, commit, WAL-growth, and checkpoint time without reading live data.

### Changed

- Runtime database diagnostics now report compaction size-gate details and per-transaction write timings: callback and DELETE work, commit time including SQLite’s inline checkpoint, WAL size before/after, and explicit checkpoint time. This makes a long maintenance hold diagnosable without ad-hoc profiling.
- Admin’s storage panel now exposes the rebuild and cleanup preview, shows rebuild phases and validation results, disables unsafe actions while a rebuild or compaction is active, and keeps polling only while a live pass is running.
- Fetch finalization now compares a canonical content hash before staging a new feed. If only observation or availability timestamps changed, the existing saved version and JSON projections stay in place instead of publishing a new generation.

### Fixed

- A rebuild now fails closed when free disk space cannot be determined instead of assuming the copy will fit. A busy WAL checkpoint during installation is retried, then reported as a failure rather than incorrectly claiming the feed was superseded.
- The canonical unchanged-feed hash now ignores `fetchedAt`, so a fetch that only stamps that field does not create a redundant generation.
- Admin no longer presents compaction as a stoppable maintenance action, and storage maintenance polling no longer continues while active work holds the gate.

### Notes

- The current feed, saved jobs, notes, and compatibility exports are preserved across the rebuild. The old database remains available as a rollback copy and is never deleted automatically.
- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain unchanged; this release does not change desktop packaging or its update manifest.
