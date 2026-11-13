## [0.3.059] - 2026-11-13

### Added

- Admin maintenance progress now shows saved versions processed against the pass total and, when available, saved-version row totals. Each staged feed records its jobs and source-row counts, and older generations that predate that metadata are counted once during maintenance instead of delaying every pass.

### Changed

- Jobs-generation cleanup now uses a bulk-delete mode with 1,000-row batches, a per-delete-transaction `secure_delete=FAST` override that restores the normal writer policy immediately, one larger WAL auto-checkpoint instead of repeated 4 MiB checkpoints, and a single final truncating checkpoint. On the measured subset it raised cleanup throughput from about 3,900 to 326,000 rows/minute, including the final checkpoint.

### Fixed

- A slow KPI summary read no longer makes the Admin page report the bridge as offline; the bridge badge follows `/app/ready` while panel data refreshes independently.

### Notes

- The current feed, saved jobs, notes, and compatibility exports remain available after upgrading.
- This release does not change the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
