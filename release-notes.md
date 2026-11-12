## [0.3.058] - 2026-11-12

### Added

- Storage health now reports cumulative successful PASSIVE checkpoint wait and hold counters, including count, totals, maxima, and last values, so checkpoint contention can be measured without copying the runtime database. Busy retries are excluded from those timings and reported separately as `walMaintenance.passiveCheckpointBusyRetries`.

### Changed

- Quick check's `rowBytesProbe` now reports `currentComplete` and `totalComplete`; Admin uses those flags instead of inferring completeness from per-table rows.

### Fixed

- Admin's storage cards now keep exact database, allocated, and free-space totals in front of partial table samples, and Quick check no longer reports a current-versus-older share when older rows were not measured.
- Quick check now measures current-generation bytes for every table before attempting whole-table samples and gives each table a fair share of its bounded budget, so a slow older-row sample no longer hides another table's current stats.
- Jobs-generation retention now caps `job_sources` deletions by row count instead of job keys and uses 250-row delete batches, so a multi-source job cannot inflate one cleanup transaction beyond the configured bound.
- Background PASSIVE WAL checkpoints now observe a 60-second cooldown, so a WAL held at the 64 MiB journal-size limit cannot schedule one after every write; a WAL crossing the 256 MiB truncate threshold still schedules TRUNCATE immediately.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved.
- No compatibility changes affect the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
