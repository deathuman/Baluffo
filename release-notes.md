## [0.3.057] - 2026-11-12

> This container rollup includes the storage, retention, maintenance, and
> write-lock changes published as 0.3.049 through 0.3.056, so an install
> upgrading from 0.3.048 receives the whole set in one step.

### Added

- Admin's Database storage panel now shows the current database write-lock holder and held duration, with a clear "No write lock held" state when the database is idle.
- Admin's Runtime Database Storage panel now shows live maintenance progress: the current cleanup phase, elapsed time, runs processed, and rows or older feed versions removed.
- Admin's Runtime Database Storage panel has a **Run maintenance** button that starts the bounded cleanup normally run after serving. It keeps the current feed and refuses to start while an active task is running.
- Admin's Runtime Database Storage panel can run an exact, read-only diagnostic on a large database. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, and the result can be exported as JSON.
- Admin's Quick check counts `job_sources` and `jobs` rows by feed generation and includes a bounded estimate of current-versus-older stored row bytes, marking the estimate partial when its deadline expires.

### Changed

- Old saved jobs-feed generations are removed during post-serve maintenance in bounded batches, keeping the current generation and at most four older generations per pass. The cleanup releases the database write lock between batches instead of holding it for the entire deletion.
- The runtime database keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup.
- Admin's Runtime Database Storage panel uses clear, bordered sections and plain-language labels, with the exact **Full storage audit** under Advanced and raw details under Technical details.

### Fixed

- A new cleanup pass clears stale generation counters left by the previous pass.
- Maintenance status reports a pass as running from the moment it starts, including cleanup triggered after serving.
- Closing the runtime database waits for background post-serve maintenance before releasing its SQLite connection, preventing a shutdown race during old-feed cleanup.
- Admin's Quick check no longer groups `job_sources` or starts a full generation scan on a large database. It reports current counters immediately and counts jobs by generation under a short deadline.
- The Runtime Database Storage panel receives table sizes while a large database is being cleaned, with a hard sampling deadline that does not load oversized source payloads.
- Admin's Quick check estimates current versus older stored row bytes with a small number of sequential rowid windows instead of dozens of random row lookups.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved. Older feed generations are retained only until the post-serve cleanup has established that the current generation is published.
- No compatibility changes affect the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
