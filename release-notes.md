## [0.3.054] - 2026-11-11

### Added

- Admin's Runtime Database Storage panel now has a **Run maintenance** button. It starts the same bounded cleanup that normally runs after serving: older saved jobs-feed generations are removed, then the database is compacted when the backlog drains. It keeps the current feed and refuses to start while an active task is running.

### Fixed

- Admin's Quick check now estimates current versus older stored row bytes with a small number of sequential rowid windows instead of dozens of random row lookups. It reads the current jobs count from the feed state and no longer groups the jobs table on a large database, so the check can finish on a 211 GB store.

### Changed

- The runtime database now removes old saved jobs-feed generations during post-serve maintenance, keeping the current generation and deleting at most four older generations per bounded pass. The existing 30-second follow-up drains the backlog, then the database is compacted so the freed pages are returned to the filesystem instead of remaining in the file.
- Admin's Runtime Database Storage panel now uses clear, bordered sections and plain-language labels. The exact **Full storage audit** sits under Advanced, while raw table, generation, byte, method, and warning details remain available in Technical details.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved. Older feed generations are retained only until the post-serve cleanup has established that the current generation is published. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
