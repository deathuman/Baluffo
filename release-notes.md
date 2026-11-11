## [0.3.055] - 2026-11-11

### Fixed

- Closing the runtime database now waits for the background post-serve maintenance pass before releasing its SQLite connection. This prevents a shutdown race during old-feed cleanup and keeps the current feed safe.

### Notes

- This release includes the storage-maintenance, storage-panel, and Quick-check changes first published in 0.3.054. The current feed, saved jobs, notes, and compatibility exports are preserved.
- Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
