## [0.3.045] - 2026-11-10

### Changed

- The Admin page now loads its database storage breakdown and its details drawer only when they are used, so the page has much less code to load before the first screen appears. Both still appear in the same place, and the storage table sizes still refresh on the same schedule.

### Fixed

- Portable Windows and Linux AppImage builds pass their startup size limit again after the storage panel was added, so packaged desktop builds can be produced from this release.

### Notes

- Existing jobs, saved jobs and notes are kept. Deployment compatibility is unchanged: the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave as before.
