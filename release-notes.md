## [0.3.051] - 2026-11-11

### Added

- Admin's Runtime Database Storage panel now has a **Quick check** that counts `job_sources` and `jobs` rows by feed generation without a full `dbstat` scan. It reports current feed counts, page and free-page totals, and the largest generations in seconds or minutes, so a runaway runtime database can be identified without waiting for an exact allocation walk.

### Notes

- The Quick check is read-only and adds no migration. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain unchanged.
