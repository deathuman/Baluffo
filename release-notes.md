## [0.3.050] - 2026-11-11

### Added

- Admin can now run an exact, read-only storage diagnostic while the runtime database is large. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, then lets the result be exported as JSON for an incident record.

### Notes

- Existing saved data and database cleanup are unchanged. Compatibility remains intact for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
