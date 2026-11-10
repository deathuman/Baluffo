## [0.3.047] - 2026-11-10

### Fixed

- Admin's runtime storage panel now answers even while a fetch is running on a slow disk. The storage read is bounded, so the panel shows the last good table sizes (marked as such) or a clear "still reading" state instead of `Bridge request timed out`.

### Notes

- Existing jobs, saved jobs and notes are kept. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all continue to work.
