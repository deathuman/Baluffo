## [0.3.049] - 2026-11-10

### Fixed

- The runtime database now keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes, so a long-lived install can reclaim space instead of growing forever. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup indefinitely.
- The Admin Runtime Database Storage panel now receives table sizes while a large database is being cleaned. Table sampling has a hard deadline and no longer loads oversized source payloads before returning a partial estimate.

### Notes

- Existing jobs, saved jobs, and notes are kept. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
