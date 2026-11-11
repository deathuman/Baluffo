## [0.3.053] - 2026-11-11

> This container rollup includes the changes published as 0.3.049 through
> 0.3.052, so an install upgrading from 0.3.048 receives the storage,
> retention, and Quick check changes in one step.

### Added

- Admin's Runtime Database Storage panel can now run an exact, read-only diagnostic while the database is large. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, and the result can be exported as JSON for an incident record.
- Admin's Quick check counts `job_sources` and `jobs` rows by feed generation without a full `dbstat` scan. It reports current feed counts, page and free-page totals, and the largest generations so a runaway database can be identified quickly.
- Admin's Quick check now includes a bounded estimate of stored row bytes in the current generation versus older feed generations. It samples current rows and table rowids instead of walking the database or grouping `job_sources`; the estimate excludes indexes and is marked partial when its deadline expires.

### Changed

- The runtime database now keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes, so a long-lived install can reclaim space instead of growing forever. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup indefinitely.

### Fixed

- The Admin **Quick check** no longer starts a full `job_sources` generation scan on a large runtime database. It reports the exact current-generation counters from `job_feed_state` and counts jobs by generation under a short deadline; older source-row totals stay in the exact diagnostic. On the live 211 GB database, the previous check spent over 20 minutes grouping `job_sources`.
- The Admin Runtime Database Storage panel now receives table sizes while a large database is being cleaned. Table sampling has a hard deadline and no longer loads oversized source payloads before returning a partial estimate.

### Notes

- Existing jobs, saved jobs, notes, and the exact diagnostic are unchanged. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
