## [0.3.052] - 2026-11-11

### Fixed

- The Admin **Quick check** no longer starts a full `job_sources` generation scan on a large runtime database. It reports the exact current-generation counters from `job_feed_state` and counts jobs by generation under a short deadline; older source-row totals stay in the exact diagnostic. On the live 211 GB database, the previous check spent over 20 minutes grouping `job_sources`.

### Notes

- Existing data and the exact diagnostic are unchanged. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain in place.
