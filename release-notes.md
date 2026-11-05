## [0.3.038] - 2026-11-05

### Fixed

- The Jobs page no longer waits on a full jobs-feed parse when its startup
  snapshot has to be rebuilt; the bridge reads only the rows that snapshot
  needs.
- Opening the Ops tab no longer re-parses the task-lifecycle ledger twice per
  poll; the JSON read is cached against the artifact's file signature and
  refreshed by the bridge's own writes. The health response also hands its
  already-loaded task rows to the schedule summary instead of reading the ledger
  again, and a cached poll skips the stale-task repair scan.
- The registry-conflicts background refresh no longer parses the whole jobs
  feed. With the feed in SQLite it reads only the conflict sources through the
  `job_sources` index; otherwise it reads at most the first 10,000 rows of the
  JSON artifact. A truncated prefix is dropped and the conflicts are kept for
  review rather than suppressing boards on an incomplete job set. The Registry
  conflicts tab shows which path the refresh used, the rows read, and whether
  a prefix was discarded. A SQLite generation older than the newest source-state
  run raises a `sqlite_jobs_feed_stale` warning so stale job evidence is not
  mistaken for a completed suppression check.
- Availability changes written back to the jobs feed no longer read that feed
  twice.

### Notes

- Distribution behavior is unchanged: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
