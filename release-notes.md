## [0.3.038] - 2026-11-05

> This is a desktop rollup: it combines the changes that shipped to the container
> channel as 0.3.035, 0.3.036, and 0.3.037 with the performance work below, so a
> desktop install upgrading from 0.3.033 receives the whole set in one step.

### Fixed

- The Jobs page no longer waits on a full jobs-feed parse when its startup
  snapshot has to be rebuilt; the bridge reads only the rows that snapshot
  needs.
- Jobs sources that need a browser fallback now keep the rendered page instead of
  falling back to the original static shell, so dynamic boards can recover rather
  than reporting an empty or failed source.
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
- Source Policy review actions and the panel refresh no longer time out on a large registry.
  Review-only actions skip loading the full registry, and the conflict list is reused while the
  underlying registry and source data have not changed.
- Registry Conflicts no longer times out when its saved list is out of date. The panel shows the
  last saved list, or a short "building" message when there is none, while a fresh copy is
  assembled in the background, then updates itself.
- Closing the desktop window while a new page is still loading no longer shuts the app down
  mid-navigation; the pending close is cancelled when the replacement page boots.
- The Admin action center no longer raises its attention chip for a source probe that used its
  full time budget but settled, and reports page failures as outcome counts instead.

### Notes

- Distribution behavior is unchanged: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
