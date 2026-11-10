# Changelog archive

Sections evicted from [`CHANGELOG.md`](CHANGELOG.md) by the five-released-section
clamp live here, newest first. The clamp keeps the install-facing file short; this
archive keeps the evicted text instead of deleting it.

The archive starts at **0.3.040**. Anything older stays in `git log`, which remains
the source of truth for that history, exactly as the changelog header says.

Do not edit a section after it lands here. New releases are added by
`python scripts/bump_version.py <next>`, which appends the evicted section.

## [0.3.033] - 2026-11-03

> This is a desktop rollup: it combines the changes that shipped to the container
> channel as 0.3.030, 0.3.031, and 0.3.032 with the performance work below, so a
> desktop install upgrading from 0.3.028 receives the whole set in one step.

### Added

- Umbrel users can again install and update Baluffo through Deathuman's private
  community app store. The store entry uses the same container image as other
  self-hosted deployments.

### Fixed

- The Admin page no longer stalls on its first call after a slow moment. Its startup request
  was cut off after 0.75 seconds while a healthy answer can take about a second, so the page
  fell back to "data delayed; retrying" on every navigation; it now waits up to three seconds,
  roughly triple the slowest answer measured on the box.
- The Ops tab badges no longer spend minutes recalculating while a run is active. The counts are
  read from the last completed scan and refreshed in the background, and a second copy of that
  work cannot start while one is already running; before any scan has finished the badges show
  as computing instead of holding the page.
- The source table reloads instantly after its first open. Building it used to re-normalize every
  registry row and re-derive the conflict groups on each open even when the registry had not
  changed; repeated opens now reuse that result until a registry or source-state file changes,
  and a registry with no removed sources no longer re-fingerprints every row's listing URL just
  to prove none match.
- Ops fetch statistics stay responsive during a run. The parts that only change when a fetch
  finishes are reused for up to 15 seconds, while the active-run status, alerts, and response
  timestamp are still read fresh, so an open Ops tab no longer pays about a second of history
  and registry scanning on every refresh.
- On installs that keep the registry in the app database, summary counts are reused until the
  registry actually changes, instead of rescanning it on every Admin and Ops poll.
- The Admin page no longer flips between "Bridge Reconnecting" and connected while the app is
  working normally. A slow storage-health read was being reported as the bridge going offline,
  which also put the page's connection banner on screen; that read now answers from a short-lived
  cached reading instead of waiting, and a single failed one is no longer treated as a lost
  connection.
- A storage-health request that cannot finish in time is answered with the last good reading,
  marked as possibly out of date for up to five minutes, instead of an error, so the Admin page
  keeps its storage numbers through a slow moment.
- The connection badge returns to its real state when a short reconnect finishes, instead of
  staying on "Bridge Reconnecting" with nothing behind it.
- A run can no longer sit silently for hours while another part of the app holds the job-feed update lock.
  While it waits, the log names the process holding the lock, and the run now ends with a recorded error
  after 20 minutes instead of waiting indefinitely; the limit can be adjusted in the container environment.
- A run that has finished collecting jobs can no longer sit on "Running fetch" for
  hours while it merges the results. If merging stops making progress, the run now
  ends with a recorded error instead of waiting indefinitely, and each phase is
  written to the log as it starts so a slow run can be told apart from a stuck one.
- A source that has stopped making progress is now given up on after 60 seconds of failed
  attempts, instead of holding the run for up to ten minutes. Runs finish sooner in the slow case,
  and no board that was still producing jobs is affected.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.032:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

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

## [0.3.039] - 2026-11-07

### Added

- The Admin Action Center now reports a failed browser-fallback probe, naming the stage, error type, sources needing browser fallback, and when the failure last happened. The item can be dismissed or copied into diagnostics.
- Ops fetcher diagnostics load only when their disclosure is opened, and reuse the same metrics request as the Dedup tab. An opened panel shows browser-fallback health and static deadline cuts.
- A completed Jobs pipeline now reports fetch-source failures and oversized sync snapshots as warnings instead of a clean run. The Jobs page says how many sources failed without treating the run as an error.

### Fixed

- Static fetches share one absolute source deadline across retries, redirect hops, and page batches, and each HTTP request has a hard total timeout, so a slow or trickling server cannot outlive its budget. A source that kept jobs stays a healthy partial result when a deadline cuts it; a source that kept none is reported as failed.
- Static deadline rows now preserve which clock fired (`request cap`, `source budget`, or `page-batch deadline`) in the fetch report and Ops view instead of collapsing to one generic timeout.
- The Jobs cache repairs a missing database index without discarding seen-job history, and a blocked database upgrade reports an error instead of hanging or silently clearing the cache.
- The browser fallback pool records the original launch or navigation error before the circuit breaker normalizes it, probes the browser after an environment-level failure, and closes a failed pool under the same lock before restarting.
- The Jobs page no longer labels a fetch-only warning as a source-sync problem; error, sync, and fetch outcomes keep their own messages.
- A run that cannot acquire the feed reconciliation lock now records the lock path, holder process, and wait limit in its failed report, so the timeout remains diagnosable after the live process is gone.
- A cached Ops summary refreshes when its browser-health, probe, or page-fetch sidecar changes, instead of serving the stale receipt until the cache expires.
- The Source Policy Review panel no longer times out or flips the bridge status to Reconnecting while its recommendations load on a large registry.
- Closing the desktop window now finishes cleanup within the five-second packaged target. The app waits two seconds for the bridge to settle, then stops even if the browser wrapper is still exiting, instead of waiting up to 20 seconds for a startup handoff after the window has closed.

### Notes

- Distribution compatibility is unchanged: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility continue to work as before.

## [0.3.040] - 2026-11-07

### Added

- Packaged desktop and container builds now record the commit and build time, and Ops Health and Admin show which build is running so an operator can confirm the deployed artifact without opening a shell.
- Container upgrades can be checked end to end with the new promote command: it restarts the compose stack and waits until Ops Health reports the expected version and commit.

### Fixed

- A fetch that finished reading exactly as its deadline fired no longer reports a timeout; only a request whose body was actually interrupted is treated as failed.
- A timed-out async fetch now cancels the request on its event loop instead of leaving it running after the caller has moved on.

### Changed

- Admin's first paint is now held to a hard 295 KiB budget. New first-paint weight must load on demand; the build fails instead of quietly growing the shell again.

### Notes

- Compatibility is unchanged across the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
