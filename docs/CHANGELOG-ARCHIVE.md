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
