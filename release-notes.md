## [0.3.033] - 2026-11-03

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

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.032:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
