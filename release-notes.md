## [0.3.030] - 2026-11-01

### Added

- Umbrel users can again install and update Baluffo through Deathuman's private
  community app store. The store entry uses the same container image as other
  self-hosted deployments.

### Fixed

- A run that has finished collecting jobs can no longer sit on "Running fetch" for
  hours while it merges the results. If merging stops making progress, the run now
  ends with a recorded error instead of waiting indefinitely, and each phase is
  written to the log as it starts so a slow run can be told apart from a stuck one.
