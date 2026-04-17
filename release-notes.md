## [0.1.23] - 2026-04-17

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.
