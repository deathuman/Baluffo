## [0.1.22] - 2026-04-16

### Changed
- The desktop Jobs-page updater now surfaces persisted background download failures directly in the update panel instead of falling back to the generic available-update state.
- Release and troubleshooting documentation now describe the explicit failed-download retry path for the portable desktop updater.

### Fixed
- Desktop update downloads that fail in the background now keep the panel open, show the persisted updater error, and offer a direct `Try download again` action.
- Failed portable ZIP downloads now clear stale install-ready state and best-effort delete bad staged artifacts so retry starts from a clean updater state.
