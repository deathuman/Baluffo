## [0.3.035] - 2026-11-04

### Fixed

- Registry Conflicts no longer times out when its saved list is out of date. The panel shows the
  last saved list, or a short "building" message when there is none, while a fresh copy is
  assembled in the background, then updates itself.
- Closing the desktop window while a new page is still loading no longer shuts the app down
  mid-navigation; the pending close is cancelled when the replacement page boots.
- The Admin action center no longer raises its attention chip for a source probe that used its
  full time budget but settled, and reports page failures as outcome counts instead.

### Notes

- Distribution behavior is unchanged from 0.3.034: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
