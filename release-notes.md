## [0.3.044] - 2026-11-09

### Fixed

- A failed availability update no longer leaves a startup feed file behind when that file did not exist before the attempt. The rollback now restores the same set of feed files as well as the same rows, instead of re-creating every projection.

### Notes

- The same-origin Linux container deployment, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain supported.
