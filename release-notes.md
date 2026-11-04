## [0.3.036] - 2026-11-04

### Fixed

- Source Policy review actions and the panel refresh no longer time out on a large registry.
  Review-only actions skip loading the full registry, and the conflict list is reused while the
  underlying registry and source data have not changed.

### Notes

- Distribution behavior is unchanged from 0.3.035: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
