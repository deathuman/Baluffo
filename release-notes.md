## [0.3.038] - 2026-11-05

### Fixed

- The Jobs page no longer waits on a full jobs-feed parse when its startup
  snapshot has to be rebuilt; the bridge reads only the rows that snapshot
  needs.
- Availability changes written back to the jobs feed no longer read that feed
  twice.

### Notes

- Distribution behavior is unchanged: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
