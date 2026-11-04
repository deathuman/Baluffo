## [0.3.037] - 2026-11-04

### Fixed

- Jobs sources that need a browser fallback now keep the rendered page instead of
  falling back to the original static shell. A successful render is used as the
  source result, so dynamic boards can recover rather than reporting an empty or
  failed source.
- The container boot-performance check now matches the full-feed-first Jobs page:
  it expects the full feed before the page is interactive and no longer treats
  that request as forbidden.

### Notes

- Distribution behavior is unchanged from 0.3.036: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.
