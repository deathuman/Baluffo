## [0.3.032] - 2026-11-03

### Fixed

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

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.031:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
