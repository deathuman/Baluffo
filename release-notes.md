## [0.3.022] - 2026-10-19

### Changed

- **The desktop app shows its window sooner, and stops making the first screen wait on background
  work.** The bridge begins serving before it finishes the rest of its post-start chain, and the
  heavy startup work — the overdue job-availability refresh, the saved-state pull and the database
  maintenance pass — is now held back until the first page has finished its opening burst of
  requests. A launch with no window still gets that work done, so headless and container runs lose
  nothing. Opening a mature runtime database also no longer adds seconds before syncing starts.

- **A packaged launch does less work before it can answer.** Around a quarter of the modules the
  packaged bridge used to load at startup — the discovery and fetch-adapter code, the jobs-feed and
  lifecycle helpers, the parsers and the sync machinery — are now loaded when a request or a task
  first needs them. Measured on the 2026-10-18 portable build across four runs, the time from
  entrypoint start to the bridge being configured fell from a 240 ms median to 113 ms.

### Fixed

- **A failing metrics read can no longer stall the bridge's request loop.** The accepted-request
  counter is read on every pass of the loop, and a value it could not represent used to raise out
  of the loop instead of degrading to zero.

### Notes

- **Duplicate detection for the Elevato studio boards reads each posting once** instead of
  re-scanning the whole feed for every pair it considers. The jobs it produces, their identities
  and the per-board outcomes are unchanged.
- An alternative browser engine that was never part of a shipped build is retired from the fetch
  fallback path; the Chromium fallback and every user-visible fetch outcome stay as they were.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.021:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
