## [0.2.134] - 2026-08-24

> Shared Desktop + Umbrel Admin performance patch: ops summary TTL caching
> with active-run bypass, alert-state write suppression and locking,
> tab-counts cache key hardening, and mutually exclusive admin poll lanes.

### Changed

- Route-layer TTL caches for `/ops/dashboard-health?view=summary` (10s) and
  `/ops/fetch-kpis?view=summary` (15s) with per-cache single-flight locks;
  entries computed during an active run are never served once idle, and the
  active-run bypass probe also covers standalone fetch/bootstrap runs via the
  hot-task snapshot.
- Alert state: `ops-alert-state.json` is no longer rewritten on every summary
  poll when unchanged, and the alert read-modify-write shares one lock with
  the `/ops/alerts/ack` route to prevent lost acknowledgements under
  concurrent requests.
- Admin Ops tab counts: `jobs-source-state.json` is size-keyed instead of
  mtime-keyed so run-heartbeat rewrites no longer invalidate badges mid-run;
  corrupt cache envelopes recompute instead of failing the route, and cache
  writes use unique temp files for concurrent writers.
- Admin frontend polling lanes are mutually exclusive: while active-run
  evidence exists, idle scheduling routes through the fast active lane instead
  of heavy dashboard-health summaries.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Fixed

- Admin pipeline schedule no longer sticks on "loading schedule..." indefinitely:
  a schedule fetch that succeeded but normalized to an unhydratable payload
  (empty/degraded shape) previously recorded neither an error nor a retry, so
  the Ops schedule control and its Enable/interval inputs stayed disabled for
  the whole active run. Such payloads now surface "schedule delayed; retrying"
  and arm the existing backoff retry until a hydratable payload lands.
