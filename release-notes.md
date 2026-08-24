## [0.2.135] - 2026-08-24

> Shared Desktop + Umbrel Admin reliability patch: instant schedule-panel
> hydration, JSON-authority source tables fix, /registry/sources legacy-mode
> removal, and dead history-projection/state-reader retirement.

### Fixed

- Admin Ops schedule panel no longer sits on "loading schedule..." for the
  first idle-poll interval (~10s) after opening or refreshing the page: the
  bootstrap payload's schedule section now seeds the panel model directly,
  with the early schedule GET kept as a fallback whenever seeding does not
  yield a hydratable model.
- Pending/Active source tables on JSON-authority deployments (default outside
  SQLite migration) no longer stick on "Source tables refreshing" forever:
  the compact-table payload now serves real limited rows from the normalized
  JSON registry state instead of a degraded-empty stub.
- Stale-report classification and live-task evidence no longer read the frozen
  `admin-task-state.json` artifact; lifecycle rows are the sole liveness
  authority. The packaged desktop also dropped its disk-fallback for conflict
  diagnosis, and the dev supervisor stopped reclaiming PIDs from it.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Removed

- `/registry/sources` legacy modes: `view=full`, the `detail=full|summary`
  selection, and the `activeCompact`/`compactActive` aliases now return HTTP
  400 with `removedParams`. The endpoint serves one authority-aware
  compact-table lane (`view=table` or omitted; JSON-authority deployments get
  real rows instead of the previous degraded-empty stub). `/registry/summary`
  no longer accepts the dead `cheap`/`storage` view aliases.
- Dead report-file history projection lane (`sync_history_from_reports`,
  `project_run_history`) and its facade/wiring; `/ops/history` and fetcher
  metrics already read the lifecycle-ledger projection.
- Runtime reads of the frozen `admin-task-state.json` artifact: stale-report
  classification and live-task evidence checks now use lifecycle/report
  signals only. The file is never consulted outside explicit migration
  tooling, and the packaged desktop no longer falls back to it for conflict
  diagnosis.
