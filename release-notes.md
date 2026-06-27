## [0.2.97] - 2026-06-27

### Fixed
- Umbrel Admin degraded bootstrap now forces an authoritative pipeline schedule refresh after rendering the shell, so the schedule row resolves from `loading` to the real next fetch date.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.96] - 2026-06-27

### Fixed
- Umbrel Admin degraded bootstrap and dashboard fallbacks no longer publish factual schedule, KPI, registry, sync, or profile data.
- Pipeline schedule rendering now ignores stale degraded fallback state so the authoritative next scheduled fetch cannot be overwritten by `due now`.

### Notes
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.95] - 2026-06-27

### Fixed
- Runtime SQLite startup now avoids quick-check scans entirely, and storage-health quick-checks are deferred for oversized runtime databases.
- Runtime SQLite WAL files now trigger size-based background checkpoint maintenance, with database/WAL/SHM/checkpoint status exposed in storage health.
- `storage-metrics.jsonl` now rotates at a bounded size and storage metrics reads only a tail window.

### Notes
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.94] - 2026-06-27

### Fixed
- Admin no longer renders an active scheduled pipeline as `due now`.
- Admin source tables use an active-safe compact registry path during running fetch/pipeline work.
- Admin KPI cards preserve or lazily hydrate historical values during active jobs instead of remaining indefinitely delayed.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.93] - 2026-06-27

### Fixed
- Pipeline schedules with no prior terminal pipeline run now anchor the next run to the schedule save time plus the configured interval instead of immediately showing `due now`.
- The Umbrel container gateway schedule fallback now uses the same no-history anchor policy as the bridge scheduler, including compatibility fallback to the existing schedule file modification time.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
