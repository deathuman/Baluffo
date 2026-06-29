## [0.2.102] - 2026-06-29

### Fixed
- Umbrel Admin Ops Overview now hydrates fetch KPI cards and Ops tab badges after schedule/history authority loads, preventing stuck loading placeholders when the backend routes are healthy.
- Admin source-table hydration is moved out of the startup window to avoid racing first-render authority hydration.

### Tests
- Bundled Admin smoke now covers the live-like degraded Ops Overview, including schedule controls, KPI cards, pending-source count, tab badges, and route evidence.

### Notes
- This supersedes the failed `0.2.101` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.101] - 2026-06-28

### Fixed
- Umbrel Admin schedule/history hydration now renders from current authoritative route state instead of stale startup render tokens.
- Heavy Admin detail routes are deferred out of the first startup hydration window to reduce 504 pressure.

### Tests
- Admin hydration smoke now exercises the hashed container bundle and fails on heavy startup route calls.

### Notes
- This supersedes the failed `0.2.100` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.100] - 2026-06-28

### Fixed
- Umbrel Admin pipeline schedule and Operations Activity now hydrate from their authoritative lightweight routes during startup.
- Degraded bootstrap/dashboard payloads can no longer leave schedule stuck loading or activity falsely empty.
- Admin preserves authoritative schedule/activity state across shell refreshes and rebinds replaced DOM targets before rendering.

### Tests
- Added a browser-based Admin hydration smoke for degraded bootstrap plus authoritative schedule/history route rendering.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.99] - 2026-06-27

### Fixed
- Umbrel Admin pipeline schedule rendering now uses a dedicated authoritative schedule model hydrated only from `/tasks/jobs-pipeline-schedule` or a successful schedule save, so degraded bootstrap/dashboard payloads cannot reset the row to unchecked `24h` defaults.
- Unknown schedule state now renders disabled loading/retrying controls instead of editable false defaults.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.98] - 2026-06-27

### Fixed
- Umbrel Admin bootstrap now treats missing schedule data as incomplete even when the shell route succeeds, and waits for the authoritative pipeline schedule route before leaving the first useful render.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

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
