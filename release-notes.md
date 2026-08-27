## [0.2.140] - 2026-08-27
### Changed

- Availability direct enforcement is promoted for the container runtime: the Umbrel
  compose now sets `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1`, so direct availability
  checks publish lifecycle transitions and reopen rows with definitive live evidence
  instead of only recording shadow results. Promotion followed the reviewed gate
  (healthy seven-day sweep, clean Saved page, reviewed 100-job stratified sample,
  no unresolved high-risk classifier family) recorded in
  `docs/snapshots/availability-direct-promotion-2026-08-27.md`. Desktop runtimes
  stay in shadow mode until separately promoted.
- Jobs page auto-hydrates the complete feed right after the startup snapshot
  renders (idle-deferred, off the boot critical path) in all runtimes, so the
  full list no longer requires pressing Reload. Boot stays bounded: the
  snapshot renders first, the full feed syncs in the background, and explicit
  Reload continues to work as before.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
