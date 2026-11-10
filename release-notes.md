## [0.3.048] - 2026-11-10

> This desktop rollup includes the fixes published to the container channel as
> 0.3.044 through 0.3.047, plus the desktop and Admin changes below.

### Changed

- Admin now uses the active fetch's live report while work is running, so progress and
  failure details update instead of showing the last published report.
- Admin loads its storage details only when they are used, and the storage read is bounded.
  The panel stays responsive while a fetch is running and shows the last good sizes when the
  disk is slow.
- Source maintenance now recognizes career boards served by supported ATS providers and can
  refresh or retire changed static sources only after the evidence and a working control pass.

### Fixed

- Finalizing a fetch preserves lock priority and finished rows if reconciliation is briefly
  busy, so a lock timeout no longer discards completed work.
- A failed availability update restores the same startup feed files that existed before the
  attempt, instead of leaving a new file behind.
- Linux browser fallback keeps Chromium's GPU work in-process, so browser fallback can render
  on hosts that block the GPU helper.
- Packaged desktop builds no longer exceed the startup size limit after the storage-panel
  change.

### Notes

- Existing jobs, saved jobs and notes are kept. The same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all continue to work.
