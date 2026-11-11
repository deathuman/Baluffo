## [0.3.056] - 2026-11-11

### Added

- Admin's Runtime Database Storage panel now shows live maintenance progress: the current cleanup phase, elapsed time, runs processed, and rows or older feed versions removed. It updates while cleanup is running and reports completion or a scheduled follow-up.
- Storage health now reports which maintenance phase is active, so a long cleanup is visible instead of appearing as a running task with no status.

### Fixed

- Maintenance status now reports a pass as running from the moment it starts, including cleanup triggered after serving, rather than showing a running task with no recorded status.

### Notes

- This release is for the Linux container and Umbrel app. The current feed, saved jobs, notes, and compatibility exports are preserved; no data migration is required.
- No compatibility changes affect the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
