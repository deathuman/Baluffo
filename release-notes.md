## [0.2.144] - 2026-09-03
### Changed

- "Check availability now" feedback overhaul (jobs + saved pages): results now show human verdicts with meaningful tones instead of raw classifier labels — green "Verified live" only for definitive live evidence, red for closed evidence, neutral "Couldn't verify" (with an Open job page action) for inconclusive outcomes like unverified pages, anti-bot blocks, or careers-page redirects. The checking toast updates in place with an elapsed-seconds counter instead of stacking duplicate toasts, and the frontend polls as long as the backend reports the run as running instead of giving up after 60 seconds.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Fixed

- Availability check reliability on slow-storage installs (Umbrel): the bridge availability service now caches resolved lifecycle identities with file-stamp validation, so repeated checks skip re-parsing the tens-of-megabytes lifecycle state file, and the worker no longer re-reads the file a second time when nothing changed since target preparation. Custom-saved lookups read the existing priority manifest first and only rebuild it when the identity is absent.
- Packaged desktop runtime now enables `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` in the launcher child environment, matching the Umbrel container, so manual availability checks apply evidence immediately instead of running in record-only shadow mode.
