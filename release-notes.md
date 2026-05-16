## [0.2.01] - 2026-05-16

### Changed
- Portable ZIP builds now embed only the required `chromium_headless_shell-*` Playwright browser payload, keeping offline browser fallback self-contained while avoiding unrelated browser cache siblings.
- No-openings detection now requires explicit visible empty-state evidence, and source reports keep hidden/script/template text and all-canonical-dropped rows in review instead of treating them as legitimate empty sources.
- Location sanity checks now preserve real city names such as Milan, Tel Aviv, and Frankfurt am Main, and treat `Unknown` country values as missing-country placeholders rather than contamination.

### Fixed
- Windows portable updater handoff confirmation no longer falsely rejects a live launcher when packaged runtimes lack optional `psutil`.
- Updater handoff failures now record non-secret diagnostics and clear stale post-install success markers before a fresh install handoff.
- Desktop update manifests for this release require updater capability `2.0.1`, so affected older clients stop attempting the broken automatic install path for future releases.
