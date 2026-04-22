# Changelog

> All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and Baluffo desktop releases use the project-specific `0.1.x` ordering documented in
[`RELEASE.md`](RELEASE.md).

---

## [Unreleased]

## [0.1.33] - 2026-04-20

### Changed
- The desktop runtime has been modularized into focused `src/ship/desktop_app/` package modules (`launcher`, `startup`, `browser`, `session`, `_windows`, `config`, `process`) behind the existing `src.ship.desktop_app` compatibility facade, and the desktop ownership docs now point editors to those focused boundaries instead of the old monolithic module.
- Windows release-preflight now includes dedicated packaged rehearsal lanes for stale-runtime orphan reclaim and managed Chromium browser-job shutdown propagation, keeping the packaged smoke gate aligned with the hardened desktop supervision path.
- Uncapped fetch now reuses the regular fetch launch/runtime path with a narrower `50 / 5 / 10` overlay, seeds existing output during force-refresh runs, and enables a deeper uncapped static profile instead of maintaining a separate aggressive behavior tree.
- Packaged `scrapy_static_sources` fallback processing now runs as a bounded parallel queue with live heartbeat/progress reporting, and the Admin fetch UI surfaces that tail as an explicit `Browser fallback X/Y` progress badge instead of leaving the last running work item opaque.
- Portable builds now bundle the Scrapy fallback runtime stack needed by packaged child runners, including the `scrapy`, `scrapy_playwright`, and `twisted` runtime path.
- Jobs-page desktop updater install confirmation now falls back cleanly when the richer dialog hook is unavailable, and packaged updater rehearsal now proves `handoff-requested.json` plus an in-flight handoff state before treating launcher exit as a valid install transition.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Windows desktop supervision is now substantially harder to escape: launcher-managed `site`, `bridge`, and managed Chromium processes are attached more strictly to the desktop Job Object, stale runtime children can be reclaimed safely on startup, and detached Chromium handoff no longer leaves the launcher waiting for the bridge's two-minute owner-idle fallback after the Baluffo window is already gone.
- Linux CI desktop-app tests no longer fail spuriously on non-Windows runners by assuming Windows-only `src.ship.desktop_app` globals exist at import time; the Windows helper tests now inject their own shimmed surface instead.
- Desktop bridge/update imports on the current public release line no longer fail across source runtime startup, packaged updater handoff, or release-preflight test collection when `src.ship.desktop_app` and `src.ship.desktop_update` are loaded through different packaged surfaces.
- Packaged static-scrapy runners no longer relaunch `Baluffo.exe` as a second top-level desktop instance in frozen mode; packaged fallback execution now dispatches through the child-script path instead.
- Packaged uncapped fetch no longer leaves `scrapy_static_sources` looking frozen as an opaque final work item while the browser-fallback queue is still advancing.
- Desktop updater status no longer regresses handoff/install-ready state back to `ready` merely because the downloaded ZIP still exists while the updater is already in handoff/install states.
- Desktop update install start now refuses to report success unless durable launcher handoff is confirmed against the live launcher session, so first-click install attempts no longer silently no-op or snap back to `Install and restart` when handoff confirmation fails.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.

## [0.1.32] - 2026-04-19

### Changed
- Desktop update and release-note dialogs now use the newer polished popup presentation layer, and the Saved page received additional UI polish around the activity/workspace flow and local-profile modal presentation.
- Frontend styles now ship as split shared/page-scoped assets under `styles/` (`base.css`, `components.css`, `jobs.css`, `saved.css`, `admin.css`), and release/runtime packaging was updated to include that new asset layout.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Desktop update handoff and recovery no longer get stuck in a stale relaunch state after an install-ready update or updater transition.
- Packaged GitHub HTTPS traffic now shares the same trust fallback across source sync and desktop update flows, including the updater helper, and the preferred PEM override is `BALUFFO_GITHUB_CA_BUNDLE` with sync-only and update-only compatibility envs still supported.
- Packaged source sync no longer bypasses the shared GitHub TLS context on the normal runtime `urlopen` path, so the portable desktop now applies the same certificate trust fallback in real sync requests that desktop update already used.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.
- Jobs-page shared action styling was restored after the stylesheet split, including the `Refresh Jobs` / `Run Discovery + Fetch + Sync` buttons and the bottom `Admin Online` status pill.
- Jobs-page pagination spacing was corrected so the pager no longer sits flush against the end of the jobs table.

## [0.1.31] - 2026-04-19

### Changed
- Desktop release version ordering now follows Baluffo's `0.1.x` scheme across the updater, recovery manager, and release tooling, and `0.1.31` is the compatibility bridge that outranks both legacy semver releases like `0.1.23` and current Baluffo-ordered releases like `0.1.3` and `0.1.29`.
- `v0.1.31` is the first public release intentionally chosen to satisfy both the old semver updater population and the newer Baluffo-specific updater ordering.
- Desktop startup probing still uses the more isolated policy and telemetry path introduced on this release line, and the compatibility bridge keeps that runtime behavior as the current shipped desktop.
- Packaged desktop smoke and CI release gates continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- The packaged desktop now reports its intended `0.1.31` app version, and mixed-client update populations can converge on the same release without contradictory `Current` / `Latest` states.
- Startup metrics continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness on the current release line.

## [0.1.3] - 2026-04-19

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.
- Packaged desktop startup probing, crash coverage, and updater finalize/retry behavior were hardened so release-preflight and smoke lanes stay aligned with the shipped runtime.
- Desktop first-use flow now explains guest-mode persistence, lists existing local desktop profiles before sign-in, shows the installed app version in page chrome, and reframes the initial Admin no-fetch state as guidance instead of an unexpected error.
- Release-notes and desktop update UI wording were tightened around finalize/retry and startup resilience.
- Static listing/detail completeness caps were removed so the fetcher can keep pursuing valid zero-yield and residual detail paths instead of cutting them off early.
- Static traversal now prioritizes recall again without giving up the async transport, capped Playwright, and packaged-runtime throughput improvements that stabilized cold fetches.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.
- Desktop startup/update resilience regressions around launch handoff, stale launch retry paths, and packaged crash recovery were removed, including cleanup of the unused desktop launch retry helper.
- Desktop sign-in no longer falls back silently to blind profile-name entry when profile listing fails; it now requires explicit `Retry`, `Create new profile`, or `Cancel`.
- The first-run `fetch_never_run` Admin guidance can no longer be dismissed away before a successful fetch clears the condition.
- Packaged cold fetch validation stayed in the fast runtime class while slightly improving final merged output after the static completeness rollback.

## [0.1.23] - 2026-04-17

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.

## [0.1.22] - 2026-04-16

### Changed
- The desktop Jobs-page updater now surfaces persisted background download failures directly in the update panel instead of falling back to the generic available-update state.
- Release and troubleshooting documentation now describe the explicit failed-download retry path for the portable desktop updater.

### Fixed
- Desktop update downloads that fail in the background now keep the panel open, show the persisted updater error, and offer a direct `Try download again` action.
- Failed portable ZIP downloads now clear stale install-ready state and best-effort delete bad staged artifacts so retry starts from a clean updater state.

## [0.1.21] - 2026-04-16

### Fixed
- Jobs-page desktop job links now open in the default browser again instead of failing when the bridge request path duplicated the local bridge base URL.

## [0.1.2] - 2026-04-15

### Fixed
- Desktop navigation to Admin and Saved no longer prompts to save and closes the app window; the packaged desktop pages now retain the Baluffo window identity token during in-app page switches.

## [0.1.1] - 2026-04-15

### Added
- Desktop in-app update flow in the Jobs desktop UI, backed by a signed GitHub release-manifest pipeline for portable releases.
- Packaged updater rehearsal coverage and release diagnostics for the helper-driven `N -> N+1` install path.
- Shared city-noise and country-acceptance contracts, plus regression coverage for exact junk tokens, country promotion, and backend/frontend location parity.
- Jobs-page pipeline progress reporting, terminal-success packaged smoke coverage, and backend regression coverage for the worker path and bridge wiring.

### Changed
- City parsing now normalizes multi-location strings, dedupes bilingual variants, and rebuilds location summaries from the surviving normalized locations.
- Country-like city values such as `EU & NA` and `UK` are now promoted into the country field instead of being dropped, while valid cities remain untouched.
- Location normalization was consolidated into the canonical parsers path and mirrored in the frontend jobs domain so backend and UI stay aligned.
- Packaged smoke and release flows now distinguish direct `dist\baluffo-portable\Baluffo.exe` artifacts from orchestrator-owned `_out\latest\build\portable\Baluffo.exe` outputs.
- Desktop updater install handoff, helper progress tracking, and packaged recovery behavior were hardened so portable releases update more reliably.
- Release tooling and packaged verification docs now reflect the current desktop build, smoke, and update pipeline.

### Fixed
- Exact city garbage, prose bleed, and chrome-like location fragments are now rejected consistently across the audit, canonicalization, and frontend normalization paths.
- The Sega M Electrical Products row no longer gets forced into the `Game` sector classification.
- Country picker dropdown now closes reliably when clicking outside it or pressing `Escape`, matching the shared popup behavior in the Jobs page.
- Source sync can now be pointed at a custom PEM CA bundle via `BALUFFO_SYNC_CA_BUNDLE` for machines with a nonstandard trust store or TLS-inspecting proxy.
- Jobs-page pipeline runs no longer fail at runtime with `'PipelineService' object has no attribute '_load_json_object'`.
- The packaged Jobs-page pipeline smoke now fails on backend worker errors after startup instead of passing once the button briefly enters a busy state.
- Packaged desktop update checks now resolve the correct release repo, avoid relaunch loops, and handle cross-platform release paths correctly.
- Closing the packaged desktop window now tears down the desktop session cleanly instead of leaving stray `Baluffo.exe` processes behind.
- Pre-submit parity and CI gate regressions that blocked the packaged release flow were corrected for the `0.1.1` release line.

## [0.1.0] - 2026-04-10

### Added
- Dedicated Jobs-page packaged smoke lane that proves the pipeline can be launched from Jobs without opening Admin.
- Changelog-backed release-note extraction for tagged releases.
- Shared dead-listing gate for static and generic careers extraction so regular pages reject as `dead_listing_page`
- Provenance-based game-sector normalization instead of a raw source-sector override
- Admin restore hooks for fetch and discovery progress after navigating away and back
- Better public-link rewriting for provider rows that exposed raw API URLs
- Transition-aware source registry sync with per-source merge, schema v2 snapshots, and local tombstone-backed deletes
- Explicit registry restore-deleted flow for locally removed sources

### Changed
- Discovery auto-approval now uses explicit eligibility rules and keeps `weakSignal` as diagnostics only.
- GitHub release notes are generated from the top versioned section of `docs/CHANGELOG.md`.
- Ship-bundle release builds use the canonical `python` entrypoint instead of `py -3.13`.
- Discovery preset semantics swapped in place: `default` now uses the former uncapped-lite behavior, and `uncapped` is the broader exploration preset
- Static plugin fallback metadata is now centralized in a shared helper to reduce duplicated boilerplate across host adapters
- Jobs UI link handling normalizes RemoteOK detail URLs to the safer listing page
- City and country filter normalization was tightened to reject obvious non-location contamination
- k-ID no longer needs a source-specific suppressor plugin; the shared dead-listing gate now handles it
- Source sync now pushes only active and pending rows; rejected stays local and tombstones are never serialized remotely
- Archived [`docs/scraping-pipeline-run-notes.md`](scraping-pipeline-run-notes.md) — Historical run notes from 2026-03-17 (outdated)

### Fixed
- Legacy sync merge comparison no longer prefers stale remote rows when transition metadata is missing on the local side.
- SmartRecruiters API links now rewrite to the public posting URL
- Game-company rows now stay classified as `Game` when provenance or company evidence supports it
- Misclassified regular pages such as About / Contact / Careers landing pages no longer become synthetic job entries
- Static extraction now stops leaking a few repeated metadata payload shapes through copy-pasted per-plugin dict construction

## [0.0.15] - 2026-03-30

### Added
- Full Milestone 1-6 roadmap delivery (health scoring, taxonomy, discovery promotion, static adapter hardening)
- Enhanced static adapter with generic fallback heuristics and location fixes
- Provenance-based game classification
- Discovery promotion pipeline with structured migration
- Browser fallback circuit breaker
- Admin bridge refactoring with improved task lifecycle and busy-state handling
- M4-M6 social experiment reporting
- Complete lint infrastructure (Python + JavaScript/ESLint + pre-commit)
- Fetch artifacts refresh and audit tooling

### Changed
- Various bug fixes and code quality improvements

### Fixed
- Multiple bug fixes from M1-M6 delivery

---

## [0.0.10] — 2026-03-23

### Added
- Release 0.0.10 with sync, pipeline, and discovery fixes

### Notes
- The public app release line is `v0.0.x`.
- Git tags follow `v<app_version>` and, for this historical release entry, the tagged release was `v0.0.10`.

---

## Legacy notes

The notes below were retained from the earlier draft release history and are now treated as historical implementation notes, not separate shipped release lines.

### Admin bridge and runtime rewrite
- Admin bridge extracted to modular services (`src/bridge/`)
- Source check API with Playwright fallback for static sources
- Task history and run history API
- Ops health and alerts system
- Jobs pipeline refactored with separate loader selection and runtime phases
- Static adapter now dispatches to plugins via `AdapterPluginContext`
- Frontend state-hub for cross-module state management
- Browser queue URL collapse by source ID
- Activision canonical listing URL resolution

### Shipping and discovery foundation
- GitHub App-based source sync for multi-PC workflows
- Source discovery package (`src/source_discovery/`) reorganized
- Static adapter plugin system for studio-specific parsing

### Browser-required and initial release work
- Playwright fallback for static source discovery and scraping
- Scrapy-Playwright integration for browser-required sources
- Admin discovery log live tailing
- 403/timeout handling in discovery probe
- Generic static source classification
- Initial release: job aggregation from Google Sheets, Remote OK, provider APIs (Greenhouse, Lever, etc.)
- Static studio page scraping
- Source discovery with web search and probing
- Admin console for source management
- Saved jobs with notes and attachments
- Local-first storage (IndexedDB + file-based)

## Known Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| Some static sources still return 0 jobs | Open | Use browser fallback queue |
| Social sources may miss recent posts | Open | Adjust lookback window |

---

## Version History

- [0.0.10] — 2026-03-23
- [0.0.9] — 2026-03-23
- [0.0.8] — 2026-03-20
- [0.0.7] — 2026-03-20

For older shipped tags, see `v0.0.1` through `v0.0.6`.

*For older releases, see the older versioned sections in this changelog.*
