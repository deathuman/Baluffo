# Changelog

> All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- [`docs/AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) — Comprehensive guide for AI coding assistants
- [`docs/INDEX.md`](INDEX.md) — Documentation navigation index
- [`docs/TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — Common issues and solutions

### Changed
- Archived [`docs/archive/scraping-pipeline-run-notes.md`](archive/scraping-pipeline-run-notes.md) — Historical run notes from 2026-03-17 (outdated)

---

## [1.3.0] — 2026-03-22

### Added
- Admin bridge extracted to modular services (`src/bridge/`)
- Source check API with Playwright fallback for static sources
- Task history and run history API
- Ops health and alerts system

### Changed
- Jobs pipeline refactored with separate loader selection and runtime phases
- Static adapter now dispatches to plugins via `AdapterPluginContext`
- Frontend state-hub for cross-module state management

### Fixed
- Browser queue URL collapse by source ID
- Activision canonical listing URL resolution

---

## [1.2.0] — 2026-02-15

### Added
- GitHub App-based source sync for multi-PC workflows
- Desktop portable EXE with PyInstaller
- Ship bundle (zip-first) release channel

### Changed
- Source discovery package (`src/source_discovery/`) reorganized
- Static adapter plugin system for studio-specific parsing

---

## [1.1.0] — 2026-01-10

### Added
- Playwright fallback for static source discovery and scraping
- Scrapy-Playwright integration for browser-required sources
- Admin discovery log live tailing

### Fixed
- 403/timeout handling in discovery probe
- Generic static source classification

---

## [1.0.0] — 2025-12-01

### Added
- Initial release
- Job aggregation from Google Sheets, Remote OK, provider APIs (Greenhouse, Lever, etc.)
- Static studio page scraping
- Source discovery with web search and probing
- Admin console for source management
- Saved jobs with notes and attachments
- Local-first storage (IndexedDB + file-based)

---

## Migration Notes

### Upgrading to 1.3.0
- Bridge routes are now in `src/bridge/routes/`
- Config file `baluffo.config.json` format unchanged
- Source registry format unchanged

### Upgrading to 1.2.0
- Source sync now uses GitHub App instead of PAT
- Run `python scripts/build_sync_app_config.py` to generate config

### Upgrading to 1.1.0
- Playwright is optional but recommended for better source coverage

---

## Known Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| Some static sources still return 0 jobs | Open | Use browser fallback queue |
| Social sources may miss recent posts | Open | Adjust lookback window |

---

## Version History

- [Unreleased] — Current development
- [1.3.0] — 2026-03-22
- [1.2.0] — 2026-02-15
- [1.1.0] — 2026-01-10
- [1.0.0] — 2025-12-01

---

*For older releases, see [docs/archive/](archive/)*
