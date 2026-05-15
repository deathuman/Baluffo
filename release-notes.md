## [0.2.0] - 2026-05-15

### Added
- A more polished desktop Jobs experience, with denser job rows, clearer save/open actions, user-facing update controls, and quick-filter presets for common browsing flows.
- A safer Saved Jobs workflow, including contextual phase overrides, clearer remove/undo behavior, and an activity timeline that opens with useful defaults.
- A stronger Admin operations view with clearer run history, selected-run analysis, pipeline diagnostics, warning explanations, and advanced bulk actions kept behind an explicit disclosure.
- Runtime SQLite/WAL storage for task history, sync runs, source runs, jobs feed exports, and source registry rows, while keeping compatibility exports available for existing flows.
- Source-sync v3 with content-addressed shard bundles, changed-shard uploads, pull no-op detection, push progress, bounded cleanup, and stronger validation.
- New source-policy, provider/static, registry-conflict, and dedup review tools that make risky source changes easier to inspect before applying.
- Performance, release-safety, and repo-safety tooling, including startup probes, benchmark reporting, packaged desktop rehearsals, secret scanning, dependency audit wiring, and bridge route inventory checks.

### Changed
- Jobs discovery, fetching, sync, and lifecycle internals were split into smaller, more testable modules without changing the normal user workflow.
- Packaged desktop builds now include the storage/runtime pieces needed for the newer local storage and sync paths.
- Admin startup and heavy review panels now defer more expensive work, improving first-load behavior while preserving access to detailed diagnostics.
- Documentation was reorganized around the active docs index, release guide, storage/sync contracts, source-policy runbook, testing guide, and AI/tooling guardrails.

### Fixed
- Desktop startup, bridge ownership, browser shutdown, updater handoff, and packaged startup readiness are more reliable across Windows desktop sessions.
- Pipeline and fetch lifecycle tracking now uses stronger task authority and better evidence, so Admin progress and diagnostics avoid stale or placeholder state.
- Source-sync writes, retries, snapshot limits, checkpoint tagging, and source-health parity were hardened.
- Source registry conflicts, provider/static overlap, dedup review pressure, Google Sheets role buckets, and static-source conflict handling now produce clearer review evidence.
- Saved Jobs back navigation, activity timeline close behavior, phase override flow, remove action, and scrollbar styling were polished.
- Admin operations rows, completed-run ordering, pipeline summaries, and diagnostics copy now render more consistently.

### Security
- Added gitleaks-based secret scanning and Python dependency audit coverage to the local and release-safety workflow.
- Updated dependency and packaging guardrails used by the desktop release path.
