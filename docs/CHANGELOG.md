# Changelog

> All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and Baluffo desktop releases use the project-specific release ordering documented in
[`RELEASE.md`](RELEASE.md).

Sections below are written for the people who use the app, not for its maintainers:
say what changed for them and what it means, in plain language. The repository
guardrail enforces a shape budget on the **top versioned section only** — at most
2,000 words and no single bullet over 1,200 characters — and prints an advisory,
non-blocking note when implementation detail (code filenames, repo paths, loader
ids, dunder attributes) leaks in.

### This file is a release log, not a history

It carries **the most recent 5 released versions plus `[Unreleased]`**, and nothing older. Sections evicted by that clamp move to [`CHANGELOG-ARCHIVE.md`](CHANGELOG-ARCHIVE.md); the archive stays empty until a later bump evicts the oldest section.

Older history is **not lost — `git log` is the source of truth** for it. Clamping
this file exists because a 153-section changelog had become a version ledger nobody
read, and because keeping it current meant restating past entries every time the
version moved. A change is described once, in the release that actually shipped it.

Five rules the guardrail enforces so this cannot quietly regress:

- **At most 5 versioned sections**, plus `[Unreleased]`.
- **The oldest section is archived, not deleted.** When a bump pushes the file past
  five sections, the evicted section moves to [`CHANGELOG-ARCHIVE.md`](CHANGELOG-ARCHIVE.md)
  instead of disappearing, so the clamp keeps the text and still leaves the file short.
- **No bullet appears in two sections.** A change is claimed by exactly one release,
  so a release can never claim to have fixed something an earlier one already fixed.
- **A versioned section that never shipped may not queue below the last tagged release.**
  One or two versions are allowed to wait untagged above the newest tag -- the in-flight
  version between a bump and its release, and the release that just shipped, because
  Baluffo publishes by version string (the GHCR image tag comes from the app-version
  declaration, not from a git tag), so a real release can exist with no git tag at all. But an older
  section whose version was skipped and then superseded by a release that did get tagged
  describes a release nobody can install, and the notes generated for the next tag would
  leave its changes unread. Roll those changes into the section being released, or delete
  that section: `git log` keeps the history. The cross-channel checkpoint in
  [`RELEASE.md`](RELEASE.md) is the decision this rule refuses to let go unmade.
- **`[Unreleased]` sits above the versioned sections.** In-progress changes accumulate at
  the top of the file, directly above the section a release rolls up, so a reader meets
  them in the order they happened.

---

## [Unreleased]

## [0.3.061] - 2026-11-14

### Added

- Admin can rebuild the runtime database around the current published feed in one action. The rebuild keeps every non-feed table — including saved jobs, notes, tasks, history, and source registry data — and copies only the current `jobs` / `job_sources` generation. It shows live progress, validated job/source counts and hashes, and reclaimed space, and retains the previous database for rollback until you explicitly delete it.
- Admin now has a read-only **Preview cleanup** step. It shows the protected current version, the saved versions that maintenance would remove, bounded job/source row and payload-byte estimates, reusable database pages, and free disk space without deleting anything or scheduling work.
- A bounded feed-cleanup pass can be stopped from Admin at its next safe batch or phase boundary. The stop control is tied to the active pass, so a stale or missing run identity cannot cancel a newer pass. An in-flight compaction is not interruptible and is labelled separately.
- Storage maintenance progress now reports when a capped pass queued a follow-up pass, when a failed pass queued a retry, and whether the current pass is fully complete. Saved-version progress separates versions processed from versions deleted.
- A tracked generation-cleanup profiler builds a disposable, versioned SQLite fixture and attributes delete, commit, WAL-growth, and checkpoint time without reading live data.

### Changed

- Runtime database diagnostics now report compaction size-gate details and per-transaction write timings: callback and DELETE work, commit time including SQLite’s inline checkpoint, WAL size before/after, and explicit checkpoint time. This makes a long maintenance hold diagnosable without ad-hoc profiling.
- Admin’s storage panel now exposes the rebuild and cleanup preview, shows rebuild phases and validation results, disables unsafe actions while a rebuild or compaction is active, and keeps polling only while a live pass is running.
- Fetch finalization now compares a canonical content hash before staging a new feed. If only observation or availability timestamps changed, the existing saved version and JSON projections stay in place instead of publishing a new generation.

### Fixed

- A rebuild now fails closed when free disk space cannot be determined instead of assuming the copy will fit. A busy WAL checkpoint during installation is retried, then reported as a failure rather than incorrectly claiming the feed was superseded.
- The canonical unchanged-feed hash now ignores `fetchedAt`, so a fetch that only stamps that field does not create a redundant generation.
- Admin no longer presents compaction as a stoppable maintenance action, and storage maintenance polling no longer continues while active work holds the gate.

### Notes

- The current feed, saved jobs, notes, and compatibility exports are preserved across the rebuild. The old database remains available as a rollback copy and is never deleted automatically.
- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain unchanged; this release does not change desktop packaging or its update manifest.

## [0.3.060] - 2026-11-13

### Fixed

- Desktop ship-bundle packaging now validates the current storage migration sequence, so packages containing the saved-version progress migration build successfully.

### Notes

- Existing feeds, saved jobs, notes, and compatibility exports are preserved.
- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged.
