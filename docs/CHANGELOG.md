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

## [0.3.060] - 2026-11-13

### Fixed

- Desktop ship-bundle packaging now validates the current storage migration sequence, so packages containing the saved-version progress migration build successfully.

### Notes

- Existing feeds, saved jobs, notes, and compatibility exports are preserved.
- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged.

## [0.3.059] - 2026-11-13

### Added

- Admin maintenance progress now shows saved versions processed against the pass total and, when available, saved-version row totals. Each staged feed records its jobs and source-row counts, and older generations that predate that metadata are counted once during maintenance instead of delaying every pass.

### Changed

- Jobs-generation cleanup now uses a bulk-delete mode with 1,000-row batches, a per-delete-transaction `secure_delete=FAST` override that restores the normal writer policy immediately, one larger WAL auto-checkpoint instead of repeated 4 MiB checkpoints, and a single final truncating checkpoint. On the measured subset it raised cleanup throughput from about 3,900 to 326,000 rows/minute, including the final checkpoint.

### Fixed

- A slow KPI summary read no longer makes the Admin page report the bridge as offline; the bridge badge follows `/app/ready` while panel data refreshes independently.

### Notes

- The current feed, saved jobs, notes, and compatibility exports remain available after upgrading.
- This release does not change the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
