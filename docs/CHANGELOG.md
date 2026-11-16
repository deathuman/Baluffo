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

## [0.3.063] - 2026-11-15

### Changed

- Automatic database storage reclaim now arms itself instead of waiting to be switched on. After upgrading, a host that never set `BALUFFO_RECLAIM_AUTO_ENABLED` runs one rehearsal — a validated copy that is measured and then discarded, never installed — and, when that measurement shows the free-space margin is adequate, automatic shrinking may start on its own once a run is safe: the database reads as bloated, the copy fits in free space, and no task or compaction is running. The Runtime Database Storage panel now says why automatic starts are allowed, and Rehearse, Run and Stop work as before. To opt out, set `BALUFFO_RECLAIM_AUTO_ENABLED=0` and restart the bridge: that blocks the arming rehearsal and every automatic start, while the manual controls keep working.

### Fixed

- Container images now record the commit and build time they were built from, so `/ops/health` reports the identity of the running image instead of the placeholder `unknown`; a local build without a stamp reports empty strings rather than a value nobody can check.
- The one-time delete of a retained pre-rebuild database now runs as a background storage-rebuild job. Admin starts it, can leave the page open, and polls the existing storage-rebuild status until validation and unlink finish; a duplicate start reuses the active run. Validation failure is terminal and keeps the backup. If the bridge restarts after the file was unlinked but before the delete journal was written, the next status read records the completed deletion without touching the live database or any other file.

### Notes

- The background-delete and reconciliation changes keep the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the delete remains explicitly confirmed and feed-validated.
- Self-arming changes only when automatic reclaim may start, never what it does: a run still validates the database before installing anything, keeps the retained copy, and refuses every safety blocker. A host with `BALUFFO_RECLAIM_AUTO_ENABLED=0` behaves exactly as before, and every host can still rehearse or run by hand.

## [0.3.062] - 2026-11-15

### Changed

- Deleting the retained pre-rebuild database now requires explicit confirmation and a fresh database check. The completed rebuild's generation must still match its counts and hashes, or, if a newer feed has superseded and removed it, the current published feed must match its own stored counts and hashes. A mismatch keeps the rollback copy in place, and Admin refreshes the freed-space summary after a successful delete.

### Notes

- The guarded delete keeps the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the rollback copy is removed only after the operator confirms deletion.
