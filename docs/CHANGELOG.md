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

## [0.3.051] - 2026-11-11

### Added

- Admin's Runtime Database Storage panel now has a **Quick check** that counts `job_sources` and `jobs` rows by feed generation without a full `dbstat` scan. It reports current feed counts, page and free-page totals, and the largest generations in seconds or minutes, so a runaway runtime database can be identified without waiting for an exact allocation walk.

### Notes

- The Quick check is read-only and adds no migration. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain unchanged.

## [0.3.050] - 2026-11-11

### Added

- Admin can now run an exact, read-only storage diagnostic while the runtime database is large. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, then lets the result be exported as JSON for an incident record.

### Notes

- Existing saved data and database cleanup are unchanged. Compatibility remains intact for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.3.049] - 2026-11-10

### Fixed

- The runtime database now keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes, so a long-lived install can reclaim space instead of growing forever. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup indefinitely.
- The Admin Runtime Database Storage panel now receives table sizes while a large database is being cleaned. Table sampling has a hard deadline and no longer loads oversized source payloads before returning a partial estimate.

### Notes

- Existing jobs, saved jobs, and notes are kept. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

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
