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

## [0.3.055] - 2026-11-11

### Fixed

- Closing the runtime database now waits for the background post-serve maintenance pass before releasing its SQLite connection. This prevents a shutdown race during old-feed cleanup and keeps the current feed safe.

### Notes

- This release includes the storage-maintenance, storage-panel, and Quick-check changes first published in 0.3.054. The current feed, saved jobs, notes, and compatibility exports are preserved.
- Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.3.054] - 2026-11-11

### Added

- Admin's Runtime Database Storage panel now has a **Run maintenance** button. It starts the same bounded cleanup that normally runs after serving: older saved jobs-feed generations are removed, then the database is compacted when the backlog drains. It keeps the current feed and refuses to start while an active task is running.

### Fixed

- Admin's Quick check now estimates current versus older stored row bytes with a small number of sequential rowid windows instead of dozens of random row lookups. It reads the current jobs count from the feed state and no longer groups the jobs table on a large database, so the check can finish on a 211 GB store.

### Changed

- The runtime database now removes old saved jobs-feed generations during post-serve maintenance, keeping the current generation and deleting at most four older generations per bounded pass. The existing 30-second follow-up drains the backlog, then the database is compacted so the freed pages are returned to the filesystem instead of remaining in the file.
- Admin's Runtime Database Storage panel now uses clear, bordered sections and plain-language labels. The exact **Full storage audit** sits under Advanced, while raw table, generation, byte, method, and warning details remain available in Technical details.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved. Older feed generations are retained only until the post-serve cleanup has established that the current generation is published. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.3.053] - 2026-11-11

> This container rollup includes the changes published as 0.3.049 through
> 0.3.052, so an install upgrading from 0.3.048 receives the storage,
> retention, and Quick check changes in one step.

### Added

- Admin's Runtime Database Storage panel can now run an exact, read-only diagnostic while the database is large. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, and the result can be exported as JSON for an incident record.
- Admin's Quick check counts `job_sources` and `jobs` rows by feed generation without a full `dbstat` scan. It reports current feed counts, page and free-page totals, and the largest generations so a runaway database can be identified quickly.
- Admin's Quick check now includes a bounded estimate of stored row bytes in the current generation versus older feed generations. It samples current rows and table rowids instead of walking the database or grouping `job_sources`; the estimate excludes indexes and is marked partial when its deadline expires.

### Changed

- The runtime database now keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes, so a long-lived install can reclaim space instead of growing forever. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup indefinitely.

### Fixed

- The Admin **Quick check** no longer starts a full `job_sources` generation scan on a large runtime database. It reports the exact current-generation counters from `job_feed_state` and counts jobs by generation under a short deadline; older source-row totals stay in the exact diagnostic. On the live 211 GB database, the previous check spent over 20 minutes grouping `job_sources`.
- The Admin Runtime Database Storage panel now receives table sizes while a large database is being cleaned. Table sampling has a hard deadline and no longer loads oversized source payloads before returning a partial estimate.

### Notes

- Existing jobs, saved jobs, notes, and the exact diagnostic are unchanged. Compatibility is unchanged for the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

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
