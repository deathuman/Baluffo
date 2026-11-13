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

### Added

- Admin maintenance progress now shows saved versions processed against the pass total and, when available, saved-version row totals. Each staged feed records its jobs and source-row counts, and older generations that predate that metadata are counted once during maintenance instead of delaying every pass.

### Changed

- Jobs-generation cleanup now uses a bulk-delete mode with 1,000-row batches, a per-delete-transaction `secure_delete=FAST` override that restores the normal writer policy immediately, one larger WAL auto-checkpoint instead of repeated 4 MiB checkpoints, and a single final truncating checkpoint. On the measured subset it raised cleanup throughput from about 3,900 to 326,000 rows/minute, including the final checkpoint.

### Fixed

- A slow KPI summary read no longer makes the Admin page report the bridge as offline; the bridge badge follows `/app/ready` while panel data refreshes independently.

## [0.3.058] - 2026-11-12

### Added

- Storage health now reports cumulative successful PASSIVE checkpoint wait and hold counters, including count, totals, maxima, and last values, so checkpoint contention can be measured without copying the runtime database. Busy retries are excluded from those timings and reported separately as `walMaintenance.passiveCheckpointBusyRetries`.

### Changed

- Quick check's `rowBytesProbe` now reports `currentComplete` and `totalComplete`; Admin uses those flags instead of inferring completeness from per-table rows.

### Fixed

- Admin's storage cards now keep exact database, allocated, and free-space totals in front of partial table samples, and Quick check no longer reports a current-versus-older share when older rows were not measured.
- Quick check now measures current-generation bytes for every table before attempting whole-table samples and gives each table a fair share of its bounded budget, so a slow older-row sample no longer hides another table's current stats.
- Jobs-generation retention now caps `job_sources` deletions by row count instead of job keys and uses 250-row delete batches, so a multi-source job cannot inflate one cleanup transaction beyond the configured bound.
- Background PASSIVE WAL checkpoints now observe a 60-second cooldown, so a WAL held at the 64 MiB journal-size limit cannot schedule one after every write; a WAL crossing the 256 MiB truncate threshold still schedules TRUNCATE immediately.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved.
- No compatibility changes affect the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.

## [0.3.057] - 2026-11-12

> This container rollup includes the storage, retention, maintenance, and
> write-lock changes published as 0.3.049 through 0.3.056, so an install
> upgrading from 0.3.048 receives the whole set in one step.

### Added

- Admin's Database storage panel now shows the current database write-lock holder and held duration, with a clear "No write lock held" state when the database is idle.
- Admin's Runtime Database Storage panel now shows live maintenance progress: the current cleanup phase, elapsed time, runs processed, and rows or older feed versions removed.
- Admin's Runtime Database Storage panel has a **Run maintenance** button that starts the bounded cleanup normally run after serving. It keeps the current feed and refuses to start while an active task is running.
- Admin's Runtime Database Storage panel can run an exact, read-only diagnostic on a large database. It reports table and index allocation, free pages, WAL overhead, and the rows and bytes held by each saved source-bundle generation, and the result can be exported as JSON.
- Admin's Quick check counts `job_sources` and `jobs` rows by feed generation and includes a bounded estimate of current-versus-older stored row bytes, marking the estimate partial when its deadline expires.

### Changed

- Old saved jobs-feed generations are removed during post-serve maintenance in bounded batches, keeping the current generation and at most four older generations per pass. The cleanup releases the database write lock between batches instead of holding it for the entire deletion.
- The runtime database keeps only the newest 30 fetch runs and removes older terminal source history in bounded, repeating passes. Full source details remain in compressed evidence archives.
- A database compaction that cannot run when first due is retried after the retention backlog drains, so a temporary busy period or low-disk condition does not strand cleanup.
- Admin's Runtime Database Storage panel uses clear, bordered sections and plain-language labels, with the exact **Full storage audit** under Advanced and raw details under Technical details.

### Fixed

- A new cleanup pass clears stale generation counters left by the previous pass.
- Maintenance status reports a pass as running from the moment it starts, including cleanup triggered after serving.
- Closing the runtime database waits for background post-serve maintenance before releasing its SQLite connection, preventing a shutdown race during old-feed cleanup.
- Admin's Quick check no longer groups `job_sources` or starts a full generation scan on a large database. It reports current counters immediately and counts jobs by generation under a short deadline.
- The Runtime Database Storage panel receives table sizes while a large database is being cleaned, with a hard sampling deadline that does not load oversized source payloads.
- Admin's Quick check estimates current versus older stored row bytes with a small number of sequential rowid windows instead of dozens of random row lookups.

### Notes

- The current feed, saved jobs, notes, task history, and compatibility exports are preserved. Older feed generations are retained only until the post-serve cleanup has established that the current generation is published.
- No compatibility changes affect the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, or desktop localhost bridge compatibility.
