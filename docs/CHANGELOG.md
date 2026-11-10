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

## [0.3.046] - 2026-11-10

### Changed

- The container image is rebuilt and published under a new version so container and Umbrel installs can update. The app itself behaves the same as the previous release.

### Notes

- No jobs data is migrated or dropped: saved jobs, notes and the current feed stay as they are. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all continue to work.



## [0.3.045] - 2026-11-10

### Changed

- The Admin page now loads its database storage breakdown and its details drawer only when they are used, so the page has much less code to load before the first screen appears. Both still appear in the same place, and the storage table sizes still refresh on the same schedule.

### Fixed

- Portable Windows and Linux AppImage builds pass their startup size limit again after the storage panel was added, so packaged desktop builds can be produced from this release.

### Notes

- Existing jobs, saved jobs and notes are kept. Deployment compatibility is unchanged: the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave as before.

## [0.3.044] - 2026-11-09

### Fixed

- A failed availability update no longer leaves a startup feed file behind when that file did not exist before the attempt. The rollback now restores the same set of feed files as well as the same rows, instead of re-creating every projection.

### Notes

- The same-origin Linux container deployment, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain supported.



## [0.3.043] - 2026-11-09

### Added

- Ops can now inspect a legacy browser-fallback queue before it is read: a new diagnostic reports pages listed more than once, which copy will be kept, and parser-error rows that are not eligible for browser retry. It is read-only and never changes the queue.
- Admin now shows a per-table breakdown of the runtime database, so where a growing store spends its space is visible without opening a shell.

### Changed

- Browser fallback queue entries now say whether fallback was attempted, whether it supplied content, and why it was not attempted. A run-wide cooldown is recorded separately, so one breaker refusal is no longer mistaken for a refusal of every queued source.
- The runtime database now keeps only the newest 30 fetch runs and 30 days of per-source fetch detail, and compacts itself after pruning, so a long-lived install no longer accumulates every fetch report it has ever written. The compaction is bounded by time and free disk space and reports its outcome in storage health.

### Fixed

- Browser fallback queue rows now deduplicate by canonical page across source IDs, and `parse_error` rows outside the documented static-queue classification allowlist are excluded from new writes and legacy queue reads.
- Admin's runtime storage panel now loads once when startup hydration finishes instead of firing a second read, so it no longer stalls the panel when the bridge is still finishing startup work.
- Browser fallback can now reopen during a long fetch. When the cooldown refuses a fallback request, the app checks whether a browser can render again and, if it can, retries that request immediately instead of leaving every later source refused until the 30-minute window ends. The check runs at most once a minute, so a browser that is still broken is not relaunched for every source that asks for fallback.

- Static fetch diagnostics no longer count sources excluded before evaluation as zero-kept
  review entries. A targeted `--only-sources` run had reported every unselected loader as
  blank residue, turning one evaluated source into more than 1,600 phantom reviews.
- Game-role detection now keeps lighting artists, QA/test roles and narrative writer titles
  that were previously dropped, recovering roles from Housemarque, KRAFTON India, 2K Vegas,
  Scape, JustPlay, Beamdog, 2K and Cloud Chamber while still rejecting technical, content and
  marketing writers.
- SmartRecruiters boards that were registered with only a compact source identifier now fetch
  correctly, and their postings link back to the right company.
- Kojima Productions, Raven Software/Activision and Eidos Montreal cross-postings now merge into
  one entry with both source links, instead of showing the same opening twice.
- Distinct Epic, Riot and Scopely postings with different requisitions or work arrangements no
  longer collapse into one entry.
- Epic job links and Greenhouse job-detail links now count as provider identity, so distinct
  provider job IDs cannot merge solely on title and location.

- After a browser health probe confirms Chromium can launch and render, browser fallback reopens for the next run instead of keeping a stale cooldown from one failed page navigation.
- Browser fallback no longer queues static fetch failures a browser cannot repair, including DNS resolution, certificate/TLS validation, and exhausted time or failure budgets. Existing fallback queues are filtered when read, so those rows stop running after updating.
- Ops no longer repeats the app version in the health header; the header shows the running build separately.

### Notes

- Distribution compatibility is unchanged: the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain supported.

## [0.3.040] - 2026-11-07

### Added

- Packaged desktop and container builds now record the commit and build time, and Ops Health and Admin show which build is running so an operator can confirm the deployed artifact without opening a shell.
- Container upgrades can be checked end to end with the new promote command: it restarts the compose stack and waits until Ops Health reports the expected version and commit.

### Fixed

- A fetch that finished reading exactly as its deadline fired no longer reports a timeout; only a request whose body was actually interrupted is treated as failed.
- A timed-out async fetch now cancels the request on its event loop instead of leaving it running after the caller has moved on.

### Changed

- Admin's first paint is now held to a hard 295 KiB budget. New first-paint weight must load on demand; the build fails instead of quietly growing the shell again.

### Notes

- Compatibility is unchanged across the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
