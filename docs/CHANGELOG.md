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

## [0.3.067] - 2026-11-18

### Fixed

- Admin stops reporting a WAL checkpoint that already succeeded. The storage warning followed the last maintenance record, and only a later maintenance attempt rewrote that record, so a database whose write-ahead log had been emptied by a vacuum, a rebuild or a bulk-delete checkpoint kept showing "WAL checkpoint could not finish" next to a log that no longer existed. The warning now follows the log's live size: it clears once the file is back under the checkpoint threshold, and it stays up when either the size or the threshold is missing, because an unmeasured file is not a fixed one.
- The storage panel quotes the sizes currently on disk instead of the sizes recorded before the last attempt, naming that attempt's own reading only when the two disagree. A resolved checkpoint no longer reads as a gigabyte log beside the database it was folded into.
- A checkpoint that completes outside the scheduled maintenance pass now updates the maintenance record: the bulk-delete and bulk-insert passes, a backup, an install, and a vacuum that leaves the log empty. The panel can no longer show a busy attempt that already finished, and the record carries the age of that attempt, shown as "just now" for anything under a minute instead of rounding to "0m ago".

### Changed

- A browser failure that happens before any page opens now reads as "context creation" instead of being blamed on navigation, so a browser that never reached a page is described as one.
- When Admin cannot capture the browser's own error log, it says so: it names the module it looked for and the installed Playwright version, instead of showing an empty log file that reads like a browser with nothing to report.

### Notes

- This release changes how Admin reports storage maintenance, not what the app serves. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no job feed, saved job, note, or sync behaves differently.

## [0.3.066] - 2026-11-18

### Notes

- Release preflight now runs the JavaScript dead-code scan, so an unused export fails the local preflight instead of passing it and then blocking the `main` or release-tag push.
- This release changes repository tooling only. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged, and no app behavior, job feed, saved job, or note is affected.

## [0.3.065] - 2026-11-17

### Added

- A desktop update that fails its startup check now has a **Repair install** action in the update panel. It restores the retained copy of the previous install or, when no usable copy is left, downloads the signed release again and rebuilds the install from it. Saved jobs, notes, attachments and other local data are kept, and the app reopens when the repair finishes.

### Fixed

- A desktop update that fails its startup check now stops the app it just launched before rolling back, then reads the restored install back from disk. If `ship\app\current.txt` or any file from the previous install is missing, the update reports a failed restore and keeps the rollback snapshot instead of leaving a half-restored folder that still answers some requests and then fails on the next module it loads. The updater also reports a specific restore failure rather than ending quietly in a failed state.
- Automatic database storage reclaim now actually arms itself. 0.3.063 described the self-arming rehearsal, but the pre-flight refused that first automatic measurement, so a host that never set `BALUFFO_RECLAIM_AUTO_ENABLED` stayed unarmed. The rehearsal now runs at startup from the configured data directory, and an adequate measurement starts the first automatic shrink once a run is safe. Setting `BALUFFO_RECLAIM_AUTO_ENABLED=0` still blocks the arming rehearsal and every automatic start, and Rehearse, Run and Stop keep working.
- Closing a completed repair result now keeps the panel closed on later launches. The panel reopened on every start because the dismissal lived only in the page; the dismissal is remembered, a newer repair result still opens, and the update check resumes after a dismissal so the repaired state clears instead of making every later launch look repaired.

### Notes

- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; a failed update keeps the previous install intact and relaunches it.
- Release preflight now runs `npm run test:frontend:packaged:update-path-rehearsal`, which updates an installed copy from a version that reports 0.0.9 and requires the relaunched bridge to answer `/ops/health` with `startupReady: true` over a complete installed tree, not just a reachable process.
- Release preflight now also runs `npm run test:frontend:packaged:update-rollback-rehearsal`, which publishes a target version the build cannot report so the real readiness check fails, then requires the rollback to stop the relaunched app, restore every previous install file, and bring the previous app back with `startupReady: true`.
- Release preflight now runs `npm run test:frontend:packaged:update-repair-rehearsal` and `npm run test:frontend:packaged:update-reextract-rehearsal`, which prove both repair paths against a packaged install: the retained-snapshot restore, and a verified download-and-rebuild that keeps `ship/data`.
- Release preflight now runs `npm run test:frontend:packaged:admin-reclaim-startup`, which starts a packaged install with an unarmed, bloated runtime and no browser, and fails unless the startup rehearsal arms automatic reclaim and a real automatic run begins without a Rehearse click.
- The dependency security gate now accepts the dev-only `knip` → `picomatch` ReDoS advisory chain by package until 2026-12-17, because no patched `picomatch` release exists yet and the chain is reachable only through the deadcode tooling, not the app, desktop package, or container image. Named advisories still need their own accepted id, and the acceptance expires on that date.

## [0.3.064] - 2026-11-17

### Fixed

- Release builds type-check against the Playwright version the shipped lockfile pins. The desktop and Linux packages are built again from a release whose gates pass; nothing else in this release changes behaviour from 0.3.063.

### Notes

- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved.

## [0.3.063] - 2026-11-15

> This is a desktop rollup: it includes the guarded delete that shipped to the container
> channel as 0.3.062, so a desktop install upgrading from 0.3.061 receives it in one step.

### Changed

- Deleting the retained pre-rebuild database now requires explicit confirmation and a fresh database check. The completed rebuild's generation must still match its counts and hashes, or, if a newer feed has superseded and removed it, the current published feed must match its own stored counts and hashes. A mismatch keeps the rollback copy in place, and Admin refreshes the freed-space summary after a successful delete.
- Automatic database storage reclaim now arms itself instead of waiting to be switched on. After upgrading, a host that never set `BALUFFO_RECLAIM_AUTO_ENABLED` runs one rehearsal — a validated copy that is measured and then discarded, never installed — and, when that measurement shows the free-space margin is adequate, automatic shrinking may start on its own once a run is safe: the database reads as bloated, the copy fits in free space, and no task or compaction is running. The Runtime Database Storage panel now says why automatic starts are allowed, and Rehearse, Run and Stop work as before. To opt out, set `BALUFFO_RECLAIM_AUTO_ENABLED=0` and restart the bridge: that blocks the arming rehearsal and every automatic start, while the manual controls keep working.

### Fixed

- Container images now record the commit and build time they were built from, so `/ops/health` reports the identity of the running image instead of the placeholder `unknown`; a local build without a stamp reports empty strings rather than a value nobody can check.
- The one-time delete of a retained pre-rebuild database now runs as a background storage-rebuild job. Admin starts it, can leave the page open, and polls the existing storage-rebuild status until validation and unlink finish; a duplicate start reuses the active run. Validation failure is terminal and keeps the backup. If the bridge restarts after the file was unlinked but before the delete journal was written, the next status read records the completed deletion without touching the live database or any other file.

### Notes

- The background-delete and reconciliation changes keep the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the delete remains explicitly confirmed and feed-validated.
- Self-arming changes only when automatic reclaim may start, never what it does: a run still validates the database before installing anything, keeps the retained copy, and refuses every safety blocker. A host with `BALUFFO_RECLAIM_AUTO_ENABLED=0` behaves exactly as before, and every host can still rehearse or run by hand.
- The guarded delete keeps the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the rollback copy is removed only after the operator confirms deletion.
