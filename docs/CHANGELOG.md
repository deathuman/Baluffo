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

## [0.3.069] - 2026-11-20

### Fixed

- The probe-wave line in Admin no longer reads as a current all-clear. It showed a "clean" chip from whatever receipt was on disk, which on the live container came from a wave that had run 16 days earlier, so a lane nobody had exercised looked verified. The row now prints when that wave finished and marks a stale or undated receipt as a warning; a fresh wave still reads clean.
- A large database now gets the integrity check it used to defer forever. Above 64 MiB the live check was skipped for size, so "healthy" only meant writes were succeeding and nothing had ever verified the file. The database is now verified in the background once a day, and the storage panel says when that last happened — or that it never has. A failed check marks the store unhealthy and quotes the database's own error.
- A source whose browser fallback attempt was refused or blocked now records why. The row already said "browser fallback unavailable (cooldown active)" but not what closed the lane, so a run-wide cooldown read as an unexplained refusal; the row now carries the browser error that closed it — taken at the moment of the refusal, so a browser that recovers later in the same run does not take the reason back — and a source that only asked for fallback without ever being tried does not claim one. A page that rendered with no jobs in it is not stamped at all: that empty is a finding about the board, and giving it the run's browser error would be the same conflation in a different field. Each refused row now keeps the cause captured for its own attempt, so two different browser failures in one run no longer make the earlier refusals quote the later one; a row whose attempt recorded no cause stays empty instead of borrowing the run's last failure.
- Admin's fetch duration reports the middle of recent runs instead of their average. One long run, or a group of quick aborts, could move an average far from how long a run usually takes — the live container read about 50 minutes while its runs took around 2.5 hours. The card shows the median, with the 90th percentile and the number of runs behind it underneath, so the figure can be checked rather than trusted.
- Admin's 7-day fetch success figure no longer turns an empty week into a total failure. With no fetch run in the window it showed 0.0%, the same reading as a week where every run failed; the card now says the week was not measured and why, while a real 0.0% still means runs happened and all of them failed.

### Notes

- This release changes only what Admin reports: fetch duration is stated as a median, a large database's file is verified in the background, a refused browser fallback keeps the cause that closed the lane, and a probe receipt is dated. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no feed, saved job, note, or synced row changes.

## [0.3.068] - 2026-11-19

> This is a desktop rollup: it includes the changes that reached the container
> channel as 0.3.067, so a desktop install upgrading from 0.3.066 receives them
> in one step.

### Fixed

- Source sync now prunes the shard files a push replaces. The cleanup step reads the repository's manifest history, and that read discarded any response that was a list, so every push reported the retention window as unavailable and deleted nothing; replaced shards accumulated on the remote indefinitely. The history now arrives intact, so superseded shards are removed while the generations a pull may still be reading are kept.
- Admin stops reporting a WAL checkpoint that already succeeded. The storage warning followed the last maintenance record, and only a later maintenance attempt rewrote that record, so a database whose write-ahead log had been emptied by a vacuum, a rebuild or a bulk-delete checkpoint kept showing "WAL checkpoint could not finish" next to a log that no longer existed. The warning now follows the log's live size: it clears once the file is back under the checkpoint threshold, and it stays up when either the size or the threshold is missing, because an unmeasured file is not a fixed one.
- The storage panel quotes the sizes currently on disk instead of the sizes recorded before the last attempt, naming that attempt's own reading only when the two disagree. A resolved checkpoint no longer reads as a gigabyte log beside the database it was folded into.
- A checkpoint that completes outside the scheduled maintenance pass now updates the maintenance record: the bulk-delete and bulk-insert passes, a backup, an install, and a vacuum that leaves the log empty. The panel can no longer show a busy attempt that already finished, and the record carries the age of that attempt, shown as "just now" for anything under a minute instead of rounding to "0m ago".
- The Jobs page can rebuild a missing feed again. After an earlier successful full run, a lost or unreadable local feed left the page showing "Local jobs feed is missing or unreadable" because the repair request was refused as already completed. That request now rebuilds the feed when the local one is missing or unreadable, and still refuses to replace a feed the app can serve.

### Changed

- A browser failure that happens before any page opens now reads as "context creation" instead of being blamed on navigation, so a browser that never reached a page is described as one.
- When Admin cannot capture the browser's own error log, it says so: it names the module it looked for and the installed Playwright version, instead of showing an empty log file that reads like a browser with nothing to report.

### Notes

- Release preflight's packaged sync rehearsal now drives the atomic Git commit path a real push uses — branch head, blobs, tree, commit and ref update — instead of only the older single-file writes, so a rehearsal can no longer pass while the shipped push path goes untested.
- This release changes how source sync cleans up replaced shard files. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no feed, saved job, note, or synced row changes.
- Release preflight now runs `npm run test:frontend:packaged:jobs-feed-recovery`, which seeds a completed pipeline and a successful report with no feed, then requires the real packaged Jobs page to rebuild the feed instead of dead-ending on the missing-feed error.
- This release also changes how Admin reports storage maintenance and how the Jobs page recovers a missing feed. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no saved job, note, or sync behaves differently.

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
