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

## [0.3.069] - 2026-11-25

> This is a rollup release: it gathers everything that reached `main` after
> 0.3.068 — the Admin reporting fixes first planned for this version, the
> storage, discovery and container-browser work that followed, and the desktop
> update-lock fixes.

### Fixed

- The probe-wave line in Admin no longer reads as a current all-clear. It showed a "clean" chip from whatever receipt was on disk, and the live container's came from a wave 16 days old, so a lane nobody had exercised looked verified. The row now prints when that wave finished and warns on a receipt older than a week or one with no finish time; a fresh wave still reads clean.
- A large database now gets the integrity check it used to defer forever. Above 64 MiB the live check was skipped for size, so "healthy" only meant writes were succeeding and nothing had ever read the file back. It is now verified in the background once a day, and the storage panel says when that last happened — or that it never has. A failed check marks the store unhealthy with the database's own error.
- A source whose browser fallback attempt was refused or blocked now records why. The row already said "browser fallback unavailable (cooldown active)" but not what closed the lane, so a run-wide cooldown read as an unexplained refusal; the cause is captured at the moment of the refusal and kept per attempt, so neither a later recovery nor a second failure in the same run rewrites it. A source that only recommended fallback, or whose page rendered with no jobs, is not stamped.
- Admin's fetch duration reports the middle of recent runs instead of their average. One long run, or a group of quick aborts, could move an average far from how long a run usually takes — the live container read about 50 minutes while its runs took around 2.5 hours. The card headlines the median, with the 90th percentile and the number of runs behind it underneath, so the figure can be checked rather than trusted.
- Admin's 7-day fetch success figure no longer turns an empty week into a total failure. With no run in the window it showed 0.0%, the same reading as a week where every run failed; the card now says the week was not measured and names the empty window, while a real 0.0% still means runs happened and all of them failed.
- Storage maintenance, a current-feed rebuild and a reclaim copy now appear as their own rows in Admin's Current Runs, with the state and age of the work instead of running invisibly. Starting a fetch, discovery, sync or pipeline while a rebuild or reclaim copy owns the database is refused, naming the running copy, and a second rebuild, compaction or retention pass is held or refused while a copy is being written, so nothing deletes rows out from under it. A reclaim copy also outlives the page that started it.
- Stale task rows are now closed by the lifecycle reaper. A row whose owning process is gone no longer sits active forever; it is finished through the same transitions a normal completion uses, so Admin stops showing work that nothing is running.
- Discovery results now reach the source registry through the registry's own merge instead of a direct pending-file write. A run that fails after discovery no longer loses the pending rows it found: they are restaged through the same merge, a board an admin deleted during the run cannot be resurrected, and a run can fill blank display fields without overwriting a fresher value another process owns. Conflicts and dropped rows are recorded in the run report.
- A registry merge that cannot get the lock is no longer dropped. The report waits in a retry queue drained on the next bridge start and when a discovery run finishes; the queue is capped at 25 entries and 2 MiB uncompressed, gzip-backed, with age and attempt limits and a marker so a crash mid-merge charges only the entry it was working on.
- Discovery counters in Admin are now stamped with when the shown values last moved, not when the run last touched them. A running pass that finds nothing new no longer makes unchanged counts look freshly updated, and a quiet stretch no longer makes them look stalled.
- Desktop update state writes are serialized across processes. A stale download-progress update can no longer overwrite a newer install status, so a completed install cannot fall back to "downloading".
- The desktop update, source registry and job-feed reconciliation locks now keep their lock file empty until the lock itself is taken. Writing a placeholder byte before locking put it inside the range the current holder had locked, so on Windows a waiting writer could crash with a permission error instead of waiting.
- The storage panel keeps its last complete table-size reading when a new one times out, reconciles capped table sizes so the rows still add up, and drops entries for tables that belonged to a database file that has since been replaced. Admin's health summary is also invalidated when the database is rewritten, so it cannot report health from before a rebuild.
- Container Chromium launches no longer trade one crash for another. Forcing the GPU path in-process replaced a GPU-helper crash with a browser crash on the first page, so the zygote is now bypassed only on root or container Linux; desktop Linux keeps its zygote and the sandbox it carries.

### Changed

- Admin's dashboard summary now reads storage and browser health directly, and ops diagnostics can probe the browser lane on demand instead of waiting for a run to exercise it.
- The Action Center names the recommended, attempted and failed browser-fallback source counts separately, and splits failures into environment-blocked and other causes, so "failed" no longer carries two different scopes.

### Notes

- This release is a rollup: Admin reporting and storage maintenance, the discovery registry merge and its retry queue, discovery counter stamps, the container browser launch fix, and the desktop update, registry and reconciliation locks. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are. No feed, saved job, note, or synced row changes format.

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
