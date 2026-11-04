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

It carries **the most recent 5 released versions plus `[Unreleased]`**, and nothing
older. `v0.3.002` was the last published release, so the sections below it are the
working record of what is queued for the next one.

Older history is **not lost — `git log` is the source of truth** for it. Clamping
this file exists because a 153-section changelog had become a version ledger nobody
read, and because keeping it current meant restating past entries every time the
version moved. A change is described once, in the release that actually shipped it.

Four rules the guardrail enforces so this cannot quietly regress:

- **At most 5 versioned sections**, plus `[Unreleased]`.
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

## [0.3.036] - 2026-11-04

### Fixed

- Source Policy review actions and the panel refresh no longer time out on a large registry.
  Review-only actions skip loading the full registry, and the conflict list is reused while the
  underlying registry and source data have not changed.

### Notes

- Distribution behavior is unchanged from 0.3.035: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.

## [0.3.035] - 2026-11-04

### Fixed

- Registry Conflicts no longer times out when its saved list is out of date. The panel shows the
  last saved list, or a short "building" message when there is none, while a fresh copy is
  assembled in the background, then updates itself.
- Closing the desktop window while a new page is still loading no longer shuts the app down
  mid-navigation; the pending close is cancelled when the replacement page boots.
- The Admin action center no longer raises its attention chip for a source probe that used its
  full time budget but settled, and reports page failures as outcome counts instead.

### Notes

- Distribution behavior is unchanged from 0.3.034: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility all work as before.

## [0.3.034] - 2026-11-04

### Fixed

- The Jobs page now opens the full feed directly and shows a loading state while it loads. The
  last saved startup snapshot is used only if the full feed cannot be loaded, so a new session
  no longer briefly shows a partial list.
- The Admin action center now explains failed-source counts by separating sources that kept zero
  jobs from sources that need browser fallback.
- Source sync now retries GitHub rate-limit and server errors instead of failing the push on the
  first transient response.
- Workable board detection no longer mistakes a site's machine-readable help file for its account
  name.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.033:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.033] - 2026-11-03

> This is a desktop rollup: it combines the changes that shipped to the container
> channel as 0.3.030, 0.3.031, and 0.3.032 with the performance work below, so a
> desktop install upgrading from 0.3.028 receives the whole set in one step.

### Added

- Umbrel users can again install and update Baluffo through Deathuman's private
  community app store. The store entry uses the same container image as other
  self-hosted deployments.

### Fixed

- The Admin page no longer stalls on its first call after a slow moment. Its startup request
  was cut off after 0.75 seconds while a healthy answer can take about a second, so the page
  fell back to "data delayed; retrying" on every navigation; it now waits up to three seconds,
  roughly triple the slowest answer measured on the box.
- The Ops tab badges no longer spend minutes recalculating while a run is active. The counts are
  read from the last completed scan and refreshed in the background, and a second copy of that
  work cannot start while one is already running; before any scan has finished the badges show
  as computing instead of holding the page.
- The source table reloads instantly after its first open. Building it used to re-normalize every
  registry row and re-derive the conflict groups on each open even when the registry had not
  changed; repeated opens now reuse that result until a registry or source-state file changes,
  and a registry with no removed sources no longer re-fingerprints every row's listing URL just
  to prove none match.
- Ops fetch statistics stay responsive during a run. The parts that only change when a fetch
  finishes are reused for up to 15 seconds, while the active-run status, alerts, and response
  timestamp are still read fresh, so an open Ops tab no longer pays about a second of history
  and registry scanning on every refresh.
- On installs that keep the registry in the app database, summary counts are reused until the
  registry actually changes, instead of rescanning it on every Admin and Ops poll.
- The Admin page no longer flips between "Bridge Reconnecting" and connected while the app is
  working normally. A slow storage-health read was being reported as the bridge going offline,
  which also put the page's connection banner on screen; that read now answers from a short-lived
  cached reading instead of waiting, and a single failed one is no longer treated as a lost
  connection.
- A storage-health request that cannot finish in time is answered with the last good reading,
  marked as possibly out of date for up to five minutes, instead of an error, so the Admin page
  keeps its storage numbers through a slow moment.
- The connection badge returns to its real state when a short reconnect finishes, instead of
  staying on "Bridge Reconnecting" with nothing behind it.
- A run can no longer sit silently for hours while another part of the app holds the job-feed update lock.
  While it waits, the log names the process holding the lock, and the run now ends with a recorded error
  after 20 minutes instead of waiting indefinitely; the limit can be adjusted in the container environment.
- A run that has finished collecting jobs can no longer sit on "Running fetch" for
  hours while it merges the results. If merging stops making progress, the run now
  ends with a recorded error instead of waiting indefinitely, and each phase is
  written to the log as it starts so a slow run can be told apart from a stuck one.
- A source that has stopped making progress is now given up on after 60 seconds of failed
  attempts, instead of holding the run for up to ten minutes. Runs finish sooner in the slow case,
  and no board that was still producing jobs is affected.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.032:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.028] - 2026-10-29

> This is a desktop rollup: it combines the fetcher changes that shipped to the container
> channel as 0.3.026 and 0.3.027 with the discovery work below, so a desktop install
> upgrading from 0.3.025 receives the whole set in one step.

### Changed

- **A fixed 200-board trial runs about 15% faster.** The fetcher now works on 16 sources at a time
  instead of 12. The 200 boards were drawn from the last full run and weighted toward its slow
  sources, so this is a sample rather than a whole fetch: the same boards finished in 181 s and
  183 s at 16 sources in parallel, against 222 s and 207 s at 12, with board failures flat
  (36 against 37 of 200). Going to 24 gave the gain back (221 s), because a handful of boards
  stalled for 100â€“160 s each and doubled the total work.

- **Slow boards are picked up first, so a run's last minutes are not spent on boards that only just
  started.** A fetch used to start boards in report order, which put the slowest ones last: the last
  full run spent its final four and a half minutes on two boards that had only just started, while
  the rest of the fetcher sat idle. Boards are now started in the order they took last time, and
  results are still combined in selection order. A bounded buffer holds results that finish early,
  so a stall deep enough to fill it (at least 256 results behind one board) can combine one result
  early â€” the only case where which copy of a duplicated job is kept could differ.

- **A single-page static board whose page has not changed is no longer parsed at all.** Static boards
  with one listing page that kept jobs and have not been read for three hours now ask the site first:
  the fetcher sends a HEAD request â€” conditional once it knows the page's version tag, plain on the
  first pass to learn it â€” and, when the answer is "not modified", finishes the board without
  parsing it, opening detail pages or trying the browser. Two live boards answered the check with
  304 in 193 ms and 251 ms; full reads of boards in the same session took 2.3 s and 14.2 s. Boards
  with one listing page that kept nothing get the same check once their twelve-hour re-check window
  expires and they have come up empty on two runs in a row, so an empty board is no longer
  re-downloaded only to confirm it is still empty. A board that answered "not modified" then waits
  out its window â€” three hours, or twelve for one that keeps nothing â€” before asking again, instead
  of spending a check on every run. Boards that publish no version tag still take the full read;
  the check costs them one HEAD first â€” and a board or provider feed that stops publishing one drops
  the stored tag rather than asking with a retired value â€” while boards whose job list is drawn by
  the browser always take the full read:
  their page looks the same whether or not the jobs behind it changed. Boards split across several
  listing pages also keep the full read, because one page's answer cannot vouch for the pages behind
  it. Several provider feeds also keep the version tag their own listing response sends, so a feed
  that only publishes one on the full read can ask the conditional question next time instead of
  downloading everything again.

- **An empty board is asked before it is downloaded, even on its first check.** A board with no
  jobs used to be read in full one more time before the fetcher trusted that it still had nothing.
  When the previous read stored the page's version tag, the board now asks "has this changed?" at
  its first check instead: "not modified" is the same evidence a second full read gave, and it
  starts the board's empty window. Boards that publish no tag keep the old behaviour, as do boards
  whose last check failed.

- **Boards split across several listing pages skip unchanged pages too.** A board with more than one
  listing page used to be re-read in full every time, because one page's "not modified" answer could
  not vouch for the pages behind it. Each page's version tag is now remembered separately and the
  fetcher asks every page, skipping the download, parsing and detail pages only when all of them say
  nothing has changed; a page that changed still triggers the full read, and a page that stops
  publishing a tag drops only its own entry. The same change stops a multi-page board from reading
  its later pages over and over inside one run â€” a five-page board used to read fifteen pages, and
  now reads five.

- **A board's version tag is kept from the ordinary read, so its first re-check can already ask
  "has this changed?"** The tag used to be learned only by the first check after a board's window
  expired, which meant every board waited another full window before an unchanged page could be
  skipped. The read that produces the jobs now stores the tag as well â€” one for a single-page
  board, one per page for a board split across several. A page that stops publishing a tag drops
  it, and a page a run could not read keeps the tag it had. Boards whose reads go through a custom
  fetcher still learn their tag at the first check, as before.

- **Discovery stops re-asking boards that already said the page does not exist.** A board whose own
  listing endpoint answered 404 or 410 used to be probed again on every full audit. That answer is
  now kept for seven days beside the audit artifact, keyed by the board's host and tenant, and a
  skipped candidate still produces a rejection row, so the report reads exactly as a live 404 would.
  On a frozen 9,050-candidate population the probe fell **40.8%** (216.5 s to 128.2 s across two
  pairs) and lost no board: 360 kept with the memo, 358-359 without it. `BALUFFO_DISCOVERY_KNOWN_DEAD_DAYS`
  sets the window and `0` turns the memo off.

- **The homepage analysis reads each fetched page for links once instead of repeatedly.** The
  extractors that look for a careers link each walked the same HTML, so a page with no candidates
  was scanned about nine times over. One lazily built index per fetched body now serves the provider
  walk, the explicit-link extractor, the static candidate builder, the no-careers reason and both
  recovery waves. On the captured production population, scans fall from **9.03 to 3.79** body walks
  per readable page and the phase falls from about **87 s to 53 s**, with results identical batch by
  batch and per collection; a live worker pair confirms **87.7/86.9 s to 65.0/62.9 s**.

- **The container channel analyzes one batch's homepages while the next batch is fetched.** The
  analysis runs in a worker process handed the parent's own function and inputs, so a batch's
  result is unchanged; any failure falls back to analyzing that batch in-process and retires the
  transport for the rest of the run. Two counterbalanced full-population pairs took **126.5 s** and
  **94.0 s** off the audit's wall (**748.8 to 622.3 s**, **729.6 to 635.6 s**). The packaged desktop
  app keeps the in-process analysis, because a frozen build does not start the worker.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.025:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
