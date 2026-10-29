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

## [0.3.028] - 2026-10-29

> This is a desktop rollup: it contains the 0.3.026 and 0.3.027 releases as well as the discovery
> work below, so a desktop install upgrading from 0.3.025 receives the whole set in one step.

### Changed

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

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.027:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.027] - 2026-10-28

### Changed

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
  its later pages over and over inside one run — a five-page board used to read fifteen pages, and
  now reads five.

- **A board's version tag is kept from the ordinary read, so its first re-check can already ask
  "has this changed?"** The tag used to be learned only by the first check after a board's window
  expired, which meant every board waited another full window before an unchanged page could be
  skipped. The read that produces the jobs now stores the tag as well — one for a single-page
  board, one per page for a board split across several. A page that stops publishing a tag drops
  it, and a page a run could not read keeps the tag it had. Boards whose reads go through a custom
  fetcher still learn their tag at the first check, as before.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.026:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.026] - 2026-10-24

### Changed

- **A fixed 200-board trial runs about 15% faster.** The fetcher now works on 16 sources at a time
  instead of 12. The 200 boards were drawn from the last full run and weighted toward its slow
  sources, so this is a sample rather than a whole fetch: the same boards finished in 181 s and
  183 s at 16 sources in parallel, against 222 s and 207 s at 12, with board failures flat
  (36 against 37 of 200). Going to 24 gave the gain back (221 s), because a handful of boards
  stalled for 100–160 s each and doubled the total work.

- **Slow boards are picked up first, so a run's last minutes are not spent on boards that only just
  started.** A fetch used to start boards in report order, which put the slowest ones last: the last
  full run spent its final four and a half minutes on two boards that had only just started, while
  the rest of the fetcher sat idle. Boards are now started in the order they took last time, and
  results are still combined in selection order. A bounded buffer holds results that finish early,
  so a stall deep enough to fill it (at least 256 results behind one board) can combine one result
  early — the only case where which copy of a duplicated job is kept could differ.

- **A single-page static board whose page has not changed is no longer parsed at all.** Static boards
  with one listing page that kept jobs and have not been read for three hours now ask the site first:
  the fetcher sends a HEAD request — conditional once it knows the page's version tag, plain on the
  first pass to learn it — and, when the answer is "not modified", finishes the board without
  parsing it, opening detail pages or trying the browser. Two live boards answered the check with
  304 in 193 ms and 251 ms; full reads of boards in the same session took 2.3 s and 14.2 s. Boards
  with one listing page that kept nothing get the same check once their twelve-hour re-check window
  expires and they have come up empty on two runs in a row, so an empty board is no longer
  re-downloaded only to confirm it is still empty. A board that answered "not modified" then waits
  out its window — three hours, or twelve for one that keeps nothing — before asking again, instead
  of spending a check on every run. Boards that publish no version tag still take the full read;
  the check costs them one HEAD first — and a board or provider feed that stops publishing one drops
  the stored tag rather than asking with a retired value — while boards whose job list is drawn by
  the browser always take the full read:
  their page looks the same whether or not the jobs behind it changed. Boards split across several
  listing pages also keep the full read, because one page's answer cannot vouch for the pages behind
  it. Several provider feeds also keep the version tag their own listing response sends, so a feed
  that only publishes one on the full read can ask the conditional question next time instead of
  downloading everything again.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.025:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.025] - 2026-10-23

### Changed

- **Restarting the app right after quitting keeps it on the same local ports.** Opening the app again
  within about a minute of closing it could land it on a different address than the usual
  `127.0.0.1:8080` and `:8877`, because the startup check counted the previous session's connections —
  still winding down after the window closed — as a port already in use, and quietly moved to a random
  one. The check now treats that brief window the way the app's own servers do, so the usual addresses
  hold. A port another program is genuinely listening on is still reported as busy.
- **The Linux AppImage now updates itself, the way the Windows build already did.** The jobs page's
  update panel used to hide on Linux, because only the Windows build shipped an installer. The
  AppImage now carries its own updater: it downloads the next release, verifies it, replaces the
  file you launched, and reopens it — keeping the old build aside until the new one reports it
  started, and putting it back if it does not. The AppImage has to sit in a folder you can write to;
  when it does not, the panel says so instead of failing halfway through. AppImages released before
  this change still report updates as unavailable, so download the newest one from the release page
  once.
- **Source discovery stops asking a studio the same question four more times.** When a studio
  homepage had no jobs link, discovery guessed `/careers` and `/jobs`, and then — even after those
  answered, or answered that they do not exist — guessed four more spellings on the same site. On a
  full directory scan that second round was most of the page fetches and almost every fetch failure,
  and it produced no studios worth adding. It now runs only for sites that could not be read at all.
  The discovery log also stops calling failed probes "fetched": each recovery round reports how many
  pages it actually read and how many failed.
- **Every failed discovery probe is now listed with its URL and reason.** The directory adapters
  counted hundreds of failed recovery fetches per run and then dropped them before writing the
  report, so the discovery failure view could show the count but never a cause. Each miss now
  appears as its own row — which page was tried, whether it was guessed or linked from the
  homepage, and what the site answered — and the admin's task-failure view groups them as expected
  404s rather than actionable errors.
- **The fetcher metrics panel shows how long a run actually took.** It reported the sum of every
  source's fetch time, capped at 24 hours, so a run that finished in eight hours read as a full
  day. The panel now shows wall-clock time and reports total source time separately.
- **A page the browser had to re-read no longer counts as a lost studio.** When a site answered the
  first page fetch with a block or a challenge, discovery retried it in the browser and often got
  the page. The earlier failure stayed in the report with no sign that it had been rescued, so the
  failure count read as studios discovery never saw. Those rows are now marked as resolved by the
  browser and counted separately, and the admin's task-failure view treats them as expected rather
  than something to fix. The discovery report's own summary carries that rescued total, so the
  admin's report and run detail show it next to the failure count instead of making you open the
  adapter audit that recorded it.
- **Every page now loads as one script, and paints what you are looking at first.** The packaged
  pages used to fetch each of their modules as a separate file, so a launch waited on roughly a
  hundred small requests before the page could settle; they ship bundled now — about 118 fewer
  requests on the jobs page and 169 fewer on the admin page. The admin page lays out its
  below-the-fold panels after the shell has painted, its operations half and the jobs page's update
  panel arrive in their own files once the page is up, and the hosted pages got the same split. Form
  controls finally use the app's font instead of the browser's default, the icons the pages drew as
  characters (the save tick, the fullwidth plus, the discovery disclosure marker) are drawn as paths
  or borders, and each runtime loads only its own local-data client. Nothing moves or looks
  different.

### Fixed

- **On Linux, the app's background servers now stop with the app.** Quitting from a terminal,
  logging out, or stopping the launcher by hand used to leave its local site and admin bridge
  running: the bridge kept answering on its port and the site kept serving the last page until
  their own timers expired, and a crash could leave them behind for good. Both now end when the
  app ends, including when it is killed outright.
- **A process that has already exited is no longer treated as running.** On Linux, a crashed
  launcher that the system had not cleaned up yet counted as alive, so the next start could report
  that Baluffo was already running and stop — with no window left to close. An exited process is
  recognised as gone now.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.023:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.023] - 2026-10-19

### Changed

- **The desktop app starts the site and the bridge together.** The bridge used to be spawned only
  after the site was ready, so its whole startup ran behind the site's. Both now start side by side
  and the window waits for whichever finishes last. `BALUFFO_STARTUP_PARALLEL_BRIDGE=0` restores the
  serial order.

- **A jobs update launch answers before its availability bookkeeping runs.** The manifest rebuild
  used to run inline, where it could queue behind a jobs-data publication; it now runs beside the
  launch. A launch also records how long each of its steps took, and the ops profile reports that
  duration, so a slow start names its slowest step instead of only a total.

### Fixed

- **Update jobs no longer flashes an error while the update is still starting.** A slow launch used to
  turn the button into "Error" and show an error message, then the progress appeared seconds later as
  if nothing had gone wrong. The button now stays on the starting state until the run is confirmed or
  the start genuinely fails, and the launch no longer waits on background bookkeeping that can be held
  up while jobs data is being rewritten.

### Notes

- A packaged launch now journals the jobs success gate on every start, per launch half, records
  which artifact settled it, and reports a crashed gate wrapper in the startup summary. The packaged
  smoke fails a release run when only one half reports, or when a missing summary sidecar forces the
  full report parse. First-run bootstrap promotion writes that sidecar too, so a fresh install no
  longer pays the parse.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.022:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
