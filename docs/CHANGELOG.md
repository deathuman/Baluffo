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

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.024:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.024] - 2026-10-22

### Changed

- **Job rows paint ~50 ms sooner on a slow machine because the save button's plus sign stopped pulling in a
  second typeface.** The button used a fullwidth `＋`, which the UI font does not have, so every row rendered
  that glyph from a CJK fallback font. It is now a plain `+`. Nothing looks different; the rows' first painted
  frame moves from 592 → 552 ms at quarter speed.
- **Every remaining icon the pages drew as a character now draws as a path or a border.** Audited against the
  font files themselves: Segoe UI has no glyph for `✓`, so the jobs save button's saved state and the saved
  page's completed phase step were painted from Segoe UI Symbol, and the admin discovery-log's `▸`/`▾` marker
  came from Cambria Math — a serif math face shaping a chevron in a sans-serif panel. The tick is now the
  shared inline SVG path the action center already used, and the disclosure marker is a drawn triangle. Nothing
  looks different; two fewer typefaces are resolved to paint the UI.
- **The admin page shows its header before it lays out panels you have not scrolled to.** The discovery and
  source-sync sections start below the fold but were laid out on the first frame; they now skip layout until
  the shell has painted, so first paint lands 128 ms sooner on a quarter-speed machine (306 → 178 ms) and the
  first layout is 82 ms cheaper. Once the page is up nothing moves: those sections, their controls and their
  data appear exactly where they were.
- **Buttons, dropdowns and search boxes now use the app's font on all three pages.** Form controls
  never inherited it, so they rendered in the browser's default Arial (and monospace on the custom-job
  notes field) while the text around them used Segoe UI. They now match the rest of the page, which
  also makes every page's first paint cheaper — the browser no longer prepares a second typeface for
  the controls. Controls sit a few pixels taller: the jobs page grows by four pixels, the admin content
  panel by nine, the Add Custom Job panel by sixteen, and buttons with long labels get up to eleven
  pixels narrower.
- **No page downloads the storage or window handling it cannot use.** Hosted pages keep saved jobs,
  attachments, backups and sign-in on the server, and the packaged desktop app keeps them in its
  local bridge — neither ever used the browser's offline database, yet every page shipped it, and
  the hosted pages also shipped the desktop window's liveness and close handling. Each runtime now
  loads its own local-data client: the hosted jobs page's script is 245 KB instead of 295 KB, saved
  212 KB instead of 268 KB, admin 292 KB instead of 347 KB; the packaged pages are 46 to 52 KB
  lighter per page (jobs 236 KB instead of 284, saved 216 KB instead of 268, admin 296 KB instead of
  348). Saved jobs, backups, attachments, sign-in, the admin panels and the desktop app's window
  behaviour all work as before. Hosted pages appear at the same moment on a fast connection and
  save about a third of a second on a very slow one; packaged pages finish loading 15 to 25 ms
  sooner on a machine running at a quarter of this one's CPU speed.

- **The packaged pages stop carrying each other's UI handles, and one copy of the sign-in dialog
  instead of four.** Every page shipped the jobs, saved and admin handle tables plus the whole
  dialog module, whether or not it used them; each page now takes only its own handles, and the
  dialog's four near-identical builders share one frame. Packaged scripts are 7 to 10 KB smaller
  per page (jobs 236 KB, saved 216 KB, admin 296 KB). Every dialog and handle behaves exactly as
  before.

- **Every desktop page now loads as one script instead of dozens.** A packaged launch fetched each
  page module as its own file before the page could settle, and it did that on all three pages. The
  ship build now bundles admin, jobs and saved the same way the hosted version already was. Against
  the packaged site's own file server, alternating nine runs per variant: the jobs feed paints its
  rows in 101 ms instead of 182 ms, the admin page settles in 162 ms instead of 260 ms, and the saved
  page finishes booting in 58 ms instead of 122 ms. At a quarter of the CPU it is 488 vs 686 ms,
  573 vs 730 ms and 284 vs 374 ms — the slower the machine, the more those 118, 169 and 97 requests
  cost. First paint stays within about 10 ms either way, and the jobs page keeps the data-request
  head start it had before.

- **The admin page no longer waits for operations health before it settles.** Its bundled script is
  split: the page shell (stored profiles, action center, bridge badge) loads first, and the
  operations-health tabs arrive in a second file as soon as the admin data has been read. The shell
  script is 347 KB instead of 607 KB, and the ops half (273 KB) is fetched only after the page's own
  data settles. On a machine running at a quarter of this one's CPU speed, the admin page settles
  534 ms after launch instead of 596 ms; at full speed the difference is inside the run-to-run noise.
  The operations tabs render the same content either way — checked against a live bridge on both
  builds — and a page whose data never settles still loads them a moment later.

- **The jobs page shows its list before it wires up the controls.** The saved snapshot used to wait
  behind the filter presets, every click handler and the sign-in handshake. Those now start right
  after the first jobs appear: the list comes up about 7 ms sooner at full speed and about 38 ms
  sooner on a machine running at a quarter of this one's CPU speed, and the filter presets are ready
  roughly 20 ms after the rows. Filter values, saved jobs and the sign-in state settle exactly as
  before, and a page whose list never appears still wires its controls a moment later.

- **The jobs page no longer carries the update panel and run controls while it draws its list.**
  Those two parts now arrive in their own files once the list is on screen: 42 KB taken out of the
  page's own script, with the update panel and the job-run controls following it. The list comes up
  about 5 ms sooner at full speed and about 10 ms sooner at a quarter of this machine's CPU speed —
  small, because that wait is the saved snapshot and the first paint rather than the script — and the
  two deferred parts behave exactly as before against a live bridge. A page whose list never appears
  still loads the update panel a moment later.

- **The hosted jobs page starts fetching its list sooner.** The hosted page loads as one script, so
  the request for the saved list used to wait until that script had downloaded and run. It now goes
  out as soon as the page is parsed, and the desktop update panel — which can never appear on the
  hosted site — is no longer part of that script (33 KB smaller). Against the hosted server with the
  saved list regenerated for every run, the first rows appeared 701 ms after navigation instead of
  760 ms, and 921 ms instead of 1,150 ms on a machine running at a quarter of this one's CPU speed.
  With a saved list already on disk the gain is smaller (92 ms instead of 106 ms) because there the
  script itself is what the rows wait on. Nothing else about the page changed.

- **The hosted admin page no longer loads its operations half before it can show anything.** On the
  hosted site each page is a single script, so the operations-health tabs used to be parsed before
  the admin page could settle. They are now their own file: the page's shell is 347 KB instead of
  622 KB, and the 273 KB operations half is fetched when that section loads. The operations panels
  render the same content either way, and the files the shell does need are preloaded rather than
  discovered one round trip later. The hosted jobs page deliberately keeps one script: splitting off
  its job-run controls measured even — the first rows arrived 394 ms after navigation on both builds
  over ten alternating runs at a quarter of this machine's CPU speed — because those controls are
  only 5% of that page, so a second request would buy nothing.

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

## [0.3.022] - 2026-10-19

### Changed

- **The desktop app shows its window sooner, and stops making the first screen wait on background
  work.** The bridge begins serving before it finishes the rest of its post-start chain, and the
  heavy startup work — the overdue job-availability refresh, the saved-state pull and the database
  maintenance pass — is now held back until the first page has finished its opening burst of
  requests. A launch with no window still gets that work done, so headless and container runs lose
  nothing. Opening a mature runtime database also no longer adds seconds before syncing starts.

- **A packaged launch does less work before it can answer.** Around a quarter of the modules the
  packaged bridge used to load at startup — the discovery and fetch-adapter code, the jobs-feed and
  lifecycle helpers, the parsers and the sync machinery — are now loaded when a request or a task
  first needs them. Measured on the 2026-10-18 portable build across four runs, the time from
  entrypoint start to the bridge being configured fell from a 240 ms median to 113 ms.

### Fixed

- **A failing metrics read can no longer stall the bridge's request loop.** The accepted-request
  counter is read on every pass of the loop, and a value it could not represent used to raise out
  of the loop instead of degrading to zero.

### Notes

- **Duplicate detection for the Elevato studio boards reads each posting once** instead of
  re-scanning the whole feed for every pair it considers. The jobs it produces, their identities
  and the per-board outcomes are unchanged.
- An alternative browser engine that was never part of a shipped build is retired from the fetch
  fallback path; the Chromium fallback and every user-visible fetch outcome stay as they were.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.021:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.021] - 2026-10-17
> **Desktop rollup.** This is the first desktop package since 0.3.017, so it also carries the
> user-facing work released on the container channel in 0.3.018 through 0.3.020.

### Changed

- **The private Umbrel app-store packaging is retired.** Baluffo still publishes the same-origin
  container image on GHCR, but the private store metadata, the Umbrel Compose deployment files, and
  the release steps that maintained them are gone. A version bump no longer looks for them, and the
  container workflow no longer treats them as a publish trigger.

### Notes

- **First desktop package since 0.3.017.** It also carries the user-facing work from 0.3.018 through
  0.3.020: provider boards are read directly instead of through a guessed careers page, saved-job
  state stays with the session that asked for it, Discovery stops re-asking boards it has already
  ruled out or that never answer, and the runtime database reclaims space instead of growing.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.020:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
