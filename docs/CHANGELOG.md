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

## [0.3.024] - 2026-10-22

### Changed

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

## [0.3.017] - 2026-10-11
### Changed

- **Fetch runs cost substantially less time for the same jobs.** The app's most repeated
  text work — cleaning a title, folding a country name into its short form, keying a city —
  is now remembered instead of derived again for every occurrence, and two hot text steps use
  cheaper spellings that produce identical output. Measured by replaying the same recorded
  responses through one workload, across 12 paired runs: **median engine wall-clock time
  33.2% lower** (95% confidence interval 30.0% to 45.1% lower), with every pair keeping the
  same number of jobs, the same job identities and the same per-board outcomes.

- **A fetch run does far less repeated work, for the same jobs.** Building a job's public record was
  the single largest cost in a run: the app rebuilt that record in full — about 69 times per job —
  almost always just to read one or two fields out of it. Reading now happens without building the
  record, and a full record is built only for a step that actually needs one. Measured by replaying
  the same recorded responses through one workload, four paired runs: **median engine wall-clock
  time 22.8% lower** (95% confidence interval 18.9% to 25.3% lower), with every pair producing the
  same jobs, the same identities and the same per-board outcomes.

- **Job web addresses are no longer re-parsed every time they are mentioned.** Cleaning a job link,
  deriving its duplicate-detection identity and deciding whether the address is publicly reachable
  are each remembered per address, so the same parsing is not repeated for the same job across a
  run: **median engine time 15.1% lower** on the same workload (95% confidence interval 13.9% to
  15.2% lower), again with identical output.

- **The availability and lifecycle pass reads the same way.** Deciding a job's lifecycle state no
  longer builds a full record for every job before it knows whether it needs one, which removes
  about 91,000 record builds from a run and roughly half a second from that stage. It is the
  smallest of the steps above, and its effect on the run as a whole was too small for this method
  to resolve — so it is described as what it is: less wasted work, not a faster run.

### Fixed

- **A run's merged job list no longer depends on which board finished first.** Boards are
  fetched in parallel and now joined in the order they were selected; before, the order they
  happened to finish decided which copy of a duplicated posting survived. Two runs over the
  same responses disagreed by 508 rows (5,541 against 6,029 merges) and neither number was
  reproducible. The merged set is now a property of the run, so a count you read once still
  holds when you fetch the same boards again.

- **One broken link on one board no longer ends the whole run.** A board published a detail
  link that could never be a valid web address, which raised an address error the run did not
  expect and stopped a 2,046-board fetch at board 1,579 — the remaining 467 boards were never
  asked. An unusable address is now recorded as that board's own failure, with a message, and
  the run carries on.

### Added

- **A run report can now say why one board was slow.** With per-board request counters
  switched on (the optional `BALUFFO_FETCH_COUNTERS=1` setting), each board's row in the
  report carries how many requests reached the network, how many repeated an address already
  fetched, the response codes and error kinds it met, and how much text it received. They are
  counted per board and never per request, so a run pays nothing for them beyond the counting
  itself, and a report recorded without them keeps exactly the shape it has today.

### Notes

- **This release covers everything since 0.3.015.** The speed-ups and fixes prepared under the
  unreleased 0.3.016 ship here, under one version, so nobody has to install two of them.
- Nothing about a collected job changes. The same titles, locations, links, identities and counts
  come out of the same responses; what changed is how much work the app does to produce them.
- Every speed-up figure on this page is **engine time only** — each is its own paired experiment on
  one representative workload of replayed responses, so the numbers describe separate steps and do
  not add up. A live run also waits on the network, so the gain there will be smaller, and the
  smallest step's effect is below what that method can resolve.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.015:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
