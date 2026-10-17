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

## [0.3.020] - 2026-10-17
### Added

- **Openings that come from Dover and Studio Gloam are collected directly.** Dover has its own provider
  adapter instead of being read through a guessed careers page, and Studio Gloam's openings are listed
  directly, so boards on either no longer depend on discovery finding them first.
- **Saving a job, or starting to track one, now says so immediately.** The control shows the action as
  pending until the answer comes back, instead of appearing to do nothing while the request is in flight.

### Changed

- **A provider response can no longer retire a studio's static board list unless it actually delivered
  openings.** An empty or failed answer used to be enough to drop the fallback list, which could hide
  jobs that were still live on the static board.
- **Saved-job state is fenced by the session that asked for it.** A request still in flight when a
  profile changed could land in the wrong account's list. Attachments, availability summaries, tracking
  reverts, lifecycle overlays, feedback acknowledgements and the custom-jobs refresh are now all bound
  to the session that started them, so a late answer is ignored rather than applied.
- **Admin review actions no longer apply twice.** A control clicked again while the first request was
  open, or a status card whose answer arrived after the view had moved on, is treated as the same
  action instead of a new one.
- **The runtime database reclaims space instead of growing without bound.** Old generations of the
  runtime store are cleaned up and their lifetime is bounded.
- **Job titles read from Teamtailor keep their real text** instead of showing the slug id in front of
  the title.
- **The portable build carries the public-suffix list it needs.** Without it, the build treated some
  studio addresses as a single domain, so a few job links were judged against the wrong site.
- **For contributors:** the Lint workflow now runs for a pull request against any branch, so a stacked
  pull request gets the full gate rather than only the workflow check, and a documentation link into
  the git-ignored run-evidence directory is no longer reported as broken.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.019:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.019] - 2026-10-14
### Added

- **The studio audit now reports what it left running when it finished.** A page that never answered is
  given up on after its own time limit, but the request itself keeps running in the background — and a
  run that ended while one was still out also left its shared connection open for it. Neither was
  visible anywhere except by reading the audit's own large output file, so a run that closed everything
  neatly and a run that walked away from a live request looked the same. The Admin ops view now shows
  it directly: **how many studio-page fetches were abandoned by their own limit, how many were still
  running when the stage finished, and how many connections a run left unclosed**, with the run they
  belong to. The counters are published as their own small file beside the audit, so reading them costs
  a fraction of a second rather than opening a file measured in tens of megabytes, and the same summary
  appears in the audit-artifact diagnostics. A run that never probed anything publishes nothing, and
  the view says so instead of showing zeros that would read as a clean run.

### Changed

- **Discovery no longer re-asks a board that has already said it does not exist.** Games studios are
  looked up on the job-board provider that hosts them, and a provider that has no such board says so
  outright. Discovery used to follow that answer with a second request to the same board's
  human-facing page before giving the candidate up — a page that names the same board, so it can only
  repeat the answer. Measured on the GameDevMap studio audit, which is the app's largest single
  discovery job: **about 36% fewer requests in its probing stage and about 43% less work performed by
  that stage**, with the same candidates failing and the same studios found. How much of that reaches
  the job's clock depends on the size of the job — on a small batch one slow studio can still hold the
  whole probing stage open — so the run gets the same answers for less work rather than a guaranteed
  shorter total.
- **Discovery no longer waits a third time on a board that never answers.** A careers page that hangs
  used to be asked three times before discovery gave up, and every ask waited the full timeout, so one
  unresponsive studio could hold its whole batch for over twenty seconds. It is asked twice now. The
  retry that remains is the one measured to actually rescue a board, boards that answer within two tries
  are untouched, and boards that refuse to answer give up sooner. Measured on the studio audit's own
  list across six paired runs: the worst single board fell from about 22 seconds to about 13, and no
  board's outcome changed because of the shorter ladder.
- **Discovery stops re-guessing a studio that has already answered.** The studio audit looks for a
  careers page under the common addresses, and when it finds nothing it used to try four more
  spellings of the same guess. Those four are now only tried for sites the first set could not read
  at all, or whose only job-ish links came from the homepage itself: a site that answered has already
  shown where it keeps its listings, so asking again in other words is the same question. Measured on
  the audit's own studio list, the four extra questions were **about 72% fewer requests** for that
  step, its time down **about 43%**, and the single studio given up on was an events page that
  produced no jobs either way — no board that had jobs was lost.
- A failed source now reports the address discovery actually asked. Previously the reason named the
  page it tried second, which pointed at a URL the studio never advertised.
- **Discovery now reads studios whose certificates this machine's own trust store rejected.** Every
  page discovery fetches is checked against the certificate authorities shipped with the app as well
  as the ones this machine carries, instead of the machine's list alone. A store that has gone stale
  refuses a studio's perfectly valid certificate with "certificate has expired", and discovery treated
  that as a studio with no careers page — while the browser-facing part of the same job, which already
  carried the shipped list, read the page fine. Measured on the studio audit's own list of 3,000
  studios: **about 19% more studio pages read** (2,326 → 2,763) and its certificate failures fall from
  **506 to 16**, which is **104 more static candidates** and **18 more active boards** on the same
  studios. Nothing is trusted that was not trusted before — the machine's authorities stay in the list,
  the shipped ones are added, and a certificate that genuinely cannot be verified still fails.
- **A concurrency setting is no longer quietly ignored.** Page fetches go through a shared
  bounded-concurrency helper. When the caller supplied its own fetch function — which the app does for
  the studio audit's own measurements, and which the job lane does for the second page of every static
  source — that helper ran those fetches on the interpreter's general-purpose thread pool instead of
  the number of lanes the run had asked for, and that pool is sized from the machine's core count:
  **20 on a 16-core machine**, whatever the setting said. A run configured for 32 lanes therefore
  fetched 20 pages at a time, with no error and no warning — which is why the audit's own phase timings
  disagreed with the numbers the app reported. Each batch now runs on a pool sized to the configured
  lane count, so the setting is the count. Measured on one recorded batch of 1,000 studio homepages at
  32 lanes: peak in flight rose from **20 to 32**, mean from **18.1 to 26.8**, and the batch's wall
  clock fell from **39.2 s to 27.9 s** with the same pages read. A batch that overruns its deadline now
  also returns at the deadline on this path instead of waiting for the slowest fetch to finish.
- **The studio audition no longer answers its own door while it is reading everyone else's.** The stage
  that tries every candidate reads thousands of career pages, and the thread that decides when to ask
  the next one was also the thread that read each answer — the connection, the headers and the body of
  every page. Its own bookkeeping therefore waited behind its downloads, which occupied that thread for
  **42.6% of the stage's total time**. Reading a page now happens on the stage's own worker pool, a
  page per lane, so the deciding thread stays free: its share of the stage fell to **about 10%**, the
  delay before it schedules the next candidate fell from ~3.7 ms to ~1 ms, and total processor work per
  candidate fell by **about a quarter**. The auditions themselves are unchanged — the same candidates
  pass and fail for the same reasons. The stage's clock improved in some runs and not in others, so
  read this as less waiting and less work rather than a guaranteed shorter audition.
- **The studio audition stopped spending its parallel requests on its own waiting line.** The stage
  that tries every candidate divides its parallel requests into a shared total and per-kind limits, and
  a candidate whose kind was already at its limit used to hold a place in the shared total while it
  waited for its own. On the audited population that meant **8.7 of the 40 places** held by candidates
  doing nothing (7.8 on average across the runs), while the kind that is four fifths of the work ran at
  **21 of the 40 requests it was allowed**. A waiting candidate no longer takes a place: the same
  auditions now take **36.8 s against 47.6 s** across three paired runs (**63.9 → 82.6 candidates per
  second**), every pair improving, with each kind's own limit and the shared total left exactly as they
  were and no candidate's outcome changed.

### Notes

- Nothing about what discovery collects changes for the cuts above. The same candidates are probed,
  the same boards are queued, and a candidate still fails for its own reason — several hundred
  candidates in the same audit keep both requests, because for those providers the first answer is not
  the board's own. The trust-store repair is the one change here that **adds** to what is collected,
  and it adds studios that were previously reported as having no careers page.
- Neither audition change raised a limit. The per-kind requests and the shared total are the values
  they were, so no provider sees a higher peak than before.
- The studio audition now holds a worker thread per page in flight, and a page that ignores its
  deadline keeps that thread until the network gives up on it — the same trade the second-page fetch
  already makes, bounded by the stage's own lane count. On the audited population no trickling page
  held a batch past its deadline. The shared connection those threads read through is closed by the
  last of them rather than at the moment the audition moves on, so a page still arriving when the
  stage has given up on it finishes or fails on its own rather than having its connection cut.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.018:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.018] - 2026-10-11
### Fixed

- **A source discovery run can no longer be parked by one slow employer board.** A careers page
  that trickles its response instead of answering kept a probe open well past its timeout — and
  was then counted as a *success* — which held the whole discovery stage until that server chose
  to finish. Measured at 40 seconds against a 2 second timeout on a local trickling server, and
  the same stall ran for minutes inside the GameDevMap audit. Each probe fetch and the probe wave
  now have their own deadline, so such a board is recorded as one failed candidate while the rest
  of the wave still completes, and the job report now says why a probe timed out instead of
  leaving the reason blank.

### Notes

- Nothing about what discovery collects changes. The same boards are probed, the same candidates
  are queued, and an unresponsive board is still reported as that board's own failure — the
  difference is that it can no longer hold the whole stage open while it stalls.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.017:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
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

## [0.3.015] - 2026-10-08
### Fixed

- **Studios hiring on Ashby are collected again.** Ashby builds its board page in the
  browser, so the page you download carries no job links at all, and Baluffo was
  reading those pages. Every Ashby board therefore looked empty: Voodoo's openings -
  122 of them, including the Technical Artist (AI) role that has been missing since the
  studio moved off Lever - never reached the feed, and roughly twenty other Ashby studios
  showed zero openings while hiring. The app now reads the same board data the page
  renders from, so discovery no longer files a live Ashby board as empty and stops
  looking at it.

- **The run's stage timings now tell the truth, so slow stages can be found.** The
  report charged network time to parsing and left about half of the static-board time
  unattributed, which made the most expensive part of a run look cheap. Timings are now
  booked to the stage that does the work, with identical job coverage.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.014:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
