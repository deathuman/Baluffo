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
- **A versioned section that never shipped may not queue above the last release tag.**
  One version is allowed to wait for its tag -- that is every day between a bump and a
  release -- but an older section whose version was bumped and then skipped describes a
  release nobody can install, and the notes generated for the next tag would leave its
  changes unread. Roll those changes into the section being released, or delete that
  section: `git log` keeps the history. The cross-channel checkpoint in
  [`RELEASE.md`](RELEASE.md) is the decision this rule refuses to let go unmade.
- **`[Unreleased]` sits above the versioned sections.** In-progress changes accumulate at
  the top of the file, directly above the section a release rolls up, so a reader meets
  them in the order they happened.

---

## [Unreleased]

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


## [0.3.014] - 2026-10-07
### Added

- **Boards whose openings exist only inside the page's own data requests are now read.**
  When a careers site is a single-page app, its openings are not in the page you download -
  they arrive in the request the page makes while rendering. The app now reads them, so those
  boards can be collected instead of being counted as empty. Measured on a board whose page
  carries no job links at all: ten openings read, one kept, the rest correctly filtered as
  non-game roles. No per-site code: a board that moves platform keeps working.

- **The fetch report now says why a browser fallback was refused, and which kind of empty a
  render was.** A refused attempt records its reason, and the counters separate "the browser
  could not launch" from "the page rendered with no jobs in it" - two opposite findings that
  shared one number, which is why a run with 716 refusals out of 843 attempts could not be
  read at all. Rows that asked for browser fallback also carry the run's cause, so a board
  recommending fallback no longer reads as a board nobody tried. The environment cause keeps
  its own field: a refusal no longer overwrites the reason the breaker was closed.

### Fixed

- **Boards that recommend a browser now carry the reason their request was refused or failed.**
  A run with 617 browser-fallback attempts refused out of 851 recorded no reason at all, so a
  closed circuit and a missing browser looked identical. The reason is now recorded, kept apart
  from the failure that closed the circuit, and stamped onto the rows that asked for a browser
  - including the ones that only record the request on their per-page details.

- **A studio board's fetch evidence is read from its own entry, not its platform's totals.**
  When an adapter serves many studios under one summary row, every one of them inherited that
  row's totals, so 25 boards registered in recent releases read as "unknown, only the
  platform's result is known". Read from each board's own record: 17 boards were asked and
  genuinely returned nothing, and 8 errored asking for a browser. Both are decidable now.

### Fixed

- **Choosing a region in the Jobs filter no longer hides the region's own country.** The
  Europe filter matched 2,146 fewer UK openings than it should have, because the UK's job
  locations arrive labelled three different ways and only one of them was recognised. All
  three - "GB", "UK" and "England" - are now found by the Europe filter, and by the
  United Kingdom filter where that applies.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.013:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.


## [0.3.013] - 2026-10-06
### Added

- **Boards whose listings live inside the page's own data now work without a browser.**
  When a careers page returns its openings as JSON embedded in the HTML - schema.org job
  data, Next.js page data, or a script array - the app now reads them directly instead of
  concluding the page has nothing. Bungie's careers page is the measured case: its
  postings arrive inside the page payload, and they are read from there now; the host
  rule that sent the page to a full browser render is gone.
- **Twenty-eight Japanese studio boards on the hrmos platform are now watched, with 833
  openings between them.** Capcom, Square Enix, Nexon, GREE, Aiming, Dwango, Lasengle and
  the others each serve their listings as plain pages, and every tenant was fetched for
  real before it was added. Two other platforms probed for the same treatment (mokahr,
  recruiterkr) did not serve their listings to a plain fetch, so they stay out rather than
  being registered on faith.

### Fixed

- **Closed and acquired studios' old board addresses are retired from the shipped list,
  with evidence rather than a guess.** Ten addresses are retired. Each had been classified
  as gone or acquired, and each was re-checked live: the address had to fail a fresh fetch
  (unreachable, 404, or redirecting to a different company), with a known-good board
  fetched through the same check in the same run, so a broken check cannot be mistaken for
  a dead board. A page that moved but still answers is left alone - retiring a live board
  is the costly direction to get wrong.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.012:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
