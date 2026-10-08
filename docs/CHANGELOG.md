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

Two rules the guardrail enforces so this cannot quietly regress:

- **At most 5 versioned sections**, plus `[Unreleased]`.
- **No bullet appears in two sections.** A change is claimed by exactly one release,
  so a release can never claim to have fixed something an earlier one already fixed.

---

## [Unreleased]

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

## [0.3.012] - 2026-10-06
### Added

- **Thirty-nine more studio boards are part of what an install watches, with around 215 game
  roles between them.** Each board was fetched for real before it was added, and only boards
  that answered with live listings were kept (one answered empty and was left out). The
  additions include Rockstar, Take-Two, NetEase, PUBG Corporation, Twitch, Samsung
  Semiconductor, Sony Pictures Imageworks and the New York Times' games team, alongside a long
  tail of smaller studios.

### Fixed

- **A studio's back-office vacancies can no longer slip into the job list from some sources
  while being filtered out of others.** Baluffo decides whether a posting is games work with a
  keyword rule, and that rule ran on most sources but not all: the HTML-based boards (Ashby,
  Breezy, JazzHR), the Greenhouse, BambooHR, Workday, Phenom and Dayforce feeds, the community
  game-industry boards, and plain career-page extraction all skipped it. Measured against the
  live job list, 4,214 of 6,887 career-page postings failed the rule and still shipped. Every
  one of those paths now applies the same rule the rest of the app uses, at the moment rows are
  kept. The one deliberate exception is the community Google Sheet: its rows carry the sheet's
  own Game/Tech label, and the default job list intentionally includes its Tech rows, so
  filtering it would have deleted half the list.
- **Boards are now paced per studio instead of per platform, so discovery can reach boards it
  kept deferring.** Every Greenhouse board shares one web host, and the discovery queue treated
  that shared host as a single family: only a couple of Greenhouse boards could be taken per
  pass and the rest were deferred every time (43 of them in the measured pass). Each studio's
  board now counts as its own family on all the shared platforms - Greenhouse, Ashby, Lever,
  Workable, SmartRecruiters, Teamtailor, Breezy, Recruitee, JazzHR, Personio, Pinpoint,
  Dayforce and Phenom.
- **Coverage numbers are no longer credited to the wrong board.** The measurement that decides
  whether a board "collects" matched every posting on a shared host to the first board in the
  list: one Greenhouse board was credited with 728 postings while fifty others read zero. It
  now resolves each posting by its own board address, and re-measuring the same pass moved the
  count of boards that collect from 322 to 438 of 688.
- **A board that fetched and kept nothing is no longer reported as either broken or empty.**
  The measurement harness now says "unknown" for the 168 boards whose own health says it cannot
  tell, and for the 23 whose extraction found nothing - instead of implying the board is empty.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.011:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.011] - 2026-10-06
### Added

- **Twenty-seven more studio boards are now part of what an install watches.** Each was added
  only after the app's real fetch for that board returned live listings, not because the
  address looked right. The biggest are Voodoo's, 2K's and Hangar 13's, alongside a long tail
  of smaller studios.
- **Those boards carry 486 listings between them, and 149 of them are game roles.** The other
  337 are the finance, marketing, legal and support vacancies that mixed companies post
  alongside their games work, and the app still declines to collect them. That split is
  deliberate: a board is worth watching, but only its game openings are worth showing.

### Fixed

- **A board can no longer be added to the registry in a way that quietly collects nothing.**
  The tool that adds boards was writing them without the field that says whether a board is
  active, so they were loaded as inactive: they appeared in the registry, they counted toward
  every total, and no fetch ever looked at them. Nothing in the app reported a problem. The
  field is now set through the same code path the running app uses, and the build refuses to
  accept a board row that would collect nothing.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.010:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
