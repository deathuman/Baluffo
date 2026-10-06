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

## [0.3.013] - 2026-10-06
### Added

- **Boards whose listings live inside the page's own data now work without a browser.**
  When a careers page returns its openings as JSON embedded in the HTML - schema.org job
  data, Next.js page data, or a script array - the app now reads them directly instead of
  concluding the page has nothing. Bungie's careers page is the measured case: its
  postings arrive inside the page payload, and they are read from there now; the host
  rule that sent the page to a full browser render is gone.

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

## [0.3.010] - 2026-10-06
### Added

- **A board that moved platforms now says so.** When a studio's careers page answers with a
  redirect to somewhere else, the fetch used to record one anonymous failure for four different
  situations — a studio that closed, one that is the same studio on a new domain, one whose
  server dropped to an insecure connection, and one whose listings genuinely moved to an
  applicant-tracking system. Each is now named. The boards that moved to a platform are
  recognised as belonging to the platform that serves them, so they can be registered there
  rather than kept on a page that no longer lists them.
- **A studio that moved domains is no longer filed as dead.** Bungie is one of them: its old
  address answers with a redirect to a new one, which is Bungie still hiring rather than Bungie
  gone — and Bungie is a board this app already tracks. A rebrand is now told apart from a
  closure or an acquisition, because a studio sold to a differently-named company is not
  recoverable the same way and should not be pointed at its buyer's openings. The direction of
  that mistake is what matters: filing a live board as dead is how a working board gets retired,
  and filing a closed one as alive credits it with someone else's jobs.
- **A new address is checked before anything moves.** Being the same studio on a new domain is
  not the same as that address listing jobs, so the two are separate steps. A rebrand is only
  re-pointed once its new address is confirmed to carry job listings, and a check that finds
  none is recorded as unconfirmed rather than treated as a failure. Five were checked against a
  live run: two were confirmed immediately, and the other three load their listings inside the
  page rather than as links, so they need a different treatment rather than a re-point.

### Fixed

- **A stray character no longer rides along on a board's address.** Some careers pages redirect
  to an address ending in a semicolon. It is a legal part of a web address and the page loads,
  but the character was being carried into the address we store for the board, leaving every
  later reader to strip it again.

### Notes

- Nothing about what the runtime will and will not fetch has changed. A redirect that leaves a
  site's own domain is still refused rather than followed, including one that leads to a
  platform we recognise; the new classification explains the refusal instead of permitting it.
  Measured on a live run, those 135 redirects were 5 studios that genuinely moved to a
  recruitment platform, 13 that are the same studio on a new domain, 13 servers that dropped to
  an insecure connection, and 104 that closed or were acquired.
- Closed boards, acquired boards and unconfirmed rebrands are labelled but deliberately **not**
  retired, and confirmed rebrands are reported without being re-pointed automatically. Each is a
  visibility change with real consequences, so none is something to do as a side effect of a
  better error message.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.009:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.

## [0.3.009] - 2026-10-06
### Fixed

- **Boards are no longer reported as broken for being slow.** The per-source static fetch
  ceiling was 25 seconds, and a board that was mid-fetch when it expired was filed as a
  failure — `time_budget_exceeded` — so a configured limit read as 134 broken boards on a live
  run. It is now 90 seconds, which is the value measured rather than a larger untested one:
  raising it alone moved time-budget errors from 14 to 1 and turned 11 boards into real
  collections. Boards with nothing to offer are unaffected, because the ceiling is a limit and
  not a wait. This brings the default path in line with the `uncapped` preset, which has run
  at 180 seconds since before.
- **Personio boards collect again.** Personio's XML feed has no URL element — a live
  posting carries an id, an office, a department and a description, and nothing that points at
  the advertisement. Every row therefore arrived with an empty link and was discarded as
  incomplete, which is why the provider read as having nothing while its feeds were being read
  successfully. Links are now built from the feed's own address and the posting id, so a
  studio's board resolves to a page a person can open.
- **A memory-growth flaw in a networking dependency is no longer pinned.** An advisory
  against the HTTP client stack's dictionary type let a remote request drive memory growth
  that was never reclaimed. The affected version was pinned but not the version actually
  installed, so the lock file now matches what ships.
- **A mistyped static tuning variable no longer fails the whole run.** The three numeric
  static environment variables were read with a bare `int()`, so an unparseable value raised
  straight out of the configuration builder and took the fetch down with it instead of
  falling back to its default.
- **Browser-fallback escalations are no longer invisible.** Per-source outcome fields are
  written into a source row's first detail entry, but the health summary read them from the
  top of the row. A live run therefore reported that **no** source needed browser fallback
  while **199** rows said otherwise — including 78 boards failing HTTP 403 — and recorded no
  reason for any source being in fallback cooldown, which is why refusals could not be
  diagnosed. Both are now read from the detail row.

### Notes

- The coverage figures in the release plan were corrected. "The live run collected 0 of 6,929
  openings" was a measurement that scored the hand-audited board list against the live
  registry and called non-membership a collection failure. Measured on its own registry rows,
  the same release writes 49,240 jobs with 1,019 boards keeping a non-zero count. Delivery is
  now reported over registry rows and catalogue gap over the audited list, rather than one
  number standing for both.
- Distribution surfaces are unchanged. The same-origin Linux container,
  GHCR multi-arch image publishing, private community app-store metadata, and
  wildcard browser CORS allow headers all behave as they did in 0.3.008, as does
  desktop localhost bridge compatibility. Umbrel raw-LAN installs are likewise unchanged.
