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

## [0.3.008] - 2026-10-05
### Fixed

- **Workday boards no longer stop after 100 openings.** The CXS collector paged five times
  regardless of board size, so a large board was silently truncated: NVIDIA's 2,000
  openings resolved to 100. Pagination now follows the total the API reports. Verified
  against the live boards — NVIDIA 100 → 2,000, Intel 100 → 602, Aristocrat 100 → 209.
- **A board is no longer suppressed for sharing a careers platform with another studio.**
  Workday and BambooHR were matched by adapter name rather than by tenant, so every board on
  either platform looked like a duplicate of whichever registered first. 64 boards carrying
  1,257 openings were affected, including NVIDIA's — the largest single block in the
  catalogue. Board identity is now host plus tenant.
- **Registration is no longer reported as delivery.** The coverage audit counted a board as
  delivered when a registry row existed, which is why the previous release reported 6,844 of
  6,929 openings delivered while a live run collected none of them. The audit now reports
  registration, readability, and collected openings separately, and fails when boards
  register but nothing collects.
- **Ubisoft's careers boards are read from the system that actually serves them.** Every
  Ubisoft board is a regional subdomain — toronto, berlin, mainz, duesseldorf, saguenay,
  stockholm, winnipeg — and none were recognised as SmartRecruiters-served, so each was
  scraped as a plain page. `toronto.ubisoft.com/jobs` spent 2,760 seconds to yield a single
  job from a 223 KB page containing no job links, while the SmartRecruiters tenant that
  carries the same listings showed 333 openings including Berlin.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, and wildcard browser CORS allow headers all behave exactly as they did in 0.3.007, as does desktop localhost bridge compatibility. The only differences are the job-source coverage and fixes above.

## [0.3.007] - 2026-10-04
### Added

- **Baluffo now watches 695 verified job boards instead of 500, covering 6,929 openings it was previously missing.** Every board was added because specific openings had been seen on it and then went missing, and each one was checked to confirm it really serves them - 98.8% of them are confirmed to reach the app's job list. This is the largest single increase in coverage the app has had, and it is mostly studios you would expect to find: 28 Japanese publishers on the hrmos platform (Capcom, Square Enix, Game Freak, Nexon, Spike Chunsoft, Bandai Namco Studios), 29 Workable boards including Keywords Studios and Rebellion, and eight European Greenhouse boards that were being missed entirely because their address pattern did not match anything Baluffo recognised.

- **1,893 job boards that had not been checked in four months will now update again.** When Baluffo decided a board was fresh enough to skip, it also pushed that board's next check further into the future - so a board that was skipped often enough was never fetched again, no matter how old it got. In the last full run this applied to 94% of all registered boards, with the average last successful check 121 days earlier. Nothing about those boards was reported as broken, which is why it went unnoticed: a skip is not a failure, and it did not look like one. Skipping now means what it says.

### Fixed

- **Real game roles are no longer filtered out of the job list.** Baluffo decides whether a posting is games work by looking for terms like "gameplay" or "technical artist", and the list was too narrow: "Senior VFX Artist", "Senior Hard Surface Artist", "3D Generalist" and "LEVEL DESIGNER" were all being discarded, as were openings published in Japanese or French, which no English keyword can match - a Marvelous game designer role and a Bandai Namco AI engineer role were invisible. 238 openings across 97 studios now come through. Business roles at game studios - a logistics director, a marketing manager - are still excluded on purpose, so this widens what counts as games work without turning the job list into a general jobs board.

- **Ubisoft's job board is no longer cut off after 100 openings.** Ubisoft lists 332 openings and the board delivers them a hundred at a time, so two thirds of it was never requested. Baluffo now asks for the rest and keeps 46 game roles instead of 12.

- **Personio job boards work again.** Personio run their recruitment sites on two different web addresses, and Baluffo only recognised one of them, so every board on the other was rejected as invalid before it was even fetched. The last full run collected nothing at all from Personio.

- **Voodoo's job openings show up again.** Voodoo moved its careers board from Lever to Ashby, and the old board was simply deleted. Baluffo was still watching the deleted one, so it collected nothing and the studio's openings vanished from the Jobs page - 120 of them, including roles posted as recently as the day before. Baluffo now watches Voodoo's Ashby board, which is verified live and serving all of them.

- **The Ashby board list no longer throws away working studios.** A maintenance routine that checks whether Ashby boards are still alive could not read the board address back out of the saved source list, so it treated nine healthy boards as broken and deleted them - studios such as thatgamecompany, k-ID and Sleeper, along with roughly 80 live openings between them. It now reads the address correctly, and it also treats a board written two different ways as one board instead of two, which was double-counting openings.

- **Deleted job boards are no longer hidden behind a healthy-looking group.** When one app fetches many companies' boards at once - Lever, Greenhouse, Workable and the rest - a single dead board was reported only as part of a long text field on an otherwise successful run, so the run looked fine and nothing drew attention to it. Twelve boards had been quietly dead this way, including one studio's after they moved to a different hiring system. Board-level failures are now listed separately with the company and the reason, so they can actually be cleaned up.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.006. The only differences are the job-source coverage and fixes above.
