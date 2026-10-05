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

## [0.3.006] - 2026-10-02
### Changed

- **A tooltip you dismissed with Escape now stays dismissed.** Admin refreshes itself in the background every thirty seconds. If one of those refreshes redrew the button your mouse was resting on, the browser re-checked the hover and the tooltip came back on its own. Escape now holds until you actually move the pointer away, after which hovering behaves exactly as before.

- **The Attachments panel no longer claims to be empty before it has looked.** On first open it read "No attachments yet." when nothing had actually been loaded. It now says the list has not been loaded and points at Refresh.

- **Agent skills and MCP configuration no longer ship inside the container image.** That folder was reaching the image, so editing a skill was treated as a change to the app itself — which could quietly replace the image behind a version you already had. Nothing in the running app reads those files.

- **The two container ignore lists can no longer half-register.** The rule that decides when a change counts as "shipped" and the rule that decides what goes into the image are now asserted to agree, so a directory cannot be counted as shipped while being absent from the image.

### Added

- **You can now reload a job's attachments on demand.** The Attachments panel has a Refresh button beside Upload, matching the one already on the History tab.

### Fixed

- **A failed attachment load no longer looks like an empty one.** If the app could not reach your saved files, the panel used to say "No attachments yet." as though you had never added anything. It now reports that the list could not be loaded and offers to try again, so a problem is never disguised as a fact about your data.

### Notes

- **Nothing stored is touched.** No saved jobs, settings, or tracked applications are read or written differently, and there is no upgrade step. Attachment storage, the backup format, and attachment counts are all exactly as before.

- **Some intermittent test failures were fixed at their source.** One check was measuring every pause in the whole system instead of its own retry delay; another let two test runs started by accident delete each other's working files. These affected development confidence, not anything you can see.

- **A developer-only report script no longer advertises an option it never had.** It accepted a time filter, ignored it, and documented it in its usage line. The option is gone and the usage line now matches what the script does.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.002. The only user-visible difference is that a tooltip you dismissed with Escape stays dismissed, and that a failed attachment load now says so.


## [0.3.002] - 2026-10-01
> Housekeeping release. Nothing you can see looks different: this removes
> styling that no page could ever display, and fixes a test that was failing on
> its own about one run in four. It exists so the cleanup below can actually
> reach you instead of sitting on the development branch.

### Changed

- **Removed styling that nothing was ever able to display.** The stylesheets carried rules for 18 classes that no part of the app could put on a page, plus two colour settings nothing referred to. Nothing you saw was affected and nothing you see will change — the pages are pixel-for-pixel identical, checked on all three main screens. Removing them makes the stylesheet smaller and easier to trust: what is left in it is styling that is genuinely in use.

- **Container updates no longer rebuild for tool-only dependency bumps.** Updating a developer-only tool — a linter, a type checker — rebuilt the container image and republished the current version under the same version number. Nothing about the running app changed, but the image behind a version you already had was quietly replaced. Those updates no longer trigger a rebuild. Real changes to what the app runs still do.

### Fixed

- **A check that was failing intermittently for no reason.** One of the Admin interface tests failed roughly one time in four depending on how busy the machine was, which was making it hard to trust the test suite. It was timing its own observation rather than recording it, so it could miss the thing it was looking at. It now records what it saw and checks that, and passed ten consecutive full runs.

### Notes

- **No behaviour, data, or upgrade changes.** No saved jobs, settings, or tracked applications are touched, and there is no migration to run.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.001. Install and update paths are the same, and the Linux AppImage, portable Windows app, and recovery bundle are all published as before.

- **What changed about how releases are guarded.** Because a stylesheet edit is part of what ships, a change of this kind can no longer land while an already-published version is current — it would replace the image behind a version you have. That is now caught automatically, with a clear explanation, before it reaches you.
