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
