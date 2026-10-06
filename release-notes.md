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
