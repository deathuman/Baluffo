## [0.3.010] - 2026-10-06
### Added

- **A board that moved platforms now says so.** When a studio's careers page answers with a
  redirect to somewhere else, the fetch used to record one anonymous failure for four different
  situations — a studio that closed, one that rebranded, one whose server dropped to an
  insecure connection, and one whose listings genuinely moved to an applicant-tracking system.
  Each is now named. The boards that moved are recognised as belonging to the platform that
  serves them, so they can be registered there rather than kept on a page that no longer lists
  them.

### Fixed

- **A stray character no longer rides along on a board's address.** Some careers pages redirect
  to an address ending in a semicolon. It is a legal part of a web address and the page loads,
  but the character was being carried into the address we store for the board, leaving every
  later reader to strip it again.

### Notes

- Nothing about what the runtime will and will not fetch has changed. A redirect that leaves a
  site's own domain is still refused rather than followed, including one that leads to a
  platform we recognise; the new classification explains the refusal instead of permitting it.
  This was measured on a live run: of 135 such redirects, 117 were studios that closed,
  rebranded or were acquired, 13 were insecure same-site redirects, and 5 were genuine moves to
  a recruitment platform.
- The closed, rebranded and acquired boards are labelled but deliberately **not** retired. A
  retirement is a visibility change with real consequences, so it is not something to do as a
  side effect of a better error message.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.009:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
