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
