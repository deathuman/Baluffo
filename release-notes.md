## [0.3.014] - 2026-10-07
### Added

- **The fetch report now says why a browser fallback was refused, and which kind of empty a
  render was.** A refused attempt records its reason, and the counters separate "the browser
  could not launch" from "the page rendered with no jobs in it" - two opposite findings that
  shared one number, which is why a run with 716 refusals out of 843 attempts could not be
  read at all. Rows that asked for browser fallback also carry the run's cause, so a board
  recommending fallback no longer reads as a board nobody tried. The environment cause keeps
  its own field: a refusal no longer overwrites the reason the breaker was closed.

### Fixed

- **A studio board's fetch evidence is read from its own entry, not its platform's totals.**
  When an adapter serves many studios under one summary row, every one of them inherited that
  row's totals, so 25 boards registered in recent releases read as "unknown, only the
  platform's result is known". Read from each board's own record: 17 boards were asked and
  genuinely returned nothing, and 8 errored asking for a browser. Both are decidable now.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.013:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
