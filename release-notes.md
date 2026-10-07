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
