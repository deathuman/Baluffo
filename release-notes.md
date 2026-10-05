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
