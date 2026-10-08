## [0.3.015] - 2026-10-08
### Fixed

- **Studios hiring on Ashby are collected again.** Ashby builds its board page in the
  browser, so the page you download carries no job links at all, and Baluffo was
  reading those pages. Every Ashby board therefore looked empty: Voodoo's openings -
  122 of them, including the Technical Artist (AI) role that has been missing since the
  studio moved off Lever - never reached the feed, and roughly twenty other Ashby studios
  showed zero openings while hiring. The app now reads the same board data the page
  renders from, so discovery no longer files a live Ashby board as empty and stops
  looking at it.

- **The run's stage timings now tell the truth, so slow stages can be found.** The
  report charged network time to parsing and left about half of the static-board time
  unattributed, which made the most expensive part of a run look cheap. Timings are now
  booked to the stage that does the work, with identical job coverage.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.014:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
