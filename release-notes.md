## [0.3.018] - 2026-10-11
### Fixed

- **A source discovery run can no longer be parked by one slow employer board.** A careers page
  that trickles its response instead of answering kept a probe open well past its timeout — and
  was then counted as a *success* — which held the whole discovery stage until that server chose
  to finish. Measured at 40 seconds against a 2 second timeout on a local trickling server, and
  the same stall ran for minutes inside the GameDevMap audit. Each probe fetch and the probe wave
  now have their own deadline, so such a board is recorded as one failed candidate while the rest
  of the wave still completes, and the job report now says why a probe timed out instead of
  leaving the reason blank.

### Notes

- Nothing about what discovery collects changes. The same boards are probed, the same candidates
  are queued, and an unresponsive board is still reported as that board's own failure — the
  difference is that it can no longer hold the whole stage open while it stalls.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.017:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
