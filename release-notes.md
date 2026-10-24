## [0.3.026] - 2026-10-24

### Changed

- **A fixed 200-board trial runs about 15% faster.** The fetcher now works on 16 sources at a time
  instead of 12. The 200 boards were drawn from the last full run and weighted toward its slow
  sources, so this is a sample rather than a whole fetch: the same boards finished in 181 s and
  183 s at 16 sources in parallel, against 222 s and 207 s at 12, with board failures flat
  (36 against 37 of 200). Going to 24 gave the gain back (221 s), because a handful of boards
  stalled for 100–160 s each and doubled the total work.

- **Slow boards are picked up first, so a run's last minutes are not spent on boards that only just
  started.** A fetch used to start boards in report order, which put the slowest ones last: the last
  full run spent its final four and a half minutes on two boards that had only just started, while
  the rest of the fetcher sat idle. Boards are now started in the order they took last time, and
  results are still combined in selection order. A bounded buffer holds results that finish early,
  so a stall deep enough to fill it (at least 256 results behind one board) can combine one result
  early — the only case where which copy of a duplicated job is kept could differ.

- **A single-page static board whose page has not changed is no longer parsed at all.** Static boards
  with one listing page that kept jobs and have not been read for three hours now ask the site first:
  the fetcher sends a HEAD request — conditional once it knows the page's version tag, plain on the
  first pass to learn it — and, when the answer is "not modified", finishes the board without
  parsing it, opening detail pages or trying the browser. Two live boards answered the check with
  304 in 193 ms and 251 ms; full reads of boards in the same session took 2.3 s and 14.2 s. Boards
  with one listing page that kept nothing get the same check once their twelve-hour re-check window
  expires and they have come up empty on two runs in a row, so an empty board is no longer
  re-downloaded only to confirm it is still empty. A board that answered "not modified" then waits
  out its window — three hours, or twelve for one that keeps nothing — before asking again, instead
  of spending a check on every run. Boards that publish no version tag still take the full read;
  the check costs them one HEAD first — and a board or provider feed that stops publishing one drops
  the stored tag rather than asking with a retired value — while boards whose job list is drawn by
  the browser always take the full read:
  their page looks the same whether or not the jobs behind it changed. Boards split across several
  listing pages also keep the full read, because one page's answer cannot vouch for the pages behind
  it. Several provider feeds also keep the version tag their own listing response sends, so a feed
  that only publishes one on the full read can ask the conditional question next time instead of
  downloading everything again.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.025:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
