## [0.3.026] - 2026-10-24

### Changed

- **A full fetch runs about 15% faster.** The fetcher now works on 16 sources at a time instead of 12.
  On a fixed 200-board trial the same run finished in 181 s and 183 s at 16 sources in parallel, against
  222 s and 207 s at 12, with the same number of board failures; going to 24 made it slower than 12,
  because a handful of boards stalled for over 100 s each and dragged the run out.

- **Slow boards are picked up first, so a run no longer ends with a few boards and nothing else running.**
  A fetch used to start boards in report order, which put the slowest ones last: the last full run spent
  its final four and a half minutes on two boards that had only just started, while the rest of the
  fetcher sat idle. Boards are now started in the order they took last time, and the results are still
  combined in the same order as before, so which copy of a duplicated job is kept does not change.

- **A board whose page has not changed is no longer parsed at all.** Static boards that kept jobs and
  have not been read for three hours now ask the site first: the fetcher re-requests the listing URL
  conditionally and, when the answer is "not modified", finishes the board without parsing it, opening
  detail pages or trying the browser. Two live boards answered that question in under 0.25 s with a
  304, against full reads of 2 s and 14 s on the same boards. Boards that kept nothing get the same
  check once their twelve-hour re-check window expires, so an empty board is no longer re-downloaded
  only to confirm it is still empty. A board that answered "not modified" then waits out its window
  — three hours, or twelve for one that keeps nothing — before asking again, instead of spending a
  check on every run. Boards that publish no version tag just fetch as before — and a board or
  provider feed that stops publishing one drops the stored tag rather than asking with a retired
  value — while boards whose job list is drawn by the browser always take the full read: their page
  looks the same whether or not the jobs behind it changed. Provider feeds also keep the version tag
  their own listing response sends, so a feed that only publishes one on the full read can ask the
  conditional question next time instead of downloading everything again.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.025:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
