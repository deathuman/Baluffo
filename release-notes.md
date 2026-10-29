## [0.3.028] - 2026-10-29

> This is a desktop rollup: it combines the fetcher changes that shipped to the container
> channel as 0.3.026 and 0.3.027 with the discovery work below, so a desktop install
> upgrading from 0.3.025 receives the whole set in one step.

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

- **An empty board is asked before it is downloaded, even on its first check.** A board with no
  jobs used to be read in full one more time before the fetcher trusted that it still had nothing.
  When the previous read stored the page's version tag, the board now asks "has this changed?" at
  its first check instead: "not modified" is the same evidence a second full read gave, and it
  starts the board's empty window. Boards that publish no tag keep the old behaviour, as do boards
  whose last check failed.

- **Boards split across several listing pages skip unchanged pages too.** A board with more than one
  listing page used to be re-read in full every time, because one page's "not modified" answer could
  not vouch for the pages behind it. Each page's version tag is now remembered separately and the
  fetcher asks every page, skipping the download, parsing and detail pages only when all of them say
  nothing has changed; a page that changed still triggers the full read, and a page that stops
  publishing a tag drops only its own entry. The same change stops a multi-page board from reading
  its later pages over and over inside one run — a five-page board used to read fifteen pages, and
  now reads five.

- **A board's version tag is kept from the ordinary read, so its first re-check can already ask
  "has this changed?"** The tag used to be learned only by the first check after a board's window
  expired, which meant every board waited another full window before an unchanged page could be
  skipped. The read that produces the jobs now stores the tag as well — one for a single-page
  board, one per page for a board split across several. A page that stops publishing a tag drops
  it, and a page a run could not read keeps the tag it had. Boards whose reads go through a custom
  fetcher still learn their tag at the first check, as before.

- **Discovery stops re-asking boards that already said the page does not exist.** A board whose own
  listing endpoint answered 404 or 410 used to be probed again on every full audit. That answer is
  now kept for seven days beside the audit artifact, keyed by the board's host and tenant, and a
  skipped candidate still produces a rejection row, so the report reads exactly as a live 404 would.
  On a frozen 9,050-candidate population the probe fell **40.8%** (216.5 s to 128.2 s across two
  pairs) and lost no board: 360 kept with the memo, 358-359 without it. `BALUFFO_DISCOVERY_KNOWN_DEAD_DAYS`
  sets the window and `0` turns the memo off.

- **The homepage analysis reads each fetched page for links once instead of repeatedly.** The
  extractors that look for a careers link each walked the same HTML, so a page with no candidates
  was scanned about nine times over. One lazily built index per fetched body now serves the provider
  walk, the explicit-link extractor, the static candidate builder, the no-careers reason and both
  recovery waves. On the captured production population, scans fall from **9.03 to 3.79** body walks
  per readable page and the phase falls from about **87 s to 53 s**, with results identical batch by
  batch and per collection; a live worker pair confirms **87.7/86.9 s to 65.0/62.9 s**.

- **The container channel analyzes one batch's homepages while the next batch is fetched.** The
  analysis runs in a worker process handed the parent's own function and inputs, so a batch's
  result is unchanged; any failure falls back to analyzing that batch in-process and retires the
  transport for the rest of the run. Two counterbalanced full-population pairs took **126.5 s** and
  **94.0 s** off the audit's wall (**748.8 to 622.3 s**, **729.6 to 635.6 s**). The packaged desktop
  app keeps the in-process analysis, because a frozen build does not start the worker.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.025:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
