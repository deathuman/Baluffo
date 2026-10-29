## [0.3.028] - 2026-10-29

> This is a desktop rollup: it contains the 0.3.026 and 0.3.027 releases as well as the discovery
> work below, so a desktop install upgrading from 0.3.025 receives the whole set in one step.

### Changed

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

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.027:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
