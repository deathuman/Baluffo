## [0.3.019] - 2026-10-14
### Added

- **The studio audit now reports what it left running when it finished.** A page that never answered is
  given up on after its own time limit, but the request itself keeps running in the background — and a
  run that ended while one was still out also left its shared connection open for it. Neither was
  visible anywhere except by reading the audit's own large output file, so a run that closed everything
  neatly and a run that walked away from a live request looked the same. The Admin ops view now shows
  it directly: **how many studio-page fetches were abandoned by their own limit, how many were still
  running when the stage finished, and how many connections a run left unclosed**, with the run they
  belong to. The counters are published as their own small file beside the audit, so reading them costs
  a fraction of a second rather than opening a file measured in tens of megabytes, and the same summary
  appears in the audit-artifact diagnostics. A run that never probed anything publishes nothing, and
  the view says so instead of showing zeros that would read as a clean run.

### Changed

- **Discovery no longer re-asks a board that has already said it does not exist.** Games studios are
  looked up on the job-board provider that hosts them, and a provider that has no such board says so
  outright. Discovery used to follow that answer with a second request to the same board's
  human-facing page before giving the candidate up — a page that names the same board, so it can only
  repeat the answer. Measured on the GameDevMap studio audit, which is the app's largest single
  discovery job: **about 36% fewer requests in its probing stage and about 43% less work performed by
  that stage**, with the same candidates failing and the same studios found. How much of that reaches
  the job's clock depends on the size of the job — on a small batch one slow studio can still hold the
  whole probing stage open — so the run gets the same answers for less work rather than a guaranteed
  shorter total.
- **Discovery no longer waits a third time on a board that never answers.** A careers page that hangs
  used to be asked three times before discovery gave up, and every ask waited the full timeout, so one
  unresponsive studio could hold its whole batch for over twenty seconds. It is asked twice now. The
  retry that remains is the one measured to actually rescue a board, boards that answer within two tries
  are untouched, and boards that refuse to answer give up sooner. Measured on the studio audit's own
  list across six paired runs: the worst single board fell from about 22 seconds to about 13, and no
  board's outcome changed because of the shorter ladder.
- **Discovery stops re-guessing a studio that has already answered.** The studio audit looks for a
  careers page under the common addresses, and when it finds nothing it used to try four more
  spellings of the same guess. Those four are now only tried for sites the first set could not read
  at all, or whose only job-ish links came from the homepage itself: a site that answered has already
  shown where it keeps its listings, so asking again in other words is the same question. Measured on
  the audit's own studio list, the four extra questions were **about 72% fewer requests** for that
  step, its time down **about 43%**, and the single studio given up on was an events page that
  produced no jobs either way — no board that had jobs was lost.
- A failed source now reports the address discovery actually asked. Previously the reason named the
  page it tried second, which pointed at a URL the studio never advertised.
- **Discovery now reads studios whose certificates this machine's own trust store rejected.** Every
  page discovery fetches is checked against the certificate authorities shipped with the app as well
  as the ones this machine carries, instead of the machine's list alone. A store that has gone stale
  refuses a studio's perfectly valid certificate with "certificate has expired", and discovery treated
  that as a studio with no careers page — while the browser-facing part of the same job, which already
  carried the shipped list, read the page fine. Measured on the studio audit's own list of 3,000
  studios: **about 19% more studio pages read** (2,326 → 2,763) and its certificate failures fall from
  **506 to 16**, which is **104 more static candidates** and **18 more active boards** on the same
  studios. Nothing is trusted that was not trusted before — the machine's authorities stay in the list,
  the shipped ones are added, and a certificate that genuinely cannot be verified still fails.
- **A concurrency setting is no longer quietly ignored.** Page fetches go through a shared
  bounded-concurrency helper. When the caller supplied its own fetch function — which the app does for
  the studio audit's own measurements, and which the job lane does for the second page of every static
  source — that helper ran those fetches on the interpreter's general-purpose thread pool instead of
  the number of lanes the run had asked for, and that pool is sized from the machine's core count:
  **20 on a 16-core machine**, whatever the setting said. A run configured for 32 lanes therefore
  fetched 20 pages at a time, with no error and no warning — which is why the audit's own phase timings
  disagreed with the numbers the app reported. Each batch now runs on a pool sized to the configured
  lane count, so the setting is the count. Measured on one recorded batch of 1,000 studio homepages at
  32 lanes: peak in flight rose from **20 to 32**, mean from **18.1 to 26.8**, and the batch's wall
  clock fell from **39.2 s to 27.9 s** with the same pages read. A batch that overruns its deadline now
  also returns at the deadline on this path instead of waiting for the slowest fetch to finish.
- **The studio audition no longer answers its own door while it is reading everyone else's.** The stage
  that tries every candidate reads thousands of career pages, and the thread that decides when to ask
  the next one was also the thread that read each answer — the connection, the headers and the body of
  every page. Its own bookkeeping therefore waited behind its downloads, which occupied that thread for
  **42.6% of the stage's total time**. Reading a page now happens on the stage's own worker pool, a
  page per lane, so the deciding thread stays free: its share of the stage fell to **about 10%**, the
  delay before it schedules the next candidate fell from ~3.7 ms to ~1 ms, and total processor work per
  candidate fell by **about a quarter**. The auditions themselves are unchanged — the same candidates
  pass and fail for the same reasons. The stage's clock improved in some runs and not in others, so
  read this as less waiting and less work rather than a guaranteed shorter audition.
- **The studio audition stopped spending its parallel requests on its own waiting line.** The stage
  that tries every candidate divides its parallel requests into a shared total and per-kind limits, and
  a candidate whose kind was already at its limit used to hold a place in the shared total while it
  waited for its own. On the audited population that meant **8.7 of the 40 places** held by candidates
  doing nothing (7.8 on average across the runs), while the kind that is four fifths of the work ran at
  **21 of the 40 requests it was allowed**. A waiting candidate no longer takes a place: the same
  auditions now take **36.8 s against 47.6 s** across three paired runs (**63.9 → 82.6 candidates per
  second**), every pair improving, with each kind's own limit and the shared total left exactly as they
  were and no candidate's outcome changed.

### Notes

- Nothing about what discovery collects changes for the cuts above. The same candidates are probed,
  the same boards are queued, and a candidate still fails for its own reason — several hundred
  candidates in the same audit keep both requests, because for those providers the first answer is not
  the board's own. The trust-store repair is the one change here that **adds** to what is collected,
  and it adds studios that were previously reported as having no careers page.
- Neither audition change raised a limit. The per-kind requests and the shared total are the values
  they were, so no provider sees a higher peak than before.
- The studio audition now holds a worker thread per page in flight, and a page that ignores its
  deadline keeps that thread until the network gives up on it — the same trade the second-page fetch
  already makes, bounded by the stage's own lane count. On the audited population no trickling page
  held a batch past its deadline. The shared connection those threads read through is closed by the
  last of them rather than at the moment the audition moves on, so a page still arriving when the
  stage has given up on it finishes or fails on its own rather than having its connection cut.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.018:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
