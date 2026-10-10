## [0.3.016] - 2026-10-10
### Changed

- **Fetch runs cost substantially less time for the same jobs.** The app's most repeated
  text work — cleaning a title, folding a country name into its short form, keying a city —
  is now remembered instead of derived again for every occurrence, and two hot text steps use
  cheaper spellings that produce identical output. Measured by replaying the same recorded
  responses through one workload, across 12 paired runs: **median engine wall-clock time
  33.2% lower** (95% confidence interval 30.0% to 45.1% lower), with every pair keeping the
  same number of jobs, the same job identities and the same per-board outcomes. Nothing about
  a collected job changes; producing the feed just costs less. That figure is engine time
  only — a live run also waits on the network, so the gain there will be smaller.

### Fixed

- **A run's merged job list no longer depends on which board finished first.** Boards are
  fetched in parallel and now joined in the order they were selected; before, the order they
  happened to finish decided which copy of a duplicated posting survived. Two runs over the
  same responses disagreed by 508 rows (5,541 against 6,029 merges) and neither number was
  reproducible. The merged set is now a property of the run, so a count you read once still
  holds when you fetch the same boards again.

- **One broken link on one board no longer ends the whole run.** A board published a detail
  link that could never be a valid web address, which raised an address error the run did not
  expect and stopped a 2,046-board fetch at board 1,579 — the remaining 467 boards were never
  asked. An unusable address is now recorded as that board's own failure, with a message, and
  the run carries on.

### Added

- **A run report can now say why one board was slow.** With per-board request counters
  switched on (the optional `BALUFFO_FETCH_COUNTERS=1` setting), each board's row in the
  report carries how many requests reached the network, how many repeated an address already
  fetched, the response codes and error kinds it met, and how much text it received. They are
  counted per board and never per request, so a run pays nothing for them beyond the counting
  itself, and a report recorded without them keeps exactly the shape it has today.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.015:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
