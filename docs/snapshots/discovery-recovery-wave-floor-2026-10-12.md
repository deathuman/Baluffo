# The recovery wave's clock is now three stragglers, not 364 requests — 2026-10-12

> - **Class:** measurement, with three levers priced live and left unshipped
> - **Trigger:** the secondary-wave cut took the wave from 1,304 requests to 364 (**−72.1%**) but its
>   wall only from 48,075 ms to 27,184 ms (**−43.5%**). This asks what holds the remaining clock.
> - **Status:** answered. The phase is no longer volume-bound: each batch's wall is **one request** —
>   the last-completing straggler — and 14-27 of the 364 requests run **past their own 5 s budget**
>   (up to 8.8 s, 10.4-18.7 s of excess) because the HTTP timeouts are per-operation, not total. A
>   hard 5 s per-request deadline takes the phase **20.0 → 17.1 s** with **no readable page lost**;
>   3 s and 2 s reach **11.4 s** and **8.6 s** for exactly one ~4 s read; ordering the
>   already-over-budget hosts first is worth a further **5.5%**.
> - **Verified against:** `9b8901f8` plus the uncommitted board-absence, timeout-ladder and
>   secondary-wave cuts. Nothing in this snapshot changes shipped behaviour.
> - **Then inspect:** `src/shared/http_batch.py` (`total_deadline_s`, the per-row budget as it
>   stands), `src/source_discovery/directory_fetch.py` (`fetch_directory_pages`, the async/sync
>   branch), `src/source_discovery/web_search_fetch.py` (`fetch_text`), and the wave-2 job list in
>   `src/source_discovery/active_audit_batch_runtime.py`.

## Where the 27,184 ms went

The retained arm keeps a per-URL fetch log written on completion, so the phase decomposes without a
harness. Shipped arm (`secondary-cut-narrow`), three batches of 132 / 96 / 136 kept requests at
`activeAuditRecoveryFetchConcurrency` **72**, per-host **4**, `activeAuditRecoveryTimeoutSeconds` **5**:

| Batch | Requests | Σ request time | Phase wall | Lanes busy | Slowest request | Last completion (start + duration) |
|---|---|---|---|---|---|---|
| 0 | 132 | 158,233 ms | 11,140 ms | 14.2 of 72 | 8,303 ms | 2,837 + 8,303 = 11,140 ms |
| 1 | 96 | 104,975 ms | 7,409 ms | 14.2 of 72 | 5,147 ms | 2,262 + 5,147 = 7,409 ms |
| 2 | 136 | 92,325 ms | 8,635 ms | 10.7 of 72 | 8,628 ms | 7 + 8,628 = **8,635 ms** |

Batch 2's **entire** wall is one request: `g-rounding.com/work-with-us` starts 7 ms in — a lane was
free immediately — and runs 8,628 ms against a 5 s budget. Batches 0 and 1 are the same shape with a
lane wait in front (2.3-2.8 s) because those stragglers were submitted behind faster work. So the
phase's 27,184 ms is **three requests**, not 364: at the configured 72 lanes the phase's 355 s of
summed request time would fit in 4,938 ms, and mean occupancy is 13.1 lanes (**18%**).

Why a request can outlive its budget: `httpx.Timeout(timeout_s)` and `urlopen(timeout=timeout_s)` are
**per-operation** budgets — the read timeout re-arms on every chunk — so a redirect loop or a
slow-drip host holds the request open. The heaviest wave-2 rows are exactly those shapes
(`TooManyRedirects` 4 rows at 7.1-8.8 s; `TimeoutError: the read operation timed out` at 5.0-10.1 s),
and the same hazard is already documented inside `http_batch.py`, which bounds a *whole batch*
(`total_deadline_s`) for precisely this reason after a draining server held a batch open for 900 s.

The stragglers are also **predictable**: every heavy wave-2 host had already blown the budget in wave
1 — `g-rounding.com` 7,730 / 7,954 ms, `fluxscopic.com` 10,084 / 10,118 ms, `fulafisken.se` 5,059 /
5,058 ms, `azerion.com` 6,250 / 6,284 ms, `capcom-games.com` 5,042 / 5,037 ms, `9heads.com` 6,252 ms,
`blyts.com` 5,013 / 5,006 ms.

## The levers, priced by replay

`wave2_floor_bench.py` replays **the exact 364 kept job lists** through the shipped
`fetch_directory_pages` path (`fetcher=fetch_text`, so the async httpx branch — the one
`orchestrator.run_discovery` uses) with the shipped limits, and timestamps every request against its
own batch start. Nine arms, one artifact (`wave2-floor-arms.json`):

| Arm | Phase wall | Batches (ms) | Mean lanes | Slowest request | Pages read | Excess over the 5 s budget |
|---|---|---|---|---|---|---|
| `async-shipped` | 19,980 | 6,166 / 5,612 / 8,202 | 20.8 of 72 | 7,460 ms | 6 | 12,337 ms (27 rows) |
| `async-shipped-r2` | 20,048 | 6,080 / 5,646 / 8,322 | 19.4 of 72 | 7,428 ms | 6 | 10,410 ms (19 rows) |
| **`async-deadline5`** | **17,273** | 5,976 / 5,517 / 5,780 | 22.6 of 72 | **5,016 ms** | **6** | **243 ms** |
| **`async-deadline5-r2`** | **17,089** | 5,686 / 5,621 / 5,782 | 22.1 of 72 | 5,016 ms | **6** | **204 ms** |
| `async-deadline3` | 11,394 | 4,023 / 3,563 / 3,808 | 29.3 of 72 | 3,016 ms | **5** | 0 |
| `async-deadline2` | 8,597 | 3,036 / 2,583 / 2,979 | 35.2 of 72 | 2,016 ms | **5** | 0 |
| `async-priorfirst` | 20,280 | 5,824 / 5,453 / 9,003 | 19.2 of 72 | 8,848 ms | 6 | 18,659 ms (24 rows) |
| **`async-deadline5-priorfirst`** | **16,266** | 5,919 / 5,187 / 5,160 | 23.1 of 72 | 5,014 ms | **6** | **151 ms** |
| `sync-shipped` (the bench's own path) | 27,768 | 10,584 / 7,342 / 9,842 | 12.0 of 72, peak **20** | 7,893 ms | 4 | 16,779 ms (20 rows) |

Three readings:

1. **A hard per-request deadline of `timeout_s` is free here and removes the run-to-run spread.** Every
   batch lands at 5.0 s + its 0.5-1.0 s ramp and the excess over budget collapses from 10.4-12.3 s to
   151-243 ms. Both arms are stable run to run *within the artifact* (shipped 19,980 / 20,048,
   deadline 17,089 / 17,273), but the shipped arm's batches inherit whichever host misbehaved that
   minute: across the day's eight runs it ranged 19,980-24,034 ms, against 16,266-17,351 ms for the
   seven deadline runs, whose worst batch is the deadline itself. Readable pages: **6 of 6 kept**
   in all four shipped/deadline-5 runs, and all six reads are fast (231-467 ms) except one at
   3.5-4.3 s. In the recorded arm the four reads were 89 / 391 / 413 / **3,714** ms — the same shape,
   all inside 5 s. The same hosts' `TooManyRedirects` and read-timeout rows become `TimeoutError` rows
   in the deadline arms: the same requests, cut at the budget.
2. **3 s and 2 s are not free, and the price is exactly one page.** Both read 5 of 6 and the page
   lost in both is `dreamflightadventures.com/join-us`, the ~4 s read (wave 1 against that host took
   2.4 s, so it is slow rather than hanging). Everything else the wave reads is under 1.1 s. Costing
   the phase at 11,394 / 8,597 ms, that is the lever's honest border: the wave's useful payload is
   fast, and one host per population is legitimately slow.
3. **Ordering the already-over-budget hosts first is a free 5.5%, and only under a deadline.** The
   prior is wave 1's own per-host wall clock, so it ships as a sort of the wave-2 job list. It moves
   the straggler's start from 449-967 ms to **150-177 ms**, and there the wall *is* the deadline plus
   the ramp: batches 1 and 2 fall to 5,187 / 5,160 ms and the phase to **16,266 ms**. On the bare arm
   it is inside the noise (20,280 vs 19,980-20,048) because one run's straggler (`be-rad.com`) had a
   *fast* wave-1 — the prior is good, not perfect.

## The instrument: the recorded 27.2 s is the sync path's, not the network's

`fetch_directory_pages` takes its async httpx branch only when the fetcher **is** `fetch_text`
(identity, not shape). The audit bench passes an instrumented callable, so its batches fall to
`asyncio.to_thread`, whose default executor is `min(32, cpu_count + 4)` = **20** threads on this
16-CPU box — the configured 72 lanes are unreachable there. The replay's `sync-shipped` arm is that
same path over the same job list and peaks at exactly 20 in flight; it lands at **27,768 ms**, 2.1%
above the recorded 27,184 ms. On the shipped async path the same list peaks at the full 72 and costs
**19,980-20,048 ms**.

So the previous snapshot's 43.5% stands as measured, and its remaining 27.2 s was **partly the
bench**: the ~7.8 s between the two paths is the fallback's 20-lane ceiling rather than the wave's own
work. From here the
phase to attack is the 20.0 s the shipped path actually spends.

## Limits

- **One population, one day.** All nine replay arms fetch the same 364 URLs (91 hosts) that the
  recorded run kept; live hosts move between arms and between days, which is why every claim above is
  made between arms of one artifact and the two key arms are run twice.
- **The deadline arms are a bench wrapper, not a shipped change.** They wrap the async fetcher in
  `asyncio.wait_for(..., timeout=timeout_s)`. What the wrapper proves is the size of the prize and the
  coverage it costs in this population; it does not prove the same for the static-listing adapters
  that share `fetch_pages_batched`, whose slow pages may be slow *and* successful.
- **The sync arm's 20-lane ceiling is this machine's.** `min(32, cpu_count + 4)` is 20 here and 32 on
  a 28-core host; the claim is about the ceiling's existence and its source, not one number.
- **Coverage is read-page counts, not candidates.** The wave-2 candidate yield was traced in the
  previous snapshot (1 of 98 recovery candidates, 0 of 16 active boards); this one prices time and
  pages read, and a page read is a precondition for a candidate rather than a candidate.
- **The wave-1 prior is a proxy for slowness**, not a model: one of six runs had a straggler with a
  fast wave-1, so the ordering lever's gain is real but not bounded below by this measurement.

## Reproducing the numbers

```bash
# the nine live arms: same 364 URLs, same shipped limits, same code path (~3 min, hits live hosts)
python _out/perf-runs/discovery/wave2_floor_bench.py

# every claim above, re-derived from the artifacts (fails if one moves)
python _out/perf-runs/discovery/verify_wave2_floor.py

# the recorded per-batch decomposition the table at the top comes from
python _out/perf-runs/discovery/analyze_recovery_wave_floor.py --labels secondary-cut-narrow
```

The arms, their log (`wave2-floor-arms.log`) and the recorded arms they replay are kept under
`_out/perf-runs/discovery/` (untracked). The harnesses live there on purpose: a `scripts/` edit is
shipped container code and trips the release gate, and these are measurement, not product behaviour.
