# The probe wave's bucket queues stopped holding its lanes — 2026-10-12

> - **Class:** shipped execution-path change in the discovery lane, priced before and after
> - **Trigger:** the probe-wall attribution snapshot measured what the wave's permits were doing and
>   found the lane budget spent on queues: with the pool acquired first, **6.8 of 40 pool permits**
>   were held by static candidates waiting on their own bucket, the provider bucket — 80% of the
>   population — probed at **21.0 of the 40 lanes** its cap allows, and swapping the two acquire lines
>   was worth −12% to −16% of the phase wall. That swap was **bench-only** because the summary of the
>   day also named the loop as the wall, and the transport change that removed the loop
>   ([`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md))
>   had to land first: with the loop no longer costing the phase anything, the pool became the *only*
>   binding resource, which is the condition under which a permit-order defect is worth real wall.
> - **Status:** shipped. `_probe_one` acquires the **bucket** permit before the **pool** permit.
>   Re-measured on the shipped sync-httpx transport over the audit's 3,038-candidate population at
>   `total = 40 / static = 16`, as **three balanced pairs**: **54.2 → 36.4 s**, **42.9 → 37.7 s**,
>   **45.6 → 36.3 s** (means **47.6 → 36.8 s**, 63.9 → 82.6 candidates/s; every pair improves, by
>   −12% to −33%). Pool-permit time spent waiting on a bucket seam falls from **371.9 s (21% of
>   pool-permit time) to 0**, all of it static's, and the provider bucket probes at **24.0 lanes**
>   against 20.1 (static 10.3 against 8.2). At the audit's 80-lane setting the occupancy effect is
>   larger (provider 24.8 → 35.6 lanes; 24.9 of 80 permits blocked) while the wall there is
>   **unresolved**: 43.0 → 33.9 s on means, but one of the two pairs reverses.
> - **No cap, deadline, verdict or result-order change.** The same politeness limits, the same pool
>   size, the same one-result-per-candidate contract, the same completion-order results; only the
>   permit that carries the wait moves. Two new deterministic contract tests pin it, and both are
>   verified to fail against the old order (see below).
> - **Verified against:** `9b8901f8` plus this session's uncommitted cuts. Changed here:
>   `src/source_discovery/probe_runtime.py`,
>   `tests/source_discovery/test_probe_batch_permits.py`.
> - **Then inspect:** [`discovery-probe-wall-attribution-2026-10-12.md`](discovery-probe-wall-attribution-2026-10-12.md)
>   for the defect this closes and the fetch-count lever that remains,
>   [`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md)
>   for the transport this was priced on. Instrument (untracked):
>   `_out/perf-runs/discovery/probe_permits_bench.py`, re-run logs
>   `_out/perf-runs/discovery/permits-remeasure{,-80l}.log`.

## The defect, and why it survived the first pass

The wave's two seams are a pool semaphore (`total`, 40 by default) and a per-family bucket semaphore
(`static` 16, `provider` 40, `teamtailor` 15). Held in the shipped order:

```python
async with total_sem:        # the pool: the wave's lane budget (40)
    async with bucket_sem:   # per-family politeness: static 16, provider 40, teamtailor 15
```

a candidate whose family is saturated **holds a pool permit while it waits**, and the buckets sum to
71 against a pool of 40, so the wait is not rare — it is the shape of the phase. Both kinds of
evidence agree on that: the instrumented histograms put **19.7% of one run's span at 24 of 40 permits
held by blocked candidates**, and an independent instrument (the lane windows of the wall bench)
saw the same thing as a plateau at 16 lanes with provider at zero.

The attribution snapshot priced the swap but left it bench-only, and that was the right call at the
time: the loop was costing 22.28 s of the phase's wall, so a 5 s permit saving sat inside a wall the
loop owned. Moving the request off the loop did not change the permit arithmetic at all — it changed
which resource binds, which is what makes the arithmetic visible in the clock.

## What shipped

- `_probe_one` in `src/source_discovery/probe_runtime.py`: `async with bucket_sem:` outside
  `async with total_sem:`, with the comment that carries the measurement.
- Nothing else. No cap moved (a bigger pool or a wider bucket is a **politeness** decision: 238 URLs
  in this population already answer 429), no deadline changed, no result reordering — `_run` still
  returns results in completion order, and every candidate still passes through both seams.
- The order is also the **deadlock-free** one: the pool is now acquired last, so no task can hold a
  pool permit and wait on a bucket. The old order had that shape; it did not deadlock only because
  every bucket holder eventually finished.

## The contract, and the proof the test can see it

Two deterministic tests in `tests/source_discovery/test_probe_batch_permits.py` (no sleeps on
the passing path; a failure costs a bounded 1.5 s wait):

| Test | What it pins |
|---|---|
| `test_a_bucket_blocked_candidate_cannot_hold_a_pool_permit` | pool 2, static 1: with static's one lane parked and a second static row queued, the provider row must start — under the old order it could not, because the queued static row was holding the second pool permit |
| `test_a_saturated_bucket_leaves_the_pool_to_the_other_families` | pool 4, static 1, four static rows against three provider rows: all three provider rows probe while static's queue is parked, and static's peak stays at its cap of **1** |

A contract test that passes under both orders is worth nothing, so both were run against a
text-patched pool-first twin of the shipped function
(`_out/perf-runs/discovery/verify_permit_order_tests_discriminate.py`):

```
[shipped (bucket-first)] 0 of 2 failed
[patched pool-first]     2 of 2 failed
    test_a_bucket_blocked_candidate_cannot_hold_a_pool_permit: the parked static probe never saw the
    provider row start: a candidate waiting on a full bucket is holding a pool permit, so the pool is
    spent on the bucket queue
    test_a_saturated_bucket_leaves_the_pool_to_the_other_families: three provider rows could not
    reach the pool while one static row held its bucket permit and three more static rows queued --
    the queue is holding pool permits
discriminating: True
```

## The phase, re-measured

`probe_permits_bench.py` instruments the semaphores themselves and stamps each candidate four times
(submitted, pool permit, bucket permit, done). Both arms run the **same** shipped function — the
order is a bench variable, text-patched, so the comparison stays available now that the swap is in
`src/`. Live replay of the audit's 3,038 candidates, `total = 40 / static = 16`, one batch each,
alternated A B B A A B:

| Pair | pool-first | bucket-first | Delta |
|---|---|---|---|
| 1 | 54.2 s | **36.4 s** | −32.8% |
| 2 | 42.9 s | **37.7 s** | −12.1% |
| 3 | 45.6 s | **36.3 s** | −20.4% |
| mean | 47.6 s (63.9 cand/s) | **36.8 s (82.6 cand/s)** | **−22.8%** |

and the permit attribution behind it (per-run means over the three runs per arm):

| Order | Pool permits held | Pool-permit time in a bucket wait | Provider probe lanes | Static probe lanes |
|---|---|---|---|---|
| pool-first | 36.3 of 40 | **371.9 s (21%)**, all static | 20.1 of 40 | 8.2 of 16 |
| bucket-first | 34.3 of 40 | **0 s** | **24.0 of 40** | **10.3 of 16** |

Three readings:

- **The lane budget stops being spent on queues.** The provider family — 80% of the population — was
  probing at half its permitted lanes while static's queue held the difference; it now probes at
  24.0, and static rises with it. That is the mechanism behind the wall, not a rearrangement of it.
- **The pool is still the binding resource.** Held permits only fall 36.3 → 34.3 of 40, because the
  fix does not add lanes: it stops spending them on nothing. The phase is pool-bound (84-92% of the
  span pinned at exactly 40 in both orders), which is the attribution snapshot's finding #1 and is
  deliberately **not** addressed here.
- **Verdicts are untouched.** Same candidate set, same one-result-per-candidate contract, and the
  bench drives the real `async_probe` — the difference is when each candidate starts, not what it
  concludes.

## The 80-lane arm: occupancy yes, wall unresolved

The same bench at `total = 80 / static = 32` (the setting the loop snapshot left "permit-bound"), two
balanced pairs, A B B A:

| Order | Wall | Pool permits held | Blocked (mean) | Provider probe lanes | Static probe lanes |
|---|---|---|---|---|---|
| pool-first | 35.5 / 50.5 s | 66.3 / 52.7 of 80 | **28.0 / 21.8** | 26.4 / 23.2 | 11.8 / 7.5 |
| bucket-first | 39.6 / 28.2 s | 45.7 / 53.6 of 80 | **0 / 0** | 34.0 / 37.2 | 11.4 / 16.0 |

At this setting the blocked permits are **provider's**, not static's — the provider cap (40) is now
the narrow seam against an 80-lane pool, so 19.4 permits (686.3 s) sat in that queue in the first run.
The occupancy effect is unambiguous: blocked permits 24.9 → 0 and provider probe lanes 24.8 → 35.6.
The wall is **not** resolved: means 43.0 → 33.9 s, but pair 1 reverses (35.5 → 39.6 s) and pair 2 does
not (50.5 → 28.2 s). Two pairs cannot separate a live 80-lane arm this noisy, and this snapshot does
not claim they do. Treat the 40-lane result as the shipped claim and the 80-lane arm as evidence that
the *mechanism* scales.

## Reproducer

```bash
# the phase, both orders, on the shipped transport (live; ~40-55 s per run)
PYTHONPATH=. python _out/perf-runs/discovery/probe_permits_bench.py --total 40 --static 16 --order pool-first
PYTHONPATH=. python _out/perf-runs/discovery/probe_permits_bench.py --total 40 --static 16 --order bucket-first
# the same at the audit's wider lane setting (~30-50 s per run)
PYTHONPATH=. python _out/perf-runs/discovery/probe_permits_bench.py --total 80 --static 32 --order bucket-first

# the contract tests, and the proof they discriminate
python -m pytest tests/source_discovery/test_probe_batch_permits.py -q
PYTHONPATH=. python _out/perf-runs/discovery/verify_permit_order_tests_discriminate.py
```

## Limits

- **Live network, one batch per run.** The phase is one batch of a frozen 3,038-candidate population,
  while a real audit probes three sequential 1,000-candidate batches; that ramp-and-tail cost is not
  decomposed here, so the bench is a floor on the audit's phase. This population also contacts real
  ATS hosts, so the run-to-run spread (pool-first 42.9-54.2 s) is real drift, and it is why the claim
  is quoted as three paired deltas rather than a single mean.
- **Two arms changed at once relative to the older baselines.** The tracked comparison (47.6 → 41.9 s,
  −12% to −16%) was measured on the async-httpx transport. These pairs are order-only and both arms
  are the shipped transport, so they are not comparable with those numbers; what *is* comparable is
  the direction and the permit attribution.
- **The bench's per-bucket interval is order-dependent.** `t_bucket - t_total` is the bucket wait in
  the pool-first order and the pool wait in the bucket-first one, so it prints with the opposite sign;
  the report now labels whichever interval is real, and the `blocked-by-bucket` histogram above
  (`max(0, pool permits - probing)`) is the figure that means the same thing in both orders.
- **The 80-lane wall is unresolved** at two pairs (one reverses), and the provider/static lane means
  move ~5% between runs. The mechanism scaling is the claim; the 80-lane clock is not.
- **Only the 40-lane measurement is a product-shaped setting** for the default configuration; the
  80-lane arm exists because the loop snapshot left it open.
- **The producer of the numbers is a bench-only text patch.** It asserts the acquire order it finds
  (both orders, one occurrence each), so a drifting function fails loudly rather than silently
  measuring something else — but the shipping path itself is exercised by the two contract tests, not
  by the bench.

## Earlier

This closes the permit finding from
[`discovery-probe-wall-attribution-2026-10-12.md`](discovery-probe-wall-attribution-2026-10-12.md)
and follows
[`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md)
(which is why the pool, not the loop, is what this fix buys). The lane-count repair under both is
[`discovery-directory-lane-ceiling-2026-10-12.md`](discovery-directory-lane-ceiling-2026-10-12.md),
and the plan this executed is
[`discovery-probe-permit-allocation-plan.md`](../plans/discovery-probe-permit-allocation-plan.md).
