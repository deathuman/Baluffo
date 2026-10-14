# The probe wave's HTTP work now runs on its pool, not on the loop — 2026-10-12

> - **Class:** shipped execution-path change in the discovery lane, with its price measured before and
>   after
> - **Trigger:** the probe-wall attribution snapshot found the wave's per-candidate cost was **8.4 ms of
>   event-loop CPU per candidate** (7.33 ms of it inside the HTTP client) and called the loop the next
>   wall. That reading needed a price before a change: which part of the client's work is on the loop,
>   and what does moving it cost.
> - **Status:** shipped. The default path's request now runs on the batch's own `probe-fetch` pool
>   through a **sync `httpx.Client`**, instead of `httpx.AsyncClient` reading and parsing responses on
>   the loop. The price of the obvious fix was wrong: handing `Response.text` to a thread moved
>   **0.26 s of 22.28 s** of loop CPU (‑1.2%), because a 178 KB fresh-host response costs **5.87 ms of
>   connection/headers, 2.11 ms of body handling and 0.22 ms of decode**. Moving the *whole request*
>   took loop CPU from **22.28 s → 4.97-5.80 s** (‑78%; 7.33 → 1.64-1.91 ms per candidate), the loop's share of
>   the phase from **42.6% → 9.5-12.3%**, total process CPU from **23.14 s → 16.53-18.41 s**, and the
>   shipped phase from **52.28 s → 45.5-53.9 s** across runs (56-67 candidates/s) with **identical
>   verdicts** (1,840-1,843 failures, 1,698 HTTP 404s in every run). The wall is the weak half of that:
>   a later re-run of the shipped code on the same settings read **53.89 s**, so at 40 lanes the time
>   effect is inside run-to-run drift while the CPU effects replicate in every run (see the limits). At the audit's wider 80-lane setting the same change moves
>   loop duty **72.7% → 14.7%** but the wall not at all (34.57 → 35.4 s), because that arm was never
>   loop-bound: it was permit-bound, which the permit-order change then addressed on the occupancy
>   (provider probe lanes **24.8 → 35.6** of 40) while leaving that arm's clock unresolved; see
>   [`discovery-probe-permit-order-2026-10-12.md`](discovery-probe-permit-order-2026-10-12.md).
> - **The ceiling question, answered:** the loop's *own* limit rises from ~136 to ~585 candidates/s,
>   but the phase does **not** exceed its CPU-bound plateau. On a loopback replay where the loop was
>   the last shared resource, the shipped code saturated at **85.3 candidates/s with the loop 95.5%
>   busy**; the new code reaches **104.6 candidates/s with the loop at 58%** — and there the limit is
>   the whole process, not the loop: **Python work is GIL-serialized, and process CPU for the replay
>   sits at ~0.9-1.0 core either way**. Per candidate, total process CPU is **7.61 ms → 5.44-6.06 ms**, so
>   the phase's CPU ceiling rises ~131 → ~170 candidates/s: past that, the next change must *cut*
>   Python work per candidate (the body parse), not relocate it.
> - **Verified against:** `9b8901f8` plus this session's uncommitted cuts. Changed here:
>   `src/source_discovery/probe_runtime.py`, `src/source_discovery/web_search_fetch.py`,
>   `tests/source_discovery/test_probe_runtime_bounded_fetch.py`.
> - **Then inspect:** [`discovery-probe-wall-attribution-2026-10-12.md`](discovery-probe-wall-attribution-2026-10-12.md)
>   for where the wall is (this change removes the part of it that lived on the loop),
>   [`discovery-directory-lane-ceiling-2026-10-12.md`](discovery-directory-lane-ceiling-2026-10-12.md)
>   for the lane repair this builds on. Instruments (untracked): `probe_loop_cost_bench.py`,
>   `probe_httpx_loop_cpu_probe.py`, `probe_loopback_ceiling_bench.py`.

## The price, before writing anything

`time.thread_time()` on Windows accrues per scheduling quantum (~15.6 ms), so single-request samples are
quantised noise; `probe_httpx_loop_cpu_probe.py` therefore measures **aggregates** over 150-200
requests per group and splits one request into three cumulative stages that differ by one piece of work
each (`stream=True` headers only, then `aread()`, then `Response.text`):

| Request shape | Connection + headers | + body transfer | + decode | Total loop CPU | Body |
|---|---|---|---|---|---|
| ATS API, kept-alive host (20 of 20 were 404s or small JSON) | 1.69 ms | 0.42 ms | 0.00 ms | **2.11 ms** | 0.6 KB |
| studio homepage, fresh host | 5.87 ms | 2.11 ms | 0.22 ms | **8.20 ms** | 178.7 KB |

The decode is **2.7%** of a fresh-host page's loop cost, and the connection/header half (the TLS
handshake, the certificate verification, the HTTP/1.1 setup) is the single largest piece. The
phase-level control agrees: an arm that kept the async client but handed `Response.text` to a thread
measured **22.02 s of loop CPU against 22.28 s** (‑1.2%) and **51.12 s against 52.28 s** of wall — a
change nobody should ship. So the whole request moves, and the client model changes with it.

## What shipped

- `web_search_fetch.fetch_text_httpx(client, url, timeout_s)` — `async_fetch_text_httpx`'s blocking
  twin, same request shape (discovery headers, `follow_redirects=True`), same `raise_for_status` and
  `encoding or "utf-8"` handling, and the same trust: `httpx.Client` and `httpx.AsyncClient` both
  default to **certifi**, so the hosts the async path can read stay readable (the reason the
  trust-anchor repair exists) and no caller gets a different certificate story.
- `probe_runtime.run_bounded_probe_batch_async` — the `probe-fetch` pool is now **unconditional**
  (it used to exist only when the caller passed a wrapping fetcher, which is what the lane ceiling was
  about), the batch creates **one shared `httpx.Client`** for the default path, and `_call_fetch`
  submits *every* path through that pool under the same per-call `asyncio.wait_for` bound. A shared
  `httpx.Client` is documented thread-safe and behaved as such here: 40 threads, the same 3,038
  candidates, the same verdict counts. The batch deadline, the one-result-per-candidate contract, the
  `pool.shutdown(wait=False)` retirement and the `fetch timed out after Ns` wording are unchanged.
- Tests: the old `test_default_fetcher_path_still_uses_the_httpx_client` asserted the default path
  calls the **async** client — it now asserts the stronger contract (the request runs on a
  `probe-fetch` thread that is *not* the loop thread, and the client it hands the helper is an
  `httpx.Client`), plus a new lane contract that the default path reaches the **configured lane count**
  rather than the shared executor's `min(32, cpu_count + 4)`.

## The phase, measured

Live, full 3,038-candidate population, identical timeouts and lane settings
(`probe_loop_cost_bench.py`; "before" is the async-client tree, "after" is this change):

| Path | Lanes | Wall | Cand/s | Loop CPU | Loop share | Loop ms/cand | Process CPU | Mark lag p95 |
|---|---|---|---|---|---|---|---|---|
| before (async httpx on the loop) | 40 | 51.58 / 52.28 s | 58.1-58.9 | 22.28 s | 42.6% | 7.33 | 23.14 s | 3.68 ms |
| after (httpx on the pool), 3 runs | 40 | **45.46 / 47.2 / 53.89 s** | 56.4-66.8 | **4.97-5.80 s** | **9.5-12.3%** | **1.64-1.91** | **17.30-18.41 s** | **0.82-1.17 ms** |
| before | 80 / 32 | 34.57 s | 87.9 | 25.12 s | 72.7% | 8.27 | 26.25 s | 12.37 ms |
| after | 80 / 32 | 35.4 s | 85.8 | 5.19 s | 14.7% | 1.71 | 17.59 s | 1.37 ms |

The wall column is the honest spread, not a single pair: the before arm was measured twice (51.58 s on
`probe_wall_bench.py`, 52.28 s here) and the after arm three times (45.46 s on the bench-local sync
control, 47.2 s on the first re-run, 53.89 s on the final-code re-run). The CPU columns replicate
across all of them; the wall does not separate from drift at this sample count.

Three readings worth keeping:

- **Loop occupancy collapses at the shipped lane count**: 42.6% → 9.5-12.3% of the wall, 7.33 → 1.64-1.91
  ms per candidate, and the batch's own scheduling lag (p95) from 3.68 ms to ~1 ms. Verdicts are
  unchanged: 1,840-1,843 failures and 1,698 HTTP 404s in every run; the 429/403 counts move by one or
  two from live churn, not from this change.
- **At 80 lanes the wall does not move.** That arm was already permit-bound (40.5 lanes in flight of
  80), so removing the loop cost buys headroom rather than time — the ordering/permit finding from
  [`discovery-probe-wall-attribution-2026-10-12.md`](discovery-probe-wall-attribution-2026-10-12.md)
  is what a wall improvement at 80 lanes needs, and shipping it (see
  [`discovery-probe-permit-order-2026-10-12.md`](discovery-probe-permit-order-2026-10-12.md)) moves
  that arm's provider probe lanes **24.8 → 35.6** while leaving its clock unresolved at two pairs.
- **Total Python CPU per candidate falls ~20-29%** (40 lanes: 7.61 → 5.44-6.06 ms; 80 lanes: 8.64 →
  5.79 ms), which is real work removed, not work relocated: the async client's per-chunk coroutine and
  task machinery costs more than the sync client doing the same job on a thread.

## The ceiling

`probe_loopback_ceiling_bench.py` points all 1,000 replayed candidates at a local server holding a real
captured page (440 KB of HTML) with the structured-adapter POSTs stubbed out, so no third-party host
sees loopback traffic. With the network out of the picture the loop is the last shared resource:

| Replay | Wall | Cand/s | Loop CPU | Loop share | Loop ms/cand | Process CPU |
|---|---|---|---|---|---|---|
| before | 11.73 s | 85.3 | 11.20 s | **95.5%** | 11.20 | 12.23 s (1.04 core) |
| after | 9.56 s | **104.6** | 5.56 s | **58.2%** | 5.56 | 8.47 s (0.89 core) |

The shipped code saturated the loop (95.5% busy) at 85 candidates/s; the new code runs the same replay
23% faster with the loop more than a third idle — and now the limit is the whole process, at **0.89
core**. Python bytecode, `h11` parsing and the probe's own HTML analysis all hold the GIL, so the
phase's CPU-bound plateau is `1 / process-CPU-per-candidate`: **131 candidates/s before, ~165-184
after** (the last-repeated run puts it at 173; this replay's oversized body makes it 85 → 105, and thelive population's lighter mix is what the 131 → ~170 range describes). The loop ceiling this change
replaces is the same number read two ways: the attribution snapshot's **8.4 ms of loop CPU per candidate
(~121 candidates/s)** comes from the *whole* audition's loop time, this bench's **7.33 ms (~136/s)** from
the probe batch alone — either way the loop ceiling is now several hundred per second, and the point is
that it stopped being the phase's. Above the ~170 figure the change that matters is cutting per-candidate
Python work — the body parse — not adding lanes or moving work to another thread.

## Reproducer

```bash
# per-request loop CPU, split connection / body / decode (live, ~1 min)
PYTHONPATH=. python _out/perf-runs/discovery/probe_httpx_loop_cpu_probe.py --per-group 150

# the phase's loop CPU and duty, shipped lanes and the wider arm (live, ~45-55 s per arm)
# `--arm async` is the shipped default path (the label predates this change); `--arm sync-httpx`
# swaps in a bench-local sync client to confirm the shipped path and that control agree.
PYTHONPATH=. python _out/perf-runs/discovery/probe_loop_cost_bench.py --arm async
PYTHONPATH=. python _out/perf-runs/discovery/probe_loop_cost_bench.py --arm async --total 80 --static 32

# the CPU-bound plateau against a loopback server (no third-party traffic)
PYTHONPATH=. python _out/perf-runs/discovery/probe_loopback_ceiling_bench.py --mode sync --limit 1000
```

## Limits

- **The replay's body is 440 KB for every candidate**, against a live mix of ~200 KB homepages and
  sub-kilobyte ATS JSON. That inflates the parse in both replay rows and is why the replay's plateau
  (85 → 105/s) sits below the live-derived ceiling (131 → 173/s).
- The two replay rows are **not** a controlled pair: the "before" row was measured with an in-process
  server, which contends for the GIL and lands its own CPU in our process total. It was saturated on
  the loop (95.5%), so the interference can only have pushed that row down; the derived ceilings
  (1 / loop-ms-per-candidate) come from the live arms and are unaffected.
- **Orphaned request threads now exist on the default path too.** A sync request cannot be cancelled,
  so a fetch that outlives its `wait_for` bound keeps its thread until its own socket timeout, and that
  thread holds a pool slot. This is the trade the sync branch already documented; the async client
  cancelled instead. Nothing on this population showed it — the drip test's batch returns inside its
  bound, and the wall *fell* — but a population of trickling boards could delay later lanes. **Who
  outlives whom was the open half of that trade, and it has since been measured and fixed:** the batch
  used to retire its shared client at exit, under a fetch that was still reading (closed with
  `in_flight=1`, the request dying with a socket error 0.1 ms later), and now closes only once the last
  fetch has left it — see
  [`discovery-probe-client-lifecycle-2026-10-12.md`](discovery-probe-client-lifecycle-2026-10-12.md).
  The abandoned thread's own lifetime is unchanged.
- **The live wall drifts ±8% between runs**, which is the same order as this change's time effect at 40
  lanes (before: 51.58, 52.28 s; after: 45.46, 47.2, 53.89 s; 80 lanes: 34.57 → 35.4 s). A tighter
  wall claim needs interleaved paired runs, which this snapshot does not have: the pre-change code was
  replaced in the working tree before the drift was visible. The loop-CPU, loop-share, process-CPU and
  lag numbers above are consistent in every run and are what the change is credited with.
- The 80-lane arm raises neither bucket cap; it is a measurement setting, not a recommended one.
