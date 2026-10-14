# The directory page fetch now runs at the concurrency it was given — 2026-10-12

> - **Class:** shipped shared-layer repair, with the audit's own instrument re-measured phase by
>   phase
> - **Trigger:** the two 2026-10-12 snapshots that decomposed the audit's biggest stages both found
>   the same thing under their numbers: the recorded occupancy sat on **20 in-flight of 32**
>   configured in the homepage fetch, and **18.7 of 72** in the first recovery wave. The cause is
>   one line — `fetch_directory_pages` takes the async httpx branch only when its fetcher is
>   `fetch_text` **by identity**, and anything else falls to `asyncio.to_thread`, i.e. the event
>   loop's shared default executor, sized `min(32, cpu_count + 4)`.
> - **Status:** shipped — the sync path gets a pool this module sizes to the batch, so
>   `total_concurrency` is what the caller gets on **both** paths. The same 1,000-homepage batch at
>   configured 32 lanes: peak in-flight **20 → 32**, mean **18.1 → 26.8**, wall **39,174 → 27,879 ms
>   (−28.8%)**. In situ the audit's directory phases now peak at their configured 32 and 72 lanes
>   instead of 20, and its run total falls **443.3 → 275.3 / 326.6 s (−38% / −26%)** on the same 3,000
>   studios, with its work and candidates unchanged. The probe wave carried the same cap and now runs
>   on a pool of its own: its fetch windows go from **pinned at 20** (10.9% / 14.7% of the phase's
>   span at exactly that level, nothing above it) to **peaks of 36 / 34 of the configured 40 with no
>   plateau** — not saturation, which this phase's own bucket caps and per-candidate work do not
>   demand, so the sweep checks the cap's fingerprint there and keeps the pre-repair pair as the
>   control that proves the check fires.
> - **Verified against:** `9b8901f8` plus the uncommitted cuts, including this session's trust-anchor
>   repair — the retained `trust-anchor` arm is the same tree *without* this change, which makes it
>   the control pair.
> - **Then inspect:** `src/shared/http_batch.py` (`_fetch_pages_batched_async`), and
>   `tests/test_shared_http_batch.py` for the lane contracts and the source guard that keeps the cap
>   from coming back; `tests/source_discovery/test_directory_fetch_lane_guard.py` sweeps the
>   configured lane counts through a wrapping fetcher. The probe wave's own cap is repaired here too —
>   a `probe-fetch` pool sized to the batch, with a lane contract and a source guard over every
>   discovery module — and the sweep re-derives the probe's fingerprint from the re-recorded pair
>   while requiring the pre-repair pair kept under `audit-runs/pre-probe-pool/` to still show it.

## What the fallback did

`fetch_directory_pages` builds its batch like this:

```python
sync_fetch=lambda job, url, _timeout_s: fetcher(url, _timeout_s),
async_fetch=(... if fetcher is fetch_text else None),
```

so the branch is chosen by **object identity**, and a wrapper — even one that only records timings —
lands on the sync path. `_fetch_pages_batched_async` then ran each sync fetch through
`asyncio.to_thread`, which submits to the loop's *shared* default executor: `min(32, cpu_count + 4)`,
so **20** on the 16-core box these runs use. Nothing raised, nothing logged, and the semaphore that
was supposed to bound the batch politely waited on a pool that could never feed it.

The retained arms show it as occupancy, computed from each phase's own fetch log
(`Σ per-request ms / phase wall`):

| Retained arm (before this change) | Homepage fetch (32 configured) | Wave 1 (72) | Wave 2 (72) |
|---|---|---|---|
| `tailfull-shipped` | 13.4 | 18.7 | 18.1 |
| `secondary-cut-narrow` | 17.4 | 18.7 | 13.1 |
| `trust-anchor` | 17.8 | 18.9 | 14.3 |

Every phase sits at or below 20 whatever it was configured for. The 72-lane recovery wave ran at
roughly a **quarter** of its budget, which is why its wall was attributed to stragglers by the
retained snapshot: on this fallback, "one slow request holds the batch" and "the batch cannot use its
lanes" look the same from the outside.

## The change

`src/shared/http_batch.py` now owns a `ThreadPoolExecutor` per batch, sized
`max(1, min(total_concurrency, len(jobs)))`, and submits through `loop.run_in_executor` on that pool.
The async path is untouched (it is already one coroutine per lane). Two consequences worth stating:

- **The lane count is the caller's budget on both paths.** There is no fallback to a smaller pool,
  and no branch of the code depends on the machine's core count.
- **The batch deadline is real on the sync path.** The pool is created for the batch and retired at
  the end; if the deadline fires, the pool is abandoned (`wait=False`) rather than waited on, because
  a sync fetch cannot be cancelled and `wait=True` would hold the phase open for exactly the
  straggler the deadline exists to walk away from. Previously the loop's shutdown waited for those
  threads, so the deadline bounded the async path's wall and not the sync path's.

`tests/test_shared_http_batch.py` pins both: 40 configured lanes must peak at 40 *and* must run on
threads named `page-fetch` (i.e. not the shared default executor), and a batch whose sync fetch
sleeps 5 s against a 2 s deadline must return inside the deadline with every row reported as a
deadline failure.

## The batch, measured

`homepage_floor_bench.py` replays one recorded batch of 1,000 homepages. `sync-*` arms pass a custom
fetcher (the capped path); the `async-32` arm passes `fetcher=fetch_text` and is served by httpx, so
it never touches this code — a control that sizes run-to-run drift.

| Arm | Readable | Wall | Mean in-flight | Peak | Peak of configured | Σ request time |
|---|---|---|---|---|---|---|
| `sync-20` recorded (pre-anchor, pre-fix) | 761 | 42,090 ms | 18.1 | 20 | 63% | 763,774 ms |
| `sync-20` (post-anchor, pre-fix) | 940 | 39,174 ms | 18.1 | **20** | 63% | 707,503 ms |
| **`sync-32` (shipped)** | 939 / 905 | **27,879 / 27,824 ms** | **26.8 / 26.7** | **32** | **100%** | 746,252 / 743,269 ms |
| `async-32` control | 891 / 855 | 20,607 / 21,341 ms | 24.8 | 32 | 100% | 510,443 / 530,206 ms |

The two shipped rows and the two control rows are the same measurement repeated (the first on the
in-tree change, the second on the final tree after a variable rename). The 32-lane rows share the
same budget and the same 1,000 pages, so the comparison that matters is `sync-20` → `sync-32`:
**−11.3 s (−28.8%)** with the fetch count unchanged (939 vs 940 readable) and the certificate residue
unchanged. The sync arm is still the slower mechanism (27.8 s against httpx's 21.3 s on *more* read
pages), which is a thread-vs-coroutine difference, not a cap — see the limits.

## In situ: the audit's phases

`audit_probe_bench.py --arm lane-ceiling --batch-size 1000 --max-batches 3 --timeout 6`, the same
instrument and population as the retained arms, both arms re-recorded after the probe repair, with
per-request timestamps recorded so each phase's in-flight occupancy can be counted directly. The
pair recorded *before* that repair is retained beside it under `audit-runs/pre-probe-pool/`: its
directory phases read the same shape as the pair below, and its probe phase is that section's
control:

| Phase | Configured lanes | Before: mean (share) | After: mean (share), two runs | After: peak | Phase wall before → after |
|---|---|---|---|---|---|
| homepage fetch | 32 | 17.8 (56%) | **24.8 / 24.6** (77% / 77%) | **32** | 127,198 → **74,614 / 95,221 ms (−41% / −25%)** |
| recovery wave 1 | 72 | 18.9 (26%) | **51.8 / 54.7** (72% / 76%) | **72** | 140,846 → **55,348 / 55,752 ms (−61% / −60%)** |
| recovery wave 2 | 72 | 14.3 (20%) | **27.2 / 21.4** (38% / 30%) | **72** | 25,651 → **15,692 / 25,156 ms (−39% / −2%)** |
| whole run | — | — | — | — | 443,335 → **275,265 / 326,607 ms (−38% / −26%)** |

Every phase in this table now reaches its configured lane count, and the phase whose budget was
most ignored (the 72-lane recovery wave, running at 19) is the one that gains most: **−85.5 /
−85.1 s**. The work is
unchanged — 2,763 → 2,689 and 2,809 homepages read, 5,072 → 4,868 and 5,264 recovery requests, and
the probe failure count 1,857 → 1,839 / 1,862 — while the candidates drift with the live population
the retained arms themselves show (static candidates 598 → 589 and 602 here, against 494–598 across
the three retained runs; active boards 106 → 109 and 106). The probe's own wall moves 92.5 →
87.1 / 96.0 s and so straddles the pre-repair pair: this phase's clock is candidate work and host
latency rather than its lane count (see below). 429s drift with the hosts rather than with the lane
count — homepage 58 → 135 and 11, wave 1 106 → 71 and 152, wave 2 156 → 136 and 304, so the two arms
of the *same* pair differ by more than the change does: the per-host semaphore, not the lane count, is
what paces a single studio's host.

## The probe wave carried the same cap

Widening the verify sweep to *every* fetch phase the audit records turned up a second instance of the
same defect class: in the pair recorded just before this repair, `probe_candidates` peaked at exactly
**20** — the shared default executor's size on this box — against its configured **40**
(`probe_concurrency_defaults()["total"]`), in **both** arms, with **10.9% / 14.7%** of the phase's
span pinned at that level and nothing above it. `probe_runtime.run_bounded_probe_batch_async` sent a
non-default fetcher through `asyncio.to_thread(fetcher, url, call_timeout_s)` too, so a wrapping
fetcher's probe fetches queued on the interpreter's shared default executor while the probe's own
semaphores (40 total, 16 static) waited behind them. The app passes `fetch_text` by identity here as
well (`orchestrator_probe._run_probe_batch` → `deps.fetcher`, defaulting to `fetch_text`), so
production's probe takes the httpx branch and is not capped; what is capped is again the bench's
instrument. The repair is the same shape as this one and is shipped here: a `probe-fetch` pool sized
to the batch's lanes, with a lane contract beside the batch and a source guard over the whole
discovery lane. The browser recovery wave (configured at 2 lanes) had the same shape and now sizes its
own `browser-fetch` pool.

Re-recorded on that repair, the same two arms read **peak 36 / 34 of 40**, with **0.0%** of the
phase's span at the peak level, across 3,664 and 3,731 probe fetches: the plateau is gone, and so is
the ceiling under it. What the phase does *not* do is saturate its budget, and the sweep no longer
pretends otherwise: a candidate's *lane* is its whole probe (a fetch is one step of it), so the
candidate-held lane mean is **27.7 / 27.2 of 40** over the phase's own wall, and the probe's own
per-bucket caps sit below the total (static 16, teamtailor 15). A peak-of-40 floor in situ would
therefore assert the workload rather than the lanes. So the probe's in-situ contract is the
*fingerprint of the cap* — a peak at or under the shared executor's 20, or a plateau at any single
level below the configured lanes (5% of the phase's span) — and the pre-repair pair is retained under
`audit-runs/pre-probe-pool/` as the control that must still exhibit it (peak 20, 10.9% / 14.7%), so
the check is shown to fire on recorded artifacts rather than assumed to. The configured lanes
themselves are asserted offline, on a batch that does have the demand for them:
`test_sync_probe_fetches_run_at_the_configured_lanes_not_the_shared_executor` drives 40 provider-bucket
candidates through a blocking fetcher and fails unless the peak is all 40, on threads named
`probe-fetch`. Lifting the cap does not shorten the phase's clock — 92.5 → 87.1 / 96.0 s, straddling
the capped pair — because the probe's wall is candidate work and host latency, not lane starvation.

## What this does not change

- **Production's discovery fetches already took the async branch.** The app passes
  `web_search_fetch.fetch_text` through by identity (`orchestrator.run_discovery`'s default, and
  `gamesmap`'s re-export, verified `is`), so the audit in the app ran at its configured lanes and its
  recorded numbers were never capped — what was capped is **the bench's reproduction of it**, and with
  it every measurement decision taken from that bench. That is the honest scope of this fix: the
  instrument, plus every caller that wraps the fetcher. The 33% run-total cut above is therefore **the
  bench's number becoming honest**, not a 33% production speedup — the app's own audit already ran
  those lanes, and any earlier decision taken from this bench was taken on a run that could not use
  them.
- **Those phase walls are the bench's, not the app's.** What production gains from this change is the
  jobs-lane path below, not the audit's clock.
- **One production path was capped.** `static_listing_traversal` passes only `sync_fetch`, so the jobs
  lane's detail fetch always took the sync path; a run configured above `min(32, cpu_count + 4)`
  (`--static-detail-concurrency`, default 10) was silently clipped at that executor size.
- **The async branch is still selected by identity**, and a caller that supplies both a sync and an
  async fetcher still gets the async one. What no longer happens is paying a *concurrency* penalty for
  wrapping.
- **The lane ceiling is now the configuration's**, which makes the config load-bearing: 72 means 72
  real lanes. Politeness is still the per-host semaphore's job (`perHostConcurrency` 3 for the
  homepage phase, 4 for recovery), and the counters to watch are the 429s.

## The guard

The caps were single calls, so the guards are call-shaped and live beside the contracts they protect.
Each was checked against the regression rather than only against the fix.

- `test_shared_page_batch_module_never_submits_to_an_executor_it_did_not_size`
  (`tests/test_shared_http_batch.py`) parses the module and fails on `asyncio.to_thread(...)`, on
  `run_in_executor(None, ...)`, on a `ThreadPoolExecutor` built without `max_workers`, and on a pool
  that lost its `page-fetch` thread prefix. It reads every call in the module, including paths no
  test drives.
- `test_fetch_directory_pages_wrapped_fetcher_runs_every_configured_lane`
  (`tests/source_discovery/test_directory_fetch_lane_guard.py`) drives `fetch_directory_pages` with a
  *wrapping* fetcher -- not `fetch_text` by identity, which is the shape that took the sync path -- at
  every lane count the bundled config actually configures (`fetchConcurrency` 24, the homepage phase
  32, the recovery wave 72), and fails naming any batch whose peak in-flight count is not its
  configured lanes, or whose fetches ran on threads that are not this module's `page-fetch` pool.
- `test_lane_bounded_batch_modules_run_sync_work_on_a_pool_they_size` and
  `test_discovery_lane_has_no_unlisted_shared_executor_submission`
  (`tests/source_discovery/test_lane_batch_executor_guard.py`) read every discovery module (plus
  `src/shared/http_batch.py`): no batch owner may submit to the shared default executor, every pool a
  batch owner creates must be sized and carry its declared prefix (`page-fetch`, `probe-fetch`,
  `browser-fetch`), and a new submission anywhere in the lane fails until it is recorded. The only
  submissions it still allows are the two per-candidate helpers in `probe.py` -- the Workday CXS POST
  and the playwright fallback, neither a batch's own lane handoff -- recorded with exact counts so the
  record cannot go stale.

With `asyncio.to_thread` put back in `_call_fetch`, the source guard fails on the call and the sweep
fails with `activeAuditRecoveryFetchConcurrency: peak 20 in-flight of 72 configured lanes; ...
fetches ran on ['asyncio_0', ..., 'asyncio_19']`. The recovery lane count is above the shared
executor's `min(32, cpu_count + 4)` ceiling on every machine, and the thread-name check catches a cap
at any lane count *below* that ceiling -- both, so that the guard does not depend on the core count of
the box it runs on.

The jobs lane's detail fetch takes the same sync path through the same primitive (its caller passes
only `sync_fetch`), so the 40-lane contract on `fetch_pages_batched` covers it from the other side:
that batch's configured lane count is a CLI value (`--static-detail-concurrency`, default 10), which
is why the guard is stated on the primitive rather than on either caller's config.

## Limits

- **Threads are not coroutines.** The sync path at 32 lanes is 35% slower than httpx at 32 lanes on
  the same pages, so a bench that wraps the fetcher still measures a *different* mechanism than the
  app runs; it now measures the same *lane count*. Letting a wrapping caller keep the async path is
  the follow-up this leaves open, and it needs an `async_fetch` seam through
  `fetch_directory_pages`, not a rule about fetchers.
- **The in-situ pair is one arm each.** `trust-anchor` and `lane-ceiling` differ only by this change,
  but they are two live runs; the per-phase occupancy numbers are mechanical (timestamps), while the
  walls carry same-day drift. That drift is bounded here: the three retained arms run 362.1–443.3 s
  on this population, and the two after-arms run 275.3 and 326.6 s — both outside that band, and the
  per-phase occupancy (17.8 → 24.8/24.6 mean, a peak of 32, on the homepage phase) is measured from
  timestamps rather than inferred.
- **Per-batch pools cost thread churn**, sized to the batch and retired at the end. The jobs lane's
  batches are small (listing batches by page count, detail batches by `static_detail_concurrency`), so
  the churn is bounded by config; nothing pools across batches.
- **The default executor is no longer used on this path at all**, so a long sync batch no longer
  competes with anything else in the process for those threads — and conversely, a run that
  configures many lanes will now actually open that many connections, which is a politeness question
  for the operator rather than for this module.

## Reproducing the numbers

```bash
# the same batch at its configured 32 lanes, with the async control (~1 min)
python _out/perf-runs/discovery/homepage_floor_bench.py --arms sync-32,async-32 --batches 0 \
    --out _out/perf-runs/discovery/homepage-floor-lanes.json

# every phase's occupancy in situ (~13 min, three batches of 1,000)
python _out/perf-runs/discovery/audit_probe_bench.py --arm lane-ceiling \
    --batch-size 1000 --max-batches 3 --timeout 6

# the retained arms' occupancy, the batch peaks, every directory phase's peak against its configured
# lanes, and the probe wave's cap fingerprint against its retained control, all re-derived (fails if
# a stated invariant moves -- including the control, which must go on showing the cap)
python _out/perf-runs/discovery/verify_directory_lane_ceiling.py

# the shipped contracts: the page batch, the configured-lane sweeps, and the lane executor guard
python -m pytest tests/test_shared_http_batch.py tests/test_shared_http_batch_total_deadline.py \
    tests/source_discovery/test_directory_fetch_lane_guard.py \
    tests/source_discovery/test_lane_batch_executor_guard.py -q
```

The retained arms, this arm's per-phase timestamps and the bench outputs live under
`_out/perf-runs/discovery/` (untracked), on purpose: a `scripts/` edit is shipped container code and
trips the release gate, and these are measurement, not product behaviour.
