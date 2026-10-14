# The audit stops re-guessing a host whose own careers guess answered — 2026-10-12

> - **Class:** measurement + one shipped cut, with a wider variant measured and left on the shelf
> - **Trigger:** the two probe-phase cuts of 2026-10-12 took the GameDevMap audit's probe stage; the
>   retained full-run reports then show what holds the lane's clock next — the homepage fetch at
>   **32.5%** and the two recovery waves at **36.6%** of a 425 s audit, with the second wave spent on
>   four blind guesses per host.
> - **Status:** shipped — the second wave is queued only where wave 1 left the question open:
>   **364 of 1,304 requests (−72.1%)**, phase wall **48,075 → 27,184 ms (−43.5%)**, its analysis
>   **4,810 → 85 ms**, priced at **1 of 98 recovery candidates (not an active board), 0 of 16 active**.
> - **Verified against:** `9b8901f8` plus the uncommitted board-absence and timeout-ladder cuts (all
>   three changes are in every arm below; the arms differ only in the secondary-wave rule).
> - **Then inspect:** `src/source_discovery/active_audit_batch_runtime.py` (the wave-2 filter),
>   `src/source_discovery/directory_page_recovery.py` (`generated_answered_recovery_homepages`,
>   `generated_common_path_not_found_homepages`), and
>   `tests/source_discovery/test_active_audit_runtime_batch.py` for the three-case contract.

## What the recovery phase costs, from the retained attempt logs

Every arm keeps a per-URL fetch log with phase, status and duration, so the two waves are countable
directly. On the audit's own population (pinned CSV, 3,000 studio rows in three batches), shipped arm:

| Phase | Requests | Read (HTTP 200) | Σ request time | Phase wall |
|---|---|---|---|---|
| wave 1 (`/careers`, `/jobs`, plus every job-ish link the homepage carries) | 3,740 | 638 (**17.1%**) | 2,008.6 s | 107,697 ms |
| wave 2 (four alternative path guesses, generated only) | 1,304 | 802 (**61.5%**) | 868.0 s | 48,075 ms |
| wave 2's analysis of those pages | — | 802 pages | — | 4,810 ms |

The two guesses in wave 1 are largely the same question asked twice, and the data says so: of the
1,803 hosts that got both, **211 answered both, 43 answered only `/careers`, 42 answered only
`/jobs`, and 1,507 answered neither** — the whole wave is dominated by hosts that have no careers page
under either name. Wave 1 still earns its 3,740 requests: it produces **66 of the run's 98
recovery-sourced candidates** (36 attributed to a `/jobs` page, 30 to `/careers`) and **all 16** of
the recovery-sourced active boards.

Wave 2's own yield is where this change looks: **1,304 requests over 326 hosts, 802 pages read, and
one candidate by URL-path attribution — a `zero_jobs` static page, no active board**, in each of the
two full arms (`legacy` 101 candidates, `shipped` 98). Differential comparison against the truncated
arms below puts the *true* wave-2 yield at three candidates in one run (the artifact does not record
which page produced a provider candidate; tracing each difference through the fetch logs is what
resolves it — see the price table).

## The cut: wave 2 runs only where wave 1 left the question open

`generated_answered_recovery_homepages` returns the homepages where a **generated** wave-1 guess was
read. The wave-2 filter now skips those, beside the rule that was already there:

| Wave 1's answer for a host | Wave 2 |
|---|---|
| every generated guess 404'd **and** nothing else answered | skipped (existing `generated_common_path_not_found_homepages`) |
| a generated guess **answered** | **skipped (this change)** — the site has a careers page we have read and found nothing on |
| a generated guess could not be read at all (403/5xx/timeout) | still fetched — the host has not been heard from |
| all generated guesses 404'd, but a page the homepage itself links answered | still fetched — the site has job-ish pages and we have not found the listings one |

The rule keys on the *source* of the wave-1 request (`generated_common_path` vs `html_jobish_link`),
not on the URL, so a homepage that links `/careers` is not mistaken for the guess.

`batch_timing["recoverySkippedByGeneratedAnswered"]` records what the rule skipped per batch (360 /
424 / 408 in the shipped arm), so a future run can see the rule working without a harness.

## The replay, on the arms that ran before the change

`verify_secondary_wave_cut.py` recomputes both variants from the retained logs and fails if a stated
invariant moves (host-level matching stands in for the payload's `homepageUrl`, which a fetcher log
does not carry):

| Retained arm | Wave-2 requests | Shipped rule cuts | Σ cut | Kept | Wider rule cuts | Σ cut |
|---|---|---|---|---|---|---|
| `tailfull-legacy` (418,967 ms) | 1,304 | **948 (72.7%)** | 535,959 ms | 356 | 1,124 | 652,344 ms |
| `tailfull-shipped` (425,323 ms) | 1,304 | **928 (71.2%)** | 502,135 ms | 376 | 1,104 | 611,202 ms |

## In situ: the shipped rule and the wider one, over the same population

Two live runs of the audit bench (`--batch-size 1000 --max-batches 3 --timeout 6`), each against the
two retained arms; the runtime's own phase keys and skip counters:

| Arm | Secondary jobs fetched | Skipped by the new rule | Wave-2 fetch | Wave-2 analysis | Recovery candidates | Active (recovery) | Run total |
|---|---|---|---|---|---|---|---|
| `tailfull-legacy` | 1,304 | — | 47,360 ms | 4,412 ms | 101 | 16 | 418,967 ms |
| `tailfull-shipped` | 1,304 | — | 48,075 ms | 4,810 ms | 98 | 16 | 425,323 ms |
| **`secondary-cut-narrow` (shipped)** | **364** | 1,192 | **27,184 ms** | **85 ms** | **96** | **15** | 362,059 ms |
| `secondary-cut` (wider variant, unshipped) | 168 | 1,480 | 18,217 ms | 68 ms | 95 | 15 | 361,701 ms |

That wider arm ran before the counter was renamed, so its skip figure is recorded there as
`recoverySkippedByReadableWave1` (452 / 512 / 516); the shipped naming is
`recoverySkippedByGeneratedAnswered`.

The counters are deterministic; the phases are not. The cut's own share of the clock is wave-2 fetch
plus its analysis: **25,616 ms of the 425,323 ms baseline (−6.0%)**; the wider variant's is 34,600 ms. The run totals move further
(−63.3 s, −14.9%) because every other phase also drifted between runs — homepage fetch 138,143 →
105,744 ms, probe 83,934 → 80,650 ms, wave-1 fetch 107,697 → 104,423 ms — none of which this rule can
touch. On the audit's historical per-phase split (950.9 s job, wave-2 fetch 91.0 s) the same 43.5%
reduction is ≈ **−40 s**, arithmetic on the measured fraction rather than a second full-audit run.
The phase does not fall proportionally to the request count (the wider variant removes 87% of the
requests for 62% of the wall): a batch's four paths per kept host still hold the phase open, which is
the same tail-behaviour the earlier snapshots measured for the probe stage.

## The price, traced candidate by candidate

The two live arms differ from `tailfull-shipped` by three candidates and one active board. Each
difference was traced through both fetch logs before being charged to the rule:

| Difference (shipped → narrow) | Trailed | Charged to the rule? |
|---|---|---|
| `atlus.com/join-us-for-the-shin-megami-tensei-online-concert-event/` | wave 1 `/careers` **answered**, wave 2 `/join-us` **answered** — a host of the cut class | **yes — the one candidate it costs** (not an active board) |
| `www.greenheartgames.com/jobs/` | wave 1 `/jobs` answered in the shipped arm and **failed** in the narrow arm, which is also why its wave 2 ran | no — a wave-1 fetch that failed in one run |
| `eu.square-enix.com/jobs` (active in `shipped`) | identical trail in both arms: homepage `/en`, wave 1 `/jobs`, wave 1 `/careers`, all answered | no — the probe returned zero jobs for it in one run |
| `arte.tv/jobs?…token66856=2015869939` → `…946` | same board, rotating query token, active in **both** arms | no — URL identity noise |

So the shipped rule costs **1 of 98 recovery-sourced candidates and 0 of 16 active boards**. The
wider variant on the shelf — skip whenever *any* wave-1 page answered — costs the same `atlus.com`
candidate **plus** `dreamflightadventures.com/join-us-at-gls/` and
`enchantedarcade.com/about/careers` (both from hosts whose generated guesses 404'd but whose linked
page or PDF answered), for **8,984 ms** more in the wave-2 phases.

## Found, measured, not shipped: the sync fetch path's trust anchor

The retained logs hold a second, larger finding that this change deliberately does not touch. Of the
3,000 audited homepages, **507 fail with `CERTIFICATE_VERIFY_FAILED`** (501 in the homepage phase, 5
in the recovery waves, 1 in the probe) — every one of them scored `homepage_fetch_failed` and dropped
from the audit. A four-arm probe over the first 40 of those URLs
(`_out/perf-runs/discovery/sync_tls_probe.py`) says why:

| Arm | Read | HTTP error | Failed |
|---|---|---|---|
| `urlopen` with no context (today's sync path) | 0 | 0 | **40** |
| `ssl.create_default_context()` (the OS store, explicit) | 0 | 0 | **40** |
| `create_default_context(cafile=certifi.where())` (certifi only) | **39** | 1 | 0 |
| default certs **plus** certifi (the shape `shared/github_https.py` uses) | **39** | 1 | 0 |

The two anchored arms disagree on **zero** URLs, and the OS store holds 87 certificates here — a
reduced or stale store that reports "certificate has expired" for chains certifi resolves. `httpx`,
which the production directory and audit fetches take (`fetch_directory_pages` uses its async branch
whenever the fetcher is `fetch_text`), reads six spot-checked hosts of the set fine (5 × 200, 1 real
403). So this is a defect of the **sync** path — `web_search_fetch.fetch_text`, the admin source
check, the jobs lane's fallback, and every local harness — not of the main production path; it is
also why these artifacts record 674 homepage failures where the async path would have read most of
them. The repo already anchors this way for Workday CXS and GitHub. It is a coverage repair rather
than a cut, it needs its own tests across the shared fetchers, and it is priced here for that
decision rather than smuggled into this one.

## Limits

- **One population.** The pinned CSV's first 3,000 rows are all four full arms and both live runs;
  the 8- and 9-host partial-batch arms produced no wave-2 candidates at all, which is consistent
  with a 1-in-326 event but does not add statistical weight to it.
- **Attribution is differential.** The audit artifact records that a candidate came from a recovery
  page, not which page, so wave-2 yield is bounded by URL-path attribution (1 candidate) and priced
  by cross-run set-difference with every difference traced (`3` candidates). Live arms carry
  rate-limit noise: `eu.square-enix.com/jobs` and one ARTE token rotate between runs with identical
  fetch trails.
- **The bench's fetcher is a Python callable**, so its batches take the sync fetch path and the
  worker-thread probe branch rather than production's httpx/async ones. Both arms share that, so the
  comparison holds; the absolute phase numbers are the sync path's (see the trust-anchor section).
- **The rule touches the GameDevMap audit's batch runtime only** (`run_active_audit_batch`). The
  other directory adapters keep their own primary/secondary wave policy, which was not measured here.
- The wider variant is left on the shelf rather than shipped: it removes 196 more requests and
  8,984 ms more of the wave-2 phases for two more candidates, and no active board either way.

## Reproducing the numbers

```bash
# the live arms: same bench, same population, secondary-wave rule as the only difference (~7 min each)
python _out/perf-runs/discovery/audit_probe_bench.py --arm secondary-cut-narrow \
    --batch-size 1000 --max-batches 3 --timeout 6

# every delta above, re-derived from the retained arms (fails if a stated invariant moves)
python _out/perf-runs/discovery/verify_secondary_wave_cut.py

# the trust-anchor probe: the TLS-failure URLs from the retained run, four contexts, one JSON per arm
python _out/perf-runs/discovery/sync_tls_probe.py --limit 40 --timeout 6 --out sync-tls-probe-40.jsonl

# the shipped contract
python -m pytest tests/source_discovery/test_active_audit_runtime_batch.py \
    tests/source_discovery/test_directory_page_recovery.py -q
```

The four full-population arms, their fetcher logs and the trust-anchor probe are kept under
`_out/perf-runs/discovery/audit-runs/` (untracked). The dev harnesses live under
`_out/perf-runs/discovery/` on purpose: a `scripts/` edit is shipped container code and trips the
release gate, and these are measurement, not product behaviour.
