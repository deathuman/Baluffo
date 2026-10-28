# Homepage analysis costs 17% of the audit and saturates one core: a 124 s phase, its profile, and what each next fix buys — 2026-10-27

> - **Class:** measurement — captures, replays and profiles the active audit's `homepage_analysis`
>   phase, and prices candidate fixes on the replay; no shipped behaviour changes.
> - **Trigger:** the production-sized lane acceptance closed the 64-lane question at 32 and named
>   homepage-analysis cost the next unaddressed stage (it is **17.0%** of an arm's wall and the
>   audit's largest CPU consumer).
> - **Status:** the phase is **one core saturated** — across the 16 production arms it costs
>   **128.8 s/arm** wall against **127.2 s** CPU (`cpu/wall` **0.988**), which is **17.0%** of the
>   **756.9 s** mean arm. A captured corpus of one full arm's homepage results (**8,369** results,
>   **7,467** bodies, **1.1 GB**, digest `66165df36a10c023`) replays the phase at **121.8 s**
>   against that arm's own **112.7 s** (+8.1%, inside same-corpus noise; a second baseline read
>   **127.8 s**, a **+4.9%** drift bound). The profile blames three things: per-page full-HTML
>   regex scans (**49.0 s** of `findall` self-time in a 235 s profiled phase), per-link provider
>   admission (**953,526** `infer_web_candidate` calls driving **49.6M** `normalized_host` and
>   **11.4M** `host_matches_any_domain_pattern` calls), and the no-candidate recovery queue
>   (**54%** of the phase; **5,874** of **7,467** readable pages take it). Size is the shape: the
>   largest decile (mean **751 KB**) costs **60 s** of the phase by itself. Priced on the replay:
>   skip the recovery queue's second provider inference **−19.9 s** and hoist the duplicated reason
>   detail **−7.8 s**, both output-identical; memoized host normalization **−7.2 s** and URL
>   inference **−7.4 s**, both output-identical; body caps **−28.2 s** (500 KB) and **−44.9 s**
>   (250 KB) but they change the output; thread scaling is **nil** (**1.02×** at four workers,
>   worse at 16) and thread overlap with the live fetch is a **wash** (fetch 11.9 → 26.3 s beside
>   the analysis while sequential totals match), while a **child process** keeps the fetch's solo
>   wall (13.2 s mixed vs 14.0/13.4 s solo) and hides the analysis (sequential 26.7 s → 15.4 s).
>   The priced next fix is therefore the overlap taken out of the process (up to **~129 s/arm**),
>   with the two output-identical hot-path cuts as the cheap complement.
> - **Verified against:** `f60de72e` (`main`), live capture arm at production settings (32 lanes,
>   per-host 3, timeout 6, 1,000-row batches, whole eligible population) on live GameDevMap CSV
>   `896da851…`, population digest `66165df36a10c023` — the same digest as the production
>   acceptance quads; 2026-10-27T18:05–18:21Z capture, replay on the same machine after it.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-profile-20261027/` —
>   `capture.py`, `replay.py`, `overlap.py`, `overlap_process.py`, `corpus/` (bodies + per-batch
>   results), `capture.json`, `replay.json`, `profile.txt`, and the pass logs.

## What the phase is

`homepage_analysis` is the local pass between the homepage fetch and the recovery waves:
`run_active_homepage_batch` walks the fetched results, and for each readable page runs
`_gamedevmap_page_outcome` (provider inference, explicit-careers link extraction, static
candidates) and, when that finds nothing, `_handle_no_candidate` (reason detail, recovery
planning, browser-recovery flagging). Everything it needs arrives in memory from the fetch, so
the phase is pure CPU — and its receipt says so: **0.988** of a core over 16 arms.

| Phase (mean of 16 production arms) | wall/arm | CPU/arm | cpu/wall |
|---|---:|---:|---:|
| homepage_analysis | **128.8 s** | **127.2 s** | **0.99** |
| recovery_wave1_fetch | 149.5 s | 95.3 s | 0.64 |
| recovery_wave1_analysis | 15.5 s | 15.3 s | 0.99 |
| probe | 207.4 s | 57.9 s | 0.28 |
| homepage_fetch | 189.4 s | 77.4 s | 0.41 |
| recovery_wave2_fetch | 61.0 s | 16.2 s | 0.27 |

The arm wall means **756.9 s** (649.1–862.5); homepage analysis is **17.0%** of it and the only
phase above 100 s of CPU, i.e. the audit's largest CPU consumer.

## The corpus and the replay

One live arm was captured at production settings with a wrapper on
`gamedevmap_active_dry_run.fetch_directory_pages`, the seam that returns the *normalized* results
the phase consumes; each homepage batch's results were written to disk together with the body each
result carried. Capture wall **776.5 s**, analysis **112.7 s** wall / **111.4 s** CPU; corpus
**8,369** results (**7,467** ok, **902** failures), **1,110.7 MB** of bodies, largest **12.24 MB**.

The replay calls the real `_analyze_gamedevmap_active_homepages` per batch over the captured
results, bodies read back from disk. Two full-phase baselines were read (before and after the
variant passes, to bound drift): **121.8 s** and **127.8 s**, against the captured arm's
**112.7 s** (**1.081×**) — the same per-batch shape (the capture's 12.7/14.4/15.6/…/5.3 s against
the replay's 14.9/16.8/17.9/…/5.3 s), with a batch-1 repeat of **13.3 s** inside the same corpus
showing the noise floor. Prices below are quoted against both baselines.

## Where the time goes

cProfile over the full phase (profiled total **235.4 s** ≈ **1.93×** the 121.8 s wall, so the
per-function numbers are relative; the counters beside them are real):

| Cumulative | calls | profiled | What it is |
|---|---:|---:|---|
| `_handle_no_candidate` | 5,874 | 126.5 s | the no-candidate path, **54%** of the phase |
| `_queue_no_careers_recovery` | 5,874 | 114.9 s | inside it: recovery planning, second inference, reason detail |
| `infer_provider_candidates_from_html` | 13,341 | 115.2 s | once per readable page plus once per no-candidate page |
| `_gamedevmap_page_outcome` | 7,467 | 108.8 s | the outcome path for every readable page |
| `infer_web_candidate` | 953,526 | 89.0 s | per embedded link, ~114 calls/page |
| `infer_provider_adapter` | 953,512 | 75.5 s | provider admission per link |
| `host_matches_any_domain_pattern` | 11,441,376 | 68.0 s | domain-pattern walk per admission |
| `plan_recovery_fetch_job_waves` | 5,874 | 56.8 s | recovery URL planning per no-candidate page |

| Self time | profiled | calls |
|---|---:|---:|
| `re.findall` | 49.0 s | 78,150 |
| `str.lower` | 22.4 s | 58,629,149 |
| `normalized_host` | 20.8 s | 49,579,628 |
| `re.sub` | 10.9 s | 63,926 |
| `urlsplit` + `urlparse` | 18.4 s | 9,621,472 |
| `host_matches_domain*` / `any` / genexprs | ~36 s | 13–27M each |
| `careers_keyword_count` genexpr | 8.1 s | 27,513 |

Real counters (no profiler): the outcome path's inference runs on all **7,467** readable pages
(**22.0 s** inside, **94** pages yield provider candidates); the recovery queue's second inference
runs on all **5,874** no-candidate pages (**15.3 s** inside, **0** non-empty); the reason detail
runs **twice** per no-candidate page — **8.6 s** from `_handle_no_candidate` plus **8.5 s** from
`_queue_no_careers_recovery`.

## Size is the shape

Partitioning each batch's results by body size into deciles:

| Decile | pages | mean body | ms/page | total |
|---|---:|---:|---:|---:|
| 0 | 837 | 0 chars | 0.003 | ~0 s |
| 2 | 837 | 8.8 KB | 1.91 | 1.6 s |
| 5 | 837 | 55 KB | 7.62 | 6.4 s |
| 8 | 837 | 228 KB | 24.5 | 20.5 s |
| 9 | 836 | **751 KB** | **72.1** | **60.3 s** |

Cost is **96–331 µs/KB** and falls with size; the largest decile — which includes the 12.24 MB
page — is **half the phase**, while the 837 empty bodies are free.

## Prices, measured

Each variant is a full-phase replay; `output` compares the batch result counts against the
baseline digest (`provider`, `static`, `rejected`, primary/secondary recovery, browser recovery).

| Variant | wall | vs 121.8 s | vs 127.8 s | Output |
|---|---:|---:|---:|---|
| skip the recovery queue's provider pass | **101.9 s** | **−19.9 s** | −25.9 s | **identical** |
| memoize `_no_careers_reason_detail` (both bindings) | 114.0 s | −7.8 s | −13.8 s | **identical** (5,874 hits / 5,874 misses) |
| memoize `normalized_host` | 114.6 s | −7.2 s | −13.2 s | **identical** (49.55M hits / 28.2k misses) |
| memoize `infer_web_candidate` (deep-copied) | 114.4 s | −7.4 s | −13.4 s | **identical** (396k hits / 557k misses) |
| cap bodies at 500 KB | 93.6 s | −28.2 s | −34.2 s | **changed** (+67 primary, +148 secondary recovery; −7 provider, −30 static, −15 browser) |
| cap bodies at 250 KB | 76.9 s | −44.9 s | −50.9 s | **changed** (+200 primary, +436 secondary; −18 provider, −91 static, −40 browser) |

The skip is the largest output-identical cut: **0 of 5,874** second-pass calls returned a
candidate, which is what a page whose outcome already found nothing should look like. It is
measured redundancy, not proven redundancy — the safe form is to hand the outcome path's own
inference result to the recovery queue instead of recomputing it, which keeps today's output by
construction and still removes the **~20 s**. The reason-detail fix is pure: the two call sites
pass identical `(page_url, html)` and the function is deterministic. The caps are *not* free even
though they are the largest numbers: truncation makes pages look link-poor, so the recovery waves
gain jobs (+148 at 500 KB) that are then fetched at network cost, and a cap can cut real links.

**Threading does not help.** Sharding three batches across a pool: **45.5 s** (2), **45.1 s** (4),
**45.4 s** (8) against **46.2 s** serial — **1.02×** at best, and **50.2 s** at 16 workers. The
phase is GIL-bound, so more Python threads buy nothing.

**Thread overlap with the fetch is a wash.** Beside a live 200-URL fetch (the audit's own
`fetch_directory_pages`, 32 total / 3 per host): analysis **14.15 → 14.73 s**, but the fetch
**11.92 → 26.34 s**; sequential totals (14.2 + 11.9 = 26.1 s) against mixed 26.3 s. The analysis
holds the GIL and the fetch threads wait for it.

**Process overlap works.** The same analysis in a child interpreter beside the same fetch: fetch
**13.96 s** solo, **13.24 s** mixed, **13.40 s** solo again — no penalty — with the child's
analysis **15.41 s** alongside; sequential **26.7 s** against overlapped **15.4 s**. So the phase
can be hidden behind I/O if it runs in another process, which is what the audit's own shape would
allow: one batch's analysis **~12.9 s** beside the next batch's homepage fetch **~21 s** (and the
recovery/probe phases after it) on a machine with 15 idle cores.

## Recommendation

> **Shipped 2026-10-27:** recommendation 1 is done — the worker transport is in the audit and was
> measured on four counterbalanced full-population arms in
> [`discovery-homepage-analysis-overlap-2026-10-27.md`](discovery-homepage-analysis-overlap-2026-10-27.md)
> (**−126.5 s** and **−94.0 s** per arm, output-identical on fixed inputs). That snapshot also
> re-prices recommendations 2 and 3 below: with the phase off the critical path, its CPU savings are
> worker headroom rather than arm wall.

1. **Overlap the phase out of the process (structural, up to ~129 s/arm — 17%).** The measured
   form is a child process per batch (bodies via a temp file or pipe) while the main process moves
   on to the next batch's fetch; threads are refuted twice (no scaling, and a wash beside the
   fetch). The open unknown is serialization/IPC of ~130 MB per batch, which the pilot excluded.
2. **Take the output-identical hot-path cuts first if the overlap is deferred:** hand the outcome's
   inference result to `_queue_no_careers_recovery` (**−19.9 s**) and compute the reason detail
   once (**−7.8 s**); together **~28 s/arm** with no behaviour change, and they reduce what the
   overlap has to hide.
3. **The memos are real but smaller** (−7.2 s and −7.4 s, output-identical); they are the natural
   next increment after the two above.
4. **Do not cap bodies as a first move.** The −28/−45 s is paid back in extra recovery fetch work
   and changed discovery; if a cap is ever wanted, the top decile is where the money is and the
   existing `>1 MB` head+tail truncation is the precedent to follow, not a straight cut.
   **Closed 2026-10-28:** the cap question was re-priced after the five output-identical cuts in
   [`discovery-homepage-analysis-body-caps-2026-10-28.md`](discovery-homepage-analysis-body-caps-2026-10-28.md)
   — every level and both shapes lose provider/static candidates and add recovery rows, so no cap
   belongs and the `>1 MB` head+tail truncation stays on the page outcome only.

## Limits

One capture arm on one day, replayed on the same machine; the two baselines bound a **+4.9%** drift,
so variant deltas carry roughly that much uncertainty (the size of each effect above is well
outside it except the memos). cProfile inflates the phase **1.93×**, so per-function shares are
relative and the counters are the real-time numbers. The second-pass redundancy is measured on this
corpus (digest `66165df36a10c023`) — 0 of 5,874 non-empty — not proven for every input, which is why
the recommended form reuses the computed result rather than deleting the pass. The overlap pilots
use a 200-URL fetch sample and one batch's analysis, not a whole phase, and the process variant's
price excludes body serialization. No deployed host, and no browser.
