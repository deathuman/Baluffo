# The no-candidate redundancy cuts ship: the worker's phase falls 139.4 → 119.0 s on a live arm and 113.4 → 93.9 s on fixed inputs, output-identical — 2026-10-28

> - **Class:** change + measurement — ships the two output-identical hot-path cuts the cost snapshot
>   priced, and measures them on a live full-population arm pair and on the captured corpus.
> - **Trigger:** [`snapshots/discovery-homepage-analysis-cost-2026-10-27.md`](discovery-homepage-analysis-cost-2026-10-27.md)
>   priced skipping the recovery queue's second provider inference at **−19.9 s** and computing the
>   reason detail once at **−7.8 s**, both output-identical, and the worker transport
>   ([`snapshots/discovery-homepage-analysis-overlap-2026-10-27.md`](discovery-homepage-analysis-overlap-2026-10-27.md))
>   turned the phase into worker headroom rather than arm wall.
> - **Status:** shipped. The queue no longer re-runs the provider inference the page outcome already
>   ran, and no longer recomputes a detail its caller already holds. On the live pair the worker's
>   wall falls **142.92 → 121.54 s** and its CPU **139.39 → 118.98 s** (**−20.41 s**), both arms
>   carrying every batch through the transport (`mode: child`, **9/9** offloaded, **0** fallbacks);
>   output parity is proven on fixed inputs rather than on the pair, because two live arms fetch
>   slightly different page sets (the after arm fetched **19** fewer homepages of the same **8,369**
>   attempts, and the **4** differing boards are named with **3** of them 429-attributed). Five
>   replays of the captured corpus (`8,369` results, `7,467` bodies, digest `66165df36a10c023`) are
>   identical in counts *and* per-collection content digests, with the removed work counted exactly —
>   the queue's provider pass **5,874 → 0**, its detail calls **5,874 → 0** — while the warm wall
>   reads **113.42 → 93.87 s**, **−19.55 s** (a **16.09–23.02 s** fall over the four warm
>   cross-readings). The phase is still the audit's largest CPU consumer at **~119 s** per arm.
> - **Verified against:** `2f62305b` (before, in a clean checkout of the build) and the same tree
>   plus the change (after), 2026-10-28T00:07–00:52Z; live arms at production settings (`--lanes 32
>   --batches 0 --batch-size 1000 --timeout 6`) on live GameDevMap CSV `896da851…`, population
>   digest `66165df36a10c023`; corpus and its capture from the cost snapshot, replay performed on the
>   same machine as the arms.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-cuts-20261028/` —
>   `design.md` (the pre-registration), `arms/a1-before/`, `arms/a2-after/` (records, artifacts,
>   worker receipts), `phase_ab.py` + `phase-{before,after,after2,before2,before3}.json`,
>   `compare.py` + `report.txt`/`report.json`, and the arm and replay logs.

## What shipped

`homepage_analysis` runs `_gamedevmap_page_outcome` on every readable homepage and, when it finds
nothing, `_handle_no_candidate` — which computed the no-careers reason detail and then handed the
page to `_queue_no_careers_recovery`, which computed the same detail again and re-ran the same
provider inference. The runtime can only reach that handler when the page outcome reported no
candidates, and `classify_fetched_page` reports that only after `analyze_fetched_page` ran
`infer_provider_candidates_from_html` on this same html and studio and returned no rows (its provider
rows pass through unfiltered). Both re-runs were therefore redundant by construction, not merely by
measurement:

* `_queue_no_careers_recovery` takes `reason_detail` (deterministic, so a caller's value is the
  value) and `provider_pass_done` (the caller's outcome already ran the identical inference), and
  skips the corresponding work.
* `_handle_no_candidate` computes the detail once, hands it down, and sets `provider_pass_done`.

The call sites are recorded in comments with the measured shape of the removed work, and
`tests/source_discovery/test_gamedevmap_no_candidate_cpu_cleanup.py` gains the audit-path oracles:
the count checks fail against the unchanged tree (two inferences, two detail computations per
no-candidate page) and pass after (one each), the parity check forces the old behaviour back on and
compares all seven outputs, and the `js_shell` branch, a candidate page, and the skipped pass's own
empty result are pinned separately. Two live arms are below; the replay is the effect.

## The live arms

One full-population arm per build, before first, same settings and population:

| Arm | wall | worker wall | worker CPU | boards | transport |
| --- | ---: | ---: | ---: | ---: | --- |
| `a1-before` | 638.0 s | **142.92 s** | **139.39 s** | 340 | child, 9/9, 0 fallbacks |
| `a2-after` | 583.0 s | **121.54 s** | **118.98 s** | 340 | child, 9/9, 0 fallbacks |

The worker is where the cut shows: its CPU falls **20.41 s** and its wall **21.38 s**, which is the
shape the replay measures precisely. The arm wall falls **55.0 s** and is reported as information
only — the phase is off the critical path, and one pair in one order cannot separate its remaining
headroom from session weather.

The pair cannot prove output parity: the after arm fetched **7,355** homepages against the same
**8,369** attempts (the before arm **7,374**), and its counts differ accordingly
(`providerCandidates` 7,321 → 7,284, `staticCandidates` 1,700 → 1,702, `rejectedForActivation`
13,482 → 13,457, `zeroJobCandidates` 3,149 → 3,146, `recoveryPagesFetched` 2,253 → 2,234,
`failures` 12,200 → 12,093; `homepageFetchAttempts`, `activeCandidates` and probes unchanged).
Boards: **338 of 340** shared, with `www.2awesomestudio.com` and `www.ypuniverse.com` only in the
before arm (both a homepage **429** on the after side) and `mightyeyes.games` and
`www.enhance-experience.com` only in the after arm (the second a homepage **429** on the before
side, the first with no host failure on either). That is the same live-arm limitation the overlap
study recorded, which is why parity is judged on fixed inputs below.

## The fixed-input replay

The captured corpus is replayed through each build's real code, five times in all — pair A ran the
before arm first, on a corpus whose 1.1 GB of bodies had not been read yet, and pair B ran the after
arm first once everything was cached:

| Reading | Order | Wall | vs its pair |
| --- | --- | ---: | ---: |
| `before` | first, cold corpus | 150.35 s | — (excluded) |
| `after` | second | 94.51 s | pair A **+55.84 s** (excluded) |
| `after2` | first, warm | 93.22 s | pair B **+17.38 s** |
| `before2` | second, warm | 110.60 s | |
| `before3` | third, warm | 116.24 s | |

The identical before code reads **150.35 s** cold and **110.60 / 116.24 s** warm, so the first pair's
**+55.84 s** is the corpus's cold read, not the cut, and is excluded. The warm readings are the
measurement: the after arm repeats to **1.4%** (94.51 / 93.22) while the before arm spans **5.1%**
(110.60 / 116.24), and across the four warm cross-readings the phase falls **113.42 → 93.87 s**, a
**−19.55 s** mean (**−15.5%**, a fall of **16.09–23.02 s**) — the same order as the recorded
prices' sum (**−27.7 s**) minus the drift the warm pair carries.

The mechanism is counted on the corpus, and the output is compared by content, not just counts:

| Counter | before | after |
| --- | ---: | ---: |
| the queue's own provider pass | **5,874** | **0** |
| provider inference through the queue's binding | **5,874** | **0** |
| provider inference through the outcome's binding | 7,467 | 7,467 |
| reason detail from the handler | 5,874 | 5,874 |
| reason detail from the queue | **5,874** | **0** |

All five replays agree on every collection's count *and* per-collection content digest — exactly:
`provider_candidates` **101** (`80a3b711ae3a3155`), `static_candidates` **1,499**
(`d81dd0934ec69284`), `rejected_rows` **1,181** (`c26e6befd583ab4d`), `primary_recovery_jobs`
**11,685** (`d336ff737eab4ec5`), `secondary_recovery_jobs` **22,380** (`37be04d753596020`),
`browser_recovery_candidates` **1,380** (`c43282f589b7610c`), `homepagesFetched` **7,467** — so the
removed work changed nothing that the phase produces. (Of the **1,181** rejections, **902** are the
corpus's own failed fetches and **279** are no-candidate pages that queued nothing — batch 1's shape
is **97** `homepage_fetch_failed` + **31** `no_careers_evidence` — because the planner generates the
common recovery paths for the rest.)

## Where this leaves the phase

The worker still carries **~119 s** of CPU per arm — the audit's largest CPU consumer — and the cost
snapshot's remaining output-identical items are the memos: host normalization **−7.2 s** and URL
inference **−7.4 s**, both priced identical, both still unshipped. The body caps (**−28.2 s** /
**−44.9 s**) remain refused: truncation adds recovery jobs that are then fetched over the network.

## Limits

One arm per build, one order, so the arm wall is not an effect and is not read as one; the effect is
the fixed-input replay, whose warm pair has n=2 per build and carries the study's own warm spread
(**5.1%** on the before side). The live pair's outputs differ by the page sets it fetched (19
homepages of 8,369 attempts) and by four boards, three of which the host record attributes to 429s;
parity is therefore claimed on the corpus, where the inputs are fixed and the content digests match.
The replay's cold first reading is kept as evidence rather than deleted, and is excluded from the
effect because identical code reads 39.75 s faster warm. No deployed host, and no browser.
