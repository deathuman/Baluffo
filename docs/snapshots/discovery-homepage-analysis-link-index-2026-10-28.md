# The link index: 9.0 → 3.8 body walks a page, 32.6 s off the phase, and the first replay that resolves its own effect — 2026-10-28

> - **Class:** change + measurement — one lazily derived link index per fetched body, carried through
>   the page outcome, the no-careers reason detail and both recovery waves.
> - **Trigger:** [`snapshots/discovery-homepage-analysis-host-hoist-2026-10-28.md`](discovery-homepage-analysis-host-hoist-2026-10-28.md)
>   closed the provider walk and named the phase's remaining **49.0 s** of HTML `findall` self-time
>   as the next cost; [`snapshots/discovery-homepage-analysis-cost-2026-10-27.md`](discovery-homepage-analysis-cost-2026-10-27.md)
>   is where that attribution was made.
> - **Status:** the index ships. On the captured production arm (nine batches, **8,369** results,
>   **7,467** readable bodies, digest `66165df36a10c023`) full-body scans fall from **67,428** to
>   **28,442** calls — href **46,498 → 14,221** (−69.4%), absolute URL **20,930 → 14,221** (−32.1%),
>   **9.52 → 4.20 GB** scanned — i.e. **9.03 → 3.79** body walks a readable page. The phase replay
>   moves **86.36 / 88.65 s → 53.77 / 51.74 s**, pairs **+32.60 / +36.90 s** against same-build
>   spreads of **2.28 / 2.02 s**, so the wall reading **resolves** — the first measurement in this
>   series whose phase replay is sharper than its own effect (CPU agrees: **+32.45 / +36.55 s**
>   against **2.05 / 2.05 s**). All four runs reproduce the reference counts batch by batch *and*
>   per-collection content digests exactly, and every warm repeat matches its own first pass.
>   A live full-population pair confirms it in the worker: **87.69 / 86.91 → 64.95 / 62.89 s**
>   (**+22.74 s** wall, **+24.02 s** CPU), both arms 9/9 child with 0 fallbacks on the same
>   **8,370**-row population, digest `66165df36a10c023` and CSV — at the lower edge of the
>   pre-registered band, and recorded as a confirmation rather than as new evidence.
> - **Verified against:** `c8f03fa2` in a clean worktree (`.tmp/link-index/before-tree`) and this
>   tree, over the captured production arm of 2026-10-27, 2026-10-28.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-link-index-20261028/` —
>   `design.md` (pre-registration, the recorded amendment, the decision), `phase.py` (the
>   scan-counting replay), `micro.py`, `compare.py`, `report.txt`, `scans-*.json` and the four
>   `phase-*` / four `micro-*` runs.

## What one page was paying for

The phase is a chain of extractors that each own a copy of the same regex, so a no-candidate page
was walked ten times:

| Step | Body walks |
| --- | --- |
| `infer_provider_candidates_from_html` | href, absolute URL, `str.lower()` |
| `extract_explicit_careers_url_from_page` | href |
| `build_static_candidate_from_page` | keyword `lower`, static probe, JSON-LD |
| `_no_careers_reason_detail` | `str.lower()` (js-shell), href |
| `plan_recovery_fetch_job_waves` | href **four times**, absolute URL **twice**, two `html.unescape` passes |

Measured over the phase that is **6.23 href scans and 2.80 absolute-URL scans per readable page**,
**9.03** in all; two of the recovery href scans were the same `same_party_jobish_urls` call made
twice inside one plan (once to build the URLs, once to attribute them). The largest body decile
averages **751 KB**, so the repetition is not cheap: **6.56 GB** of href scanning and **2.96 GB** of
URL scanning over a 1.1 GB corpus.

## The index

`PageLinks` ([`src/source_discovery/page_links.py`](../../src/source_discovery/page_links.py)) holds
one body and lazily derives each view **at most once**: `hrefs()`, `links()`, `absolute_urls()`,
`jobish(base_url)`, `lowered()`, `url_candidates()`. Its identity is the body object, so two equal
strings stay two pages, and `page_links_for(body, index)` returns a supplied index only when it
belongs to that exact object — a caller that rebuilds a body pays its own scan rather than reading
another page's links.

The extractors keep their public signatures and gained an optional `page_links`:
`extract_links_from_html` and `extract_jobish_links` now delegate to `links_from_hrefs` /
`jobish_links_from_hrefs` over one `href_values` scan (with the href pattern compiled once),
`careers_keyword_count_lowered` reuses an already-lowered body, and
`recovery_url_planner.html_url_candidates` delegates to `PageLinks.url_candidates`.

Three path changes carry the reuse:

* `analyze_fetched_page` builds one index and hands it to the provider pass, the explicit-careers
  pass and the static-candidate builder, so the analysis half is one href scan and one URL scan.
* `_extract_candidates_from_homepages` keeps the analysis's index beside the body for the handler
  that runs immediately after it (dropping it when the page found candidates), so the reason detail
  and both recovery waves reuse the same scan instead of making four more.
* `plan_recovery_fetch_job_waves` computes `same_party_jobish_urls` **once per wave** and passes it
  to the URL list and to the attribution, removing the planner's duplicate pass outright.

The `str.lower()` of the whole body in the provider pass moved inside its `if keyword_match:` guard.
On this corpus **0 of 7,467** page URLs contain a careers keyword, so body lowercases fall from
**13,341** (7,467 in the provider pass plus 5,874 in the js-shell test) to **5,874**, one per
no-candidate page shared by every consumer.

## Scan volume

`phase.py --record-scans` counts calls and characters by pattern class on both trees — inline href
`re.findall` on the old build, every bound `href_values` on the new one, every bound
`find_urls_in_text` on both — so the reading is body walks, not one implementation's call shape:

| Class | Old calls | Old scanned | New calls | New scanned | Calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| href | 46,498 | 6.56 GB | 14,221 | 2.10 GB | **−69.4%** |
| absolute URL | 20,930 | 2.96 GB | 14,221 | 2.10 GB | **−32.1%** |
| other (JSON-LD, probe heuristics) | 2,320 | 0.56 GB | 2,320 | 0.56 GB | 0% |

Per readable page: **6.23 → 1.90** href walks and **2.80 → 1.90** URL walks. What survives is the
analysis's own href and URL scan (**one each**, both required) plus **one unescaped pass** on the
pages whose body really contains an entity or an escaped slash — 632 of batch 1's 903 bodies. The
`other` class is untouched, which is the attribution: this change removed repeated link extraction,
not parsing.

**The amendment, recorded rather than rewritten.** The pre-registration predicted `~19k → ~7.5k`
URL calls from "the unescape transform is an identity on most bodies". The one-batch pre-check
showed that assumption is wrong: `html.unescape` allocates whenever the body contains any character
reference, so identity holds on only 271 of 903 bodies and the other 632 legitimately need the
unescaped text (`&amp;` becomes `&`, which changes the URLs). Before any timed reading the rules were
amended to href ≥ 50% (unchanged) plus href + URL ≥ 45% combined, with the URL class reported and no
floor; combined calls fall **67,428 → 28,442** (**−57.8%**).

## The replay, and it resolves

`phase.py` replays the nine corpus batches over the real `_analyze_gamedevmap_active_homepages`, one
tree per process, in the order `old, new, new, old`:

| Run | Tree | Wall s | CPU s | batch-1 repeat s | equal |
| --- | --- | ---: | ---: | ---: | --- |
| 1 | `c8f03fa2` | 86.36 | 86.14 | 10.71 | yes |
| 2 | indexed | 53.77 | 53.69 | 6.10 | yes |
| 3 | indexed | 51.74 | 51.64 | 6.13 | yes |
| 4 | `c8f03fa2` | 88.65 | 88.19 | 10.04 | yes |

Pairs in run order (old − new): **+32.60 s** and **+36.90 s**; same-build spreads **2.28 s** (old)
and **2.02 s** (new). Both pairs agree and each exceeds both spreads, so the replay **resolves** the
effect under the rule fixed in advance — **1.61×** on the phase wall, **38%** of the old total. The
process-CPU reading says the same thing (**+32.45 / +36.55 s** against **2.05 / 2.05 s**), which is
expected of a phase whose `cpu/wall` is **0.988**.

Every run reproduces the reference count digest for all nine batches and one per-collection content
digest for the whole phase; `compare.py` checks both, and the warm repeat of batch 1 equals its own
first pass in all four.

## The lifecycle micro

`micro.py` runs corpus batch 1 through the audit's own `_extract_candidates_from_homepages` — the
function that owns both the page outcome and the no-candidate handler, so the measurement includes
the sharing rather than an approximation of it — three timed rounds per process, `old, new, new,
old`:

| Tree | Runs (mean s) | Rounds (s) |
| --- | --- | --- |
| `c8f03fa2` | 10.51, 10.22 | 10.43, 10.05, 11.06 / 9.84, 10.44, 10.38 |
| indexed | 6.75, 6.29 | 6.23, 7.08, 6.94 / 6.04, 6.49, 6.34 |

Saving **3.85 s a batch-1 lifecycle** (**37.1%**), **3.8×** the largest single-round spread
(**1.01 s**); the implied phase price, at eight full batches plus 370/1,000, is **32.2 s** — within
**1.7 s** of the replay's smaller pair, from an instrument that never loads the whole phase.

## The live confirmation

One live arm per build at production settings (`--lanes 32 --batches 0 --batch-size 1000 --timeout
6 --no-trace`), before first, each launched from its own checkout so the harness imports that tree,
with the arm's own `BALUFFO_DATA_DIR`:

| Reading | `c8f03fa2` | indexed | Delta |
| --- | ---: | ---: | ---: |
| Worker wall | 87.69 s | 64.95 s | **+22.74 s** |
| Worker CPU | 86.91 s | 62.89 s | **+24.02 s** |
| Arm wall (information only) | 654.8 s | 613.5 s | +41.3 s |
| Parent hand-off | 1.55 s / 1,053 MB | 1.56 s / 1,049 MB | flat |

Both arms report population **8,370**, digest `66165df36a10c023`, `remainingUrls` 0 and the same
CSV sha256; both are `mode: child` with **9/9** batches offloaded and **0** fallbacks. The CPU pair
sits **8.4 s below** the replay's smaller CPU pair (**32.45 s**) and inside the
pre-registration's **±10 s** band (**22.6-46.9 s**) — **at its lower edge, not comfortably inside**, which is the honest reading: the live page set is not
the captured one, and per-batch CPU deltas range from **0.09 s** (batch 2, effectively flat) to
**4.80 s** (batch 4), with eight of nine directions favouring the index.

Counts differ as two live arms always do — homepages fetched **7,453 → 7,417**, provider candidates
**7,322 → 7,292**, static **1,733 → 1,725**, zero-job **3,193 → 3,184**, failures **12,310 → 12,038**,
recovery pages **2,274 → 2,278** — and boards are **345 / 345** with four lost and four gained, both
lists named in the study's `report.txt`. That is a bound on the pair, not a parity claim: parity is
the replay's reading on fixed inputs, above.

## Where this leaves the homepage-analysis cost

The phase's three output-identical cuts now stand at **−19.9 s** (queue's provider pass), **−7.8 s**
(reason detail), **−3.28 s** (host hoist) and **−32.6 s** (this index), each measured on the same
corpus. What remains attributed in the cost snapshot is the per-link provider inference itself
(**953,526** calls, now paying one admission each rather than 52 normalizations plus repeated
scans), the static probe's own heuristics (**2,320** `other` regex calls, **0.56 GB**, untouched
here), and body size: the largest decile is still half the phase. The next cut has to be inside
`infer_web_candidate` or in what the phase reads, not in how many times it reads it.

## Instrument limits

* One machine, one corpus, one session. This replay's same-code spread was **2.3 s** in this session,
  smaller than the **3-9 s** earlier windows here; the effect (**33-37 s**) is well outside both, but
  the wall comparison is only as good as that spread.
* `re.findall` and `find_urls_in_text` counting measures regex work; it excludes allocation, the
  `lower()` copies and the `html.unescape` passes, which the lifecycle micro does include. The two
  instruments answer different halves and agree on the size.
* The micro fixes the batch and the allocator and prices one batch, not nine; the replay carries the
  whole-phase figure.
* The corpus covers one production arm. Parity beyond it rests on the unit tests that compare every
  `PageLinks` view against the public extractor it replaces, plus the batch-scan oracle that pins one
  href scan for a whole no-candidate page.
* Production runs the phase in a child worker, so this saving is worker headroom (and, through the
  worker, arm wall) rather than a direct parent-side number. The live pair prices exactly that, and
  lands below the replay's per-batch price because its bodies are a different fetch.
* The live pair is one order and one session; its page sets differ by 0.5% of homepages, which is
  why it is a confirmation of direction and rough size, not a second effect measurement.
