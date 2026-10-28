# The memos re-priced on the build that removed their traffic: the host memo is 27 ns a call, the URL memo 0.8% repeats — 2026-10-28

> - **Class:** change + measurement — ships a bounded memo for `normalized_host`, measures and
>   **refuses** the `infer_web_candidate` memo, and re-prices both after the no-candidate cut.
> - **Trigger:** [`snapshots/discovery-homepage-analysis-cost-2026-10-27.md`](discovery-homepage-analysis-cost-2026-10-27.md)
>   priced a memoized host normalization at **−7.2 s** and a memoized URL inference at **−7.4 s**,
>   both output-identical, and named them the next increment after the two hot-path cuts. Those
>   cuts shipped on 2026-10-28 (`a8a37e9c`), and the URL memo's repetitions came from the queue
>   pass that cut removed.
> - **Status:** the host memo ships — `normalized_host` is memoized on the text it produces, in a
>   **32,768**-entry LRU, and the per-call instrument puts it at **27.2 ns saved per call**,
>   **0.79 s** over the phase's **29.19M** calls. The URL memo does **not** ship: on this build
>   `infer_web_candidate` runs **561,964** times over **557,473** distinct keys — **4,491** repeats,
>   **0.8%** — and the variant read **+0.15 s**. Both recorded prices were inside the cost
>   snapshot's own **±4.9%** drift bound (±6 s), and the per-call number caps what the old build
>   could have saved at **1.35 s**; the phase replay here could not resolve the effect in either
>   direction (**−2.12 s** in one window, **+3.69 s** in the next), which is why the per-call
>   reading is the evidence. Output is identical in all **14** full-phase replays (counts *and*
>   per-collection content digests) and on the live arm's population.
> - **Verified against:** `a8a37e9c` (before, in a clean worktree) and the same tree plus the memo,
>   current corpus of one production arm's homepage results (8,369 results, 7,467 bodies, digest
>   `66165df36a10c023`), and one live full-population arm on the shipped build (8,370 rows, same
>   digest, `--lanes 32 --batches 0 --batch-size 1000 --timeout 6`), 2026-10-28T06:5x–07:4xZ.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-memos-20261028/` — `design.md`
>   (pre-registration and the decision), `price.py` + `price-{before,b1,h1,h2,b2,shipped,w1..w4,
>   characterize}.json`, `micro.py`, `arms/a1-memo/` (arm record, artifact, worker receipt), and
>   `price-reread.log`, `arm-a1-memo.log`.

## What was asked, and what the measurement changed

The request was to ship both memos and measure a full arm. One of them no longer exists as a
proposition: the URL memo's repetitions were the recovery queue's duplicate provider pass — the
same **5,874** second passes the previous snapshot removed by construction. A memo whose hits were
another cut's waste cannot be shipped on the old price.

## The URL memo has nothing left to hit

`price.py --variants characterize` wraps the delegating wrapper at all seven bindings the phase's
call sites hold and counts keys without timing anything:

| Reading | Value |
| --- | ---: |
| `infer_web_candidate` calls | **561,964** |
| distinct `(url, studio, nl_priority, discovery_method)` keys | **557,473** |
| calls that repeat a key already seen (hits a memo would serve) | **4,491** (**0.8%**) |

On the build the cost snapshot measured, the same memo reported **396k** hits. Its price was
**−7.4 s** then and **+0.15 s** now, because a deep copy and a dict entry per call cost more than
0.8% of hits save. Refused; no cache was added.

## The instrument could not see it

Two windows of the same nine-batch replay, each run in its own process, one recipe per process:

| Window | `b1`/`w1` | memo host | memo host | `b2`/`w4` | Paired |
| --- | ---: | ---: | ---: | ---: | ---: |
| quiet, memo monkeypatched | 101.72 s | **99.01 s** | **97.63 s** | 99.16 s | **−2.71 / −1.53 s** |
| loaded, memo **shipped** vs `a8a37e9c` | 123.63 s | 127.23 s | 123.56 s | 119.78 s | **+3.60 / +3.78 s** |

The same instrument reports both signs depending on what else the machine is doing, and the quiet
window's own baseline spread (**2.56 s**) is larger than the effect it was asked to resolve. Both
windows are therefore consistent with "below resolution", and the pre-registered magnitude rule
failed; the ship decision rests on the per-call reading below, not on these totals.

## Per-call cost, where the effect lives

`micro.py` runs the plain transform and the shipped `normalized_host` over the same
**2,000,000**-call sequence of real corpus hosts and provider patterns, ABBA in four rounds each,
with the cache pre-warmed (8.0M warm hits / 3,006 misses):

| Form | Rounds | Mean |
| --- | --- | ---: |
| `str(value or "").strip().lower().rstrip(".")` | 0.3016 / 0.3234 / 0.3310 / 0.3229 s | **0.3197 s** |
| shipped `normalized_host` | 0.2605 / 0.2725 / 0.2644 / 0.2636 s | **0.2653 s** |

**27.2 ns** saved per call. The phase makes **29,190,516** `normalized_host` calls over
**28,200** distinct strings on this corpus (a memo variant counted them), so the honest price is
**0.79 s per arm** — **0.7%** of the **~119 s** phase, not the **7.2 s** the cost snapshot
recorded. On the pre-cut build the same per-call number caps the saving at **1.35 s** across its
49.6M calls, which is the strongest evidence that the recorded memo prices were drift.

What ships is three lines: an `lru_cache` on a private `_normalized_host_text`, bounded at
**32,768** entries because a long-lived app process sees every host it ever audits, keyed on the
text the transform produces so an unhashable or exotic input cannot raise where the uncached call
did not, with `normalized_host` converting exactly as before for every non-`str` value.

The memo's oracles in `tests/test_url_hosts.py` fail against `a8a37e9c` — **2 failed, 5 passed**
(`failing-before-pytest.log`) — and pass here beside **76** neighbouring discovery tests: the pin
that a repeat is served from the cache, the bound pin, and the equivalence table the transform must
reproduce for hosts, patterns, empty and `None`, falsy non-strings, numbers, bytes and a tuple.

## The live arm

One full-population arm on the shipped build, against the arm the no-candidate snapshot recorded on
the same population:

| Arm | wall | worker wall | worker CPU | pages | transport |
| --- | ---: | ---: | ---: | ---: | --- |
| recorded (`a8a37e9c`) | 583.0 s | 121.54 s | **118.98 s** | 7,355 | child, 9/9, 0 fallbacks |
| memo (`a1-memo`) | 671.5 s | 126.70 s | **123.27 s** | **7,516** | child, 9/9, 0 fallbacks |

The machine was busy during this arm (its wall is 15% above the recorded arm's), so the wall is
not read as an effect — and CPU per page, which normalizes the different page sets, is
**16.40 ms** against **16.18 ms** (**+1.4%**), so the arm does **not** corroborate a 0.7% cut
either. It is reported as a health and parity reading: population digest `66165df36a10c023` with
all **8,370** rows completed and **8,369** homepage attempts, the worker carrying **9/9** batches
with **0** fallbacks and a **0.12 s** parent wait, hand-off **1.92 s** / **1,064 MB**. Its counts
differ from the recorded arm's in the way a different page set predicts
(`providerCandidates` 7,324 vs 7,284, `staticCandidates` 1,740 vs 1,702,
`rejectedForActivation` 13,481 vs 13,457, `zeroJobCandidates` 3,184 vs 3,146,
`homepagesFetched` 7,516 vs 7,355, `failures` 12,607 vs 12,093). Board identity sets (host +
tenant) are **352** vs **346** with **343** shared: three recorded-only
(`arsanesia.com`, `careers.kepler-interactive.com`, `www.littlechicken.nl`) and nine memo-only, all
with no host failure on their own side.

## Where this leaves the phase

The worker still carries **~123 s** of CPU per arm on a busy machine and **~119 s** on a quiet one;
the memo is **0.79 s** of that. The cost snapshot's remaining output-identical items are now
exhausted: the two hot-path cuts shipped, the host memo ships at a seventh of its recorded price,
and the URL memo is refuted by its own hit rate. The next cut has to change work rather than
repeat it — the body caps (**−28.2 s** / **−44.9 s**) remain refused because truncation adds
recovery jobs that are then fetched, and the hoist of the host normalization out of the pattern
walk (normalize the host once per admission, pre-normalize the twelve adapters' patterns) would
subsume this memo and is the same order of saving.

## Limits

The per-call reading is one machine and one string sample (real corpus hosts plus the provider
patterns), four rounds per form, ABBA; it prices the transform, not the phase, and the phase-level
effect is inferred from the counted call volume. The two phase windows disagree in sign, so the
phase replay is quoted as an instrument limit rather than as evidence. The live arm ran under load
and against a different live page set, so it carries health and parity only. No deployed host, and
no browser.
