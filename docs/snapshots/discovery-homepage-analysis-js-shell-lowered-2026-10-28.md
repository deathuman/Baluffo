# The js-shell test stops lowering the page a second time: 2.5 s off the phase, and the last output-identical cut — 2026-10-28

> - **Class:** change + measurement — the no-candidate reason detail now hands the js-shell test the
>   body its page index already lowercased; no other behaviour moves.
> - **Trigger:** [`snapshots/discovery-homepage-analysis-link-index-2026-10-28.md`](discovery-homepage-analysis-link-index-2026-10-28.md)
>   closed with the phase still paying a per-body `str.lower()` for the js-shell test beside the one
>   `PageLinks.lowered()` the static builder had already paid. The pre-registration and the four
>   refused candidates are in `_out/perf-runs/discovery/homepage-analysis-next-cut-20261028/design.md`.
> - **Status:** ships. `looks_like_js_shell` and `no_candidate_reason_detail` take an optional
>   `lowered`, and `gamedevmap_recovery_queue._no_careers_reason_detail` passes
>   `links.lowered()`. Output is identical by construction — lowercasing changes neither length nor
>   whitespace, so `text.strip()` and every substring test read the same characters — and the
>   captured corpus confirms it batch by batch: identical count digests and one per-collection
>   content digest on all nine batches, every arm. The pre-registered pair puts the phase at
>   **49.77 / 49.93 → 47.29 / 47.44 s** (**+2.48 / +2.49 s**, CPU **+2.33 / +2.48 s**) against
>   same-build spreads of **1.31 s** (four baselines) and **0.17 s** (four lowered arms). After this
>   cut, **no output-identical item left in the phase is worth 2 s**.
> - **Verified against:** the pre-change tree at `323fc3c8` in a clean worktree
>   (`.tmp/js-shell-before`) and this tree, over the captured production arm of 2026-10-27 (nine
>   batches, **8,369** results, **7,467** readable bodies, digest `66165df36a10c023`), 2026-10-28.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-next-cut-20261028/` — `design.md`
>   (both pre-registrations, the four refusals, the decision), `replay.py` (the six priced variants),
>   `attrib.py` (the real-time segment instrument), `profile.txt`, and the `base*` / `low*` /
>   `shipped-*` runs.
> - **Tests:** `tests/source_discovery/test_page_diagnostics.py` and
>   `tests/source_discovery/test_gamedevmap_no_candidate_cpu_cleanup.py` — the five-fixture
>   equivalence, the forwarding oracle and two call-site oracles; 28 pass across those files plus
>   `test_gamedevmap_page_outcomes.py`.

## What was still being paid

After the link index, the no-candidate path built one `PageLinks` per body and lowercased it for the
static builder (`careers_keyword_count_lowered(links.lowered())`). The reason detail then called
`looks_like_js_shell(html)`, which does `str(html or "").lower()` again on the same body — **6,280**
calls on this corpus, each a full-body copy. A real-time segment run (no profiler) priced the pieces
at **52.55 s**:

| Segment | Seconds |
| --- | ---: |
| `build_static_candidate_from_page` | **18.94** |
| `plan_recovery_fetch_job_waves` | **10.22** |
| `static_probe_evidence` (inside the builder) | **8.38** |
| `looks_like_js_shell` | **4.35** |
| `PageLinks.hrefs` / `absolute_urls` / `jobish` | 4.36 / 4.42 / 3.14 |

The profiler is not the instrument here: its cumulative totals over a path of ~630k tiny calls
inflated the per-link admission to **23.19 s**, and the pre-registered prefilter that removes
**99.96%** of those calls (629,330 of 629,568) moved the phase by **1 s**. Each later candidate was
therefore priced as real seconds, and four were refused before this one cleared its floor:

| Refused | Phase wall | Why |
| --- | ---: | --- |
| provider-token prefilter | 50.13 / 50.09 s | ~1 s against its 5 s floor |
| single-pass keyword gate | 51.88 s | a 27-way alternation is slower than 27 C substring searches |
| anchor-label guard | 50.24 s | inside noise |
| same-party cache | 50.81 s | 477,879 checks, 702 repeats |

## The change

`looks_like_js_shell(html, *, lowered=None, ...)` uses the supplied lowered body when it is given,
and `no_candidate_reason_detail` forwards it. The queue's `_no_careers_reason_detail` already owns
the page's index (`links = page_links_for(html, page_links)`), so it passes `links.lowered()`. The
identity is guaranteed by `page_links_for`: the index belongs to that exact body object, and its
`lowered()` is `str(body or "").lower()` — the same string the test would produce itself.

Callers that do not hold an index are untouched: no other call site passes `lowered`, and the
default branch is the old `text.lower()`. That keeps the bridge pure — `page_diagnostics` gains a
parameter, not a dependency on the discovery page index.

## The readings

The pre-registered pair (one arm per variant, order `baseline, lowered, lowered, baseline`):

| Reading | Baseline | Lowered | Delta |
| --- | ---: | ---: | ---: |
| Pair A wall | 49.77 s | 47.29 s | **+2.48 s** |
| Pair B wall | 49.93 s | 47.44 s | **+2.49 s** |
| CPU | 49.22 / 49.42 s | 46.89 / 46.94 s | **+2.33 / +2.48 s** |
| Same-build spread | **1.31 s** (four arms) | **0.17 s** (four arms) | — |

A shipped-code A/B/B/A against the parent commit came later, in a window where every batch ran
**12-17%** slower than the pair above (another process was active on the machine):

| Reading | Before (`323fc3c8`) | Shipped | Delta |
| --- | ---: | ---: | ---: |
| Pair A wall | 66.92 s | 63.12 s | **+3.81 s** (CPU +3.27) |
| Pair B wall | 61.65 s | 59.40 s | **+2.25 s** (CPU +2.06) |
| Same-build spread | **5.28 s** | **3.72 s** | — |

Both pairs favor the change, but their spreads exceed the smaller delta, so **that session does not
resolve** the effect under the rule fixed in advance. It is recorded as direction and parity only;
the resolving evidence is the pair above, whose spreads are **0.17 / 1.31 s**. This is the one
honest weakness: a 5% cut sits close to this instrument's floor whenever the machine is busy.

Parity held in all four shipped arms (counts and content digests equal, warm repeat equal), and in
every priced variant — each candidate was refused for size, not for changing output.

## What this closes

The phase is **~47.4 s** on this corpus in a quiet window, and nothing output-identical left is worth
**2 s**. What remains is required or already refused:

* the one href (**4.36 s**) and one absolute-URL (**4.42 s**) scan per body, both needed by the
  provider pass and the recovery planner;
* the static builder's 27-token keyword gate (~7-8 s of its **18.94 s**), with no exact faster form
  found — replacing it with one alternation regex is *slower*;
* the probe's own anchor pass (**4.30 s** inside **8.38 s**), which needs the anchor labels the link
  index does not carry;
* recovery party filtering (**9.80 s**), whose duplicate-check hypothesis was refuted.

The next price is the one the cost snapshot refused to take first: body caps change output and are
paid for in extra recovery fetches, so they are a discovery decision rather than a pure cut.

## Limits

* One machine, one corpus, one session; the shipped A/B ran in a loaded window and does not resolve.
* The corpus is one production arm (digest `66165df36a10c023`); parity beyond it rests on the
  equivalence test over the five fixtures (short shell, `__next`, uppercase ids, plain page, noscript
  branch) plus the two call-site oracles.
* Production runs the phase in a child worker, so the saving is worker headroom rather than arm
  wall.
