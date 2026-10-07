# Phase 5 — the static zero-set re-classification — Evidence Snapshot — 2026-10-07

> - **Status:** complete. **0 of 351** candidate boards are registerable, and the reason is
>   recorded rather than rounded up to a coverage win. The three boards that appeared to collect
>   are all false greens, and the filter that admitted them is **not** what is wrong.
> - **Basis:** a control-first probe of every `unknown` static board with recorded openings,
>   run through the real static runtime with `force_refresh_all`, plus a read of each collecting
>   board's own HTML.
> - **Canonical for:** Phase 5's outcome, the `unknown`-board verdict distribution, and the
>   decision *not* to touch `GAME_ROW_KEYWORDS`. Delivery detail:
>   [`delivery-v9-2026-10-07.md`](delivery-v9-2026-10-07.md). Filter detail:
>   [`shadow-cleanup-and-country-filter-2026-10-07.md`](shadow-cleanup-and-country-filter-2026-10-07.md).

## What was probed

The `unknown` bucket was 1,217 boards. After removing those with no report row and those with no
recorded GJI openings behind them, **351 boards** remained — a probe worth spending, since each had
openings on record and Baluffo was credited with none.

Each board went through `run_static_studio_pages_source` with `force_refresh_all=True`, preceded by
a **known-good control** (`careers.tencent.com/search.html`, 2,238 bytes served) so a degraded
network could not read as "these boards have nothing".

## The verdict distribution

| verdict | boards | meaning |
|---|---:|---|
| `collects` | **3** | ≥1 game role kept |
| `empty` | 348 | fetched, kept nothing game-shaped |
| `unknown` | **0** | never used — every page was really retrieved |

Zero `unknown` is the load-bearing number: **no board is recorded `empty` on the strength of a
failed fetch.** All 351 pages answered.

The 348 empties decompose, from the v9 run's own shape evidence:

| shape | boards |
|---|---:|
| page fetched, nothing job-shaped on it | 250 |
| followed detail links, kept nothing | 98 |
| **had captured-JSON rows the plain path ignored** | **0** |

That last row matters: **no empty board is being lost to the capture lane.** The v9 run found
`renderedJsonRowsFound` on none of them, so lane 2.6 is not the missing instrument here — these
pages simply do not publish extractable postings.

## The three "collects" are all false greens

Read individually, all three are not jobs:

**`ordibeheshtstudio.com`** — 1 row, title `Tehran Game Convention`, linking to
`http://www.tehrangamecon.com/`. A convention, not a posting.

**`workwithindies.com`** — 1 row, title `Work With Zymartu Games`, which is the **page's own
`<title>`**. Not a role.

**`ichigoichie.org`** — 2 rows, titles `Guide our release to the outstretched arms of gamers
worldwide.` and `Help us tighten up the game and rally our allies 'round the globe'`. These are
marketing sentences that the static parser took as job titles. The board *is* real and *does*
publish postings — the index carries three anchors to `2026-08-marketing-manager`,
`2026-07-marketing-assistant` and `2026-08-social-media-intern`, each anchor reading only
`Apply now` — but the real titles (`Motivated Marketing Manager`, `Meticulous Marketer
(Freelance)`, `Instabook Aficionado (Intern)`) are **marketing roles**, not game roles. Correct
extraction would have produced zero game rows, not two.

So the honest count of registerable boards is **zero**, and the probe's own `collects` verdict is
shown here to be unreliable when its only test is a keyword match over an extracted title.

## The filter is not the defect, and tightening it would be a disaster

All three rows were admitted because `GAME_ROW_KEYWORDS` contains the bare token `game`. The
obvious "fix" is to drop it. Measured on the v9 feed first:

| | rows |
|---|---:|
| feed rows | 39,260 |
| rows kept by any game keyword | 9,432 |
| **rows whose only evidence is a generic token** (`game`/`games`/`gaming`/`gamer`/`gamers`) | **4,684** (11%) |

And those 4,684 rows are overwhelmingly **real game work** — `Game Designer` (118), `Senior Game
Designer` (26), `Game Developer` (22), `Game Producer` (12), `Lead Game Designer` (10), `Game
Artist` (8), `Technical Game Designer` (8), `Game Director` (8), `Senior Game Programmer` (7).
Removing the bare token would delete **half the game's rows** to kill three non-jobs.

**`GAME_ROW_KEYWORDS` is therefore left alone.** The defect is upstream and narrower: on
`ichigoichie.org` the static parser took a card's body prose for its title. That is a
title-extraction problem in the static listing path, it affects every static board that renders
`Apply now` anchors, and fixing it needs its own measurement — it is not a filter change and not
Phase 5's job.

## Outcome against the plan ledger

The ledger's Phase 5 row asked to re-classify the `unknown` + `rollup_only` buckets and "register
only the readable ones on measured counts". Measured on 351 probed boards, the readable ones
number **three**, and all three turn out to be false greens. **No rows were added to
`data/defaults/source-registry-active.seed.json`**; the seed stays at 1,878.

The largest untouched bucket is now measured rather than merely counted: of the 1,217 `unknown`
boards, **351 had openings worth probing and 866 did not**, and the 348 empties are pages that
answer with no extractable postings. The bucket is understood, not just tallied.
