> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 0ae8bce7
> - **Status:** phase 0 and phase 1 landed; delivery measured and blocked on a probe defect — 70 of 500 boards land (762 of 4,356 openings, 17%)

# Closing the catalogue coverage gap

## The blocking finding

**Registering boards does not deliver openings, and the registration delivers 17% of
what it appears to.**

Draining discovery over the 500 curated boards in an isolated data directory
(`tools/coverage_drain.py`, 8 rounds, uncapped preset, seeded with the live 2,249-row
registry):

| | |
|---|---|
| Boards registered | 500 |
| Boards that reached `active` | **70 (14%)** |
| Openings promised | 4,356 |
| Openings actually delivered | **762 (17%)** |

And the drain **stalls**. Round 1 approves 71; rounds 2 through 8 approve 0, with
`active` frozen at 2,320 and ~258 candidates deferred by cap in every round. Repeated
runs deliver nothing more.

| Adapter | Boards | Landed | Openings |
|---|---|---|---|
| static | 276 | 30 | 1,514 |
| workday | 17 | **0** | 1,065 |
| greenhouse | 44 | **0** | 497 |
| smartrecruiters | 16 | 16 | 432 |
| ashby | 39 | **0** | 334 |
| bamboohr | 47 | **0** | 192 |
| lever | 22 | **0** | 181 |
| breezy | 15 | **0** | 69 |
| teamtailor | 13 | 13 | 26 |
| jazzhr | 8 | 8 | 32 |
| recruitee | 3 | 3 | 14 |

### Why: two counters that were assumed to agree

`tools/coverage_verify.py` counts rows from the vendor's **JSON API**.
Discovery's probe counts job links in the fetched **HTML**. Auto-approval requires a
positive `jobsFound`. For a single-page app those numbers are unrelated:

- An Ashby board returns **120 rows** from `api.ashbyhq.com` (verified by fetch) while
  its page ships **no job links at all**.
- The stuck pending rows all report `jobsFound=0` with `lastProbeStatus=ok`.

So the probe says "this board is healthy" and "this board has zero jobs" at the same
time, and the gate believes the second number. The row sits in pending forever, and
because a pending row is deduped as `existing_id` on the next cycle, it is never
re-probed for the evidence it needs. That is the stall.

Every adapter that lands is exactly the one whose board page is server-rendered:
SmartRecruiters, Teamtailor, JazzHR, Recruitee, plus 30 static pages. Every
JavaScript-rendered adapter lands nothing — and those hold the large boards.

**So the honest position is that phase 1 is 17% delivered, and the remaining 83% is
blocked on a probe defect rather than on registration.** This is now the top priority
in the plan, ahead of everything else, because it gates the entire effort.

## Why this plan exists

The catalogue sweep says Baluffo holds 4,867 of the index's 15,332 openings
(**31.7%**). The premise is that no opening should be left behind, because a job
board's whole purpose is the openings it carries. Four earlier estimates in this
effort were wrong before measurement, so every number here is quoted with how it
was obtained — including the two that were wrong in this plan's favour.

## Where the gap stands

Baseline, matched on **posting URL** first, which is the only definitive signal:
both boards publish the same URL for the same requisition.

| | Count |
|---|---|
| GJI openings considered | 15,332 |
| Matched | 4,867 (31.7%) |
| — by URL, definitive | 3,368 |
| — by title only (over-claims) | 1,499 |
| Missing | 10,465 |

Of the 10,465 missing:

| | Openings |
|---|---|
| **Registered by phase 1** — board now in the curated tables | **4,465** |
| Remains on an unregistered board | 3,522 |
| Remains, blocked on a missing adapter | 1,689 |

5,211 openings still missing. Two corrections are baked into those numbers:

**The "registered but not collecting" bucket was a measurement bug.** This plan
previously reported 4,589 rows in that state and scoped a phase around diagnosing
them. The audit matched registry rows by testing whether *any* path segment was a
substring of *any* registry id, which labelled 4,564 of the 4,589 as registered when
those boards were never registered at all. The real figure is **58**. The bug named a
collection failure where the cause was registration, so phase 2 would have hunted a
defect that does not exist while phase 1 quietly fixed it.

**Measured coverage is 31.7%, not the 25.4% first quoted.** The 25.4% came from
title matching alone. Matching on posting URL is definitive, and the two bases are
now reported separately because title matching over-claims — `HR Business Partner -
US Operations (West)` and `(East)` collapse to one title.

## Landed: 513 boards, 4,465 openings

Registered in `src/curated_coverage_boards.json` (500 rows) plus 13 hand-curated
rows in `config.py`. Every row was proposed by a specific missed opening and
registered only after the board was verified to serve openings.

| Verdict | Boards | Meaning |
|---|---|---|
| `collects` | 270 | Returned rows when fetched |
| JS-rendered | 234 | Listing renders only in a browser |
| Review | 208 | Undecided — HTTP error or unrecognised shape |

Adapter mix: static 276, bamboohr 47, greenhouse 46, ashby 40, lever 22, workday 17,
smartrecruiters 17, breezy 15, teamtailor 13, jazzhr 8, recruitee 3.

The 234 browser-rendered boards were registered on measured evidence, not
assumption: of the boards this repo already collects, **101 of 159** reach their rows
only through the browser path, so a listing a plain GET cannot read is the normal
case. Refusing them would have discarded openings for want of a verification method
the runtime does not use.

Spot-checked after landing: 14 of 14 sampled boards across five adapters return rows.

### The split-store hazard, now pinned by tests

The audit dedupes against the **live registry**; the registration target is the
**repo's curated tables**. Those are different stores and only the live one reflects
reality on a given box, so the first landing registered four boards twice — Voodoo,
2K Czech, Hangar 13 and Yggdrasil, all already present in the hand-curated literal
from earlier work. Two tests now forbid it: no board may appear twice in the merged
list, and the opening counts of the removed duplicates must survive on the rows that
replaced them.

## Next, in priority order

### 1. Make the probe able to see a JavaScript-rendered board — blocking, everything

The single defect standing between 17% and most of the rest. Discovery's probe counts
job links in fetched HTML; auto-approval requires a positive count; every
JavaScript-rendered ATS ships none, so those boards sit in `pending` with
`jobsFound=0` and are never re-probed.

The runtime already has the machinery — the fetch report carries
`browserFallbackRecommended`, and `browserRecovery` / `web_browser_recovery` exist in
`source_discovery`. The gap is that a curated seed for a *known* vendor with a *known*
API never reaches them.

Two routes, and the choice should be made on evidence rather than taste:

1. **Count rows from the vendor API during the probe** for known adapters, the way
   `coverage_verify.py` does. Smallest change, and it reuses logic that already
   exists and is tested. Risk: two row-counting implementations.
2. **Route JS boards through the existing browser fallback.** No new counting, but
   far slower, and it would put a Playwright fetch in front of all 276 static
   candidates.

Recommendation: route 1, and validate it against the drain harness — the acceptance
criterion is that the landed share rises above 17% on a re-run, measured the same way.

Whichever is chosen, the stall must be addressed too: a pending row deduped as
`existing_id` is never re-probed, so a seed that fails once can never recover. That is
a separate defect from the counting one.

### 2. Fix the 30 extraction-limited boards — 764 openings

Diagnosed, not started. Every one carries the same signature:
`fetchedCount == keptCount` with `lowConfidenceDropped == 0`. Nothing is filtered, so
the quality gate is not at fault — extraction finds fewer rows than the board serves.

| Misses | Kept | Source |
|---|---|---|
| 283 | 340 | `hrmos.co/pages/cygames` |
| 134 | 145 | `jobs.ea.com` |
| 121 | **11** | `jobs.jobvite.com/amberstudiocareers` |
| 74 | **7** | `careers.garena.com` |
| 43 | 80 | `careers.activision.com/careers` |
| 13 | **1** | `outfit7.com` |

A board keeping 11 rows while the index lists 121 is an extraction failure — and
given item 1, likely the same JavaScript root cause rather than a separate one. Worth
re-diagnosing *after* item 1 lands, since the two may collapse into one fix.

One confirmed defect to fix first: `https://careers.activision.com/careers` now
returns **HTTP 404** while its source still reports `ok` with 80 kept. A 404 reporting
`ok` will recur, so this is worth fixing on its own account.

### 3. Resolve the 208 review boards — up to 1,801 openings

Left undecided on purpose. 81 are HTTP errors and 125 are unrecognised payload shapes.
Neither class may be registered blind or discarded: an undecided verdict is not an
empty board. Also likely to shrink once item 1 lands.

### 4. Re-scope the "adapter backlog" — 1,689 openings

**Probably mis-scoped, and item 1 may dissolve it.** `tools/coverage_boards.py` calls
hrmos, feishu and csod `unsupported_vendor` because no dedicated adapter exists — but
`hrmos.co/pages/cygames` is already registered as a static row and keeps 340 rows. If
the probe can read a rendered board, the static path may reach all 31 hrmos tenants
(845 openings, including Capcom, Square Enix, Cygames, Nexon, Game Freak and Spike
Chunsoft) without any new adapter. Do not build three vendor integrations before
testing that.

### 5. Blank country — 4,493 rows

Deferred by agreement. Orthogonal to coverage: these rows are present but
unclassifiable by region, which understates EU counts rather than losing openings.

### 6. GB/UK and England/Scotland/Wales — ~365 rows

Deferred by agreement, pending a `country_acceptance.json` contract change.

### 7. Phase 3 — the 82% Google Sheet dependency

81.9% of the feed is one community spreadsheet. That is simultaneously the ceiling on
coverage and a single point of failure. Kept separate because it changes the shape of
the system rather than filling it.

## Carried over

**Gamucatex junk rows** (`Explore content`, `Community Program`, `Open positions (4)`)
— pre-existing, not a regression. Defect localised to `static_detail_link_rows` at
`_runner.py:343`, which has no title gate. Only `larian.py` and `supercell.py` call it,
so gate it on `looks_like_listing_role_title` after confirming their real titles pass.

**`docs/plans/voodoo-ashby-board-migration-plan.md`** — delete once a release lands.

## Standing verification for every step

- Adapters exercised against a registered control board with a known expected count,
  never a raw endpoint alone.
- `npm run test:py` and `npm run lint:repo-guardrails` before each commit.
- `git status data/` to confirm no discovery audit artefacts leaked.
- Coverage re-measured after each step, with URL-matched and title-only reported
  separately, and **measured against the live feed** — `data/baluffo-runtime.db` is
  generation 2026-09-17 and produced three wrong conclusions during this effort.
