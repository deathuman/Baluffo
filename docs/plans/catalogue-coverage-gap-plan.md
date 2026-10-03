> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 0ae8bce7
> - **Status:** phase 0 and phase 1 landed — 513 boards behind 4,465 openings registered; 5,211 openings remain

# Closing the catalogue coverage gap

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

### 1. Verify the registrations reach a running install — blocking, small

**These registrations have never run.** They are discovery *seed candidates*:
`STATIC_DISCOVERY_CANDIDATES` feeds `stage_curated_seed_candidates`, and a seed
becomes a registry row only after it is probed and auto-approved
(`source_registry_auto_approval.py`). So 4,465 openings are currently promised and
zero are proven delivered.

What is known about the path: `curated_seed` gets a +20 evidence bonus and short-circuits
the registry-penalty ranking (`core_scoring.py:92`), so seeds are prioritised rather
than demoted for resembling an existing row. What is **not** known is whether 504
seeds survive probing, or whether auto-approval demotes them.

The work: run discovery over the new candidates in an isolated `--output-dir`, and
count how many reach `active`. Expect attrition and expect the reasons to be
interesting — that measurement is the whole point. **Until it is done, treat 4,465 as
an upper bound, not a result.** This is the highest-value next step because every
other number here is downstream of it.

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

A board keeping 11 rows while the index lists 121 is an extraction failure. Likely
causes: JS-rendered listings, unpaged endpoints, detail links that never resolve to a
job page — the same shape as the Gamucatex fall-through.

One confirmed defect to fix first: `https://careers.activision.com/careers` now
returns **HTTP 404** while its source still reports `ok` with 80 kept. A stale
registration the fetch report does not surface as an error, which is itself worth
fixing — a 404 that reports `ok` will recur.

### 3. Resolve the 208 review boards — up to 1,801 openings

Left undecided on purpose. 81 are HTTP errors (retry, then decide whether the board
moved) and 125 are unrecognised payload shapes. Neither class may be registered blind
or discarded: an undecided verdict is not an empty board.

Cheapest path is to re-run the probe with the browser fallback the runtime already
has, which should convert most JS-rendered and shape-unknown cases into `collects`
without new code.

### 4. Adapters for hrmos, feishu and csod — 1,689 openings

**Re-scope this before starting it.** `tools/coverage_boards.py` classifies these
vendors as `unsupported_vendor` because no dedicated adapter exists — but that may be
the wrong conclusion. `hrmos.co/pages/cygames` is *already registered as a static row*
and keeps 340 rows, so the static scraper reads hrmos fine. The likely finding is that
these 1,689 openings need **static registrations per tenant**, not new adapters, which
is a much smaller job than three vendor integrations.

31 hrmos tenants and 845 openings, including Capcom, Square Enix, Cygames, Nexon,
Game Freak and Spike Chunsoft. Prove the static path on two tenants first before
committing to either approach.

### 5. Blank country — 4,493 rows

Deferred by agreement. Orthogonal to coverage: these rows are present but
unclassifiable by region, which understates EU counts rather than losing openings.
Independent of everything above and safe to pick up at any time.

### 6. GB/UK and England/Scotland/Wales — ~365 rows

Deferred by agreement, pending a `country_acceptance.json` contract change. The
contract work from earlier landed (`src/jobs/country_contract.py`, 10 labels
accepted) and deliberately did not touch GB/UK.

### 7. Phase 3 — the 82% Google Sheet dependency

81.9% of the feed is one community spreadsheet. That is simultaneously the ceiling on
coverage and a single point of failure. Kept separate from the phases above because
it changes the shape of the system rather than filling it.

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
