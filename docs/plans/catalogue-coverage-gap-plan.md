> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** d5491e13
> - **Status:** phases 0 and 1 landed; 4,280 of 4,356 registered openings proven delivered (98%); the registration is effectively complete

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. See [`INDEX.md`](../INDEX.md).

## Where this stands

Registration covers **4,356 openings** across 513 boards. **4,280 of them are proven to
reach the registry** — up from 762 when delivery was first measured. The difference is
tracked as two numbers throughout, because "registered" is not "delivered" and only the
second one is a result.

| | Openings |
|---|---|
| Registered and **proven delivered** | **4,280 (98%)** |
| Registered but not landing | 76 |
| Still on an unregistered board | 4,713 |
| Of which blocked on a missing adapter | 1,689 |

Measured with `tools/coverage_drain.py`, which drains discovery over the curated boards
in an isolated data directory and counts what reaches `active`.

| Adapter | Boards | Landed | Openings | Delivered |
|---|---|---|---|---|
| static | 276 | 271 | 1,514 | 1,492 |
| workday | 17 | 17 | 1,065 | 1,065 |
| greenhouse | 44 | 43 | 497 | 459 |
| smartrecruiters | 16 | 16 | 432 | 432 |
| ashby | 39 | 39 | 334 | 334 |
| bamboohr | 47 | 47 | 192 | 192 |
| lever | 22 | 20 | 181 | 165 |
| breezy | 15 | 15 | 69 | 69 |
| jazzhr | 8 | 8 | 32 | 32 |
| teamtailor | 13 | 13 | 26 | 26 |
| recruitee | 3 | 3 | 14 | 14 |
| **Total** | **500** | **492 (98%)** | **4,356** | **4,280 (98%)** |

Delivery is reported in openings, not boards: one board with 176 promised openings and
one with a single opening are not comparable units, and a board-count headline hides
exactly the concentration that matters.

The eight boards still not landing are two empty Greenhouse and Lever boards, four thin
static boards, and `vivastudios.com`, whose server disconnects.

## What is left, in priority order

Ordered by measured size, and by whether the item blocks others. Everything below is
outside the registration, which is finished.

### 1. The freshness window is not ageing boards out — 1,892 boards

**The largest remaining item, and it is not on any board.** In the most recent full
fetch, **1,892 of 2,013 registered boards (94%) were skipped** as
`cache_within_freshness_window`. Their median last *successful* fetch was **121 days
ago**, and 1,599 have not fetched successfully in over 90 days.

| Registered boards | Count |
|---|---|
| Skipped as fresh | 1,892 |
| Fetched, kept jobs | 159 |
| Fetched, error | 84 |
| Fetched, kept zero | 16 |

The 16 are all `time_budget_exceeded` after 30-47 seconds, so not one board is
confirmed dead-and-silent. The 1,754 excluded rows are `excluded`, which per the repo's
own rule means *not fetched*, never "no openings".

The mechanism is in `src/jobs/state_incremental.py`. `get_incremental_cache_decision`
returns `skip_fresh` whenever `nextEligibleCheckAt` is in the future, and that value is
computed from **when the source was last checked** rather than when it last succeeded.
The one guard against exactly this — `not _has_fetch_success_history(entry)` — only
rescues a board that has *never* succeeded, so a board that succeeded once long ago and
then went stale keeps being skipped.

**This is stated as a strong hypothesis, not a confirmed diagnosis**, because the
fetch report does not carry `nextEligibleCheckAt` and the state store is not available
locally. Step one is therefore to read the actual values before changing anything.

- Confirm by reading `nextEligibleCheckAt` for the 1,892 skipped boards.
- Then either advance the window from `lastSuccessAt` rather than the last check, or
  add a hard staleness ceiling so no board can sit unrefreshed indefinitely.
- Acceptance: a full run fetches a meaningful share of the 1,599 boards stale beyond 90
  days, and the median age of a skipped board stops growing between runs.

### 2. Second registration wave — up to ~2,600 openings

4,713 openings sit on boards that are still unregistered. The pipeline for this is built
and proven (`coverage_audit` → `coverage_boards` → `coverage_verify` → `coverage_drain`),
and it now has the audit-evidence fallback that the first wave lacked, so boards no
probe can see will still land.

Highest-value sub-slice first: **hrmos, feishu and csod — 1,689 openings.** These are
classified `unsupported_vendor` only because no *dedicated adapter* exists, but
`hrmos.co/pages/cygames` is already registered as a **static** row keeping 340 rows. So
the likely answer is 31 per-tenant static registrations rather than three vendor
integrations. Prove it on two tenants before building anything.

### 3. The 208 review boards — up to 1,801 openings

81 HTTP errors and 125 unrecognised payload shapes, deliberately left undecided. Now
cheaper to resolve: the audit-evidence fallback means a board can land on recorded
evidence even when the verdict stays unknown. Re-run the probe with the browser path,
then register what is still genuinely undecided rather than discarding it.

### 4. Phase 3 — the 82% Google Sheet dependency

81.9% of the feed is one community spreadsheet: simultaneously the ceiling on coverage
and a single point of failure. Kept separate from the above because it changes the shape
of the system rather than filling it. **It should be started before item 1 lands**, since
a stale-refresh fix delivers more openings only if the feed is not still dominated by
one sheet.

### 5. Data quality, deferred by agreement

- **Blank country — 4,493 rows.** Orthogonal to coverage: present but unclassifiable by
  region, which understates EU counts rather than losing openings.
- **GB/UK and England/Scotland/Wales — ~365 rows.** Pending a `country_acceptance.json`
  contract change.

### 6. Carried-over defects

- **The 30 extraction-limited boards** (764 openings). `fetchedCount == keptCount` with
  `lowConfidenceDropped == 0`, so nothing is filtered and extraction finds fewer rows.
  Re-diagnose now that the delivered set has changed.
- **`careers.activision.com/careers` returns 404 while reporting `ok` with 80 kept** — a
  stale registration the fetch report does not surface as an error. A 404 reporting `ok`
  will recur, so this is worth fixing on its own account.
- **Gamucatex junk rows** — pre-existing. `static_detail_link_rows` at `_runner.py:343`
  has no title gate; only `larian.py` and `supercell.py` call it.
- **`plans/voodoo-ashby-board-migration-plan.md`** — delete once a release lands.

## Sequencing note

Nothing here reaches a running install until a release ships, and 0.3.007 is
deliberately untagged. The six probe fixes are worth having regardless: before them a
release would have carried 17% of the registration, and it now carries 98%.

## Correction on record

An earlier version of this plan reported 4,589 rows as "registered but not collecting"
and scoped a phase around diagnosing them. That was a **measurement bug**: the audit
matched registry rows by testing whether *any* path segment was a substring of *any*
registry id, labelling 4,564 of the 4,589 as registered when those boards were never
registered at all. The real figure is **58**.

Measured coverage is **31.7%**, not the 25.4% first quoted: that came from title
matching alone, whereas posting-URL matching is definitive. Both bases are reported
separately because title matching over-claims — `HR Business Partner - US Operations
(West)` and `(East)` collapse to one title.

## Carried over

- **The 30 extraction-limited boards** (764 openings). `fetchedCount == keptCount` with
  `lowConfidenceDropped == 0`, so nothing is filtered and extraction finds fewer rows.
  Given the JavaScript root cause above, re-diagnose *after* the rendered-path work
  rather than fixing separately.
- **`careers.activision.com/careers` returns 404 while reporting `ok` with 80 kept** — a
  stale registration the fetch report does not surface as an error.
- **Gamucatex junk rows** — pre-existing, not a regression. Localised to
  `static_detail_link_rows` at `_runner.py:343`, which has no title gate. Only
  `larian.py` and `supercell.py` call it, so gate it on `looks_like_listing_role_title`
  after confirming their real titles pass.
- **`plans/voodoo-ashby-board-migration-plan.md`** — delete once a release lands.

## Sequencing note

Nothing here reaches a running install until a release ships, and 0.3.007 is
deliberately untagged. The six probe fixes are worth having regardless: before them a
release would have carried 17% of the registration, and it now carries 98%.

## Standing verification for every step

- Adapters exercised against a registered control board with a known expected count,
  never a raw endpoint alone.
- `npm run test:py` and `npm run lint:repo-guardrails` before each commit.
- `git status data/` to confirm no discovery audit artefacts leaked.
- Coverage re-measured against the **live feed**, with URL-matched and title-only
  reported separately. `data/baluffo-runtime.db` is generation 2026-09-17 and produced
  three wrong conclusions during this effort.
- **Delivery re-measured with `tools/coverage_drain.py` after any change to board rows
  or the probe**, compared case-insensitively — the registry lowercases path segments,
  and an exact comparison reported Workday as 3/17 when all 17 had landed.
