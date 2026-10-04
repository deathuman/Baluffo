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

### 1. The freshness window was not ageing boards out — 1,892 boards — **DONE**

In the most recent full fetch, **1,892 of 2,013 registered boards (94%) were skipped** as
`cache_within_freshness_window`, with a median last *successful* fetch of **121 days**
and 1,599 beyond 90 days.

| Registered boards | Count |
|---|---|
| Skipped as fresh | 1,892 |
| Fetched, kept jobs | 159 |
| Fetched, error | 84 |
| Fetched, kept zero | 16 |

The 16 are all `time_budget_exceeded` after 30-47 seconds, so not one board is
confirmed dead-and-silent. The 1,754 excluded rows are `excluded`, which per the repo's
own rule means *not fetched*, never "no openings".

**Cause.** `_apply_status_state` called `refresh_next_eligible_check_at` for *every*
terminal status, including `excluded`, while `apply_excluded_source_state` deliberately
never moves `lastSuccessAt`. So each skip pushed the deadline out by another window while
the success clock stayed frozen. Eligibility had become a function of how often the
pipeline runs rather than of the configured freshness window — with a 720-minute
empty-source window and a shorter cadence, the deadline slid forever.

Nothing surfaced it, because `lastCheckedAt` did not move either — a skip is not a
check — so the only clock being advanced was the deadline itself. That is why the report
showed 94% skipped with no visible staleness anywhere in it.

**Fix.** A skip now leaves `nextEligibleCheckAt` alone. The skip reason is threaded
explicitly from the excluded report rather than through a transient entry field, so a
stale reason cannot suppress the advance after a later successful fetch. `not_modified_304`
counts as a non-fetch skip on purpose: a 304 asserts the cached copy is current, which is
a reason to leave the deadline alone rather than extend it. Real fetches, successful and
failed, still advance it.

Six tests pin it; four fail against the pre-fix behaviour, verified by reverting the
guard.

**To confirm it works in production:** after a release, two consecutive full runs should
show the skipped count falling rather than holding near 94%, and the median age of a
skipped board should stop growing between runs.

### 2. Second registration wave — 2,383 openings, 213 boards

The catalogue has moved since the first sweep, and the extractor now deduplicates against
both the live registry *and* the repo's curated tables. 213 boards / 2,383 openings are
genuinely new — not already registered, not already curated.

| Adapter | Boards | Openings |
|---|---|---|
| static | 156 | 1,528 |
| workable | 29 | 440 |
| oracle_hcm | 2 | 169 |
| personio | 21 | 104 |
| ashby | 1 | 90 |
| greenhouse | 3 | 49 |
| smartrecruiters | 1 | 3 |

Splitting the 4,713-opening `unregistered_board` bucket by whether a probe can read the
board now:

| Sub-slice | Openings | Note |
|---|---|---|
| Probeable now | 3,584 | register through the existing pipeline |
| Unsupported vendor (hrmos, feishu) | 1,129 | see below |

**hrmos needs no adapter — 845 openings.** The `unsupported_vendor` label is simply
wrong. The tenant listing pages are fully server-rendered and the runtime's own detector
reads them directly:

| Tenant | Rows read |
|---|---|
| cgames | 100 |
| capcom | 92 |
| gamefreak | 59 |
| square-enix | 35 |
| nexon | 17 |

And `hrmos.co/pages/cygames` is *already* registered as a static row keeping 340 rows in
production, which is the control that settles it. So this is 31 per-tenant static
registrations, not a vendor integration. Highest value per unit of risk in the whole
remainder.

**feishu needs real work — 826 openings.** `moonton.jobs.feishu.cn` and
`lilithgames.jobs.feishu.cn` return HTTP 200 with **zero anchors**, the same JS-shell
shape as the 246 static boards. Unlike hrmos, no server-rendered path exists, so these
genuinely need either a JSON endpoint discovery or the browser path. Do not register them
blind.

Order of work: hrmos static registrations → the 3,584 probeable → workable/oracle_hcm/
personio → feishu as a separate investigation.

### 3. The 208 review boards — up to 1,801 openings

81 HTTP errors and 125 unrecognised payload shapes, deliberately left undecided. Now
cheaper to resolve: the audit-evidence fallback lets a board land on recorded evidence
even when its verdict stays unknown, so an undecided verdict no longer blocks coverage.
Re-probe with the browser path, then register what is still genuinely undecided rather
than discarding it — an undecided verdict is not an empty board.

### 4. The 82% Google Sheet dependency — an asset, not a liability

**Corrected framing.** This was previously written up as a single point of failure to be
reduced. That is wrong: the community spreadsheet is the best base source available, and
it is the reason the feed has breadth at all. The goal is therefore **not** to remove the
dependency but to stop it being the *ceiling* — i.e. to make sure the direct ATS and
static-scraped sources behind it are actually reaching the feed.

This is now item 1's successor rather than a parallel track. A freshness fix delivers more
openings only if the feed is not still dominated by one source, so the two are
sequential: fix the window, then confirm the recovered boards show up in the feed.

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
release would have carried 17% of the registration, and it now carries 98%. The same is
true of the freshness fix — before it a release would carry a registry that is 94%
unrefreshed, which caps everything above regardless of how many boards are registered.

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
