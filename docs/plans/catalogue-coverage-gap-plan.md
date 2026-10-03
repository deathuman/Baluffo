> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** defa6eea
> - **Status:** phase 0 and phase 1 landed; delivery measured at 40% after two probe fixes; remaining 60% blocked on a queue deadlock and on boards with no fetchable listing

# Closing the catalogue coverage gap

## The blocking finding, and the two fixes that answered most of it

**Registering boards does not deliver openings.** Draining discovery over the 500
curated boards in an isolated data directory (`tools/coverage_drain.py`, uncapped
preset, seeded with the live 2,249-row registry) first measured delivery at **762 of
4,356 openings — 17%** — and the drain stalled after one round.

Two independent defects were blocking the same gate: auto-approval's requirement of a
positive `jobsFound`. Both were found by running discovery, not by reading it.

**1. The rows carried no `api_url`.** `endpoint_url()` probes the first of `api_url`,
`feed_url`, `board_url`, `listing_url`, so a row with only a `board_url` sent the probe
at the human-facing page — which for a single-page app ships no job links. The board
read as *healthy with zero jobs*, the gate believed the second number, and the row sat
in pending forever. The evidence was unambiguous: the only two adapters carrying an
`api_url` (SmartRecruiters, Recruitee) were the only provider adapters where every
board landed, while Greenhouse, Lever, Ashby, Workday, BambooHR and Breezy landed
none — despite APIs returning 18–122 rows on a plain GET.

**2. Ashby and Breezy were missing from the probe's `provider_specs`.** With no spec the
count fell through to a branch matching only HTML anchors, so a board whose `api_url`
returns JSON probed as zero. Ashby landed 0 of 39 while Greenhouse landed 40 of 44.

| Adapter | Boards | Landed before | Landed after | Openings |
|---|---|---|---|---|
| static | 276 | 30 | 30 | 1,514 |
| workday | 17 | 0 | 0 | 1,065 |
| greenhouse | 44 | 0 | **40** | 497 |
| smartrecruiters | 16 | 16 | 16 | 432 |
| ashby | 39 | 0 | **37** | 334 |
| bamboohr | 47 | 0 | 0 | 192 |
| lever | 22 | 0 | **20** | 181 |
| breezy | 15 | 0 | **15** | 69 |
| teamtailor | 13 | 13 | 13 | 26 |
| jazzhr | 8 | 8 | 8 | 32 |
| recruitee | 3 | 3 | 3 | 14 |
| **Total** | **500** | **70 (14%)** | **182 (36%)** | **4,356** |

**Delivered openings: 762 → 1,777. 17% → 40%.** Verified before wiring: 32 of 33
sampled endpoints return rows.

Workday stays at 0 deliberately. Its CXS endpoint is a POST behind a certifi-anchored
context, so no `api_url` can fix it, and a test asserts Workday rows carry none rather
than pretending otherwise.

### What still blocks the remaining 60%

**A queue deadlock, not a throttle.** The drain still stops after round 1: round 1
approves 185, every later round approves 0, `active` frozen at 2,434. The cause is that
the *same 21 rows* occupy every queue slot in every round. All 21 report
`jobsFound=0` with `lastProbeStatus=ok`, so auto-approval correctly refuses them — and
because they never approve, they never release their slot. Meanwhile
`healthyButDeferredByAdapter` is `{static: 226}`: 226 candidates the probe *did* call
healthy are starved behind them and never get a turn.

That is a deadlock, not a slow trickle, and it is fixable: a pending row that has made
no progress for N cycles should be evicted or demoted so it stops occupying a slot. The
repo's own guidance already warns against reading a zero as an answer — "the probe
signal is positive-only" — and these 21 rows are precisely that case being treated as
one.

Two smaller items behind it:

- **70 probe failures**, down from 148. Not yet triaged.
- **BambooHR 0 of 47** and **246 of 276 static boards**: boards with no public JSON
  listing, which need the rendered path rather than an `api_url`.

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
| Registered and **provably delivered** | **1,777** |
| Registered but blocked on delivery | 2,579 |
| Remains on an unregistered board | 3,522 |
| Remains, blocked on a missing adapter | 1,689 |

Two corrections are baked into those numbers:

**The "registered but not collecting" bucket was a measurement bug.** This plan
previously reported 4,589 rows in that state and scoped a phase around diagnosing
them. The audit matched registry rows by testing whether *any* path segment was a
substring of *any* registry id, which labelled 4,564 of the 4,589 as registered when
those boards were never registered at all. The real figure is **58**. The bug named a
collection failure where the cause was registration, so phase 2 would have hunted a
defect that does not exist while phase 1 quietly fixed it.

**Measured coverage is 31.7%, not the 25.4% first quoted.** The 25.4% came from title
matching alone. Matching on posting URL is definitive, and the two bases are reported
separately because title matching over-claims — `HR Business Partner - US Operations
(West)` and `(East)` collapse to one title.

**"Registered" is not "delivered", and this plan now treats those as different
numbers.** The registration covered 4,356 openings; 1,777 of them are proven to reach
the registry. The rest are real rows awaiting the delivery work above, not a claim.

## Landed: 513 boards, 4,356 openings registered

In `src/curated_coverage_boards.json` (500 rows) plus 13 hand-curated rows in
`config.py`. Every row was proposed by a specific missed opening and registered only
after the board was verified to serve openings.

| Verdict | Boards | Meaning |
|---|---|---|
| `collects` | 270 | Returned rows when fetched |
| JS-rendered | 234 | Listing renders only in a browser |
| Review | 208 | Undecided — HTTP error or unrecognised shape |

The 234 browser-rendered boards were registered on measured evidence, not assumption:
of the boards this repo already collects, **101 of 159** reach their rows only through
the browser path, so a listing a plain GET cannot read is the normal case.

`tools/coverage_refresh_curated.py` regenerates the file, so a hand-edit cannot
silently drop an `api_url` and return a board to the pending-forever state.

### The split-store hazard, pinned by tests

The audit dedupes against the **live registry**; the registration target is the
**repo's curated tables**. Those are different stores and only the live one reflects
reality on a given box, so the first landing registered four boards twice — Voodoo,
2K Czech, Hangar 13 and Yggdrasil. Two tests now forbid it.

## Next, in priority order

### 1. Break the queue deadlock — blocking

21 rows permanently occupy every queue slot because they report `jobsFound=0` and
never release. Until they are evicted, the 226 healthy static candidates the probe
already approved can never be promoted, and re-running discovery is pointless.

The fix is a progress-based eviction: a pending row that has not gained evidence for N
cycles should be demoted or dropped from the queue. This is also the honest reading of
the repo's own rule that a probe zero is not an answer — those 21 rows are undecided,
not empty, and must not be allowed to hold the queue.

Acceptance criterion: re-run `tools/coverage_drain.py` and see `approved` stay above 0
across rounds, with `deferredByCap` falling toward 0.

### 2. Triage the 70 probe failures

Down from 148 after the `api_url` fix. Not yet looked at individually.

### 3. BambooHR and the 246 remaining static boards

Neither has a public JSON listing, so `api_url` cannot help. These need the rendered
path — the runtime's existing browser fallback — or an adapter-specific extractor.
Together they hold ~1,256 undelivered openings.

### 4. Workday's 1,065 openings

The largest single undelivered block. Its CXS endpoint is a POST needing a
certifi-anchored context; the runtime already implements that in
`provider_structured_listing.py`, so the work is wiring the discovery probe to it, not
writing it.

### 5. Resolve the 208 review boards — up to 1,801 openings

Left undecided on purpose. 81 HTTP errors and 125 unrecognised payload shapes.
Neither class may be registered blind or discarded: an undecided verdict is not an
empty board.

### 6. Re-scope the "adapter backlog" — 1,689 openings

**Probably mis-scoped.** `tools/coverage_boards.py` calls hrmos, feishu and csod
`unsupported_vendor` because no dedicated adapter exists — but
`hrmos.co/pages/cygames` is already registered as a static row and keeps 340 rows. Test
the static path on two tenants before building anything.

### 7. Blank country — 4,493 rows

Deferred by agreement. Orthogonal to coverage: these rows are present but
unclassifiable by region, which understates EU counts rather than losing openings.

### 8. GB/UK and England/Scotland/Wales — ~365 rows

Deferred by agreement, pending a `country_acceptance.json` contract change.

### 9. Phase 3 — the 82% Google Sheet dependency

81.9% of the feed is one community spreadsheet. That is simultaneously the ceiling on
coverage and a single point of failure. Kept separate because it changes the shape of
the system rather than filling it.

## Carried over

**Gamucatex junk rows** (`Explore content`, `Community Program`, `Open positions (4)`)
— pre-existing, not a regression. Defect localised to `static_detail_link_rows` at
`_runner.py:343`, which has no title gate. Only `larian.py` and `supercell.py` call it,
so gate it on `looks_like_listing_role_title` after confirming their real titles pass.

**The 30 extraction-limited boards** (764 openings) — `fetchedCount == keptCount` with
`lowConfidenceDropped == 0`, so nothing is filtered and extraction simply finds fewer
rows. Given items 1-4 are the same JavaScript root cause, re-diagnose *after* those
land rather than fixing separately now.

**`careers.activision.com/careers` returns 404 while reporting `ok` with 80 kept** — a
stale registration the fetch report does not surface as an error.

**`docs/plans/voodoo-ashby-board-migration-plan.md`** — delete once a release lands.

## Standing verification for every step

- Adapters exercised against a registered control board with a known expected count,
  never a raw endpoint alone.
- `npm run test:py` and `npm run lint:repo-guardrails` before each commit.
- `git status data/` to confirm no discovery audit artefacts leaked.
- Coverage re-measured after each step, with URL-matched and title-only reported
  separately, and **measured against the live feed** — `data/baluffo-runtime.db` is
  generation 2026-09-17 and produced three wrong conclusions during this effort.
- Delivery re-measured with `tools/coverage_drain.py` after any change to board rows
  or the probe, because *registered* and *delivered* are different numbers and only
  the second one is a result.
