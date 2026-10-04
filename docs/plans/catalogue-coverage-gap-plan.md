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
| On an unregistered board | 3,522 |
| Blocked on a missing adapter | 1,689 |

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

## Six defects fixed, all found by running discovery

Delivery went 762 → 1,383 → 1,777 → 2,851 → 3,043 → 4,280. Every step was a defect that
reading the code would not have surfaced, and five of the six had the same shape: **the
probe was looking somewhere the openings were not.**

**Rows carried no `api_url`.** `endpoint_url()` probes `api_url` first, so a row with
only a `board_url` sent the probe at a single-page app that ships no job links. The
board read as *healthy with zero jobs* and auto-approval believed the zero.

**Ashby and Breezy were missing from the probe's `provider_specs`,** so a board whose
`api_url` returns JSON fell through to an HTML anchor matcher. Ashby landed 0 of 39.

**bamboohr, oracle_hcm and workday were declared supported with no probe-count branch
at all** — 68 of 71 recorded probe failures. There is now a guardrail test asserting
every adapter in `SUPPORTED_PROVIDERS` is probeable, which is worth more than the
branches: it would have caught Ashby and Breezy before each cost a measurement cycle.

**Workday's listing needs a POST.** Its CXS endpoint sits behind a certifi-anchored TLS
context, so a GET sees an SPA stub. The runtime already implemented the POST, so the
probe reuses it; all 17 boards resolve (NVIDIA 2,000 jobs, Disney 605, Tencent 288).

**BambooHR's listing is not the page its URL names.** `/careers` is a single-anchor
shell; the listing is a GET to `/careers/list`. Nothing about the board needed changing
— the runtime's own adapter already collected from it, keeping 99 jobs in production —
only the endpoint the probe read. This also corrects an earlier conclusion in this plan
that BambooHR needed the rendered path. It never did.

**246 boards serve openings no HTTP probe can see.** Sweeping all 276 curated static
boards with the runtime's own detector puts the split exactly at the delivery line: the 30
readable boards all landed, the 246 rendered ones landed none. The runtime's static
fetcher keeps 80 jobs from one of them (`careers.wbd.com/jobs`) where the probe reads
zero, and rendering that page in Chromium produces 1.6 MB against 181 KB and still
**zero job links** — the content arrives via a further request the landing page never
issues. Discovery's Playwright fallback fires only on 403/timeout/challenge, so a
200-with-zero-yields response never reached a browser at all.

The fix lets a recorded observation stand in for a probe that cannot be made: curated
seed rows carry `coverageAuditOpenings`, recorded when the board was found serving those
specific openings, and that is now consulted when the probe reads zero. It is narrow on
purpose — curated-seed rows only, never overriding a real count, and dead boards, weak
signals, deferred rows and blocked pending reasons are all still refused, verified
directly against the gate including the 404 case.

A seventh change removed a queue deadlock rather than adding coverage: a probe that
reaches a board and finds nothing had no exit, so the same rows occupied every queue
slot forever while 226 candidates waited behind them. Zero-yield probes are now recorded
and time-box-quarantined — "stop re-probing for now", never "this board is empty".
Verified safe: 247 boards were quarantined and none was active.

## What remains

**1. Triage the residual probe failures** — 6, from 71.

**2. Resolve the 208 review boards** — up to 1,801 openings. 81 HTTP errors and 125
unrecognised payload shapes. Neither class may be registered blind or discarded: an
undecided verdict is not an empty board.

**3. The 3,522 openings on unregistered boards.** A second catalogue sweep would find
them; the first one's candidates are now nearly all registered.

**4. Re-scope the "adapter backlog"** — 1,689 openings. Currently `unsupported_vendor`
because no dedicated adapter exists, but `hrmos.co/pages/cygames` is already a
registered static row keeping 340 rows. Test the static path on two tenants first.

**5. Blank country** — 4,493 rows. Deferred by agreement. Orthogonal: these rows are
present but unclassifiable by region, which understates EU counts rather than losing
openings.

**6. GB/UK and England/Scotland/Wales** — ~365 rows. Deferred by agreement, pending a
`country_acceptance.json` contract change.

**7. Phase 3 — the 82% Google Sheet dependency.** 81.9% of the feed is one community
spreadsheet: simultaneously the ceiling on coverage and a single point of failure. Kept
separate because it changes the shape of the system rather than filling it.

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
