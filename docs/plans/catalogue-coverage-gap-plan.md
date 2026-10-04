> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 2b5759cd
> - **Status:** phases 0 and 1 landed; 3,043 of 4,356 registered openings proven delivered (69%); every structured adapter lands fully; the remaining 31% is static boards with no listing a GET can read

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. See [`INDEX.md`](../INDEX.md).

## Where this stands

Registration covers **4,356 openings** across 513 boards. **3,043 of them are proven to
reach the registry** — up from 762 when delivery was first measured. The difference is
tracked as two numbers throughout, because "registered" is not "delivered" and only the
second one is a result.

| | Openings |
|---|---|
| Registered and **proven delivered** | **3,043 (69%)** |
| Registered but blocked on delivery | 1,313 |
| On an unregistered board | 3,522 |
| Blocked on a missing adapter | 1,689 |

Measured with `tools/coverage_drain.py`, which drains discovery over the curated boards
in an isolated data directory and counts what reaches `active`.

| Adapter | Boards | Landed | Openings | Delivered |
|---|---|---|---|---|
| workday | 17 | **17** | 1,065 | **1,065** |
| greenhouse | 44 | 40 | 497 | 456 |
| smartrecruiters | 16 | 16 | 432 | 432 |
| ashby | 39 | **39** | 334 | **334** |
| bamboohr | 47 | **47** | 192 | **192** |
| lever | 22 | 20 | 181 | 165 |
| breezy | 15 | 15 | 69 | 69 |
| jazzhr | 8 | 8 | 32 | 32 |
| teamtailor | 13 | 13 | 26 | 26 |
| recruitee | 3 | 3 | 14 | 14 |
| static | 276 | 30 | 1,514 | 258 |
| **Total** | **500** | **248 (49%)** | **4,356** | **3,043 (69%)** |

Delivery is reported in openings, not boards: one board with 176 promised openings and
one with a single opening are not comparable units, and a board-count headline hides
exactly the concentration that matters.

## Five defects fixed, all found by running discovery

Delivery went 762 → 1,383 → 1,777 → 2,851 → 3,043. Every step was a defect that reading
the code would not have surfaced, and every one had the same shape: **the probe was
looking somewhere the openings were not.**

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

A sixth change removed a queue deadlock rather than adding coverage: a probe that
reaches a board and finds nothing had no exit, so the same rows occupied every queue
slot forever while 226 candidates waited behind them. Zero-yield probes are now recorded
and time-box-quarantined — "stop re-probing for now", never "this board is empty".
Verified safe: 247 boards were quarantined and none was active.

## What remains

**1. 246 of 276 static boards — ~1,256 openings.** The only remaining block on the
registration. These have no listing a plain GET can read, so no endpoint change helps.
Checked rather than assumed: `static_probe_evidence` and an independent counter agree on
14 of 14 sampled boards, so this is the boards being unreadable rather than a detector
disagreeing with itself. They need the rendered path — the runtime's existing browser
fallback, which discovery's probe does not currently use — or a per-vendor extractor.

**2. Triage the residual probe failures** — 6, from 71.

**3. Resolve the 208 review boards** — up to 1,801 openings. 81 HTTP errors and 125
unrecognised payload shapes. Neither class may be registered blind or discarded: an
undecided verdict is not an empty board.

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
deliberately untagged. The five probe fixes are worth having regardless: before them a
release would have carried 17% of the registration, and it now carries 69%.

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
