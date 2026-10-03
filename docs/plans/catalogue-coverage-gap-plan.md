> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 3d31491e
> - **Status:** phase 0 and phase 1 landed; 1,777 of 4,356 registered openings proven delivered (40%); the remaining 60% is blocked on three measured defects, all named below

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. Supersedes no other page; see [`INDEX.md`](../INDEX.md).

## Where this stands

Registration covers **4,356 openings** across 513 boards. **1,777 of them are proven to
reach the registry.** The difference is tracked as two separate numbers throughout,
because "registered" is not "delivered" and only the second one is a result.

| | Openings |
|---|---|
| Registered and **proven delivered** | **1,777** |
| Registered but blocked on delivery | 2,579 |
| On an unregistered board | 3,522 |
| Blocked on a missing adapter | 1,689 |

Measured with `tools/coverage_drain.py`, which drains discovery over the curated
boards in an isolated data directory and counts what reaches `active`. That harness is
what caught the two defects below, and it is the only check that distinguishes a
registered board from a delivered one.

## The three defects blocking the remaining 60%

Each was found by running discovery, not by reading it, and each has a named mechanism
to fix.

### 1. A queue deadlock — 226 healthy candidates starved

The drain stops after round 1: round 1 approves 185, every later round approves 0,
`active` frozen at 2,434. The cause is that **the same 21 rows occupy every queue slot
in every round.** All 21 report `jobsFound=0` with `lastProbeStatus=ok`, so
auto-approval correctly refuses them — and because they never approve, they never
release their slot. Meanwhile `healthyButDeferredByAdapter` is `{static: 226}`:
candidates the probe *did* call healthy, waiting behind them forever.

A quarantine mechanism already exists and is nearly the right shape:
`probe_failure_memory.ProbeFailureMemory.record_failure` counts consecutive outcomes per
identity and stamps a **time-boxed** `quarantinedUntil`, with `quarantine_index()`
gating later candidates.

It does not fire here for a precise reason: `_QUARANTINE_CLASSES` is `{"dns", "ssl"}`
and `classify_probe_failure_class()` reads an **error string**. These 21 rows have no
error — they succeed, with zero. So they are never recorded, never quarantined, never
evicted.

**Fix:** record a zero-yield outcome as its own failure class. The existing time-boxed
semantics are exactly right, because quarantining must mean "stop re-probing this for N
days", not "this board is empty" — the repo's own rule is that a probe zero is not an
answer, and these 21 rows are undecided rather than empty.

**Acceptance:** re-run the drain and see `approved` stay above 0 across rounds with
`deferredByCap` falling toward 0.

### 2. Three supported adapters cannot be probed — 1,257 openings

`SUPPORTED_PROVIDERS` declares 14 adapters. `parse_probe_count` has no branch for
**`bamboohr`, `oracle_hcm` or `workday`** and raises `ValueError("unsupported adapter")`
for all three. That accounts for **68 of the 71 recorded probe failures.**

Those adapters look supported and are unprobeable: no URL helps, because the count
function refuses before reading anything. Workday alone is 1,065 openings and BambooHR
192.

**Fix:** give each a count path. Workday's is a POST to a CXS endpoint behind a
certifi-anchored context, and the runtime already implements exactly that in
`provider_structured_listing.py` — the work is wiring, not authoring.

**Guardrail, and the reason this is item 2:** an adapter can be declared supported while
being unprobeable, and nothing checks it. Add a test asserting every adapter in
`SUPPORTED_PROVIDERS` has a probe-count path. That would have caught `bamboohr`, and it
would have caught Ashby and Breezy too before each cost a full measurement cycle.

### 3. Boards with no fetchable listing — ~1,256 openings

246 of 276 static boards, and BambooHR's 47, have no public JSON listing a GET can read.
`api_url` cannot help them; they need the rendered path — the runtime's existing
browser fallback — or a per-vendor extractor. Largest remaining block after Workday and
the most open-ended, so it is sequenced last of the three.

## Then, in order

4. **Triage the residual probe failures** — 3 after item 2, plus one
   `Server disconnected without sending a response` on `vivastudios.com`.
5. **Resolve the 208 review boards** — up to 1,801 openings. 81 HTTP errors and 125
   unrecognised payload shapes. Neither class may be registered blind or discarded: an
   undecided verdict is not an empty board. Much should resolve once items 1-2 land.
6. **Re-scope the "adapter backlog"** — 1,689 openings. Currently `unsupported_vendor`
   because no dedicated adapter exists, but `hrmos.co/pages/cygames` is already a
   registered static row keeping 340 rows. Test the static path on two tenants before
   building anything.
7. **Blank country** — 4,493 rows. Deferred by agreement. Orthogonal: these rows are
   present but unclassifiable by region, which understates EU counts rather than losing
   openings.
8. **GB/UK and England/Scotland/Wales** — ~365 rows. Deferred by agreement, pending a
   `country_acceptance.json` contract change.
9. **Phase 3 — the 82% Google Sheet dependency.** 81.9% of the feed is one community
   spreadsheet: simultaneously the ceiling on coverage and a single point of failure.
   Kept separate because it changes the shape of the system rather than filling it.

## Two fixes already landed, for context

Both blocked the same gate — auto-approval's requirement of a positive `jobsFound`.

**The rows carried no `api_url`.** `endpoint_url()` probes the first of `api_url`,
`feed_url`, `board_url`, `listing_url`, so a row with only a `board_url` sent the probe
at the human-facing page, which for a single-page app ships no job links. The board read
as *healthy with zero jobs* at the same time. The evidence was unambiguous: the only
two adapters carrying an `api_url` were the only provider adapters where every board
landed, while Greenhouse, Lever, Ashby, Workday, BambooHR and Breezy landed none —
despite APIs returning 18-122 rows on a plain GET.

**Ashby and Breezy were missing from the probe's `provider_specs`.** With no spec the
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

Delivered openings went 762 → 1,777, 17% → 40%. Verified before wiring: 32 of 33 sampled
endpoints return rows.

## Correction on record

An earlier version of this plan reported 4,589 rows as "registered but not collecting"
and scoped a phase around diagnosing them. That was a **measurement bug**: the audit
matched registry rows by testing whether *any* path segment was a substring of *any*
registry id, which labelled 4,564 of the 4,589 as registered when those boards were
never registered at all. The real figure is **58**. The bug named a collection failure
where the cause was registration, so that phase would have hunted a defect that does
not exist.

Measured coverage is **31.7%**, not the 25.4% first quoted: that came from title
matching alone, whereas posting-URL matching is definitive. Both bases are reported
separately because title matching over-claims — `HR Business Partner - US Operations
(West)` and `(East)` collapse to one title.

## Carried over

- **The 30 extraction-limited boards** (764 openings). `fetchedCount == keptCount` with
  `lowConfidenceDropped == 0`, so nothing is filtered and extraction simply finds fewer
  rows. Given items 1-3 share the JavaScript root cause, re-diagnose *after* those land
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
deliberately untagged. Items 1-3 are worth doing anyway because they change what a
release would deliver: without them it carries 40% of the registration.

## Standing verification for every step

- Adapters exercised against a registered control board with a known expected count,
  never a raw endpoint alone.
- `npm run test:py` and `npm run lint:repo-guardrails` before each commit.
- `git status data/` to confirm no discovery audit artefacts leaked.
- Coverage re-measured against the **live feed**, with URL-matched and title-only
  reported separately. `data/baluffo-runtime.db` is generation 2026-09-17 and produced
  three wrong conclusions during this effort.
- **Delivery re-measured with `tools/coverage_drain.py` after any change to board rows
  or the probe.**
