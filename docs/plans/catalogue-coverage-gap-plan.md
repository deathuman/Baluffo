> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 79ef50b9
> - **Status:** phase 0 landed; phase 1 extraction and verification landed, registration pending; phase 2 rescoped after its premise was disproved

# Closing the catalogue coverage gap

## Why this plan exists

The catalogue sweep says Baluffo holds 4,867 of the index's 15,332 openings
(**31.7%**). The premise for the plan is that no opening should be left behind,
because a job board's whole purpose is the openings it carries. Three earlier
estimates in this effort were wrong before measurement, so every number below is
quoted with how it was obtained.

## Honest baseline

Matched on **posting URL** first, which is the only definitive signal: both boards
publish the same URL for the same requisition.

| | Count |
|---|---|
| GJI openings considered | 15,332 |
| Matched | 4,867 (31.7%) |
| — by URL, definitive | 3,368 |
| — by title only (over-claims) | 1,499 |
| **Missing** | **10,465** |

Missing, decomposed against the live registry:

| Cause | Count |
|---|---|
| Board not registered | 5,279 |
| Studio is in the feed, this role is not | 5,128 |
| Board registered, opening not collected | 58 |

**5,279 of 10,465 (50%) are unreachable only because the board was never
registered.** That is mechanical work, not a research problem.

### Correction: the "registered but not collecting" bucket was a measurement bug

An earlier version of this table reported 4,589 rows as "board registered, opening
not collected", and phase 2 was planned around diagnosing them. That number was
wrong. The audit matched a registry row by testing whether *any* path segment of a
job URL was a substring of *any* registry id, which is loose enough to call most
things registered: 4,564 of the 4,589 matched only that way, and **0** matched by
board identity.

The real figure is **58**, and they are genuine per-role gaps. Matched and missing
totals did not move, so the fix redistributed rows between causes rather than
inventing or hiding coverage.

This matters beyond the bookkeeping. The wrong bucket named a *collection* failure
— "Baluffo reads this board but does not collect the role" — where the cause was
registration. Phase 2 would have hunted for a collection bug that does not exist
while phase 1 quietly fixed it.

### Board identity is host + tenant

Both sides of the registry comparison now resolve identity through one shared leaf,
`tools/coverage_board_identity.py`, because the audit's join and the board tool's
candidate builder must agree exactly or the same opening lands in one bucket when
measured and another when registered.

| Rule | Why the obvious alternative is wrong |
|---|---|
| Host + tenant, never the id string | The registry is internally inconsistent about trailing slashes — of 2,030 active static rows, 778 end in `/`, 1,252 do not. String comparison calls one real board two boards. |
| Both sides use one tenant resolver | Reading a registry id's tenant from the last path segment while reading a candidate's from the job URL's host made every single-site static board look unregistered. |
| Never the studio label | One board is registered both as "Lost Boys Interactive" and "Lost Boys Interactive (Embracer Group)". |

Correcting the join dropped proposed boards from 966 to **712** and recoverable
openings from 8,374 to **6,706**; the earlier figure double-counted 319 boards the
registry already serves.

### Tenant position is per platform

Getting this wrong is the worst available outcome: a multi-tenant platform collapsed
onto one host-root row leaves the registry looking served while every tenant's
openings stay missing.

| Shape | Examples |
|---|---|
| Subdomain | `kurogame.jobs.feishu.cn`, `nintendoeurope.csod.com` |
| First segment | `job-boards.greenhouse.io/2kczech`, `jobs.jobvite.com/asus` |
| After chrome | `hrmos.co/pages/capcom`, `herp.careers/v1/charabank` |
| Host | `blooberteam.recruitee.com`, `jobs.ea.com` |

`hrmos.co` is the case that proved it: 31 tenants and 845 openings on the apex
host — Capcom, Square Enix, Cygames, Nexon, Game Freak, Spike Chunsoft — which a
host-root rule reports as a single served board.

### Provenance of what Baluffo *does* carry

Recomputed on the live feed, not the stale local snapshot:

| Source | Share |
|---|---|
| community Google Sheet | 81.9% |
| static career page (scraped) | 9.4% |
| direct ATS API | 5.7% |
| other aggregators | 2.9% |

An earlier figure of "0.3% direct ATS" was wrong twice over: it came from a
2026-09-17 local generation *and* miscounted. The structural point stands and is
stronger than first stated — the feed leans on one community spreadsheet.

## Phase 0 — make the measurement trustworthy (done, `79ef50b9`)

- URL-first matching, keeping the query string. Stripping it collapses up to 218
  distinct openings onto one base URL, because Ashby's job id is `?ashby_jid=`.
- Match basis reported separately so proven coverage is never confused with
  similar-looking coverage.
- Two looser matchers were tried and **rejected on evidence**: token-set equality
  recovered 139 rows and erased the qualifier that distinguishes two openings
  (`(West)` vs `(East)`); fuzzy ratio wanted to merge `Staff Platform Engineer`
  with `Data Platform Engineer`. Both are pinned by tests so they do not return.
- **Game-relevance scoping was measured and deliberately not used.** 62% of misses
  lack game evidence on title+company alone, but that detector is vocabulary-bound:
  `Senior Scene Editor (NARAKA: BLADEPOINT)` is plainly a game role and scores
  false. Pre-filtering boards by it would drop real openings, so the pipeline's own
  per-role gate decides.

Still open in phase 0: `registered_no_role` is inflated because a registered but
dead row masks a live unregistered board (2K Czech), and the registry persists no
URL fields at all, so board identity has to be recovered by parsing `id`.

## Phase 1 — register the missing boards

Target: 712 boards behind 6,706 openings, plus a vendor backlog that needs adapters
rather than registrations.

1. **Board candidate extraction** — `tools/coverage_boards.py`, done. Resolves
   host + tenant per missed opening and dedupes on board identity.
2. **Verify before proposing** — `tools/coverage_verify.py`, done. Resolving a
   tenant does not mean the board would collect anything, so each candidate is
   fetched and given a three-way verdict.
3. **Register the `collects` boards.** Not started.
4. **Apply under the repo's mutation guardrails** — match rows by host rather than
   studio label, print the plan, assert the size, require an explicit apply flag,
   back up to `_out/`, read back after writing.

### Verification is three-way on purpose

`collects` and `empty` are answers; `unknown` is the absence of one and is never
folded into either. Writing a failed fetch as `empty` asserts a board has no
openings when in fact a timeout proved nothing — and that quietly re-creates the
gap this effort exists to close. A broken adapter control downgrades all of that
adapter's boards to `unknown`, because a dead fetcher is otherwise indistinguishable
from a vendor of dead boards and the fix never gets made.

Building the verifier found four defects, each of which would have made registration
look better than reality:

- **Workday reported `collects, 1 row` for every board.** Its CXS endpoint is a POST
  behind a certifi-anchored TLS context, because the OS cert store poisons chain
  building for `*.myworkdayjobs.com`. A GET hits the SPA redirect stub
  `{"widget":"redirect","externalSpa":true}`, and the row counter read that single
  object as one row. Now verified through the production runner's own CXS helper.
- **Three of four adapter controls were dead URLs.** A dead control silently marks
  every board on that adapter `unknown`, so all four were re-picked by fetching them
  and confirming the count: greenhouse 3, lever 12, ashby 157, smartrecruiters 200.
- **The row counter fell back to "any non-empty dict is one row"** — the Workday bug.
- **An unrecognised shape was reported as `empty`** rather than `unknown`.

### Adapter backlog

Vendors with no adapter are a counted backlog, not a silent gap. Grouped by tenant
because board count is meaningless without an adapter:

| Vendor | Openings | Boards |
|---|---|---|
| hrmos | 845 | 31 |
| feishu | 826 | 8 |
| csod | 18 | 1 |

1,689 openings — 16% of the gap — are unreachable until an adapter exists, not
until a row is registered. That is a different piece of work from phase 1.

## Phase 2 — the remaining 5,186 rows

Rescoped. The original phase 2 was 4,589 rows diagnosed as "registered but not
collecting", and that bucket turned out to be 98% measurement artifact. What remains
is 5,128 studio-covered-but-role-absent plus 58 registered-no-role.

### Diagnosis: 30 boards, and it is extraction, not filtering

Joined the 5,128 against the live fetch report by parsing the board URL out of each
source name (`static_source::<registry id>` — the `name` field is an identifier, not
a URL, and joining on it directly yields 32 nonsense "hosts"):

| | Rows | Meaning |
|---|---|---|
| No source on the board host | 3,689 | Not registered. Belongs to phase 1. |
| `excluded` | 675 | Not fetched this run. Not evidence of anything. |
| Fetched `ok`, role absent | **764** | Genuine gap. |

**All 764 come from just 30 sources**, and the same signature appears on every one:
`fetchedCount == keptCount` with `lowConfidenceDropped == 0`. Nothing is being
filtered out — the quality gate is not dropping these roles. The extraction simply
finds fewer rows than the board serves.

| Misses | Kept | Source |
|---|---|---|
| 283 | 340 | `hrmos.co/pages/cygames` |
| 134 | 145 | `jobs.ea.com` |
| 121 | **11** | `jobs.jobvite.com/amberstudiocareers` |
| 74 | **7** | `careers.garena.com` |
| 43 | 80 | `careers.activision.com/careers` |
| 16 | 166 | `riotgames.com` |
| 13 | **1** | `outfit7.com` |
| 12 | **6** | `flixinteractive.com` |
| 9 | **0** | `nordeus.com`, `interactive.innovina.it` |

A board keeping 11 rows while the index lists 121 is an extraction failure, and no
quality gate is involved. The likely causes are JS-rendered listings, unpaged
endpoints, and detail links that never resolve to a job page — the same shape as the
Gamucatex case, where extraction falls through to `static_detail_link_rows` and
returns nav text.

One concrete defect found while checking: the registered URL
`https://careers.activision.com/careers` now returns **HTTP 404**, while the source
still reports `ok` with 80 kept. That is a stale registration that the fetch report
does not surface as an error.

Candidate causes still to confirm per source:

- pagination not followed (SmartRecruiters `limit=100`, Lever, Greenhouse)
- per-source detail-fetch budgets — a Bandai note records "38 sibling detail
  verifications burning the source budget"
- boards registered against a stale path after a site redesign
- `excluded` rows: cadence and cooldown, which must not be read as "no openings"

Note the distinct question this asks. Phase 1 is "is the board registered?". Phase 2
is "the board is registered and the role is live — why did the pipeline not see it?",
which is an extraction problem, not a registration one.

## Phase 3 — the spreadsheet dependency

81.9% of the feed is one community Google Sheet. That is simultaneously the ceiling
on coverage and a single point of failure. Separate plan; it should not be mixed
into phases 1-2, because it changes the shape of the system rather than filling it.

## Carried over from the Voodoo/Ashby plan

Four boards were queued for registration from that audit. Three are now registered
(`5b7ccc45`): `2kczech`, `hangar13`, `YggdrasilSandbox`. **Companion Group turned
out to be already registered and active** — it was missing from the plan only
because that plan was built from the stale 2026-09-17 snapshot. Its remaining gap
is collection, not registration, so it belongs to phase 2.

## Verification for every phase

- Provider adapters exercised against a registered control board whose expected
  count is known, never a raw endpoint alone.
- `npm run test:py` plus `npm run lint:repo-guardrails` before each commit.
- `git status` on `data/` to confirm no discovery-audit artefacts leaked.
- Coverage re-measured after each phase, with URL-matched and title-only counts
  reported separately.
