> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** d5491e13
> - **Status:** v0.3.007 shipped and the live run **collected 0 of the 6,929 registered openings**; the delivery metric that certified them was measuring registry presence, not collection. Five defects found by running it; fix order below

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. See [`INDEX.md`](../INDEX.md).

## Where the remaining openings actually are

Registration and delivery are in [Where this stands](#where-this-stands). These are the
buckets outside them, measured against the live feed and the live fetch report.

| Bucket | Openings | Status |
|---|---:|---|
| B2 — fetched and still returns short | 325 | per-board extraction work |
| C — classifier rejects the role | 988 | 238 recovered; most correctly rejected |
| D — board registered nowhere | 1,510 | 826 Feishu (out of reach) + ~684 unreadable |

Bucket C is not a gap of the size it looks: of the 988 the classifier still rejects,
`tools/coverage_classifier_candidates.py` shows the remainder is dominated by business roles
at studios, which this product should not be collecting.

## Where this stands

**Two populations, and conflating them is what produced every wrong number in this plan.**
They answer different questions and neither subsumes the other.

| Population | What it is | Size |
|---|---|---|
| **Delivery** (operational) | rows the install actually watches | 2,818 active + 871 pending, machine-discovered |
| **Catalogue gap** (audit) | hand-audited target list vs the Games Jobs Index | 695 curated boards / 6,929 openings |

Overlap is high on static and low on provider boards, and neither direction is empty:

| | curated | live | shared | curated-only | live-only |
|---|---:|---:|---:|---:|---:|
| one board per host | 466 | 2,880 | 379 | 87 | 2,501 |
| tenant boards | 229 | 277 | 74 | 155 | 203 |

So the metrics are named rather than merged:

**Delivery — live, `fetch_15458e437b`, appVersion 0.3.008 portable, 49,240 jobs written:**

| | |
|---|---|
| static rows keeping > 0 | **1,019** of 2,460 |
| static fetched, kept 0 | 995 |
| static errored | 711 |
| provider boards whose adapter collected | **325 of 337** |
| distinct hosts in the output | 1,082 |

**Catalogue gap — curated boards absent from the live registry: 242 boards / 1,990 openings**
(87 static / 644; 155 provider / 1,346). These are **real, not stale**: a control-first probe
of the curated-only provider boards returned HTTP 200 with live job counts on 12 of 12 —
2K 125, Applovin 44, Bluehole 18, Crystal Dynamics 1. It is a *discovery* gap.

**Local (`tools/coverage_drain.py --verify-collected`, isolated `--data-dir`, run `v7`):**
322 of 688 registered boards keep a non-zero count, against 688 of 695 registered and 7 / 71
unregistered. The harness reported four wrong numbers before that one, each from applying one
identity rule to rows that do not share a shape; 42 tests now pin the invariants.

The 168 boards that fetched and kept nothing are **not** cache skips and **not**
misattribution. Measured on the source rows: `cacheDecision: run_now`, row-level
`durationMs` 420–5,884 ms, `fetchedCount: 0`, `healthReason: "latest fetch kept no jobs"`.
They asked, spent seconds, and extracted nothing. All but one are `static`, across **194
distinct hosts** — 189 singletons plus five small platforms. 91% are individual boards.

### The per-source time budget was a coverage defect, and it is a knob

`BALUFFO_STATIC_SOURCE_TIME_BUDGET_S` defaults to **25 seconds**. Fourteen boards were
mid-fetch when that clock expired, and they were being reported as failures —
`time_budget_exceeded`, 298 openings. Raising it to 90s, the per-task timeout to 120s, and
changing nothing else, moved **`time_budget` errors from 14 to 1** and converted **11
boards to real collections**; the other 6 stopped timing out and confirmed empty. Total
attribution went 291 → 322 boards and 6,731 → 9,074 openings in one run. This was filed
under "extraction gaps" and would have been diagnosed per-board; it was a configured limit.

### Thirty-four "errors" are duplicate rows over working boards

`adapter_mismatch` — "HTML contains workday signature — consider adapter reclassification" —
was 34 boards / 128 openings and looked like a reclassification job. **All 34 already
collect**: not one has zero attributed openings. A redundant `static` row shadows a
provider path that is serving the studio's postings. So the fix is row cleanup, not adapter
work, and it is the duplicate-row class already listed below (Ubisoft, WBD, EA). Third time
in this plan that "the error is real, the board is broken" turned out to be false.

### The provider-attribution zero, answered

**169 provider boards had zero attributed openings — answered.** The 274 provider rows
collapse to 16 rollup rows by design, and the rollups collected 7,956 openings; but 190
of 267 prefix entries held an **empty** prefix (the 193 rows with no `listing_url`), so
those boards could not be attributed at all. Matching the tenant — the posting URL's
first path segment — fixed it: **all 7,186 rollup jobs now attribute, zero unmatched**,
194 by tenant, collecting boards 322 → 331. The 41 greenhouse boards still at zero are a
real zero, not a measurement artifact.

Delivery is reported in openings, not boards: one board with 176 promised openings and one
with a single opening are not comparable units, and a board-count headline hides the
concentration that matters. Both prior delivery tables are withdrawn — the per-adapter one
reported workday 17/17 when the live run collected none, and the "128 boards / 5,500
openings" one credited 70 "n-ix jobs" to a board that kept zero.

## Live verification, 2026-10-04: the "0" was a population conflation

v0.3.007 shipped and the pipeline completed — `fetch_9c0eb3659c` on `appVersion: 0.3.007`,
2,479 source rows, 519 selected, 1,956 skipped fresh, 442 ok, 77 error, **40,395 jobs
written**. This section recorded "of the 6,929 registered openings, 0 were collected" and
spent a release treating that as the central open finding. It was a measurement error.

The measurement scored **curated** boards against the **live registry**: 277 of 695 were
`active`, and the run's output was attributed to them by host and path prefix. Because the
curated provider slugs are largely disjoint from the registry's (greenhouse shares 8 of 52),
that join found almost nothing to attribute and read as zero collection. The v0.3.008 run
measured on its own registry rows writes 49,240 jobs with 1,019 static boards keeping a
non-zero count — see [Where this stands](#where-this-stands).

Two predictions from that run were still worth making, and were still wrong:

| Prediction | Result |
|---|---|
| `cache_within_freshness_window` falls from 1,893 | **1,956 — it rose** |
| `personio_sources` stops reading kept 0 of 2 | kept 0 → kept 0; fetched **2 → 54** |
| Ubisoft ~46 game jobs, not 12 | **1**, after **2,760 s** in the *static* adapter |

### Five more, found by running the fetch

Same lesson — each a harness assumption recorded as a coverage gap. **A, B, C and E are
done**; the fix-order table carries the outcomes, so only the mechanism is kept here.

**A. Cross-board duplicate identity — 64 boards, 1,257 openings, 0 correct matches.**
`REDUNDANT_STATIC_IF_PROVIDER` declares Workday and BambooHR with
`provider_id_field: adapter` / `provider_id_value: workday` — the *adapter name*, not a
tenant. Every board on either platform hashed to one key, and each matched whichever tenant
registered first: NVIDIA's board was recorded as a duplicate of *Aristocrat Gaming's*. 66
candidates carried such a claim and not one resolved to its own board. **Fixed:** identity
resolves per tenant, keyed most-specific-first, accepted only when `_row_serves_board`
confirms host *and* tenant. Two sub-defects surfaced by running it — the platform must come
from the **host**, not the declared adapter, and a board advertising its ATS in `atsLinks`
resolves from that link. Replayed live: **0 of the 66 claims survive.**

**B. Workday is never dispatched.** The adapter exists and works, and its CXS POST was
verified live against NVIDIA's `total: 2000`. The run showed no `workday` entry in
`adapterTimings` because **A** had suppressed the boards before they could register.

**C. Workday pagination has a hardcoded 100-job ceiling.** `_collect_workday_rows` loops
`range(0, limit * 5, limit)`; called directly it emitted 40 rows against NVIDIA's 2,000.

**D. Silent zero: `ok` + `kept 0` + empty error.** 268 curated static boards reported
`status: ok`, `keptCount: 0`, `error: ''` while discovery counted jobs on the same boards.
An empty error string passes as success.

**E. The delivery metric measured the wrong thing.** It reported workday 17/17 and 1,065
openings delivered; "delivered" meant *a registry row exists*. **Fixed:** three numbers
instead of one, and `--verify-collected` runs a real fetch and **exits 3** when boards
register and nothing collects.

## Fix order

Order set by measured size and by whether an item blocks others. **E** led as a gate:
without it nothing below is verifiable. Workday looked like a missing adapter and would have
led instead; it is not missing, and could not land until **A** was fixed.

| # | Fix | Scope |
|---|---|---|
| **E** | Delivery reports registered / readable / collected, and exits 3 when boards land and nothing collects | metric — **DONE** |
| **A** | Identity on host + tenant; reject cross-tenant duplicate matches | 64 boards / 1,257 openings — **DONE**, 0 of 66 claims survive |
| **B**+**C** | Workday end-to-end; replace `limit * 5` with `total`-driven paging | 17 boards / 1,065 openings — **DONE** |
| — | Curated rows with no `listing_url` resolve by adapter + tenant | 193 rows / 2,206 openings — **DONE**, 688 of 695 register |
| — | **Name the two populations** so delivery and catalogue gap stop sharing a join | metric — **DONE**, see [Correction on record](#correction-on-record) |
| — | Split `failedSources` into its component findings in the drain tool | 86 rows → 6 kinds — **DONE** |
| — | Drain the curated queue behind `domain_cap` 259 / `adapter_cap` 412 | 482 boards / 4,533 openings — **DONE**, `deferredByCap` 662 → 0 |
| **T** | `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S` default 25s → **90s** | 134 live rows time out; locally 11 of 14 boards converted — **agreed, not landed** |
| **BF** | `browserFallbackRecommendedSources` reads the top-level field; the flag is written to `details[0]` | health reports **0** while **199** rows need fallback — **agreed, not landed** |
| **BF2** | Persist `browserFallbackLastError` into report rows | cooldown causes unrecorded; 988 of 1,032 attempts refused — **not started** |
| **DG** | Discovery gap: 242 audited boards / 1,990 openings never reached by the probe, and demonstrably real (12 of 12 sampled return 200 with jobs) | discovery, not measurement — **not started** |
| **P** | `personio` kept 0 while parsing 54 — every row dropped `missing_job_link` | 12 boards / 54 openings — **DONE**, the feed carries no URL element |
| **R** | Cross-site static redirects classified instead of refused anonymously | 135 live rows — **DONE**, see below |
| **D** | `ok` + `kept 0` + no error → `unknown` | 168 boards / 1,081 openings — see below |
| — | Retire redundant `static` rows shadowing working provider paths | 34 boards / 128 openings, **already collecting** — cleanup |
| — | Five small platform hosts in the zero set | 18 boards / 256 openings — needs embedded-JSON extraction, no host rule |
| — | Classify the remaining fetched-and-empty static boards by extraction shape | ~825 openings — per-board |
| — | Duplicate rows: WBD 5 rows for one 80-job board, EA 4 overlapping rows, 72 `site_changed` | cleanup |

**On D.** It was written as "`ok` + `kept 0` + no error → `unknown`". The premise is now
verified against a real population: the 168 fetched-and-empty boards do carry `status: ok`,
`fetchedCount: 0` and `healthReason: "latest fetch kept no jobs"`, and all are already
`needs_review` with **0** `legit_empty` and **0** `health: healthy`. A further 23 reach the
same state through the `error` channel — `no jobs extracted from source pages` — which
`reporting_breakdowns` already buckets as `needs_review`. The reporting layer handles the
shape; what remains is that `failedSources` counts those 23 as failures, and that feeds
`failedSourceRatioLatest`. Narrowing a persisted contract is a compatibility change, so the
split lives in the drain tool. Write D against the 168 and the 23 together.

**On T, and why the cap is not the lever.** Browser fallback demand was 1,032 attempts,
988 refused, 40 served. Refusal comes from a *per-source 30-minute cooldown* set after a
fallback attempt errors — `browserFallbackCap` only limits concurrency, so raising it changes
nothing. Until `BF2` records why sources enter cooldown, the 78 HTTP-403 rows are not
decidable. `transport` (83 rows, median 15s, max 298s) is a different class: hangs and
timeouts, not blocks.



**On D.** It was written as "`ok` + `kept 0` + no error → `unknown`". That premise is now
verified against a real population rather than an assumed one: the 168 fetched-and-empty
boards do carry `status: ok`, `fetchedCount: 0`, and
`healthReason: "latest fetch kept no jobs"`, and all are already classified `needs_review`
with **0** `legit_empty` and **0** `health: healthy`. A further 23 boards reach the same
state through the `error` channel instead — `no jobs extracted from source pages` — which
`reporting_breakdowns._classify_unknown_static_shape` already buckets as `needs_review`. So
the reporting layer handles the shape; what remains is that `sourceHealth.failedSources`
counts those 23 as failures, and that number feeds `failedSourceRatioLatest`. Narrowing a
persisted report contract is a compatibility change, so the split lives in the drain tool
for now. Write D against the 168, and treat the 23 as the second half of the same finding.

### R: a refused redirect now says what it refused

The 135 rows were never one class, and the shape of the class is why no token would have
worked. Replayed through the new classifier:

| tag | rows | what it is |
|---|---:|---|
| `site_gone_or_moved` | 117 | the studio closed, rebranded, or was acquired |
| `insecure_downgrade` | 13 | same site, scheme dropped — a server misconfiguration |
| `platform_migration` | **5** | the careers page moved to an applicant-tracking host |

Only the last is the Ubisoft shape. The 117 are the dominant case and **none** of it is a
platform move: `foxandsheep.com → linkedin.com` (closed), `exozet.com → endava.com` and
`game-labs.net → stillfront.com` (acquired), `roblox.com → corp.roblox.com` (rebrand). Calling
those migrations would point discovery at `linkedin.com`.

The classification is attached to the **existing refusal**; the guard still declines to follow
every one of these redirects, and that is pinned in
`tests/jobs_static/test_static_redirect_guard.py`. The classifier reads a target's **host and
nothing else** — no fetch, no socket — because feeding a rejected target back into a request
is the thing the guard exists to prevent. The old message is a substring of the new one, so
existing log searches still match.

**The `;` fix is narrower than I first described it.** Servers do emit a trailing semicolon
(`https://corp.roblox.com/careers;`), and I called the target malformed. It is not: `;` is a
legal path character under RFC 3986, and the URL parses with a clean host. The real harm is
downstream — copied verbatim into a registration URL it leaves a stray character on the end.
Worth noting that `urlparse` puts the text before `;` in `path` and the rest in `params`, so
stripping `path` is a no-op and the character has to come off the raw string. My first
implementation did exactly the no-op version and the test caught it.

The 5 migrations are registered by discovery from the classification. The 117 are classified
but **not retired** — retiring a row is a visibility change with real consequences, and this
plan's standing rule is not to retire a board for one without saying so.

### Verification gate for each step

A step is done when `coverage_drain.py --verify-collected` reports the board's own openings
kept by a fetch run — not when it appears in the registry. Two runs before a release, one on
the branch and one live. Any step reporting "landed" without a kept count has reproduced the
defect it was meant to fix.

## Nine defects found by running discovery, not reading it

All nine have one shape: **the harness was looking somewhere the openings were not, and the
zero was recorded as an answer.** Condensed; the mechanisms are in the commits and the tests.

- **A board's JSON API lives on a different host from its career page.** All 39 Ashby boards
  register as `ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/<slug>`, which
  resolved to host *and* tenant both empty. Greenhouse, Lever and SmartRecruiters share it.
- **Greenhouse's EU hosts matched no host rule.** `job-boards.eu.greenhouse.io` does not match
  the `job-boards.greenhouse.io` rule — it ends `eu.greenhouse.io` — so 158 openings on 8
  tenants collapsed onto one host-root row.
- **Workable was lost twice to the same wrong URL.** The probe read
  `/api/v1/accounts/<a>/jobs` (HTTP 400 for every account) and the row it built carried only
  `account`, which `endpoint_url` cannot resolve. Fixing one still registered 0 of 29.
- **A static candidate's listing URL is its host root, which is often not the careers page.**
  All 109 unreadable static boards return HTTP 200 with zero anchors at the root.
- **Two curated tables are concatenated, not merged**, so a board in both is fetched twice.
- **Personio operates two domains and the probe accepted one**, refusing every
  `.jobs.personio.com` board as an invalid host before it was fetched — nine boards, all
  serving a real feed, all registering zero.

The lesson: **every one was a harness assumption, not a board failure, and none was visible
without running discovery.** A recorded zero is the most expensive kind of finding, because it
is indistinguishable from a board that genuinely has nothing.

## What is left, in priority order

The actionable list, with current numbers, is [Fix order](#fix-order). This section keeps only
what that table cannot carry: the items deferred by agreement, and the two findings whose size
is the argument for leaving them.

Items 1–3 and 6–8 are **done** — the freshness window, the second registration wave, the review
boards, the 404-while-active registry rows, the per-board extraction gaps and the carried-over
defects. Their mechanisms are in the commits and the tests.

### Feishu — 826 openings, not reachable by anything the runtime has

Investigated and found genuinely out of reach rather than merely unlabelled. The hrmos
reasoning would say these eight tenant boards are just JS pages, registerable as static rows
on recorded openings — 105 boards of that shape already are. It would be false.
`tools/coverage_render_probe.py` measured four things:

| Question | Answer |
|---|---|
| Does a GET see job rows? | HTTP 200, 141 KB, **zero anchors** |
| Does rendering see them? | Chromium produces 5.7 MB, still **5 anchors, no job rows** |
| Where do the rows come from? | `POST /api/v1/search/job_post/count`, from the JS bundle |
| Can that be called directly? | **405** unauthenticated, then **429**; no cookies, `/api/v1/csrf/token` is an SPA catch-all |

**And the delivery metric would have called it landed** — a static row reaches the registry
and counts as delivered, exactly as the 105 JS boards do, while producing zero jobs. Hence
Feishu stays unregistered. Recovering it is a project, not a registration task: a CSRF-aware,
rate-limit-respecting client, a payload parser, and a fixture. Kurogame alone is 411 openings.

### The 1,468 extraction gaps are mostly not coverage work

`tools/coverage_classifier_candidates.py` shows the remainder after the 238 recovered roles is
dominated by business roles at studios — Logistics Director, Marketing Manager — which this
product should not be collecting. Chasing them would be volume for its own sake.

### The 82% Google Sheet dependency is an asset, not a liability

The Sheet is the best base source and stays. It was never the ceiling — it was being read as
one, because the delivery metric credited sheet rows to whichever board's host the posting
link happened to point at.

### 5. Data quality, deferred by agreement

Blank country (4,493 rows) and GB/UK (~365) remain deferred.

## Sequencing note

0.3.007 shipped and carried all six fixes below. The live run showed them necessary and
not sufficient: it collected none of the registration, because the question was never
"is the board registered" but "can a fetch read it".

| Fix | A release without it |
|---|---|
| Six probe fixes | 17% of the registration instead of 98% |
| Freshness window | a registry that is 94% unrefreshed, capping everything above |
| Personio domains | `personio_sources` kept 0 of 2 fetched, as the live report shows |
| SmartRecruiters pagination | a third of Ubisoft's board never requested |
| Board-identity fixes | the audit undercounts registered coverage by ~1,000 openings |

The last row is the lesson: a board-identity fix that made the *audit* more accurate made
*registration* less complete, because identity was compared across tenants.

## Correction on record

**Fourth correction. "The live run collected 0 of 6,929" was false, and it was the
load-bearing claim of this plan.**

It came from scoring **curated** boards against the **live registry** and calling
non-membership a collection failure. 277 of 695 curated boards were `active` live, and the
run's output was attributed to them by host and path prefix; because the curated provider
slugs are largely disjoint from the registry's, the join found nothing and read as zero.

Measured on the v0.3.008 run's own registry rows (`fetch_15458e437b`, portable 0.3.008):
**49,240 jobs written, 1,019 of 2,460 static rows keeping > 0, and 325 of 337 provider boards
on a collecting adapter.** Every provider except `personio` collects.

Two supporting claims were also false, and both were load-bearing:

- **"The live registry carries `board_url`."** It carries none. A live row is
  `{id, adapter, studio, registryState, pendingReason, stateChangedAt, stateChangedBy,
  lastPromotedAt, lastDemotedAt, name}` — ten keys, the URL inside `id` only. `board_url`: 0
  rows. `listing_url`: 0 rows. `pages`: 0 rows. This was the stated reason local runs were
  treated as untrustworthy and the live run as authoritative, and it was false in the
  direction that made the live number look like ground truth.
- **"242 curated boards / 1,990 openings are unregistered, so that is the gap."** They are not
  in the live registry because discovery never found them, not because the table is stale: a
  control-first probe returned HTTP 200 with live job counts on **12 of 12** curated-only
  provider boards. The gap is real; the population it was measured against was not the one
  the run fetches.

The generalisable form, and the reason this section exists four times over: **a coverage claim
is meaningless without naming the population it was measured over.** The same run supports
"1,019 boards collecting" and "242 audited boards unregistered" simultaneously, and this plan
spent a release unable to hold both.

**Third correction. Registration was reported as 6,929 openings; it is 6,858.**

`registration_report` resolved every curated board by `listing_url`. **193 of the 695
curated rows carry no `listing_url` at all** — they identify the way the registry does, by
adapter plus tenant (`{"adapter": "smartrecruiters", "company_id": "Bet3651"}`). Those 193
resolved to `("", "")`, matched no registry row, and were reported unregistered. The
intermediate claims built on that were **197 boards / 2,223 openings**, then **158 boards /
2,042 openings** once the matcher was fixed but the *producer* of the identity was not.

Resolved from the tenant field, against the drained registry, they match at 190 of 193.
Every provider adapter is essentially complete — greenhouse 51/52, ashby 39/39,
workable 29/29, lever 20/22, smartrecruiters 16/16, and breezy/jazzhr/personio/recruitee/
teamtailor 48/48. The true residue is **7 boards / 71 openings**: greenhouse `2k`,
lever `fliff`, lever `branch.gg`, and four static hosts (`r-force.co.jp`,
`bexide.co.jp`, `playedwithfire.com`, `vivastudios.com`).

That is four wrong numbers from one mistake. The other three, all in the same harness:

- host-matching credited Google-Sheet rows to boards — 70 "n-ix jobs" for a board that
  kept zero;
- `tenant == host` on one side where `registry_identity` yields `tenant == ""` — 57 of 695
  boards read as registered when 498 were;
- reading `jobs-fetch-report.json` beside `jobs-fetch-report.json.gz`, so a run reported 0
  collected while 41,277 rows sat in the file it never opened.

The generalisable lesson, and the reason `tests/tools/test_coverage_drain_identity.py`
exists: **assert identity as an invariant, never a count.** A count is what moved; all four
wrong numbers were reported with the same confidence as a correct one, and each was
discovered only by checking the underlying rows after the summary had already been written
down.

**Second correction.** This plan reported "6,844 of 6,929 openings proven delivered
(98.8%)" and a per-adapter table showing workday 17/17. The live run collected **0**.
"Delivered" meant *a registry row exists*; delivery needs that row to carry the declared
adapter, to be selected, and to keep a non-zero count. A metric that cannot tell a collected
board from a registered one keeps reporting 98% while delivering nothing.

**First correction.** An earlier version reported 4,589 rows as "registered but not
collecting" and scoped a phase around diagnosing them. That was a **measurement bug**: the
audit matched rows by testing whether *any* path segment was a substring of *any* registry
id, labelling 4,564 as registered when those boards were never registered. The real figure
is **58**.

Measured coverage is **31.7%**, not the 25.4% first quoted: that came from title matching
alone, whereas posting-URL matching is definitive. Both bases are reported separately because
title matching over-claims — `HR Business Partner - US Operations (West)` and `(East)`
collapse to one title.

## Standing verification for every step

- **A board is delivered when a fetch run keeps its openings.** Registry presence, adapter
  presence, and a probe's job count are not delivery.
- **Identity is asserted as an invariant, never as a count.** A curated board resolves to
  the same `(host, tenant)` as its registry row whether or not it carries a `listing_url`;
  an unidentifiable board resolves to `("", "")` and must register `false`; two tenants on
  one platform host stay distinct. All four wrong numbers in this plan were counts asserted
  before the rule was verified.
- **A zero is never read as "empty" without its fetch evidence.** Check `cacheDecision`,
  row-level `durationMs` and `fetchedCount` on the source row. `run_now` + seconds spent +
  `fetchedCount: 0` means fetched and found nothing; `skip_fresh` + `durationMs: 0` means
  never asked. Note that `details[0].durationMs` is not the row-level field — quoting the
  wrong one produced a false "these never fetched" here.
- Adapters exercised against a registered control board with a known expected count,
  never a raw endpoint alone.
- `npm run test:py` and `npm run lint:repo-guardrails` before each commit; `git status
  data/` to confirm no discovery audit artefacts leaked.
- Coverage re-measured against the **live feed**, URL-matched and title-only reported
  separately. `data/baluffo-runtime.db` is generation 2026-09-17 and produced three wrong
  conclusions during this effort.
- **Delivery re-measured with `tools/coverage_drain.py --verify-collected` after any change
  to board rows or the probe**, compared case-insensitively — the registry lowercases path
  segments, and an exact comparison reported Workday as 3/17 when all 17 had landed.
- Pagination ceilings asserted against the API's own reported `total`, never a literal.
  The Workday `limit * 5` ceiling hid 1,900 of NVIDIA's 2,000 openings.
- A duplicate match that crosses host or tenant is a defect, not a dedupe — 64 curated
  boards were suppressed that way.
- **The platform is not the tenant.** `*.myworkdayjobs.com` and `*.bamboohr.com` host
  hundreds of unrelated studios; derive tenancy from the host, never from the row's declared
  adapter, because a provider board is routinely registered as `static`.
- A provider's URL is not always the board's URL. A board advertising its ATS in
  `atsLinks` resolves its provider from that link.
- Default coverage workflow: the `baluffo-coverage-delivery` skill.
