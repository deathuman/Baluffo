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

Two numbers are sound, from two different environments, and they do not agree. Both are
reported because neither supersedes the other.

**Live (v0.3.007, Umbrel, 2026-10-04):** of the 6,929 registered openings, **0 collected.**
That is the only measurement taken where the registry is correct by construction, and it has
never been explained. See [Live verification](#live-verification-2026-10-04-the-release-delivered-0).

**Local (`tools/coverage_drain.py --verify-collected`, isolated `--data-dir`, run `v7`):**
322 of 688 registered boards keep a non-zero count. The harness reported four wrong numbers
before that one, each from the same mistake — applying one identity rule to rows that do not
share a shape — and is now covered by 42 tests pinning the invariants. Until 0.3.008 ships
and is read live, treat the local figure as **provisional and the live 0 as the finding**.

| | Boards | Openings |
|---|---|---|
| Curated | 695 | 6,929 |
| Registered | 688 | 6,858 |
| Registered and keeping > 0 (local, provisional) | 322 | 9,074 |
| Fetched, kept 0 | 168 | 1,081 |
| Registered, fetch errored | 69 | 461 |
| — of which already collecting via a provider path | 34 | 128 |
| — of which genuinely nothing found | 23 | 226 |
| — of which real transport/blocked failures | 12 | 107 |
| Never selected by the fetch | 1 | 12 |
| Unregistered | **7** | **71** |

The 168 boards that fetched and kept nothing are **not** cache skips and **not**
misattribution — the assumption every earlier version of this table rested on. Measured on
the source rows: `cacheDecision: run_now`, row-level `durationMs` 420–5,884 ms,
`fetchedCount: 0`, `healthReason: "latest fetch kept no jobs"`. They asked, spent seconds,
and extracted nothing. All but one are `static`, spanning **194 distinct hosts**: 189
singletons plus five small platforms carrying 256 openings. There is no single
vendor-shaped fix behind them — 91% are individual boards.

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

## Live verification, 2026-10-04: the release delivered 0

v0.3.007 shipped, Umbrel updated, the pipeline completed — `fetch_9c0eb3659c`, 21:39:19Z to
22:43:37Z, closed 22:55Z, on `appVersion: 0.3.007`. 2,479 source rows, **519 selected**, 1,956
skipped fresh, 442 ok, 77 error, 40,395 jobs written.

**Of the 6,929 registered openings, 0 were collected.**

| Curated board | Count |
|---|---|
| Landed `active` in the live registry | 277 |
| Of those, probed `ok` by discovery, counting jobs | 277 (3,598 jobs) |
| Of those, kept by the fetch run | **0** |
| Still deferred by discovery queue caps | 482 boards / 4,533 openings |
| Produced no candidate at all | 6 boards / 24 openings |

The three numbers this plan predicted, read honestly:

| Prediction | Result |
|---|---|
| `cache_within_freshness_window` falls from 1,893 | **1,956 — it rose** |
| `personio_sources` stops reading kept 0 of 2 | kept 0 → kept 0; fetched **2 → 54** |
| Ubisoft ~46 game jobs, not 12 | **1**, after **2,760 s** in the *static* adapter |

### Five more, found by running the fetch

Same lesson as the nine above — each a harness assumption recorded as a coverage gap.

**A. Cross-board duplicate identity — 64 boards, 1,257 openings, 0 correct matches.**
`REDUNDANT_STATIC_IF_PROVIDER` declares Workday and BambooHR with
`provider_id_field: adapter` / `provider_id_value: workday` — the *adapter name*, not a
tenant. Every board on either platform hashed to one key, the duplicate index held a single
arbitrary entry, and each board matched whichever tenant registered first: NVIDIA's board
was recorded as a duplicate of *Aristocrat Gaming's*. 66 candidates carried such a claim —
16 workday, 47 bamboohr, **5,289 probe-counted jobs** — and not one resolved to its own
board. This suppresses registration across both platforms, not just Workday.

**A — fixed.** Identity resolves per tenant (`multi_tenant_provider`, `tenant_host_label`,
`tenant_path`, `board_identity_key`), keyed most-specific-first, accepted only when
`_row_serves_board` confirms host *and* tenant. Two sub-defects surfaced by running it: the
platform must come from the **host**, not the declared adapter (a Workday board is routinely
registered as `static`); and the provider URL is not always the board URL — a board
advertising its ATS in `atsLinks` resolves from that link. Both pinned in
`tests/source_discovery/test_board_identity_tenancy.py`. Replayed against the live report:
**0 of the 66 cross-tenant claims survive.**

**B. Workday is never dispatched.** The adapter exists and works — `run_workday_sources_source`
iterates `registry_entries("workday")`, it is wired into `DEFAULT_SOURCE_LOADER_NAMES`, and
its CXS POST was verified live against NVIDIA's `total: 2000`. The run showed **no `workday`
entry in `adapterTimings`** because **A** had suppressed the boards before they could
register, so the loader had nothing to read.

**C. Workday pagination has a hardcoded 100-job ceiling.** `_collect_workday_rows` loops
`range(0, limit * 5, limit)`; called directly it emitted 40 rows against NVIDIA's 2,000.

**D. Silent zero: `ok` + `kept 0` + empty error.** 268 curated static boards reported
`status: ok`, `keptCount: 0`, `error: ''` while discovery counted jobs on the same boards
— jobvite 87, hrmos 100, gamesjobsindex 97. An empty error string passes as success.

**E. The delivery metric measured the wrong thing.** `tools/coverage_drain.py` reported
workday 17/17 and 1,065 openings delivered; the live run collected zero. "Delivered" meant
*a registry row exists*; delivery needs that row to carry its declared adapter, to be
selected, and to keep a non-zero count.

**E — fixed.** Three numbers instead of one: `registered`, `readable by some loader`,
`collected`; `--verify-collected` runs a real fetch, attributes kept jobs by host + tenant,
returns unmatched jobs rather than dropping them, and **exits 3** when boards register and
nothing collects. The `delivered` column is gone from the landing test rather than
corrected. `collectable_adapters` mirrors `registry_entries`' static→Workday/BambooHR
migration, because a strict adapter check false-alarms on a board that does collect.

## Fix order after the live run

The order proposed before triage is wrong, and running it is why. Workday looked like a
missing adapter and would have led; it is not missing, and cannot land until **A** is fixed.
**E** leads as a gate: without it nothing below is verifiable.

| # | Fix | Scope |
|---|---|---|
| **E** | Redefine delivered: registered / readable / collected, and exit 3 when boards land and nothing collects | metric — **DONE** |
| **A** | Identity on host + tenant; reject cross-tenant duplicate matches | 64 boards / 1,257 openings — **DONE**, 0 of 66 cross-tenant claims survive |
| **B**+**C** | Workday end-to-end: confirm dispatch, replace `limit * 5` with `total`-driven paging | 17 boards / 1,065 openings — **DONE** |
| — | Curated rows with no `listing_url` resolve by adapter + tenant | 193 rows / 2,206 openings — **DONE**, 688 of 695 boards now register |
| **D** | `ok` + `kept 0` + no error → `unknown` | 168 boards / 1,081 openings — **NOT STARTED**, see below |
| — | Per-source time budget: `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S` 25s → 90s | 14 boards / 298 openings — **DONE**, `time_budget` errors 14 → 1 |
| — | Split `failedSources` into its component findings in the drain tool | 86 error rows → 6 kinds — **DONE** |
| — | Retire the 34 redundant `static` rows shadowing working Workday paths | 34 boards / 128 openings, **already collecting** — cleanup, not coverage |
| — | Drain the curated queue behind `domain_cap` 259 / `adapter_cap` 412 | 482 boards / 4,533 openings — **DONE**, `deferredByCap` 662 → 0 by round 2 |
| — | Why 41 greenhouse boards collect nothing while the rollup fetched 927 | real zero — not started |
| — | Five small platform hosts in the zero set (`recruiter.co.kr`, `career.greetinghr.com`, `careers.hibob.com`, `herp.careers`, `recruit.charliehr.com`) | 18 boards / 256 openings — not started |
| — | Classify the remaining 150 fetched-and-empty static boards by extraction shape | ~825 openings — not started |
| — | The 7 unregistered boards: greenhouse `2k`, lever `fliff`, lever `branch.gg`, `r-force.co.jp`, `bexide.co.jp`, `playedwithfire.com`, `vivastudios.com` | 71 openings — not started |
| — | Duplicate rows and the redirect class: Ubisoft 46 min, WBD 5 rows for one 80-job board, EA 4 overlapping rows, 72 `site_changed` | cleanup |

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

### Verification gate for each step

A step is done when `coverage_drain.py --verify-collected` reports the board's own openings
kept by a fetch run — not when it appears in the registry. Two runs before a release, one on
the branch and one live. Any step reporting "landed" without a kept count has reproduced the
defect it was meant to fix.

## Nine defects found by running discovery, not reading it

The first six are recorded below. Three more arrived with the second and third waves, and
every one has the same shape: **the harness was looking somewhere the openings were not, and
the zero was recorded as an answer.**

**A board's JSON API lives on a different host from its career page.** All 39 Ashby boards
register as `ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/<slug>`, which
resolved to `('ashby', 'api.ashbyhq.com', '')` — host and tenant both lost. Greenhouse, Lever
and SmartRecruiters share the shape. `registry_identity` now folds a known API host onto its
canonical career-page host and reads the tenant out of the API path.

**Greenhouse's EU hosts matched no host rule at all.** `job-boards.eu.greenhouse.io` does not
match the `job-boards.greenhouse.io` rule — it ends `eu.greenhouse.io` — so it fell through
to `static` and 158 openings on 8 tenants collapsed onto one host-root row, the multi-tenant
failure this repo's own guardrail warns about. Eight per-tenant rows now.

**Workable was lost twice to the same wrong URL.** The probe read
`/api/v1/accounts/<a>/jobs` (HTTP 400 for every account) and the row it built carried only
`account`, which `endpoint_url` cannot resolve. Fixing only the first still registered 0 of 29
— both were needed, and both now use the runtime's own `JsonFeedSpec` template.

**A static candidate's listing URL is its host root, which is often not the careers page.**
The BambooHR defect again, except the tool held only a host, never a path to be wrong about.
All 109 unreadable static boards return HTTP 200 with zero anchors at the root.
`tools/coverage_listing_discovery.py` derives the listing from the URLs of the missed
openings and measures each ancestor path with the runtime's own detector.

**Two curated tables are concatenated, not merged.** A board present in both is fetched twice
and double-counted. Four were known; AppLovin's Greenhouse board arrived with the second wave
and `_drop_already_curated` now removes the class by board identity rather than by studio
label, since one board is registered under two labels.

**Personio operates two domains and the probe accepted one.** `validate_candidate_for_probe`
required `jobs.personio.de`, so every `.jobs.personio.com` board was refused as an invalid
host before it was fetched — nine boards, all serving a real feed, all registering zero. The
runtime itself never had the restriction. Alongside it, `count_rows` counted HTML anchors in
an XML feed, so all 21 Personio boards read as zero in the first place.

The recurring lesson across all nine: **every one was a harness assumption, not a board
failure, and none was visible without running discovery.** A recorded zero is the most
expensive kind of finding, because it is indistinguishable from a board that genuinely has
nothing — right up until the openings stay missing.

## What is left, in priority order

Ordered by measured size, and by whether the item blocks others. Everything below is
outside the registration, which is finished at 688 of 695 boards — the residue is the
7 boards / 71 openings listed in [Fix order](#fix-order-after-the-live-run), and it is
small enough to fold into any of the slices below rather than stand alone.

### 1. The freshness window was not ageing boards out — 1,892 boards — **DONE, unverified live**

**Cause.** `_apply_status_state` called `refresh_next_eligible_check_at` for every terminal
status including `excluded`, while `apply_excluded_source_state` deliberately never moves
`lastSuccessAt`. Each skip pushed the deadline out another window while the success clock
froze, so eligibility became a function of run frequency rather than the configured window.
Nothing surfaced it because `lastCheckedAt` did not move either — a skip is not a check — so
the only clock advancing was the one that should not have been.

**Fix.** A skip leaves `nextEligibleCheckAt` alone; the skip reason is threaded explicitly
from the excluded report so a stale reason cannot suppress the advance later. `304` counts as
a non-fetch skip deliberately: a 304 asserts the cache is current.

**Verified by replay, not asserted.** `tools/coverage_freshness_replay.py` drives the real
decision function with the pre-fix policy as a control: over 30 days the current policy
fetches 55× at 60-minute cadence, the pre-fix policy 0×. The control is a test — if the
pre-fix policy ever fetches, the replay has stopped measuring the policy.

**The live run did not confirm it.** `cache_within_freshness_window` went 1,893 → **1,956**.
A mechanism replay is not production evidence; this stays open until two consecutive live
runs show the skipped count falling.

### 2. Second registration wave — **DONE**, 1,415 openings across 88 boards

213 boards / 2,383 openings resolved to 1,415. Five of six blocks were measurement faults,
not missing adapters: hrmos needed no adapter at all (833), Workable was lost twice to the
same wrong URL (440), Greenhouse EU hosts matched no host rule so 8 tenants collapsed to one
row (158), a JSON API host is not the board (94), and a static candidate's listing URL was
its host root (70).

### 3. The review boards — **DONE**, 1,070 openings across 116 boards

179 undecided boards resolved to 116 registered — on recorded openings rather than a probe,
since 105 return HTTP 200 with zero anchors — and 63 deliberately left alone:

| Class | Boards | Openings | Why not registered |
|---|---|---|---|
| HTTP error (404/400/0/403/429/401) | 47 | 514 | says the board cannot be read, not that it is empty |
| oracle_hcm | 2 | 169 | own REST endpoint returns empty `items`; UI ships zero anchors |
| personio | 5 | 33 | feed 404s; the careers page is HTML the Personio adapter cannot read |

Registering an HTTP-error board trades a missing opening for a source reporting zero forever.
Two of the three classes were then fixable anyway — **230 openings, 22 boards**: 13 of the 47
were registered against a URL that errors (`herp.careers/v1/<tenant>` answers 200 where
`herp.careers/<tenant>` answers 400, plus four boards each missing a path segment), and
Personio was double-blocked by an HTML-anchor counter on an XML feed and a `.de`-only
validator.

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
Feishu stays unregistered. Recovering it is a project, not a registration task: a
CSRF-aware, rate-limit-respecting client, a payload parser, and a fixture. Kurogame alone
is 411 openings.

### The 1,468 extraction gaps — mostly not coverage work

1,468 openings on registered, working boards where the studio has other roles but this one is
absent. Over 155 boards, and the causes are not the same kind of thing.

**SmartRecruiters capped its response — 214 openings — FIXED.** One request per source with no
`limit`/`offset`; the API returns at most 100 however asked. `ubisoft2` reports
`totalFound=332` against 100 rows. Only SmartRecruiters is paginated — the other four JSON
feeds return everything at once and it rate-limits.

**The sector classifier was dropping real game roles — FIXED, on an explicit product
decision.** 1,226 roles rejected outright; 35 measured craft tokens recover 238 across 97
studios, admitting none of a 15-role control set of business roles at studios. Safe because
they live in `GAME_ROLE_KEYWORDS`, separate from `GAME_KEYWORDS`: `has_positive_game_evidence`
is a *sector* classifier where a bare role word must not imply Game, and folding them
together made "UX Designer" at an arbitrary employer classify as Game.

**Some were never gaps.** dontnod's three "missing" roles are all "Spontaneous Application".

### 4. The 82% Google Sheet dependency — an asset, not a liability

This is item 1's successor rather than a parallel track: a freshness fix delivers more
openings only if the feed is not still dominated by one source, so the two are sequential —
fix the window, then confirm the recovered boards actually show up in the feed.

**Corrected framing.** This was previously written up as a single point of failure to be
reduced, which inverts it. The community spreadsheet is the best base source available and the
reason the feed has breadth at all. The goal is not to remove the dependency but to stop it
being the *ceiling* — to make sure the direct ATS and static-scraped sources behind it reach
the feed. With 6,862 openings registered behind that work, this is the question the next live
run answers.

### 5. Data quality, deferred by agreement

- **Blank country — 4,493 rows.** Orthogonal to coverage: present but unclassifiable by
  region, which understates EU counts rather than losing openings.
- **GB/UK and England/Scotland/Wales — ~365 rows.** Pending a `country_acceptance.json`
  contract change.

### 6. The live registry holds rows that 404 while reporting `active`

All three Electronic Arts rows answer **404** and have done since they were promoted on
2026-04-10, while `registryState` reads `active`:

- `static:listing_url:https://jobs.ea.com/en_us/careers`
- `static:listing_url:https://jobs.ea.com/en_us/careers/home/?4538=8369&4538_format=3021&listfiltermode=1`
- `static:listing_url:https://jobs.ea.com/en_us/careers/searchjobs/?4538=[8354]&4538_format=3021`

`https://jobs.ea.com/careers` answers 200 with 29 rows. So EA looks registered, contributes
almost nothing, and 134 catalogue openings sit behind it — the same shape as the Activision
row that reports `ok` with 80 kept while 404ing.

This is **not** fixed here. Correcting live registry rows is a data mutation, and the repo's
rule is that it takes a printed plan, an asserted row count, an explicit apply flag, a backup
to `_out/` and a read-back. Worth doing deliberately rather than as a side effect. Note also
that a curated seed row cannot substitute: board identity is host + tenant, so a second row
for `jobs.ea.com` would be a duplicate board fetched twice.

### 7. Per-board extraction gaps — 325 openings

The boards that *were* fetched and still return fewer roles than the catalogue lists. The
static runner already follows `?page=N` and `/page/N` anchors within a budget, so this is not
paging: EA, Roblox and Garena expose **zero** pagination anchors and their lists are
JS/API-driven. Each board needs its own diagnosis against what it actually serves, and the
largest are Roblox (123, 9 rows visible), Garena (74, 10 rows), Activision (43) and
`jobs.jobvite.com` (39).

Provider boards are a separate 159 openings, and they cannot be triaged from the fetch report
at all: providers roll up to one row each (`greenhouse_boards` kept 1,108, `lever_sources`
296, `workable_sources` 348), so a single underperforming board is invisible inside a healthy
provider total.

### 8. Carried-over defects

- **`careers.activision.com/careers` returns 404 while reporting `ok` with 80 kept** — the
  same defect shape as the three EA rows above, and one of a family: a stale registration the
  fetch report does not surface as an error. Worth fixing on its own account, since a 404
  reporting `ok` will recur.
- **Gamucatex junk rows** — pre-existing. `static_detail_link_rows` at `_runner.py:343`
  has no title gate; only `larian.py` and `supercell.py` call it.
- **`plans/voodoo-ashby-board-migration-plan.md`** — delete once a release lands.

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
