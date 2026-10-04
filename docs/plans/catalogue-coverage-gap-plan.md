> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** d5491e13
> - **Status:** phases 0 and 1 landed; 4,280 of 4,356 registered openings proven delivered (98%); the registration is effectively complete

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. See [`INDEX.md`](../INDEX.md).

## Where the remaining 10,464 openings actually are

Every number below is measured against the live feed and the live fetch report, with the
board-identity fixes applied — the audit was undercounting registered coverage by ~1,000
openings until they were in.

| Bucket | Openings | Status |
|---|---:|---|
| **A — registered in the repo, awaiting a release** | **6,862** | done; inert until shipped |
| **B1 — board was skipped as "fresh"** | **779** | fixed; needs a live run to realise |
| B2 — board was fetched and still returns short | 325 | per-board extraction work |
| C — the sector classifier rejects the role | 988 | 238 just recovered; most of the rest is correctly rejected |
| D — board registered nowhere | 1,510 | 826 Feishu (out of reach) + ~684 unreadable |

**7,641 openings — 73% of the entire remaining gap — are already fixed and blocked on exactly
one thing: a release.** That is the single most important fact about this plan, and it is why
no further registration work is worth starting before one ships.

Two details worth keeping in view. Bucket B1 is the freshness-window fix showing up in the
data: 779 openings sit on the 1,893 boards the last run skipped as `cache_within_freshness_window`.
And the live report shows `personio_sources` **kept 0 of 2 fetched** — the Personio validator
bug cost the entire provider in production, which is why those 49 openings are in bucket A
rather than merely theoretical.

Bucket C is not a gap of the size it looks. Of the 988 the classifier still rejects, the
measurement in `tools/coverage_classifier_candidates.py` shows the remainder is dominated by
business roles at studios — Logistics Director, Marketing Manager, Junior .NET Developer —
which this product should not be collecting.

## Where this stands

Registration now covers **6,929 openings** across 695 boards. **6,844 of them are proven to
reach the registry** — up from 762 when delivery was first measured. The difference is
tracked as two numbers throughout, because "registered" is not "delivered" and only the
second one is a result.

| | Openings |
|---|---|
| Registered and **proven delivered** | **6,844 (98.8%)** |
| Registered but not landing | 85 |
| Still on an unregistered board | ~1,510 |
| Of which Feishu, unreachable by any current path | 826 |

Measured with `tools/coverage_drain.py`, which drains discovery over the curated boards
in an isolated data directory and counts what reaches `active`.

| Adapter | Boards | Landed | Openings | Delivered |
|---|---|---|---|---|
| static | 425 | 420 | 3,440 | 3,409 |
| workday | 17 | 17 | 1,065 | 1,065 |
| greenhouse | 52 | 51 | 655 | 617 |
| workable | 29 | 29 | 440 | 440 |
| smartrecruiters | 16 | 16 | 432 | 432 |
| ashby | 39 | 39 | 334 | 334 |
| bamboohr | 47 | 47 | 192 | 192 |
| lever | 22 | 20 | 181 | 165 |
| breezy | 15 | 15 | 69 | 69 |
| personio | 9 | 9 | 49 | 49 |
| jazzhr | 8 | 8 | 32 | 32 |
| teamtailor | 13 | 13 | 26 | 26 |
| recruitee | 3 | 3 | 14 | 14 |
| **Total** | **695** | **687 (98.8%)** | **6,929** | **6,844 (98.8%)** |

Delivery is reported in openings, not boards: one board with 176 promised openings and
one with a single opening are not comparable units, and a board-count headline hides
exactly the concentration that matters.

The seven boards still not landing are two empty Greenhouse and Lever boards and five thin
static boards, one of which (`vivastudios.com`) disconnects mid-response.

## Nine defects found by running discovery, not reading it

The first six are recorded below. Three more arrived with the second and third waves, and
every one has the same shape: **the harness was looking somewhere the openings were not, and
the zero was recorded as an answer.**

**A board's JSON API lives on a different host from its career page.** All 39 Ashby boards
register as `ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/<slug>`, which
resolved to `('ashby', 'api.ashbyhq.com', '')` — host and tenant both lost. Greenhouse, Lever
and SmartRecruiters share the shape. `registry_identity` now folds a known API host onto its
canonical career-page host and reads the tenant out of the API path. This one was a delivery
report understating coverage by 334 openings, not a real gap.

**Greenhouse's EU hosts matched no host rule at all.** `job-boards.eu.greenhouse.io` does not
match `job-boards\.greenhouse\.io$` — it ends `eu.greenhouse.io`, not
`job-boards.greenhouse.io` — so it fell through to `static` and 158 openings on 8 tenants
collapsed onto one host-root row, which is the multi-tenant failure this repo's own guardrail
warns about. Eight per-tenant rows now; the runtime's API serves every one.

**Workable was lost twice to the same wrong URL.** The probe read
`/api/v1/accounts/<a>/jobs` (HTTP 400 for every account) and the row it built carried only
`account`, which `endpoint_url` cannot resolve. Fixing only the first still registered 0 of 29
— both were needed, and both now use the runtime's own `JsonFeedSpec` template.

**A static candidate's listing URL is its host root, which is often not the careers page.**
This is the BambooHR defect again, except the tool never held a path to be wrong about, only a
host. All 109 unreadable static boards return HTTP 200 with zero anchors at the root.
`tools/coverage_listing_discovery.py` derives the listing from the URLs of the openings that
were missed on the board — the board was found because those specific openings exist — and
measures each ancestor path with the runtime's own detector.

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

**Verified by replay rather than asserted.** `tools/coverage_freshness_replay.py` drives the
real decision function over a simulated run cadence, with the pre-fix policy alongside as the
control. Over 30 days the current policy fetches a board **55 times** at a 60-minute cadence
and **30 times** at 720; the pre-fix policy fetches **zero at both**, which is exactly the
121-day staleness the live report showed.

Two earlier versions of that replay measured nothing and both looked like null results — one
rewrote the deadline every round so it could never expire, the other never rewrote it so real
time never reached it. The control is now a test: if the pre-fix policy ever fetches, the
replay has stopped measuring the policy and the test says so.

**This is a mechanism replay, not production.** It shows a board is refetched now and was not
before. Whether the *skipped count in a live run* falls from 94% is still only observable
there, and that stays the check to make after a release.

**To confirm it works in production:** after a release, two consecutive full runs should
show the skipped count falling rather than holding near 94%, and the median age of a
skipped board should stop growing between runs.

### 2. Second registration wave — **DONE**, 1,415 openings

The wave was 213 boards / 2,383 openings. It resolved to **1,415 registered and delivered**
across 88 boards, and the difference is the interesting part: five of the six blocks turned
out to be measurement faults rather than missing adapters.

| Adapter | Boards | Openings | What it actually was |
|---|---|---|---|
| static (hrmos) | 28 | 833 | no adapter needed at all |
| workable | 29 | 440 | lost twice to the same wrong URL |
| greenhouse (EU) | 8 | 158 | host matched no rule; 8 tenants collapsed to one row |
| ashby / greenhouse / smartrecruiters | 5 | 94 | API host is not the board |
| static (listing discovered) | 3 | 70 | listing was not the host root |

**hrmos needed no adapter — 833 openings.** Labelled `unsupported_vendor` purely because no
dedicated adapter exists. Its tenant listing pages are server-rendered and the runtime's own
detector reads all 32 (cygames 100 rows, capcom 92, gamefreak 59, square-enix 35, nexon 17),
and 4 were already registered as static rows with cgames keeping 340 in production as the
control. 28 static rows; all 28 land.

**Workable was lost twice to one wrong URL — 440 openings.** `list_url_for` built
`/api/v1/accounts/<a>/jobs`, which answers HTTP 400 for every account; the row it then built
carried only `account`, which `endpoint_url` cannot resolve. The first fix alone still
registered 0 of 29 — both were needed. Both now use the runtime's own `JsonFeedSpec`
template, where keywords-intl1 serves 282 jobs and sideinc 378.

**Greenhouse's EU hosts matched no rule — 158 openings.** `job-boards.eu.greenhouse.io` does
not match `job-boards\.greenhouse\.io$`, so it fell through to `static` and 8 tenants
collapsed onto one host-root row — the multi-tenant failure the repo's own guardrail warns
about. Now 8 per-tenant greenhouse rows; the runtime's API serves every one.

**A static candidate's listing URL is its host root, which is often not the careers page.**
The BambooHR defect again, except the tool never held a path to be wrong about, only a host.
`tools/coverage_listing_discovery.py` derives the listing from the URLs of the openings that
were missed on the board and measures each ancestor path: `koeitecmo.co.jp/recruit/career`
serves 43 rows where the root serves none.

### 3. The review boards — **DONE**, 1,070 openings registered, 63 left undecided

The 179 undecided boards resolved to **116 registered carrying 1,070 openings — all 116
land** — and 63 that stay undecided on purpose.

**105 alive but unreadable** return HTTP 200 with zero anchors, so they are registered on
their recorded openings rather than on a probe: an observation rather than an inference from
a zero, and the same evidence the first wave's 246 boards used.

**63 are left alone, and that is the honest answer rather than a gap in the work:**

| Class | Boards | Openings | Why not registered |
|---|---|---|---|
| HTTP error (404/400/0/403/429/401) | 47 | 514 | says the board cannot be read, not that it is empty |
| oracle_hcm | 2 | 169 | own REST endpoint returns empty `items`; UI ships zero anchors |
| personio | 5 | 33 | feed 404s; the careers page is HTML the Personio adapter cannot read |

Registering an HTTP-error board would trade a missing opening for a source that reports zero
forever, which is the failure mode the zero-yield quarantine exists to prevent.

**Two of those three classes turned out to be fixable after all — 230 openings, 22 boards.**

**Thirteen of the 47 were registered against a URL that errors.** `herp.careers/<tenant>`
answers 400 where `herp.careers/v1/<tenant>` answers 200 — nine boards and 147 openings —
and Dayforce, Briohr, EyeLine and Hurma each lost a URL segment the same way for 34 more.

**Personio publishes XML and the probe was counting HTML anchors**, so all 21 boards read as
zero. With the runtime's own parser they split into 9 that collect, 7 whose feed parses and
holds no *game* roles, and 5 whose feed 404s. Then the probe's validator rejected every
`.jobs.personio.com` host because it accepted only `jobs.personio.de` — and the 9 registered
0 of 9. The runtime never had that restriction; `provider_personio` fetches `feed_url`
directly. Both domains are accepted now.

The 7 non-collecting Personio boards are a real answer, not a defect: instinct3's feed is
full of positions, all marketing roles like "(Junior) Brand Partnerships Manager", which
Baluffo correctly does not collect. The verdict says "parsed and yielded no game openings" so
it cannot be confused with an unreachable board.

### Feishu — 826 openings, and not reachable by anything the runtime has

The last block worth engineering, investigated and found to be genuinely out of reach rather
than merely unlabelled. This is the opposite of the hrmos finding, and the difference is
worth stating precisely, because the cheap-looking fix here is the exact pattern this effort
has spent its whole duration undoing.

**The reclassification would have worked on paper.** hrmos was labelled a vendor adapter when
it is a static platform the runtime collects; the same reasoning says Feishu's eight tenant
boards are just JS pages and could be static rows on their recorded openings. 105 boards of
exactly that shape are registered and reach the registry today.

**It would also have been false.** Four measurements, each of which is the reason:

| Question | Answer |
|---|---|
| Does a GET see job rows? | HTTP 200, 141 KB, **zero anchors**, no job data in the HTML |
| Does rendering see them? | Chromium produces 5.7 MB and still **5 anchors, no job rows** |
| Where do the rows come from? | `POST /api/v1/search/job_post/count`, discovered from the JS bundle |
| Can that be called directly? | **405** unauthenticated, then **429 ratelimit** on repeat; no cookies, and `/api/v1/csrf/token` is an SPA catch-all |

The runtime has no Feishu handling at all — no plugin, no adapter, nothing in `src/jobs`.
And the crucial point: **the delivery metric would have said "landed" and been wrong.** A
static row reaches the registry and counts as delivered, exactly as the 105 JS boards do,
while producing zero jobs. That is a false green, which is worse than an acknowledged gap
because it stops anyone looking.

So Feishu stays unregistered. Recovering its 826 openings is a real project, not a
registration task: a CSRF-aware, rate-limit-respecting client for a POST search API, a
parser for its payload, and a fixture to test both against. That is worth doing on its own
merits if Feishu matters — Kurogame alone is 411 openings — and not as part of this effort.

`tools/coverage_render_probe.py` is what established the above: it renders a tenant page,
counts job links with the runtime's own `is_probable_job_detail_url`, and records the
API-shaped responses so the data path is visible rather than guessed.

### The 1,468 extraction gaps — mostly not coverage work

1,468 openings sit on boards that are registered and working, where the studio has other roles
in the feed but this role is absent. Spread over 155 boards, and the causes are not the same
kind of thing, which matters because only one of them is a coverage defect.

**SmartRecruiters capped its response — 214 openings, and a real bug — FIXED.** The runtime
issued one request per source with no `limit` or `offset`, and the API returns at most 100
however the request is phrased. `ubisoft2` reports `totalFound=332` against 100 rows, and the
runtime kept 12 game jobs where the board holds 46; 175 of the missing Ubisoft roles sat in
pages nobody had requested. Now paginated: **kept 12 → 46**, verified against the live API,
with `cdprojektred` and `gameloft` unchanged at 43 and 55 because they already fit one page.
Only smartrecruiters is paginated — the other four JSON feeds return everything at once and
SmartRecruiters rate-limits.

**The sector classifier is dropping real game roles — and that is not a coverage call.**
`looks_like_game_job` rejects CD Projekt Red's "Senior VFX Artist" and "Expert Concept Artist".
`GAME_KEYWORDS` holds 19 entries, all multi-token or unambiguous ("character artist",
"tech artist", "world artist"), and `vfx` and `concept artist` are simply absent.

**The sector classifier was dropping real game roles — FIXED, on an explicit product
decision.** `looks_like_game_job` rejected 1,226 catalogue roles outright: Ubisoft's "Senior
Hard Surface Artist", CD Projekt Red's "Senior VFX Artist", Marvelous's ゲームデザイナー. 35
measured craft tokens now recover 238 of them across 97 studios, admitting none of a 15-role
control set of business roles at studios.

Two things make that safe rather than a widening of blast radius. The craft tokens live in
`GAME_ROLE_KEYWORDS`, separate from `GAME_KEYWORDS`, because `has_positive_game_evidence` is a
*sector* classifier where a bare role word must not imply Game — folding them together made
"UX Designer" at an arbitrary employer classify as Game and broke an existing invariant. And
the addition was measured for what it *admits* as well as what it recovers, because a games
studio hires project managers and finance business partners too. Residual rejections are
mostly correct.

**Some of the 1,468 were never gaps at all.** dontnod's three "missing" roles are all
"Spontaneous Application", correctly dropped.

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

Nothing here reaches a running install until a release ships, and 0.3.007 is deliberately
untagged. Everything above is worth having regardless, because of what a release would
otherwise carry:

| Fix | A release without it |
|---|---|
| Six probe fixes | 17% of the registration instead of 98% |
| Freshness window | a registry that is 94% unrefreshed, capping everything above |
| Personio domains | `personio_sources` kept 0 of 2 fetched, as the live report shows |
| SmartRecruiters pagination | a third of Ubisoft's board never requested |
| Board-identity fixes | the audit undercounts registered coverage by ~1,000 openings |

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
