> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** d5491e13
> - **Status:** phases 0 and 1 landed; 4,280 of 4,356 registered openings proven delivered (98%); the registration is effectively complete

# Closing the catalogue coverage gap

Canonical plan for the catalogue coverage effort: baseline, board registration, and
what still blocks delivery. See [`INDEX.md`](../INDEX.md).

## Where this stands

Registration now covers **6,929 openings** across 695 boards. **6,844 of them are proven to
reach the registry** — up from 762 when delivery was first measured. The difference is
tracked as two numbers throughout, because "registered" is not "delivered" and only the
second one is a result.

| | Openings |
|---|---|
| Registered and **proven delivered** | **6,844 (98.8%)** |
| Registered but not landing | 85 |
| Still on an unregistered board | ~2,500 |
| Of which blocked on a genuinely unreadable board | ~350 |

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

## Eight defects found by running discovery, not reading it

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
