# Jobs Entry Validation Audit — 2026-08-12

> - **Status:** Evidence snapshot (read-only audit; no code changed) — **superseded in part**: Track A fixes (static title noise + country normalization + country contract v3) were implemented and validated on 2026-08-12 after this snapshot; Track B decision doc: `docs/snapshots/non-game-employer-evidence-2026-08-12.md`.
> - **Basis:** Local `data/` artifacts from the 2026-07-17 full pipeline run (40,586 rows), pre-0.2.130 memory batch. Live Umbrel feed (0.2.130, ~48k rows) is pending the user app-store update; this snapshot is the reproducible baseline, not the post-release feed.
> - **Canonical for:** current coverage, trustiness, and entry-quality baseline of the jobs feed.
> - **Not canonical for:** live Umbrel numbers or post-0.2.130 behavior.
> - **Then inspect:** `docs/DATA_CONTRACT.md`, `docs/scraping-pipeline.md`, `docs/source-policy-runbook.md`, `scripts/jobs_artifact_quality_gate.py`, `src/jobs/text_utils.py` (country contract), `src/jobs/common/exact_category_titles.py`.

## Summary verdict

| Dimension | Verdict | Headline numbers |
|---|---|---|
| Coverage | **Weak provider coverage; heavy Google Sheets dependence; 19% run failure rate** | 2,123 sources → 1,720 ok / 403 failed; sheets = 78% of feed |
| Trustiness | **Gate-clean for category/company leaks, but raw parser contamination visible outside gate scope** | 0 gate blockers; 50 CSS/JS-code titles; 1,364 non-game company rows; 994 country-contract misses |
| Proper entries | **Structurally sound (no missing title/company/link, no dup links), with field-quality gaps** | 0 bad links; 26% empty city; contractType Unknown 92%; postedAt missing 95% |

## 1. Coverage

### 1.1 Run-level (2026-07-17, `jobs-fetch-report-summary.json`)

- Sources selected: **2,123**; successful **1,720 (81%)**; **failed 403 (19%)**; excluded 0; circuit-breaker 0.
- Wall clock ≈ 38 min (`wallClockDurationMs` 2,309,359); fetch+parse dominated (20.8 s of 23.6 s total median durations).
- Output: **40,586** rows.
- Failure taxonomy (cumulative `jobs-source-state.json`, 4,895 sources): `static_source` prefix 436, `no jobs extracted from source pages` 368, `Network error` 91, `crawl failed` 58, HTTP 403 26, HTTP 404 9, `time budget exceeded` 9, ashby-board 5, HTTP 429 3, subprocess timeout 3. Last-run statuses: ok 3,792 / error 1,011 / excluded 92.

### 1.2 Adapter yield (`summary.adapterTimings`)

| Adapter | Sources | Fetched | Kept | Errors | Zero-kept |
|---|---|---|---|---|---|
| static | 2,098 | 11,514 | 10,862 | 403 | **1,130** |
| csv (sheets) | 3 | 33,608 | 33,583 | 0 | 0 |
| greenhouse | 1 | 1,021 | 1,019 | 0 | 0 |
| html | 5 | 2,647 | 2,646 | 0 | 0 |
| lever | 1 | 312 | 287 | 0 | 0 |
| workable | 1 | 424 | 409 | 0 | 0 |
| teamtailor | 1 | 105 | 96 | 0 | 0 |
| smartrecruiters | 1 | 107 | 106 | 0 | 0 |
| recruitee | 1 | 73 | 73 | 0 | 0 |
| api | 2 | 165 | 165 | 0 | 0 |
| scrapy_static | 1 | 26 | 26 | 0 | 0 |
| others (breezy, personio, bamboo, workday, oracle, ashby, pinpoint, jazzhr) | ~8 | ≤6 | ≤4 | 0 | 1 each mostly |

Zero-kept static sources: **1,130 of 2,098 (54%)** — the single largest coverage pressure. High-cost low-yield examples: `lionhearts.co.kr/career` (80.9 s, 0 kept), `redemptiongames.com/jobs/` (72.6 s, 0), `brbent.com/careers/` (65 s, 0), `fairplaystudios.net/careers/` (64.6 s, 0).

### 1.3 Feed composition by source class (40,586 rows)

- **Google Sheets: 31,821 (78.4%)** — `google_sheets` 29,888, `google_sheets_1er2oaxo` 1,666, `google_sheets_1mvqhxat` 267.
- Static: **5,645 (13.9%)**.
- Provider/board: **2,994 (7.4%)** — greenhouse_boards 1,008, gracklehq 917, workable 293, lever 287, smartrecruiters 106, epic_games 117, gamejobs 110, remote_ok 7, teamtailor ~96, others.
- Other: 126.

Provider coverage is thin relative to the sheet reliance; provider rows are 99.7% `Game` sector vs sheets 69% `Tech` (see §3.3).

### 1.4 Secondary queues

- Parser regression queue: **398 entries**, all `site_changed`, fingerprint unchanged; 329 last-status error, 69 ok. Adapters: static 395, lever 1, greenhouse 1, recruitee 1.
- Browser fallback queue: **130 entries** (scrapy_static only): timeout 91, anti_bot_or_challenge 20, blocked_or_challenge 14, parse_error 3, js_required 2.
- Registry conflicts: **289 families / 650 rows** (winner 289, loser 361) — `registry-conflicts-summary.json`.
- Source-policy recommendations: 2 pairs, both `moreHistory`.

## 2. Trustiness

### 2.1 Shipped-artifact quality gate (`scripts/jobs_artifact_quality_gate.py` on full unified JSON)

- **status: warning / ok: true** — no blockers.
- `exactCategoryTitleLeaks` 0, `staticContainerTitleLeaks` 0, `cityFilterCandidateLeaks` 0, `cityFilterCompoundWarnings` 0, `unknownCompanyStrongEvidenceLeaks` 0.
- `unknownCompanyWeakEvidenceWarnings` **11** — all `google_sheets`, mostly expired LinkedIn redirect links (`trk=expired_jd_redirect`), 1 Jobvite 404 (`jobs.jobvite.com/careers/amberstudiocareers/jobs?error=404`). Examples: "Senior Product Manager - Star Trek Fleet Command", "LiveOps Specialist - Monopoly GO!", "VFX Artist - Monopoly GO!". Product-visible rows with no resolvable company.
- `audit_jobs_sanitizer.py`: all counts zero (no GS category titles, no repair/hydration candidates, no suspicious titles).

### 2.2 Raw-title contamination outside gate scope (light feed)

The gate covers exact category titles, container artifacts, city filters, and Unknown-company. These classes are **not** covered and are present:

- **CSS/JS code as title: 50 rows** — e.g. `.css-ttson6{...}` (AppLovin 2), `.sendgrid-subscription-widget...` (Beamdog), `const t="undefined"...nprogress...` (Beyond Frames 8, New Folder Games), Valve 8, Flix Interactive 5, Rendever 4. 46 of 50 from `static_source::`.
- **Foreign-language UI text as title: 20 rows** — `홈`/`重置` (Glu Mobile via `jobs.ea.com/ko_KR/careers/Home`, `zh_CN/careers/Home`), `首頁`, `또는`, `東京`, `営業`, `企画`.
- **Nav-word exact titles: 4** — `Register`, `Login`, `Home` (jobs.ea.com/en_US/careers/...), `FAQ` (sybogames.com).
- **Titles ≤2 chars: 14** — including zero-width space, `UA`, `NL`, `mx`, `KR`, `JP` (country codes as titles, static sources).
- **Title length max 4,619 chars** (CSS payload); median 29.

These are all static-adapter extraction misses. The gate does not currently treat code/JS/nav-text titles as leaks, so they ship.

### 2.3 Non-game company contamination (Google Sheets)

Using a conservative explicit employer list, **1,364 rows** in the feed come from clearly non-game employers, all via `google_sheets`: Dominos 255, Boschgroup 189, Nationalvision1 169, Accorhotel 161, Turnertownsend 112, Jsheldllc 90, Abercrombieandfitchco 79, Varonis 55, Fliff 39, Pilotcompany 33, Oportun 29, Xplor 27, Publicstorage 25, Deangelocontractingservices 25, Relaischateaux 21, AjnaLens 20, Endeavourgroupcareers 14, Securitas 13, Barriere 8. These are real ATS-hosted jobs (smartrecruiters/greenhouse/workday hosts) but are not game roles (e.g. "Optometrist" ×126 National Vision, "Assistant Manager" ×~107 Domino's). The sector gate `BALUFFO_STRICT_GAME_ONLY` is opt-in; default behavior ships them.

### 2.4 Dedup evidence

- Rows with `sourceBundleCount > 1`: **2,952 (7.3%)**.
- Outliers ≥12 bundles: 124 rows. Top: **255** (Kforce "Accounting Operations & Credit Manager" — location list spanning Bradford/Tempe/Phoenix…; staffing-agency role bucket), 40 (Sony "Manager, Corporate Development"), 35 (Sandbox UI Lead), 32 (Coldwood UI Lead), 28 (Nexon Japan title `REQUIREMENTS`), 24 (Wargaming 3D Character Animation Team Lead), 17 (LNW 2D Animator, "Monetization" inside location), 16 (Nixxes, "On-site"/"Odpowiedz na ofertę" static rows).
- **172 title+company pairs with >3 rows (1,189 rows)**; max pair 126 (Optometrist/National Vision). Mostly the same non-game sheet buckets, plus legit multi-location rows (e.g. Wargaming team leads).
- Identity preflight: `repairedIdentityCount 0`, `contaminatedIdentityCount 0`, `quarantinedIdentityCount 0`, `unresolvedMissingIdentityCount 0` — clean.

### 2.5 Availability & lifecycle

- Feed: 40,586/40,586 `available`, evidence kind `source_present` 100%, confidence `definitive`, check age 0.1 d (same-run).
- `availabilityHealth.status` **degraded**: sweep shadow mode, 1,000 selected / **43,122 deferred**; `verifiedWithinSevenDaysCoverage` 0.979 (target 0.95, `healthTargetMet` true) but `directCheckedWithinSevenDaysCoverage` **0.0** (`directHealthTargetMet` false).
- Lifecycle ledger (122,614): active 44,122 / archived 61,816 / likely_removed 16,676; preserved 2,890 (`source_skipped` 1,598, `source_failed` 1,292); availability `unavailable` 78,492 / `available` 44,122.
- Feed 40,586 vs lifecycle-active 44,122: 3,536 difference (availability-id rows not in public light feed — e.g. identity-quarantined or non-available-only projections; the light feed is `available`-only by contract).

## 3. Proper entries

### 3.1 Structural completeness (light feed)

- 0 missing title / company / jobLink / profession / sector; **0 malformed jobLinks; 0 duplicate jobLinks** (40,586 distinct).
- `sourceJobId` present on all rows (full JSON).
- Locations: `locations` list present on 35,336 (87%); `locationSummary` empty on **5,250**; >1 location entry on 356 rows.

### 3.2 Location gaps

- Empty city **10,662 (26.3%)**; empty country **6,230 (15.4%)**; both empty **5,161 (12.7%)**; `Unknown` country 88 (valid placeholder per contract).
- `country=Remote` 304 (should arguably be workType-only).
- City-filter validation (repo logic `classify_city_filter_rejection` + city options): **0 rejections** — the city filter path is clean on this artifact.

### 3.3 Country field issues

- **994 rows** with a country value that `resolve_country_acceptance_value` does not accept, split:
  - **729 real ISO codes missing from the acceptance contract** (filter-side gap, data is fine): `MY` 278, `TR` 199, `HK` 132, `LT` 15, `VN` 13, `QA` 10, `CY` 8, `UA` 6, `CI` 6, `EE` 4, `RO` 4, `BG` 3… The contract (`data/contracts/country_acceptance.json` v2) carries full names + ~27 ISO codes only.
  - **227 US state codes as country** (WA 34, TX 26, AZ 22, FL 19, NY 18, PA 17, CO 14, NJ 12, GA 9, IL 8, AL 6, ID 5…): mostly greenhouse/lever/static rows where state abbreviation landed in `country`.
  - **38 non-Latin garbage country values**: `東京` 17, `首頁` 7, `企画` 3, `給与` 2, `時給`, `또는`, `搜索`, `日吉`, `営業`, `大阪`, `여행` (static sources, Japanese/Korean career pages).
- Ambiguous codes: `UI` 7, `AB` 4, `EN` 4, `EU` (timezone), `GI` — mixed.

### 3.4 City text pollution

- 39 non-Latin city values with title-like text in `city` (e.g. `桜組マネージャー K.K. 2018年入社`, `ライセンス Kimi`, `ミュージカル Yumi` — Marvelous APAC, Silicon Studio) plus ~500 additional rows with any non-ASCII city text (much of it legitimate localized city names such as 東京, 大阪 — needs per-row triage, not blanket cleanup).

### 3.5 Field-quality gaps

- `contractType` Unknown: **37,254 (91.8%)**; Full-time 860, Internship 1,358, Temporary 1,114.
- `profession=other`: **27,658 (68.2%)** — top populated: engine 8,687, designer 1,361, ai 569, animator 398, gameplay 316.
- `postedAt` missing: **38,576 (95%)** — freshness surface is essentially empty.
- `qualityScore`: sheets 91-heavy (25,215), static 73-heavy (3,209 of 5,645), provider 100-heavy (1,611 of 3,120) — score tracks source class, matches expectations.
- Sector split: Tech 25,099 vs Game 15,487 (62% Tech) — mostly the sheet's Tech companyType rows (21,319 of 31,821 sheet rows typed Tech).

## 4. Ranked findings & suggested fix order

| # | Finding | Count | Suggested fix (smallest safe first) |
|---|---|---|---|
| 1 | Non-game employers in `google_sheets` (Dominos, Bosch, National Vision, Accor…) | 1,364 rows | Source-policy triage: sheet row provenance check vs strict-sector evidence; do **not** drop without explicit product approval (guardrail). Needs `BALUFFO_STRICT_GAME_ONLY`-style evidence or sheet-level review. |
| 2 | CSS/JS/nav/foreign-UI text as titles (static adapter) | ~74 rows (50 code + 20 UI + 4 nav) | Extend static extraction title validation (reject code/CSS/JS/zero-width/nav tokens) + regression tests; gate these as leaks. |
| 3 | US state codes + non-Latin garbage in `country` | 227 + 38 | Country normalization: state-code → US map + non-Latin reject in canonicalize; contract-side accept real ISO codes (729 rows) separately. |
| 4 | Country acceptance contract missing real ISO codes | 729 rows | Contract addition (MY, TR, HK, LT, VN, QA, CY, UA, CI, EE, RO, BG, ID, PK, AZ, GE, MD, MK, PH, GT, PA…); filter-side, no data rewrite. |
| 5 | Unknown-company weak evidence (expired LinkedIn/Jobvite 404) | 11 rows | Gate/watch; likely dead-link policy candidates; validate live before dropping. |
| 6 | `contractType` Unknown 92% / `postedAt` 95% missing | 37,254 / 38,576 | Provider adapters carry contract/posted data (greenhouse, lever, workable) — sheet rows won't; decide product policy, then normalize provider-side only. |
| 7 | Static zero-kept pressure | 1,130 sources / run | Source-policy runbook: dead-source evidence sweep (like 2026-04-29 batch) vs parser misses (130 in fallback queue). |
| 8 | Bundle outliers (Kforce 255, Sony 40, multi-location role buckets) | 124 rows | Dedup-evidence review; confirm role-bucket buckets are intentional or need generic-role guard coverage. |
| 9 | Availability sweep shadow mode / zero direct checks | 43,122 deferred | Operational follow-up (enable direct-check path or accept degraded status). |
| 10 | `profession=other` 68% | 27,658 | Normalization expansion is optional product polish, not a data-quality defect. |

## 5. Evidence sources

- `data/jobs-unified.json.gz` (full, 40,586), `data/jobs-unified-light.json.gz` (public projection).
- `data/jobs-fetch-report-summary.json` (2026-07-17 run), `data/jobs-source-state.json.gz` (4,895), `data/jobs-lifecycle-state.json.gz` (122,614).
- `data/jobs-parser-regression-queue.json` (398), `data/jobs-browser-fallback-queue.json` (130), `data/registry-conflicts-summary.json` (289), `data/source-policy-recommendations.json`, `data/jobs-availability-sweep-plan.json`.
- Local run dirs under `_out/runs/20260717_*` (detail fetch reports are stub-only locally).
- Gate runs: `scripts/jobs_artifact_quality_gate.py` and `scripts/audit_jobs_sanitizer.py` against the decompressed July artifacts.
