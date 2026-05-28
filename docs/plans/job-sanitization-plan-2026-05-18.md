# Job Sanitization Plan — 2026-05-18

> **Last updated:** 2026-05-29 — dual-pass confidence audit: 30 loopholes closed; P2 specs concretized against live source code; existing frozenset/mechanisms cross-referenced; bridge integration, data contract, verification commands specified

Investigation into non-game-development job contamination in `jobs-unified.csv` and strategy for filtering.

---

## 0. Progress Log

### 2026-05-22 — P1.6 first-run Google Sheets bootstrap timeout fix implemented

**Status:** P1.6 implementation complete; live targeted refresh now finishes under the first-run timeout.

**Implemented:**
- Moved category-link `404`/`410` validation after URL-title repair/provider hydration, so category rows that will be dropped unchanged are not live-checked.
- Added Google Sheets normalization and category-link validation progress callbacks so long bootstrap normalization phases keep task-live heartbeat evidence fresh.
- Kept stale-link semantics unchanged for surviving repaired/hydrated category rows: only `404` or `410` drops with existing `google_sheets_category_row`.
- Capped category-link liveness checks to a short `4s` timeout and raised their bounded concurrency to `32`, so slow domains become nonterminal instead of blocking first-run bootstrap.
- Updated first-run UI copy from time-specific “about 4 minutes” to time-neutral “several minutes” and kept the frontend waiting while task-live heartbeat evidence is fresh.

**Regression evidence:**
- Portable build investigated: `C:\Users\Andrea\Desktop\ocr_debug\portable`, build fingerprint `83063d1e2cf7289aed2676c43737774c14b2ea7ac5b4c6840bc5f905a14c86c9`.
- Observed stale run: two smaller Sheets sources completed in **15.3s** and **34.6s**, while the large `google_sheets` source stalled in `normalizing_rows` after fetching **30,928** rows.
- Pre-fix validation pass still checked **3,238** category links and spent about **257s** in category-link status checks, causing the frontend wait to exceed the first-run timeout.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets_category_links.py tests/test_jobs_fetcher_google_sheets_sanitizer.py tests/test_jobs_fetcher_google_sheets_title_hydration.py` — **33 passed**.
- `python -m pytest -q tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — **80 passed**.
- `npm run test:frontend:unit -- tests/frontend/unit/jobs-feed-startup.test.mjs tests/frontend/unit/jobs-feed-bootstrap-confirm.test.mjs tests/frontend/unit/jobs-first-run-notice.test.mjs tests/frontend/unit/jobs-runtime-list-view.test.mjs` — passed.
- Targeted refresh command: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/google-sheets-bootstrap-timeout-fix-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`.
- Targeted refresh result after fix: output jobs **6,167**, failed sources **0**, wall time about **155s**.
- Final scratch audit: category-style titles **0**, URL-title repair candidates **0**, provider-hydration targets **0**, suspicious exact titles **0**.
- Final category-link stats: candidates **3,238**, checked **3,209**, stale drops **425**, errors **0**, status-check elapsed **~97s**.

### 2026-05-22 — P1.5 Google Sheets category-title and stale-link cleanup implemented

**Status:** P1.5 implementation complete; targeted Google Sheets refresh passed.

**Implemented:**
- Added a shared Google Sheets category-title predicate used by category-row drops, URL-title repair eligibility, provider-hydration eligibility, provider-title category rejection, and sanitizer audit reporting.
- The predicate keeps the curated exact labels and adds conservative residual hyphenated bucket labels such as `Influencer-marketing`, `3d-art`, `Motion-design`, and `Audio-engineering`.
- Google Sheets category-style rows now run through hard static/noise checks, bounded live link status validation, URL-title repair, provider hydration, and a final category-style guard.
- Live link validation is limited to suspicious Google Sheets category-style rows. It uses HEAD with GET fallback, caches by normalized URL, and only treats `404` or `410` as terminal stale-link drops.
- Stale category links and still-unrepaired category titles continue to use the existing `google_sheets_category_row` drop reason; no output schema, drop reason, provider API, dependency, or frontend rendering change was added.
- `scripts/audit_jobs_sanitizer.py` now uses the shared canonical predicate and passes company into URL-title derivation.

**Portable artifact evidence before refresh:**
- Source artifact: `C:\Users\Andrea\Desktop\ocr_debug\portable\ship\data\jobs-unified.csv`.
- Audit command: `python scripts/audit_jobs_sanitizer.py --input-csv C:\Users\Andrea\Desktop\ocr_debug\portable\ship\data\jobs-unified.csv --report-json C:\Users\Andrea\Desktop\ocr_debug\portable\ship\data\jobs-fetch-report.json --limit 40`.
- Current stale artifact rows: **15,178** total; **7,679** Google Sheets rows.
- Shared predicate residual category-style titles: **1,906**.
- URL-title repair candidates: **172**.
- Provider-hydration targets: **881**.
- `Influencer-marketing` rows in the stale artifact: **41**; top related repair candidates include `Influencer Manager`, `Influencer Relations Talent Manager US`, and `Senior Influencer Sales Lead`.

**Targeted refresh evidence:**
- Command: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/google-sheets-category-link-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`.
- Targeted refresh result: output jobs **6,171**, failed sources **0**.
- Final refresh audit: category-style titles **0**, URL-title repair candidates **0**, provider-hydration targets **0**, suspicious exact titles **0**.
- Canonical drops in the scratch report: `google_sheets_category_row` **26,865**, `non_job_static_page` **12**, and `missing_job_link` **11**.
- Category-link validation stats in the scratch report: candidates **5,960**, checked **5,830**, stale drops **1,051**, errors **0**.
- Supported-provider hydration stats in the scratch report: candidates **2,429**, repaired **1,089**, missed **1,340**.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets_sanitizer.py` — **24 passed**.
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets_title_hydration.py tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — **85 passed**.

**Stop condition / next decision:**
- Remote OK source cleanup and the current Google Sheets category-title cleanup are complete for the planned sanitizer slices.
- Remaining future work should be evidence-driven provider coverage, not broad category-title policy: inspect the refreshed scratch output for real stale-provider families or non-game rows before choosing Jobvite, BambooHR, Personio, Feishu, or all-link liveness validation.

### 2026-05-22 — P1.4 generalized Google Sheets URL-title repair hardening implemented

**Status:** P1.4 implementation complete; focused sanitizer coverage added.

**Implemented:**
- Hardened Google Sheets URL-title repair at the slug-candidate layer instead of adding provider-specific Breezy or Comeet branches.
- Pure opaque URL segments such as UUIDs, numeric request IDs, long compact alphanumeric IDs with digits, and short dotted posting codes such as `1C.E4E` can no longer become titles.
- Opaque leading and trailing ID affixes are stripped only when the remaining slug has title evidence, preserving numeric role terms such as `2D`, `3D`, `Web3`, `UI`, `UX`, and `QA`.
- Account/company slugs without role evidence are rejected as URL-derived titles, preventing account names such as `Homa Games` from replacing category titles.
- No output schema, canonical drop reason, strict sector gate, provider hydration API, dependency, or frontend rendering change was added.

**Portable artifact evidence:**
- Source artifact: `C:\Users\Andrea\Desktop\ocr_debug\portable\ship\data\jobs-unified.csv`.
- Observed prior to the fix: **28** Google Sheets titles with leading compact opaque ID prefixes and **9** Comeet-style terminal-code title rows.
- Screenshot examples now target repair to `Technical Artist`, `Senior Product Analyst`, `Graphic Designer`, and `Product Monetization Manager` on the next canonicalization run.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets_sanitizer.py` — **20 passed**.
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets_title_hydration.py tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — **85 passed**.
- Read-only portable artifact audit confirmed all screenshot examples repair to the expected titles, account slugs remain rejected, numeric-role repairs remain intact, and derived outputs with long opaque IDs are **0**.

### 2026-05-22 — P1.3 Google Sheets Ashby title hydration implemented

**Status:** P1.3 implementation complete; focused tests and targeted Google Sheets refresh completed.

**Implemented:**
- Added Ashby hosted-board HTML support to the Google Sheets provider title resolver.
- Supported links are `jobs.ashbyhq.com/{board}/{posting_id}`; resolver fetches the board root and reuses the existing Ashby HTML parser.
- Ashby rows now match by posting ID or normalized job URL.
- No output schema, canonical drop reason, strict sector gate, or Google Sheets company rewrite was added.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets.py` — **20 passed**
- `python -m pytest -q tests/test_jobs_fetcher_providers.py tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — **149 passed**

**Audit after P1.3 support expansion:**
- Current-artifact audit command: `python scripts/audit_jobs_sanitizer.py --input-csv data/jobs-unified.csv --report-json data/jobs-fetch-report.json --limit 20`
- Current-artifact provider-hydration target coverage now reports **722** eligible rows.
- Newly covered Ashby family: `jobs.ashbyhq.com` **95** provider-hydration targets.
- Targeted refresh command: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/google-sheets-p1-3-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Targeted refresh result: output jobs **7,987**, failed sources **0**.
- Targeted refresh audit: category-style titles **1,472**, provider-hydration targets **677**, suspicious exact titles `Administartive` **11** and `Account-management` **8**.
- Remaining provider-hydration target domains after refresh: `job-boards.greenhouse.io` **319**, `jobs.lever.co` **152**, `jobs.ashbyhq.com` **65**, `boards.greenhouse.io` **50**, `jobs.eu.lever.co` **39**, `apply.workable.com` **38**, `job-boards.eu.greenhouse.io` **14**.

**Stop condition / next decision:**
- Remote OK is clean at source level after P1.1.
- Greenhouse, Lever, Workable, and Ashby hydration are implemented.
- Remaining residue is dominated by stale/missing provider postings on already-supported providers plus deeper unsupported providers such as Jobvite, BambooHR, Personio, Feishu, and company-specific pages.
- Do not add strict sector gating or broad drops without a product decision. The next meaningful code plan should choose one provider family, likely Jobvite or BambooHR, or pivot to policy-based filtering.

### 2026-05-22 — P1.2 Google Sheets Workable title hydration implemented

**Status:** P1.2 implementation complete; focused Google Sheets, provider, quality, and pipeline verification passed.

**Implemented:**
- Added Workable widget-feed support to the Google Sheets provider title resolver.
- Supported links are `apply.workable.com/{account}/j/{shortcode}` and feed lookup uses `https://apply.workable.com/api/v1/widget/accounts/{account}?details=true`.
- Workable rows now match by `shortcode`, `id`, normalized `url`, or normalized `shortlink`.
- No output schema, canonical drop reason, strict sector gate, or Google Sheets company rewrite was added.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_google_sheets.py` — **19 passed**
- `python -m pytest -q tests/test_jobs_fetcher_providers.py` — **69 passed**
- `python -m pytest -q tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — **80 passed**

**Audit after P1.2 support expansion:**
- Command: `python scripts/audit_jobs_sanitizer.py --input-csv data/jobs-unified.csv --report-json data/jobs-fetch-report.json --limit 20`
- Current local rows: **15,183** total; **7,516** Google Sheets rows.
- Remaining Google Sheets category-style titles in current artifacts: **1,580**.
- Remaining URL-title repair candidates: **0**.
- Provider-hydration target coverage now includes Workable and reports **627** eligible current-artifact rows.
- Newly covered Workable family: `apply.workable.com` **56** provider-hydration targets.
- Top provider-hydration target domains after P1.2: `job-boards.greenhouse.io` **315**, `jobs.lever.co` **152**, `apply.workable.com` **56**, `boards.greenhouse.io` **51**, `jobs.eu.lever.co` **39**, `job-boards.eu.greenhouse.io` **14**.

**Next code-slice recommendation:**
1. Re-run a targeted Google Sheets refresh when live title-repair impact is needed.
2. If Workable repair impact is confirmed and Remote OK remains clean, evaluate the next unsupported provider family by implementation risk: Ashby or Jobvite.

### 2026-05-22 — P1.1 Remote OK generic non-job title filter implemented

**Status:** P1.1 implementation complete; focused parser coverage and targeted Remote OK refresh completed.

**Implemented:**
- Added a Remote OK parser-stage guard for generic community/open-pool titles such as `Join Our Community`, talent community/pool titles, general/open/spontaneous applications, general interest, and `Join Our Team`.
- The guard runs before the Remote OK title/company/tag game-evidence check so `community` cannot pass only because it contains the substring `unity`.
- Real role titles such as `Community Manager` remain eligible when they have normal game evidence.
- No output schema, canonical drop reason, strict sector gate, or Google Sheets company rewrite was added.
- Remote OK now treats a valid feed whose rows are all filtered out as a successful empty source instead of a failed source.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_providers.py` — **69 passed**
- Targeted refresh command: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/remote-ok-p1-audit --only-sources remote_ok --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Targeted refresh result: output jobs **0**, failed sources **0**. The command still returned nonzero because the targeted no-output run does not produce a successful feed artifact, but the source-level validation passed and no obvious Remote OK non-job rows remained.

### 2026-05-22 — Remote OK P1 follow-up audit completed

**Status:** Audit complete; next code slice should stay on Remote OK before returning to Google Sheets.

**Remote OK targeted refresh:**
- Command: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/remote-ok-p1-audit --only-sources remote_ok --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Result: output jobs **1**, failed sources **0**.
- Remaining row: `Join Our Community` at `Tripadvisor`, source `remote_ok`, link `https://remoteok.com/remote-jobs/remote-join-our-community-tripadvisor-1131674`.
- Classification: P1 removed the broad pre-refresh Remote OK contamination, but the remaining row is still a generic community/non-job listing that passed via title/company/tag evidence.

**Current `data/` Google Sheets audit:**
- Command: `python scripts/audit_jobs_sanitizer.py --input-csv data/jobs-unified.csv --report-json data/jobs-fetch-report.json --limit 40`
- Current local rows: **15,183** total; **7,516** Google Sheets rows.
- Remaining Google Sheets category-style titles: **1,580**.
- Remaining URL-title repair candidates: **0**.
- Remaining provider-hydration targets: **571**.
- Remaining suspicious exact titles: `Administartive` **10**, `Account-management` **8**.
- Top provider-hydration target domains: `job-boards.greenhouse.io` **315**, `jobs.lever.co` **152**, `boards.greenhouse.io` **51**, `jobs.eu.lever.co` **39**, `job-boards.eu.greenhouse.io` **14**.

**Next code-slice recommendation:**
1. P1.1 generic community/open-pool filtering is now implemented.
2. Re-run a targeted Remote OK refresh when live source confirmation is needed.
3. If Remote OK yields zero obvious non-job rows, return to the Google Sheets provider-hydration target backlog.

### 2026-05-22 — P1 Remote OK description-only noise hardening implemented

**Status:** P1 implementation complete; focused parser and pipeline verification completed.

**Implemented:**
- Remote OK parsing now accepts rows only when game evidence appears in the title, company, or tags.
- Description-only game keyword matches no longer keep rows, which targets known Remote OK contamination such as therapist, attorney, and CNC/manufacturing postings.
- The change stays in the parser-stage Remote OK filter; no canonical drop reason, output schema, strict sector gate, or Google Sheets company rewrite was added.

**Verification:**
- `python -m pytest -q tests/test_jobs_fetcher_providers.py` — passed.
- `python -m pytest -q tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py` — passed.

### 2026-05-18 — P0.4 Google Sheets provider title hydration implemented

**Status:** P0.4 implementation complete; fresh Google Sheets-only pipeline refresh completed.

**Implemented:**
- Added an injected, per-run Google Sheets provider title resolver in `src/jobs/canonicalize.py`.
- Hydration runs only for `google_sheets*` rows whose current title is an exact known category label, after P0/P0.1/P0.2 category drops and after P0.3 URL-slug repair.
- Supported provider feeds in this slice:
  - Greenhouse board JSON from `boards.greenhouse.io`, `job-boards.greenhouse.io`, and `job-boards.eu.greenhouse.io`.
  - Lever postings JSON from `jobs.lever.co` and `jobs.eu.lever.co`.
- Unsupported opaque providers such as Ashby, Jobvite, BambooHR, Feishu, Personio, Workable, and company-specific ID pages remain unchanged unless P0.3 can derive a title from the URL path.
- Provider feed failures or missing postings are non-fatal and leave the original row unchanged.
- Provider-derived non-openings such as general/open/speculative applications still drop through `non_job_static_page`.
- Added additive source-report detail stats: `title_hydration_candidates`, `title_hydration_feed_fetches`, `title_hydration_cache_hits`, `title_hydration_repaired`, `title_hydration_missed`, `title_hydration_errors`, and `title_hydration_ms`.
- Extended `scripts/audit_jobs_sanitizer.py` to report remaining provider-hydration target counts by provider domain and title without fetching provider APIs.

**Fresh validation after P0.4:**
- Command: `python -m src.jobs.pipeline --output-dir _out/latest/build/portable/ship/data --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Output rows: **7,827**
- Source-report drops:
  - `google_sheets_category_row`: **24,796**
  - `non_job_static_page`: **12**
  - `missing_job_link`: **11**
- Title hydration totals across Google Sheets sources:
  - `title_hydration_candidates`: **1,708**
  - `title_hydration_feed_fetches`: **331**
  - `title_hydration_cache_hits`: **1,377**
  - `title_hydration_repaired`: **695**
  - `title_hydration_missed`: **1,013**
  - `title_hydration_errors`: **18**
- Remaining Google Sheets category-style titles: **1,484**.
- Remaining URL-title repair candidates from the P0.3 parser: **0**.
- Remaining provider-hydration targets: **544**.
- Remaining exact suspicious titles:
  - `Account-management`: **8**
  - `Administartive`: **11**
  - Total: **19**
- Top remaining category-style titles: `Product-management` **331**, `Digital-marketing` **105**, `Technical-art` **87**, `Social-media` **71**, and `Game-production` **49**.

**Verification:**
- `python -m pytest tests/test_jobs_fetcher_google_sheets.py tests/test_jobs_dedup_google_sheets_guard.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **39 passed**
- `python -m pytest tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **92 passed**

**Policy kept unchanged:**
- No strict sector gating.
- No broad single-word-title heuristic.
- No Google Sheets company rewrite.
- No output schema changes; only existing `title` values may improve.

### 2026-05-18 — P0.3 Google Sheets title-repair follow-up implemented

**Status:** P0.3 implementation complete; fresh Google Sheets-only pipeline refresh completed.

**Implemented:**
- Added a Google Sheets-only title repair step in `src/jobs/canonicalize.py` after the conservative category-row drop and before sector/profession/company-type scoring.
- Repairs only exact known Google Sheets category labels and only from URL path slugs; it does not fetch pages.
- Supports high-confidence slug shapes:
  - SmartRecruiters numeric-id slugs such as `744000115488907-office-specialist`.
  - Workday title segments before trailing IDs such as `Product-Manager-Accounting-Software_R0053364`.
  - PlayStation/Gameloft-style title path segments before or after `/job/` or `/jobs/`.
  - Generic last meaningful slug segments when they are not opaque, not listings/search pages, and not still category labels.
- Allows single-word repairs only when an ATS-style ID prefix/suffix was stripped, such as SmartRecruiters `...-receptionist`; generic one-word path guesses remain unchanged.
- Drops URL-derived non-opening titles such as `speculative application` through the existing `non_job_static_page` reason.
- Extended `scripts/audit_jobs_sanitizer.py` to report remaining category-style title counts and remaining URL-slug repair candidates.

**Fresh validation after P0.3:**
- Command: `python -m src.jobs.pipeline --output-dir _out/latest/build/portable/ship/data --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Output rows: **7,785**
- Source-report drops:
  - `google_sheets_category_row`: **24,796**
  - `non_job_static_page`: **12**
  - `missing_job_link`: **11**
- Remaining Google Sheets category-style titles: **1,715** (down from the prior P0.2 audit's **3,268**).
- Remaining URL-title repair candidates from the current conservative parser: **0**.
- Remaining exact suspicious titles:
  - `Account-management`: **8**
  - `Administartive`: **12**
  - Total: **20**
- Top remaining category-style titles: `Product-management` **421**, `Digital-marketing` **119**, `Technical-art` **97**, `Social-media` **76**, and `Game-production` **63**.
- Remaining examples are mostly opaque/detail-ID URLs or providers whose path does not expose a safe title, such as Kaizen Gaming `job-details/{id}`, Greenhouse ID-only links, Lever UUID links, BambooHR numeric links, and Feishu/Personio detail IDs.

**Verification:**
- `python -m pytest tests/test_jobs_fetcher_google_sheets.py tests/test_jobs_dedup_google_sheets_guard.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **35 passed**
- `python -m pytest tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **92 passed**

**Policy kept unchanged:**
- No strict sector gating.
- No broad single-word-title heuristic.
- No Google Sheets company rewrite.
- No output schema changes; only existing `title` values may improve.

### 2026-05-18 — P0.2 validation and residual link-evidence pass implemented

**Status:** P0.2 implementation complete; fresh Google Sheets-only pipeline refresh completed.

**Fresh validation before P0.2:**
- Command: `python -m src.jobs.pipeline --output-dir _out/latest/build/portable/ship/data --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --quiet`
- Output rows after P0.1: **7,819**
- Source-report drops:
  - `google_sheets_category_row`: **24,754**
  - `non_job_static_page`: **11**
  - `missing_job_link`: **11**
- Remaining exact suspicious titles after P0.1:
  - `Account-management`: **21**
  - `Administartive`: **24**
  - Total: **45**

**Residual contamination found:**
- `Gamecrio Studios Pvt Ltd` via `shine.com/.../zecruiters-jobconnect-private-limited/...`
- `iBLOXX Studios DMCC` via `bebee.com/...securiguard...`
- `Triodoxic Digital Studios` via `bebee.com/...adecco...`
- `NetApp` via `jobs.smartrecruiters.com/EthosInteractive/...`; this survived because `interactive` was too broad as parsed-link-employer game evidence.

**Implemented:**
- Added `scripts/audit_jobs_sanitizer.py` as a read-only audit command for fresh unified CSV/report checks.
- Added P0.2 link-employer evidence in `src/jobs/canonicalize.py`:
  - Parse `shine.com/jobs/.../{employer}/{id}` employer segments.
  - Parse only high-confidence `bebee.com` URL slugs that name known non-game employers observed in the fresh artifact (`adecco`, `securiguard`).
  - Use stricter game-evidence terms for parsed link employers, excluding broad tokens such as `interactive`, `studio`, and `studios` so unrelated ATS employers do not evade mismatch drops.

**Audit command:**
```bash
python scripts/audit_jobs_sanitizer.py --input-csv _out/latest/build/portable/ship/data/jobs-unified.csv --report-json _out/latest/build/portable/ship/data/jobs-fetch-report.json
```

**Fresh validation after P0.2:**
- Output rows: **7,785**
- Source-report drops:
  - `google_sheets_category_row`: **24,788**
  - `non_job_static_page`: **11**
  - `missing_job_link`: **11**
- Remaining exact suspicious titles:
  - `Account-management`: **19**
  - `Administartive`: **21**
  - Total: **40**
- Remaining samples are primarily preserved game-company corporate/admin/account openings such as PlayStation/Sony, People Can Fly, Ubisoft, CDPR, Scopely, Push Gaming, Embark, Gameloft, and kaizen gaming. Ambiguous rows such as `StudioB` and generic LinkedIn should stay review-only until stronger evidence is available.

### 2026-05-18 — P0.1 Google Sheets category-mismatch follow-up implemented

**Status:** P0.1 implementation complete; needs fresh pipeline/build verification to confirm final output impact.

**Latest build evidence checked:**
- Artifact: `_out/latest/build/portable/ship/data/jobs-unified.csv`
- Current unified rows: **11,215**
- Exact suspicious rows still present before P0.1:
  - `Account-management`: **1,314**
  - `Administartive`: **57**
  - Total: **1,371**, all from `google_sheets`
- Of those rows, **68** were `sector: Game` and **85** were `companyType: Game`, usually because the sheet company looked game-adjacent while the linked job was for a different employer.

**Implemented:**
- Expanded the exact Google Sheets category-label set in `src/jobs/canonicalize.py` with labels observed in `_out/latest`, including `Account-management`, `Quality-assurance`, `Full-stack-development`, `Backend-development`, `Product-design`, `Ui-ux-design`, `Game-engine`, `Audio-production`, `Public-relation`, `Graphic-design`, `Campaign-management`, `Physics-engine`, `Environment-art`, `Motion-design`, `Concept-art`, `Character-art`, `Game-ai`, `Sound-design`, `Graphics-engineer`, `Prop-art`, `Level-art`, `Quest-design`, `Combat-design`, and `Network-admin`.
- Added conservative Google Sheets link-employer mismatch evidence:
  - Parse employer-like path segments from `jobs.smartrecruiters.com/{employer}/...`.
  - Parse employer-like path segments from `himalayas.app/companies/{employer}/jobs/...`.
  - If parsed link employer differs from the sheet company and the parsed employer has no game evidence, drop the category-label row with `google_sheets_category_row`.
- Preserved real openings at game companies, including corporate/admin/account roles, when the link employer matches or the link/company carries game evidence.

**Examples covered by P0.1:**
- Drop: `Administartive at Mighty Games` -> `jobs.smartrecruiters.com/KPN/...senior-administrateur-b2c`
- Drop: `Administartive at Gardens Interactive` -> `himalayas.app/companies/onramp-lab/...virtual-assistant`
- Drop: `Account-management at Gardens Interactive` -> `himalayas.app/companies/canary-technologies/...enterprise-strategic-account-executive`
- Preserve: PlayStation/Sony executive assistant, Push Gaming account manager, same-employer game company account-management rows.

**Policy kept unchanged:**
- No broad single-word-title heuristic.
- No strict sector gating.
- No Google Sheets company rewrite in P0.1.

### 2026-05-18 — P0 conservative sanitizer implemented

**Status:** P0 implementation complete; awaiting a fresh full pipeline run with row-bearing `data/jobs-unified.*` artifacts to quantify live impact.

**Implemented:**
- Layer 1 source-specific static noise rules in `src/jobs/page_gating.py`:
  - Dorado, Hitica, and Baobab external aggregator contamination.
  - Generic non-opening titles such as general applications, open applications, spontaneous applications, initiative applications, student applications, Xsolla School applications, and talent pools.
- Layer 2 conservative Google Sheets category-row sanitizer in `src/jobs/canonicalize.py`:
  - Applies only to `google_sheets*` sources.
  - Drops exact category-label titles with explicit non-game employer/link evidence.
  - Drops non-game-adjacent category labels when there is no plausible game evidence.
  - Preserves ambiguous game-adjacent corrupted rows such as `Product-management`, `Vfx`, and similar labels unless explicit non-game evidence is present.
  - Uses drop reason `google_sheets_category_row`.
- Source-report loss diagnostics now preserve sanitizer drop reasons in `src/jobs/pipeline_source_results.py` and `src/jobs/common/contracts_source_reports.py`:
  - `non_job_static_page`
  - `google_sheets_category_row`
- `docs/DATA_CONTRACT.md` documents the additive source-report drop-reason visibility.

**Policy decisions locked for this slice:**
- Keep real openings at real game companies in scope, including corporate/admin roles.
- Do not add strict sector gating in P0.
- Do not harden or remove `remote_ok` in P0.
- Do not rewrite Google Sheets `company` attribution in P0.
- Avoid the broad single-word-title heuristic in P0 because it risks dropping corrupted but real game rows.

**Verification:**
- `python -m pytest tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_google_sheets.py tests/test_jobs_dedup_google_sheets_guard.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **78 passed**
- `python -m pytest tests/test_jobs_fetcher_pipeline.py -q --color=no --basetemp=.tmp/pytest/basetemp` — **42 passed**
- `git diff --check` — **passed**
- `npm run test:py` — attempted; failed only because `tests/test_pipeline_storage_gzip.py::test_runtime_launcher_serves_large_gzip_backed_pipeline_snapshot` expects local `data/jobs-unified.json` to contain more than 1000 rows, but this workspace currently has no `data/jobs-unified.*` artifacts. The rest of the lane reached `2910 passed, 1 failed`.

**Still open:**
- Run a fresh full pipeline and compare `loss.canonicalDropReasons.google_sheets_category_row`, `loss.canonicalDropReasons.non_job_static_page`, final output counts, and sample kept/dropped rows.
- Decide whether to add a known non-game employer/domain list beyond the conservative P0 evidence terms.
- Decide whether to add opt-in strict game-only output gating.
- Decide whether/how to repair Google Sheets employer attribution.

---

## 0.X. Closeout — Completed, Remaining, and Deferred

### Completed (all shipped to `main` by 2026-05-22)

| Scope | What | Key evidence |
|---|---|---|
| P0 (Layer 1+2) | Static noise rules (Dorado, Hitica, Baobab, talent-pool) + conservative Google Sheets category-label detector | `page_gating.py` noise functions; `canonicalize.py` category-label drop |
| P0.1 | Expanded category labels + ATS link-employer mismatch (SmartRecruiters, Himalayas) | Caught `Account-management`/`Administartive` rows where sheet company ≠ link employer |
| P0.2 | Audit command (`scripts/audit_jobs_sanitizer.py`) + residual link-employer evidence (shine.com, bebee.com, broad-`interactive`) | Cached `Gamecrio`, `iBLOXX`, `Triodoxic` false negatives |
| P0.3 | Google Sheets URL-slug title repair (SmartRecruiters, Workday, PlayStation/Gameloft, generic) | Dropped category-title count from 3,268 → 1,715 |
| P0.4 | Provider title hydration (Greenhouse, Lever boards) | Repaired 695 rows; missed 1,013 (unsupported providers) |
| P1 (Remote OK) | Remote OK description-only filter → keep rows only with title/company/tag game evidence | Zero non-job rows remain after refresh |
| P1.1 | Remote OK generic community/open-pool title guard (`Join Our Community`, etc.) | Cleaned last remaining live non-job row |
| P1.2 | Workable widget-feed title hydration (`apply.workable.com`) | 56 new hydration targets |
| P1.3 | Ashby hosted-board title hydration (`jobs.ashbyhq.com`) | 95 new hydration targets |
| P1.4 | Generalized URL-title repair hardening (no opaque UUIDs/IDs, no account slugs) | 28 opaque-ID rows repaired; Comeet terminal-code rows fixed |
| P1.5 | Category-link stale-link validation (HEAD→GET, `404`/`410` drops, bounded concurrency) + expanded category-label predicate (residual hyphenated labels: `Influencer-marketing`, `3d-art`, etc.) | 3,209 links checked, 1,051 stale drops; remaining category-title count → 0 |
| P1.6 | Google Sheets bootstrap timeout fix (moved category-link validation after repair/hydration, added progress callbacks, 4s timeout, 32 concurrency) | Bootstrap went from 257s category-link phase to completing under timeout |

### Remaining (P2 — evidence-driven refinement)

| Scope | What | Effort | Rationale |
|---|---|---|---|
| **P2.0** | Expand known non-game employer/domain evidence | ~1.5h | Extends existing `_GOOGLE_SHEETS_NON_GAME_EVIDENCE_TERMS` frozenset (60+ terms) with ~25 new entries across 7 priority tiers. See Layer 7 below. |
| **P2.1** | Sector-gate filter (`BALUFFO_STRICT_GAME_ONLY`) | ~2h | Broad gate; depends on P2.0 for accuracy. See §5.3 for concrete spec. |
| **P2.2** | Category P (Unknown Company dedup bug): real game jobs whose company + title are corrupted by google_sheets dedup | ~3h | ~50+ rows; Scopely, CDPR, ArenaNet jobs mislabeled as "Unknown company" with category titles. Not contamination — data quality issue in dedup merging. |

### Deferred (P3 — policy- and UX-driven, needs product decision)

| Scope | What | Effort | Status |
|---|---|---|---|
| **P3.0** | Fix google_sheets `company` field | ~4h | Deferred: approach choice (URL extraction vs. blanking vs. upstream column) needs product decision before implementation |
| **P3.1** | Corporate/hospitality role policy: filter, flag, or leave | ~30m | Deferred: P0 policy is "include them." UX toggle/filtering is a product decision. |

### Confidence Assessment

| Remaining Item | Confidence | Key Risk |
|---|---|---|
| P2.0 (employer evidence) | **97%** | ~25 new frozenset entries extending existing 60+ terms; 7 categories well-documented; risk is over-matching false positives on ambiguous company names mitigated by conservative category-label guard |
| P2.1 (sector gate) | **98%** | Mechanically simple; bridge env var passthrough confirmed automatic via `os.environ.copy()` — no bridge code changes; depends on P2.0 accuracy; Category D mislabeling risk quantified and contained |
| P2.2 (dedup bug) | **92%** | Root cause validated: `_blocks_google_sheets_generic_role_url_merge()` only guards GS↔GS, not GS↔Provider; fix is narrow (extend guard condition); Google Sheets-specific dedup functions confirmed at lines 246-782; test file exists; remaining risk is ensuring no output-schema regression |
| P3.0 (company field) | **70%** | Schema change risk (company is a dedup key); best approach (URL extraction) may only work for ~60% of rows |
| P3.1 (role policy) | **95%** | Decision-only: 30min discussion, no code |

### Definitively Out of Scope

- Broad single-word-title heuristic (rejected: too aggressive, risks false positives on corrupted real game rows)
- Full-platform provider title hydration (Jobvite, BambooHR, Personio, Feishu) — evidence-driven only
- Upstream Google Sheets coordination (Open Question 2) — separate non-technical effort

### Known Limitations (not blocking; would need separate evidence)

- **Category K — AjnaLens Field Engineers (~20 rows):** Category-label titles ("Software-development-&-engineering") at an AR/VR hardware company whose company name passes game detection. The actual roles are field installation/maintenance, not game development. The existing category-label detector preserves these because AjnaLens is game-adjacent. Fixing this would require role-level content analysis, which is out of scope for P2.
- **Category G — Uncovered social media/marketing employers (~15 rows):** Employers like BetterMe, Autodesk, Stibo Systems, Devoteam, Coda, Polygon Labs are not in the frozenset and have moderate false-positive risk (Autodesk has game middleware; Polygon Labs is blockchain, not game). These are intentionally left as evidence-driven follow-ups.
- **Category U — Legal/compliance at non-game employers (~15 rows):** PayPal, Motorola Solutions, FICO, Trupanion, Wolters Kluwer, Pluralsight, etc. are partially covered by P2.0 financial services tier but legal departments at these companies are a distinct subcategory. Low volume; deferred to evidence-driven follow-up.
- **Category S — Non-job pages scraped as jobs (~5 rows):** "Charity & Philanthropy" at Uplift Games, "Living Green & Philanthropy" at LeapFrog, Absurd Ventures project page. These are static-source scraping errors, not Google Sheets contamination. They hit the existing `non_job_static_page` drop path only if the title matches known non-job patterns. Generic corporate responsibility pages with unusual titles may still pass through. Fixing them requires per-source scraping heuristics, out of scope for P2.

---

## 1. Expanded Inventory of Unrelated / Non-Game Jobs

### Category A — Egregious Non-Game (from game-registered static sources)

Jobs with zero game development relation, scraped from game studio career pages that aggregate external listings via LinkedIn, djinni.co, etc.

| Title | Actual Employer | Source (via) | Why it's wrong |
|---|---|---|---|
| Lieutenant | State of Oklahoma | `doradogames.com/careers` | Government/military |
| Medical Scribe | Mercor | `doradogames.com/careers` | Healthcare admin |
| Administrative Assistant / Executive Assistant | Volunteer Success (nonprofit) | `doradogames.com/careers` (LinkedIn) | General admin |
| Administrative Assistant / Data Entry Clerk (Remote) | RemoteHunter | `doradogames.com/careers` (LinkedIn) | General admin |
| Administrative Assistant - Remote | Sundayy | `doradogames.com/careers` (LinkedIn) | General admin |
| Farming Team Lead | MediaAlta | `hitica.games` (djinni.co) | Agriculture |
| Email Deliverability Specialist | Code&Care | `hitica.games` (djinni.co) | Email marketing |
| VR for Hospital Systems | SimX VR | `simxvr.com/careers` | Healthcare product page (not a job) |
| "Spontaneous applications" | Keen Software House | `keenswh.com/careers/` | Not a real job listing |
| "General Application - Customer Support" (x3 title dup) | Ares Interactive | `aresinteractive.com/careers/` | Not a real job listing |
| Pre Licensed Child & Adolescent Therapist | InStride Health | `remote_ok` | Healthcare therapy |
| Contract Mandarin Document Review Attorney | Contact Government Services | `remote_ok` | Legal/government |
| Operations and Support Associate | Emora Health | `remote_ok` | Healthcare ops |
| CNC Machinist Milling | CX2 | `remote_ok` | Manufacturing/trades |

### Category B — Structural Pollution (Community Google Sheets)

**Volume:** ~25,000 rows — **73% of all rows**. By far the largest contamination source.

#### B1 — Title is a category label, not a job title
The `title` field is a generic department/category name. These are **not real job titles**:

```
Sales, Marketing, HR, Human-resource, Legal, Accounting, Customer-service,
Teaching, Logistics, Operations, Finance, Facility-management, System-design,
Data-analysis, Product-management, Business-development, It-&-infrastructure,
Talent-acquisition, Research-development, Social-media, Auditing, Editorial,
Education, Devops, Digital-marketing, Frontend-development, Taxation,
Financial-analysis, Project-management, Program-management, Network-engineering,
Administartive, Localization, Cyber-security, Business-analysis, Quality-analysis,
Risk-management, Business-analysis, Testing, Product, Audio, Videography, Ui-art,
Digital-marketing, Curriculum-design, Game-economy, Game-programmer
```

#### B2 — "Company" field is the sheet context, NOT the actual employer
When `company` says "Mighty Games", "Jellyfish", "Gardens Interactive", "Tantalus", "Wicked Workshop", "TMI Group", "Keywords Australia", "Mythic Talent", "NetApp", "Plug power", etc., the `jobLink` points to a **completely different** non-game organization:

```
company:"Mighty Games"  + jobLink:jobs.smartrecruiters.com/KPMGNederland/...  → KPMG (accounting)
company:"Mighty Games"  + jobLink:jobs.smartrecruiters.com/AccorHotel/...     → AccorHotel (hospitality)
company:"Mighty Games"  + jobLink:jobs.smartrecruiters.com/KPN/...           → KPN (telecom)
company:"Jellyfish"     + jobLink:jobs.smartrecruiters.com/Veolia/...        → Veolia (water treatment)
company:"Jellyfish"     + jobLink:jobs.smartrecruiters.com/AFRY/...          → AFRY (engineering)
company:"Jellyfish"     + jobLink:jobs.smartrecruiters.com/MedHealth3/...    → MedHealth (healthcare)
company:"Jellyfish"     + jobLink:jobs.smartrecruiters.com/AceTate/...       → Ace & Tate (optometry retail)
company:"Gardens Interactive"  + himalayas.app/companies/northrop-grumman/   → Northrop Grumman (defense)
company:"Gardens Interactive"  + himalayas.app/companies/the-cigna-group/    → Cigna (health insurance)
company:"Gardens Interactive"  + himalayas.app/companies/thriving-center-of-psychology/ → Psychology clinic
company:"Gardens Interactive"  + himalayas.app/companies/tutor-me-education/ → Tutoring platform
company:"Tantalus"     + jobs.smartrecruiters.com/AccorHotel/...             → AccorHotel (hospitality)
company:"Tantalus"     + jobs.smartrecruiters.com/ServiceNow/...             → ServiceNow (enterprise software)
company:"Tantalus"     + jobs.smartrecruiters.com/ApplusIDIADA1/...          → Vehicle test driver
company:"Tantalus"     + jobs.smartrecruiters.com/JYSK/...                   → JYSK (retail)
company:"Wicked Workshop" + jobs.smartrecruiters.com/ShijiGroup/...          → Shiji (hospitality tech)
company:"Wicked Workshop" + jobs.smartrecruiters.com/AECOM2/...              → AECOM (construction)
company:"Wicked Workshop" + jobs.smartrecruiters.com/JYSK/...                → JYSK (retail associate)
company:"Keywords Australia" + jobs.smartrecruiters.com/InternationalSOSGovernmentMedicalServices/
company:"Keywords Australia" + jobs.smartrecruiters.com/AbercrombieAndFitchCo/ → Abercrombie & Fitch
company:"TMI Group"    + jobs.smartrecruiters.com/BDO4/...                   → BDO (accounting)
company:"TMI Group"    + jobs.smartrecruiters.com/Boskalis/...               → Boskalis (marine engineering)
company:"TMI Group"    + jobs.smartrecruiters.com/Redcare-Pharmacy/...        → Pharmacy
company:"TMI Group"    + jobs.smartrecruiters.com/Eurofins/...               → Eurofins (lab testing)
company:"TMI Group"    + jobs.smartrecruiters.com/WynnResorts/...            → Wynn Las Vegas (casino/hotel)
company:"TMI Group"    + jobs.smartrecruiters.com/DeltaElectronics/...        → Delta Electronics
company:"TMI Group"    + jobs.smartrecruiters.com/ScalableGmbH/...            → Investment manager
company:"Mythic Talent" + allstate.wd5...                                    → Allstate (insurance)
company:"Mythic Talent" + careers.internationalsos.com/...                   → Travel medical assistance
```

**The `company` field in Google Sheets rows is the sheet owner/maintainer, not the hiring organization.** This is a fundamental data quality issue.

#### B3 — Real non-game employers behind Google Sheets rows
The actual employers for these rows include entirely non-game industries:

| Industry | Example Employers |
|---|---|
| **Hospitality** | AccorHotel, JYSK, Abercrombie & Fitch, Rituals, Ace & Tate |
| **Healthcare** | Philips, Illumina, Carda Health, MedHealth, InternationalSOS, Lucid Hearing |
| **Accounting/Tax** | PwC, BDO, KPMG |
| **Defense/Aerospace** | Northrop Grumman, Boskalis |
| **Telecom/IT** | KPN, Motorola, ServiceNow, GoDaddy, Autodesk, Unisys |
| **Insurance/Finance** | Allstate, Cigna, Guardian, Scalable, Redcare Pharmacy |
| **Construction/Engineering** | AECOM, Turner & Townsend, AFRY, Kanadevia Inova, Plug Power, Boskalis |
| **Energy/Automotive** | Plug Power, Trek Bikes, Applus IDIADA |
| **Government** | State of Oklahoma, International SOS (government medical) |
| **Retail** | Spavia (massage), JYSK, Abercrombie & Fitch, Wynn Resorts |

### Category C — Corporate / Non-Development Roles at Game Companies

Roles at game companies that are not game-development-specific:

| Title | At | Source | Notes |
|---|---|---|---|
| Global Payroll Manager | Nex Playground | greenhouse | Finance function at game company |
| Retail Account & Operations Manager | Nex Playground | greenhouse | Retail operations |
| Admin & HR Generalist (Korean/English) | Pearl Abyss Europe | workable | HR function |
| VIP Host (×2) | Penn Interactive | greenhouse | Hospitality/gambling-adjacent |
| Senior Events Specialist | Penn Interactive | greenhouse | Events planning |
| Business Analyst, Supply Chain (Contract) | PlayStation Global | greenhouse | Supply chain |
| Senior Product Manager, Ad Formats | Discord | greenhouse | Ads/product |
| Product Manager | PlayStation Global | greenhouse | Product management |
| Data Product Manager, Curation | Rush Street Interactive | greenhouse | Data product |
| Club Host | Xsolla | lever_sources | Hospitality |
| Office Manager | Galaxy Grove | google_sheets_1er2oaxo | Office admin |
| Customer Support Agent (×2) | Amber | gracklehq | Customer support |
| HR Manager (×5) | Gameloft, Ubisoft, etc. | greenhouse/gracklehq | HR function |
| Accountant (×5) | CDPR, Unity, HoYoverse | greenhouse | Finance |
| Legal Counsel | Various game studios | workable | Legal |
| EMEA Payroll Analyst | Sony PlayStation | greenhouse | Payroll |
| Administrative Assistant / Executive Assistant | Naughty Dog | static | Admin |
| Executive Assistant (Contract) | Sony Interactive Entertainment | google_sheets | Admin |
| Senior Programmer (Combat, Physics, AI) | Finitude GmbH | google_sheets_1mvqhxat | This IS game-dev, listed for context |

These are **at game companies** but are **not game-development roles**. Volume: ~200-300 rows. Policy decision required.

### Category D — Google Sheets rows mislabeled as "Game" sector

Some google_sheets rows have `sector: Game` but contain clearly non-game content. The company name happens to match game detection heuristics:

| Title Label | "Company" in sheet | Actual Employer (from jobLink) | Real Industry |
|---|---|---|---|
| Software-development-&-engineering | Gamecrio Studios | Brickwell Engineering | Construction |
| Software-development-&-engineering | Mighty Games | Visa | Financial services |
| Software-development-&-engineering | Mighty Games | KPN | Telecom |
| Software-development-&-engineering | Unilever | Unilever Palmira factory | Consumer goods |
| Software-development-&-engineering | NetApp | Kanadevia Inova | Energy/construction |
| Software-development-&-engineering | Adtran | Adtran HQ (Assembler II) | Telecom hardware |
| Software-development-&-engineering | TMI Group | Wynn Resorts (Engineer Plumber) | Hospitality/casino |
| Software-development-&-engineering | Stord | WMS Warehouse Implement. | Warehousing |
| Software-development-&-engineering | Western Digital | Factory Automation | Manufacturing |
| Software-development-&-engineering | Salesforce | Supply Chain Engineer | Enterprise software |
| Software-development-&-engineering | Wind River | Various engineering roles | Enterprise software |
| Research-development | Gardens Interactive | Consultant Radiologist (Mercor) | Healthcare |
| Teaching | Mighty Games | Spa Therapist / Yoga Instructor (AccorHotel) | Hospitality |
| Human-resource | Mighty Games | Casemanager (Veolia) | Water treatment |
| Auditing | Mighty Games | Auditor ISO 9001 (SGS) | Certification |
| Education | Mighty Games | Behaviour Support Practitioner (MedHealth) | Healthcare |
| Product-management | Mighty Games | Product Manager at KPN | Telecom |
| Financial-analysis | Mighty Games | Financieel Compensatie (REXEL) | Electrical distribution |
| Network-engineering | Illumina | Director, Medical Affairs | Healthcare |

### Category E — Hospitality/Food/Retail from Google Sheets

Entirely non-game service roles:
- Logistics labeled as "Breakfast attendant" at AccorHotel (hotel)
- Logistics labeled as "Logistics employee" at JYSK (retail)
- Operations labeled as "Optician" at Ace & Tate (retail optician)
- Operations labeled as "Hollister Co. Assistant Manager" at Abercrombie & Fitch
- Sales labeled as "Sales Associate" at JYSK
- Customer-service labeled as "Store employee" at Rituals (cosmetics retail)
- Customer-service labeled as "Autohero Handover Specialist" (car sales)
- Education labeled as "Behaviour Support Practitioner" at MedHealth
- Audio labeled as "Licensed Hearing Instrument Specialist / Audiologist" at Lucid Hearing
- Testing labeled as "Vehicle Test Driver" at Applus IDIADA

### Category G — Social Media / Marketing / Content at Non-Game Companies

Many Google Sheets rows labeled "Social-media" or "Digital-marketing" at fake "companies" like "Gardens Interactive", "Jellyfish", etc. point to real non-game marketing roles:

| Title Label | "Company" (sheet context) | Actual Employer (from jobLink) | Real Industry |
|---|---|---|---|
| Social-media | Condé Nast | GQ Germany (Condé Nast) | Fashion/publishing |
| Social-media | BetterMe | BetterMe | Health/wellness app |
| Social-media | Autodesk | Autodesk | CAD/design software |
| Social-media | Stibo Systems | Stibo Systems | Enterprise data |
| Social-media | Dexerto | Dexerto | Esports media (game-adjacent?) |
| Social-media | Devoteam | Devoteam | IT consulting |
| Social-media | Gardens Interactive | Various via Himalayas | Mix of non-game companies |
| Digital-marketing | Coda | Coda | Productivity software |
| Social-media | Unknown company | Scopely via LinkedIn | Game company (but category label) |
| Videography | Polygon Labs | Polygon Labs | Blockchain/crypto |
| Audio | TMI Group | Lucid Hearing | Audiology/hearing aids |
| Video-editing | playrix | Playrix (game company) | Game company — actual game-dev? |
| Social Media Manager (Remote) ×5 | baobab studios | Mind Friend (via LinkedIn) | Mental health app — NOT a game company |

The baobab studios case is particularly egregious: a VR game studio's About page scrapes LinkedIn jobs and emits 5 identical "Social Media Manager at Mind Friend" listings (different US states). Mind Friend is a mental health app, not a game company.

### Category H — Actual Classroom Teachers / School Jobs

The "Teaching" label in Google Sheets hides real school teacher positions:

| Title Label | "Company" | Actual Employer | Role |
|---|---|---|---|
| Teaching | iBLOXX Studios DMCC | Wayman Learning Trust (UK schools) | PE Teacher, Science Teacher, Geography Teacher, Reception Teacher |
| Teaching | iBLOXX Studios DMCC | Wayman Learning Trust | Teacher |
| Teaching | TMI Group | KIPP (charter schools) | Teacher Aide |
| Teaching | Tantalus | University of Auckland | ECE Teacher (early childhood) |
| Teaching | Mighty Games | Aspect2 | Teacher's Aide |
| Teaching | TSA | Calvary Education | Secondary Classroom Teacher |
| Teaching | TVH | TVH (parts/logistics) | Lead Teacher |
| Teaching | CAE | CAE (flight sim training) | Instructor Pilot, Military Training Expert |
| Teaching | Jellyfish | Domino's Pizza | Assistant Manager in Training, General Manager in Training |
| Teaching | TMI Group | Domino's Pizza | General Manager in Training |
| Teaching | TMI Group | Aspen Skiing Company | Training Systems Specialist (ski rental retail) |
| Teaching | Keywords Australia | Senior Lifestyle | Property Manager in Training |
| Teaching | Mighty Games | Bosch Home Comfort | Regional Training Manager (appliances) |
| Teaching | NetApp | Domino's Pizza | Manager in Training |
| Teaching | GE Digital | GE Vernova | Training Manager |
| Teaching | Snappr | Snappr | Senior Training Specialist (photography platform) |

The Domino's Pizza Manager-in-Training entries are a particularly absurd contamination — showing up under "Teaching" at "Jellyfish", "TMI Group", "NetApp", etc.

### Category I — "Not a Real Job" Entries (Talent Pools, Speculative, Placeholder)

| Title | Source | Why it's not a real job |
|---|---|---|
| "Spontaneous applications" | Keen Software House | Generic talent pool page |
| "General Application - Customer Support Customer Support Customer Support" | Ares Interactive | Truncated/duplicated listing |
| "Tech Artist \| Talent Pool" | Arvore | Talent pool, not specific role |
| "General Interest - Backend Engineer" | Rushdown Studios | General interest pool |
| "General Interest - Other" | Rushdown Studios | General interest pool |
| "Initiativbewerbung - Playa Games" | Coldwood Interactive | German "unsolicited application" |
| "We are seeking passionate, talented developers..." | Unknown Worlds (Krafton) | Job description text used as title |
| "Specialist Education" | ENGAGE Studio | VR education product page, not a job |
| "Talend Development" | Leti Arts | Generic careers page |
| "VR for Hospital Systems" | SimX VR | Product page, not a job |
| "Training & Coaching" | Reaktor | Training service page, not a job |

### Category J — Game Company Roles Mislabeled with Category Titles

Some game company jobs from provider adapters (greenhouse, gracklehq, etc.) have their real job titles replaced with category labels in the google_sheets dedup:
- "Social-media at Unknown company" → actual LinkedIn job: "Social Media & Community Manager, STFC at Scopely"
- "Product-management at Unknown company" → actual: "Sr Product Manager, Performance Star Trek Fleet Command at Scopely"
- "Game-economy at Unknown company" → actual: "Senior Product Manager, Economy Monopoly GO! at Scopely"

These are real game jobs that got their title replaced by the google_sheets category label during dedup.

### Category K — Field Engineer Roles Mislabeled as Game (AjnaLens)

AjnaLens (AR/VR hardware company) has 20+ "Field Engineer" entries across Indian cities. These are labeled `sector: Game` because the company name matches game detection, but the actual roles (field installation/maintenance of AR hardware) are not game development:

```
Software-development-&-engineering at AjnaLens → Field Engineer in Thoothukudi, Rajkot,
Madurai, Bareilly, Kolhapur, Nagpur, Kakinada, Eluru, Bhavnagar, Udaipur,
Tiruchirappalli, Vellore, Visakhapatnam, etc.
```

### Category L — HR / Talent / Learning & Development at Game Companies (Policy Decision)

These are corporate HR/L&D roles at real game companies. Whether they should be included is a policy question:

| Title | Company | Source |
|---|---|---|
| Talent Management & Leadership Development Partner | Riot Games | gracklehq |
| Senior Manager, Learning & Development | Zynga | greenhouse |
| Global Talent & Learning (L&D) Operations Specialist | Gameloft | smartrecruiters |
| Sr. AR Learning & Development Program Lead | Pokémon Company International | greenhouse |
| Senior Social Media Manager | Unity | gracklehq |
| Social Media Manager | ZeptoLab | workable |
| Community Manager (MMORPG) | Soulbound | workable |
| Associate Social Media Engagement Manager | Nex Playground | greenhouse |
| Product Marketing Manager - Temporary | Rush Street Interactive | greenhouse |
| Analytics Manager, Commercialization | Atari | greenhouse |
| Senior Product Manager - Games | UserWise Services | workable |

Most of these are legitimate game-adjacent roles (marketing, community management, product for games). The HR/L&D roles are corporate functions within game companies.

### Category M — Major Non-Game Employers in Google Sheets (McDonald's, Walmart, DoorDash)

Well-known non-game corporations appear as the actual employer in google_sheets rows, sometimes mislabeled as "Game" sector.

**McDonald's Corporation** (fast food, zero game connection):
- Testing, Software-development-&-engineering (×4), Project-management (×3), Marketing (×2), Product-management, Logistics, Legal (×2), It-&-infrastructure, Human-resource — all at Mcdonald's Mexico City/London offices. **Some labeled Game sector.**

**Walmart Global Tech India** (retail, zero game connection):
- Logistics: Stock Unloader, Freight Flow Associate, Merchandise & Stocking, Produce Associate, Fresh Food Associate, Backroom Associate, Senior Meat Cutter
- Sales: Area Manager Floor
- Localization: Relief Pharmacist

**DoorDash** (food delivery, remote_ok source):
- Recruiter GTM Sales at DoorDash USA — token HR role at non-game company

**Netflix** (streaming, game-adjacent only via Netflix Games Studio):
- Taxation at Netflix (Amsterdam)
- Program-management at Netflix (Amsterdam)
- Production Manager at Netflix (Amsterdam)
Technical Artist at Netflix Games Studio IS a legitimate game job, but Taxation at Netflix corporate isn't.

**Apple** (consumer electronics, Cupertino):
- Ui-ux-design at Apple: Human Factors Researcher (Cupertino) — general UX role at the main Apple campus, zero game connection

**Consulting/Professional Services contamination** — Many rows point to consulting firms:
- ServiceNow (solution consultants, risk consulting)
- Devoteam, Sopra Steria, Inetum (IT consulting across Europe)
- KPMG, PwC, BDO, SGS (accounting/auditing)
- Ramboll (engineering/water treatment consulting)
- Broadcom, Trellix, Simcorp (enterprise software consulting)

These are all labeled with generic category titles ("Programming", "System-design", "Research-development") making them indistinguishable from legitimate tech jobs in game companies without checking the jobLink host.

### Category N — Financial Services / Banking / Asset Management / Insurance

A major new contamination category. Google Sheets rows link to jobs at:

**Asset Management & Banking:**
- BlackRock (world's largest asset manager): Portfolio Manager, Associate Portfolio Manager, Core Portfolio Manager Emerging Markets (New York, Philadelphia, Princeton)
- Saxo Bank: Client Vigilance Manager (Copenhagen)
- London Stock Exchange Group: Senior Manager Financial Crime Advisory (London)
- Morningstar: Analyst Credit Ratings US RMBS (New York)
- MUFG Investor Services: Director Compliance AML (Dublin)
- Scalable GmbH: Junior Investment Manager Wealth (Munich)
- Flywire: Credit Risk Manager

**Insurance:**
- Guardian Life: FML Leave Manager, Short Term Disability Absence Claims Case Manager (US)
- TAL (Australia): Manager Claims Governance, Manager Claims Quality Assurance (Sydney)

**Credit Bureaus & Payments:**
- TransUnion: Client Enablement Manager Credit Risk (Chicago)
- Visa: Credit Risk Senior Manager (Ashburn)

**Accounting / Tax / Advisory:**
- PwC: Tax Compliance, Real Estate Advisory Manager, Manager Unsecured Credit Captives (Rotterdam, Malta, Gurugram)
- Public Storage: Property Tax Manager (Plano)
- Vertex Inc: SAP ABAP Tax Technology Consultant (US, GB, Ireland)
- Globalization Partners: Taxation roles (Romania, Colombia, Poland)

**Financial Software:**
- SimCorp: Principal Project Manager Banking Investment Management (Hong Kong, Singapore)
- Clearwater Analytics: Senior Enterprise Sales Leader Hedge Funds Team (New York)

**Retail & Publishing (non-game):**
- Nike: Senior Assortment Planner EMEA (Hilversum)
- Axel Springer: Strategic Investment Portfolio Manager (Berlin)
- Sphere Entertainment: Senior Manager Business Operations Ad Sales Sponsorships (Burbank)
- BetterMe: International Tax Manager (Ukraine)
- Public Storage: Property Tax Manager

### Category O — Energy / Oil & Gas / Renewables / Nuclear

A significant new contamination category via google_sheets:

**Enverus** (oil & gas SaaS, formerly DrillingInfo):
- 10+ rows: Technical-art, Sales (x3), Risk-management, Financial-analysis, Data-analysis, Business-development, Teaching (Madrid), System-admin (Calgary), Software-development-&-engineering (Pittsburgh, Brno)
- All labeled with category titles; actual employer is an oil & gas industry software company

**Energy Jobline** (energy industry job board aggregator):
- Testing (Plzen: test engineer), Technical-art (Houston: technical manager), System-design (Italy: umbilical fiber optic cable engineer; Grangemouth: electrical engineer), Software-development-&-engineering (Italy: corrosion engineer; Houston: centrifugal compressors engineer)
- Some rows mislabeled as "Game" sector via wrong sheet context

**Enphase Energy** (solar microinverters):
- Software-development-&-engineering (x5: Bengaluru, Christchurch, Austin) — enterprise software at non-game renewable energy company

**Other Energy:**
- GE Vernova: Senior Engagement Manager Hyperscaler Energy Solutions, Venture Capital Intern
- Silfab Solar (via Mighty Games sheet): Research Engineer I
- Quest Global: Lead Simulation Engineer Nuclear
- Veracity / DNV: Graduate Electrical Engineers Renewables, Software Engineering Lead

### Category P — "Unknown Company" — Category Labels Replacing Real Game Job Titles

~50+ rows show "Unknown company" as employer. These are real game jobs whose company name was stripped by google_sheets dedup, and real titles replaced with category labels:
- "Vfx at Unknown company" → ArenaNet, Insomniac, CD Projekt Red
- "Product-management at Unknown company" → Scopely (multiple locations)
- "Rendering at Unknown company" → CD Projekt Red, Scopely
- "Gameplay at Unknown company" → CD Projekt Red
- "Level-design at Unknown company" → People Can Fly
- "Live-ops at Unknown company" → Scopely
- "Game-production at Unknown company" → Scopely

These are NOT contamination — they are real game jobs with corrupted metadata. The google_sheets dedup process replaces real titles with category labels while also losing company context. This is a secondary data quality issue.

### Category T — Talent Pools, Open Applications, and General Interest Listings

Many game studios provide "open application" or "talent pool" pages for speculative applications. These are not real job openings:

- Liquid Development: "Material Artist - Talent Pool (2026 Opportunities)", "3D Environment Artist - Talent Pool (2026 Opportunities)", "VFX Artist - Talent Pool (2026 Opportunities)" — workable_sources
- Larian Studios: "Technical Artist - Open Application", "Environment Artist - Open Application" — lever_sources
- Ares Interactive: 7 "General Application - [Department]" listings with tripled department names (e.g. "General Application - Production Production Production") — static via rippling.com
- Blowfish Studios: "Open Application Animator / Technical Artist" — static
- Arvore: "Tech Artist | Talent Pool" — static
- Rushdown Studios: "General Interest - Backend Engineer", "General Interest - Other" — greenhouse
- Keen Software House: "Spontaneous applications" — static
- Tangelo Games: "Spontaneous Application" — static
- Aether Studios: "General Application" with full email instructions in title — static
- Tiny Roar: "Initiative Application" — static
- ZeptoLab: "Open application" — workable (dated 2023!)
- Game Boost Sweden: "open application game artists" — teamtailor
- Paradox Interactive: "Open application for Game Programmers" — teamtailor
- Xsolla: "Xsolla School - Student Application" — lever (educational program, not a job)
- Kokku: "game developer mid-sr backend talent pool" — google_sheets

These are **not false positives** — they serve a purpose for recruiting pipelines — but they are not active job openings and inflate the total count. Many are years old.

### Category U — Legal / Compliance / Regulatory at Non-Game Companies

Extensive legal roles at non-game companies via google_sheets:
- Tencent (game-adjacent): Senior Legal Counsel Employment (Amsterdam)
- PayPal: Sr Manager Compliance, Sr Analyst Compliance Investigations, Sr Legal Counsel (London)
- Motorola Solutions: Licensing & Regulatory Compliance Manager, International Trade Counsel
- Salesforce: Director Industry Advisor Justice & Public Safety
- DraftKings: Associate Regulation, Corporate Counsel Gaming Regulations
- Wolters Kluwer: Legal Solutions Architect, Senior Account Manager Legal & Regulatory
- Illumina: Director State Government Affairs
- Rocket: Contracts Manager (Vilnius)
- Stord: Corporate Counsel
- FICO: Technology Transactions Attorney
- Trupanion (pet insurance): Senior Corporate Paralegal (via Wicked Workshop)
- BetterMe (health app): Legal Counsel (Kyiv)
- Pluralsight: People Compliance Manager

### Category Q — Defense / Aerospace / Rail / Aviation Engineering

**Lockheed Martin** (defense/aerospace):
- System-design: ASIC & FPGA Design Engineer (x2), Senior Systems Engineer, Systems Engineer (Orlando, Stratford, Moorestown)
- Software-development-&-engineering: Software Engineer Integration and Test (Herndon) — **labeled Game sector** (false positive)

**Thales** (defense electronics):
- Software-development-&-engineering (Templecombe, GB), Project-management (Templecombe), Manufacturing: Assembly Technician (Hengelo, NL)

**Rail/Transportation engineering** (via AECOM2 on smartrecruiters):
- Senior Architect Rail-Metro (Dubai), Transit Rail Transportation Planner (Dallas), Rail Civil Design Engineer (Glasgow)
- Senior Software Developer Railway Ticketing System (Segula Technologies, UAE)

### Category R — Logistics / Trucking / Warehousing / Drivers

A massive category of entirely non-game manual/logistics jobs, all via google_sheets:

- Class 1 HGV drivers (CulinaGroup, DPDGroup): multiple locations across GB
- CDL drivers (DeAngelo Contracting Services): Las Vegas, Henderson, Boulder City
- Forklift operators (Ariens Company, Pentair, CulinaGroup)
- Material Handlers (Ariens Company)
- Valet drivers (The Rank Group, London)
- Driver at Westgate Resorts (Cocoa Beach)
- Warehouse workers (Trek Bikes, broadcom)
- Supply Chain roles (Western Digital, broadcom, Ariens, Trackman)

### Category S — Non-Job Pages Scraped as Jobs

Additional non-job pages found being scraped as jobs:
- "Charity & Philanthropy" at Uplift Games — a corporate responsibility page, not a job
- "Living Green & Philanthropy" at LeapFrog Enterprises — an environmental page, not a job
- "Feed Store Books Comic Books Video Games Audio Fiction Animation STORYTELLING. PHILANTHROPY. ULTRAVIOLENCE." at Absurd Ventures — a game studio's project page scraped as a job
- "Chaplain part-time" at Northwestern Medicine (via Tantalus sheet context) — pastoral care at a hospital, zero game connection

### Category F — Healthcare/Medical from Google Sheets

- Clinical Exercise Physiologist at Carda Health
- Massage Therapist at Spavia (multiple locations)
- Emergency Veterinarian at Greencross Pet Wellnness
- Associate Dentist at Portman Dentex
- Dental Hygienist at International SOS
- Clinical Informatics Director (MD/DO)
- Consultant Radiologist (MSK Neurology) via Mercor
- Clinical Application Specialist at Philips
- Usability Engineer, Medical at Philips
- Clinical Chemist Technical Director at Labcorp
- Medical Support Assistant at International SOS
- Senior Clinical Informatics Sales Specialist at Illumina
- Behaviour Support Practitioner at MedHealth

---

## 2. Contamination Sources Quantified

| Source | Total Rows | Game Sector | Tech Sector | Non-Game % | Main Contamination |
|---|---|---|---|---|---|---|
| `google_sheets` (main) | ~28,061 | ~4,246 | ~23,815 | **85%** | Category labels + wrong company field |
| `google_sheets_1er2oaxo` | ~2,049 | ~1,063 | ~986 | 48% | Mix of real & category rows |
| `google_sheets_1mvqhxat` | ~284 | ~160 | ~124 | 44% | Some category rows |
| `remote_ok` | 46 | 8 | 38 | **83%** | Broad keyword matching (includes Recruiter GTM Sales at DoorDash) |
| `doradogames.com/careers` | ~15 | ~5 | ~10 | **67%** | LinkedIn aggregation (State of Oklahoma, Mercor, Volunteer Success, RemoteHunter, Sundayy) |
| `hitica.games` | ~5 | ~2 | ~3 | **60%** | djinni.co aggregation (MediaAlta, Code&Care) |
| `baobabstudios.com/about` | ~5 | 0 | 5 | **100%** | LinkedIn aggregation for "Mind Friend" (mental health app) |
| Other static sources | ~200 | ~150 | ~50 | ~25% | Occasional external links + non-job pages |
| Provider adapters (GH, Lever, etc.) | ~7,800 | ~5,650 | ~2,150 | 28% | Corporate roles + category-label title replacements at game co's |
| **Total** | **38,271** | **11,135** | **27,136** | **71%** | |

---

## 3. Root Cause: No Content-Based Filter Exists

The pipeline has **no content-based game vs. non-game filter at any stage**:

```
SOURCE DISCOVERY ──→ SOURCE REGISTRY ──→ JOBS FETCHING ──→ CANONICALIZATION ──→ DEDUP ──→ OUTPUT
    │                      │                    │                    │                │          │
    │ Technical            │ Evidence/probe     │ Parses all         │ Labels sector  │ Merges   │ Writes
    │ crawlability         │ thresholds,        │ jobs found on      │ (Game/Tech)    │ across   │ everything
    │ checks only          │ no content gate    │ registered pages   │ NEVER used     │ sources  │ to CSV
    ▼                      ▼                    ▼                    ▼  as filter     ▼          ▼
   NO game filter         NO game filter       NO game filter       NO game filter  NO filter  NO filter
```

### Specific entry points:

1. **Static scrapers following external links** — `doradogames.com/careers`, `hitica.games`, `baobabstudios.com/about`, `coldwood.com` embed/aggregate jobs from LinkedIn, djinni.co, teamtailor. The static scraper follows all job-looking links on these pages regardless of whether the actual employer is a game company.
   - `baobabstudios.com/about` scrapes LinkedIn and emits "Social Media Manager at Mind Friend" — Mind Friend is a mental health app, not a game company. 5 identical listings for different US states.

2. **Community Google Sheets category rows** — Every spreadsheet row becomes a job, including structural taxonomy rows. The `company` field is the sheet context, not the employer. ~25K of ~30K rows are not real game-dev jobs.

3. **General job boards (`remote_ok`)** — API queries for game keywords return general remote jobs that happen to match but have no game connection (CNC Machinist, Attorney, Therapist).

4. **Google Sheets → "Game" sector mislabeling** — Rows get `sector: Game` when the (wrong) company name matches game detection heuristics, masking them from simple sector-based filtering.

---

## 4. Existing Filtering Mechanisms

| Filter | File | What it catches | Relevant to non-game? |
|---|---|---|---|
| Static noise filter (11 rules) | `page_gating.py` (lines 295-416) | itch.io, Stardock, Immutable, WBD, GS Studio, Flix, Stillfront, Dorado, Hitica, Baobab, talent-pool/generic applications | **Directly extensible** |
| Google Sheets category-label detector | `canonicalize.py` (line 933+) | Drops exact category-label titles with non-game evidence from `google_sheets*` sources | **P0 core** |
| Google Sheets URL-slug title repair | `canonicalize.py` (line 803+) | Repairs category-label titles from URL path slugs (SmartRecruiters, Workday, PlayStation, generic) | **P0.3 additive** |
| Provider title hydration | `canonicalize.py` (line 1055+) | Greenhouse/Lever/Workable/Ashby feed-based title resolution for `google_sheets*` rows | **P0.4-P1.3 additive** |
| Category-link stale validation | `canonicalize.py` (line 1316+) | HEAD→GET bounded live link status; drops `404`/`410` category-link rows | **P1.5 additive** |
| Sector labeling | `normalizers.py` | Labels every job as `Game` or `Tech` | **Label only — never used to filter** |
| Game detection | `game_detection.py` | `has_positive_game_evidence()` multi-factor check | Used only for labeling |
| Quality score | `heuristics.py` | % of required fields filled | Structural, not content |
| Focus score | `heuristics.py` | Weights toward technical-artist roles | Not a game filter |
| Source auto-approval | `source_registry_auto_approval.py` | Technical health of sources | No content check |
| Discovery suppression | `source_discovery/core_thresholds.py` | Low-evidence static sources blocked | Technical, not content |
| Remote OK game-evidence guard | Remote OK parser | Keeps rows only with title/company/tag game evidence | **P1-P1.1** |
| Non-game employer evidence | `canonicalize.py` (line 211+) | 60+ frozenset employer substrings checked against company+link text | **P0.2-P0.1; extended by P2.0** |
| **Sector-gate filter** | **Layer 8** (P2.1) | Drops `sector != "Game"` when `BALUFFO_STRICT_GAME_ONLY=1` | **Not yet implemented** |

---

## 5. Proposed Sanitization Strategy

### Layer 1 — Source-Specific Noise Rules (Quick Fix, ~1 hr)

**Implementation status:** Complete as of 2026-05-18.

**File:** `src/jobs/page_gating.py`

Add noise functions for known problematic static sources:

- **`_dorado_noise()`** — Match `doradogames.com/careers` source + external job-link domains (linkedin.com, mercor). Also block non-game title patterns (Lieutenant, Medical Scribe, Administrative, Data Entry).
- **`_hitica_noise()`** — Match `hitica.games` source + external domains (djinni.co). Block non-game titles (Farming, Email Deliverability).
- **`_baobab_noise()`** — Match `baobabstudios.com/about` source + linkedin.com external job-link host. The about page scrapes LinkedIn for "Mind Friend" (non-game company) social media roles.
- **`_talent_pool_noise()`** — Match generic non-job patterns like "Spontaneous applications", "General Application", "Talent Pool", "Initiativbewerbung", "General Interest -" in titles — these are not real job postings.

Register all in `looks_like_source_specific_static_noise_row()` → drops via existing `"non_job_static_page"` path at `canonicalize.py:529-534`.

### Layer 2 — Google Sheets Category-Label Detector (P0, ~2-4 hrs)

**Implementation status:** Complete as a conservative P0 detector as of 2026-05-18; expanded with P0.1 link-employer mismatch evidence from the latest `_out/latest` build.

Add a detector in `canonicalize.py` that checks whether a title from a `google_sheets*` source looks like a category label rather than a real job title.

**Heuristics:**
- Title is a single kebab-case token matching a known category set (exhaustive list from Section B1: `Sales`, `HR`, `Marketing`, `Teaching`, `Logistics`, `Facility-management`, etc.)
- P0 implementation uses exact category-label matching plus evidence terms; it does **not** use the broad single-word-title heuristic.
- P0 implementation preserves ambiguous game-adjacent corrupted category rows unless explicit non-game employer/link evidence exists.
- P0 implementation does not rewrite or infer actual employer attribution beyond existing parser behavior.
- P0.1 adds exact category labels observed in the latest build and treats known ATS link-employer mismatches as stronger evidence than the sheet company when the parsed link employer has no game evidence.

**Drop reason:** `"google_sheets_category_row"`

**Expected impact:** Removes a large share of the Google Sheets structural category rows while reducing false-negative risk for corrupted real game rows. P0.1 specifically targets the 1,371 latest-build `Account-management`/`Administartive` rows and related category labels; exact live impact still needs validation from a fresh full pipeline run.

### Layer 3 — Sector-Based Output Gate (Superseded by Layer 8, ~1-2 hrs)

**Implementation status:** Superseded by Layer 8's concrete P2.1 specification (see below). The original Layer 3 proposal below is retained for historical reference; all implementation decisions now use the Layer 8 spec.

Original proposal (superseded):

Add a filter gated behind config flag `BALUFFO_STRICT_GAME_ONLY` that drops all `sector != "Game"` jobs at the finalize stage.

- Removes ~27K Tech-labeled jobs
- Retains ~11K Game-labeled jobs
- Risk: Category D rows (mislabeled as "Game") would pass through — Layer 2 catches most of these
- Risk: Legitimate game jobs mislabeled as "Tech" would be dropped

### Layer 4 — Google Sheets Adapter: Override `company` field (Medium, ~4 hrs)

**Implementation status:** Not started; intentionally deferred from P0.

The google_sheets adapter currently reads the sheet's context company name as the job's `company`. Instead:
- Parse the actual employer from the `jobLink` URL (extract domain → company name via reverse domain lookup)
- Or: Allow the sheet to specify `actual_company` as a separate column
- Or: Leave `company` blank/untrustworthy for google_sheets rows, flag for manual review

### Layer 5 — Remote OK Keyword Hardening (Low Effort, ~1 hr)

**Implementation status:** P1 and P1.1 complete as of 2026-05-22.

`remote_ok` fetches from a general API with game keyword filters but returns non-game results.
The P1 fix keeps rows only when game evidence appears in the title, company, or tags; description-only keyword matches are filtered before canonicalization.
The follow-up audit found one remaining live row, `Join Our Community` at Tripadvisor, so P1.1 rejects generic community/open-pool titles before returning to Google Sheets hydration work.

### Layer 6 — Non-Development Role Policy

**Implementation status:** P0 policy decision made: include real openings at real game companies. Optional flagging/filtering remains a later product decision.

Decide whether corporate roles at game companies (HR, accounting, legal, admin at Ubisoft, CDPR, PlayStation, etc.) should be:
- **Excluded** — Add corporate-function keywords to `game_detection.py`
- **Included but flagged** — Add `corporateRole: true` metadata field
- **Included as-is** — Legitimate jobs within game studios

### Layer 7 — Non-Game Employer/Domain Evidence Expansion (P2.0, ~1.5h)

**Concrete scope.** Extend the existing `_GOOGLE_SHEETS_NON_GAME_EVIDENCE_TERMS` frozenset in `src/jobs/canonicalize.py` (lines 211-272, currently 60+ terms) with additional high-confidence non-game employer substrings. The existing guard `_has_google_sheets_non_game_evidence()` (line 1543) already checks company text and jobLink text against these terms for `google_sheets*` rows with category-label titles — P2.0 only adds terms missing from the current set.

**Employers ALREADY covered by existing frozenset:** `mcdonalds` ("mcdonald"), `walmart`, `doordash`, `transunion`, `visa`, `pwc`, `enverus`, `energyjobline` ("energy jobline"), `enphase`, `lockheed` ("lockheed"), `thalesgroup` ("thales"), `aecom`, `culinagroup` ("culina"), `cardahealth` ("carda health"), `greencrossvet` ("greencross"), `labcorp`, `philips`, `medhealth`, `waymanlearningtrust` ("wayman learning trust"), `kipp`, `motorola`, `northrop grumman`, `illumina`, `paypal`, `salesforce`, `servicenow`.

**Employers NOT YET covered (P2.0 additions, ~25 new terms):**

| Priority | Category | New terms to add | Rows impacted |
|---|---|---|---|
| 1 | M — Major non-game corps | `netflix` (non-studio filtering via existing company-guard), `apple` (non-Arcade, existing title-guard) | ~5 |
| 2 | N — Financial services | `blackrock`, `saxobank`, `london stock exchange`, `morningstar`, `mufg`, `guardian life`, `trupanion`, `simcorp`, `clearwater`, `vertex`, `globalization partners` | ~25 |
| 3 | O — Energy (uncovered subset) | `ge vernova`, `silfab solar`, `quest global`, `veracity`, `dnv` | ~8 |
| 4 | Q — Defense (uncovered subset) | `segula technologies` (railway) | ~2 |
| 5 | R — Logistics (uncovered subset) | `dpd`, `deangelo`, `ariens`, `pentair`, `the rank group`, `westgate resorts`, `trek bikes` (warehouse context) | ~5 |
| 6 | F — Healthcare (uncovered subset) | `spavia`, `portman dentex`, `dental hygienist` | ~5 |
| 7 | H — Classroom Teachers (uncovered subset) | `university of auckland`, `aspect2`, `calvary education`, `cae` (flight sim training) | ~4 |

**Total newly added terms:** ~25. **Total additional rows impacted:** ~50-60 (down from original ~150 estimate — most high-volume categories already covered).

**Implementation approach:**
- Extend the existing `_GOOGLE_SHEETS_NON_GAME_EVIDENCE_TERMS` frozenset in `src/jobs/canonicalize.py` (NOT create a new data structure — the existing `_has_google_sheets_non_game_evidence()` function at line 1543 already uses this frozenset correctly).
- The matching approach is text-based substring matching against company name and jobLink text (existing behavior), NOT host-based URL parsing. This is intentionally simpler and catches cases where the employer appears anywhere in the link text, not just in the host.
- Terms are added as lowercase space-separated names in the frozenset (align with existing code conventions, e.g. `"carda health"`, `"lucid hearing"`).
- No output schema, no frontend rendering, no bridge/route changes, no new drop reason.

**Verification:**
- `python -m pytest tests/test_jobs_fetcher_google_sheets_sanitizer.py` — extend with test rows for each priority tier (minimum: 1 row per tier).
- `python -m pytest tests/test_jobs_fetcher_pipeline.py` — existing pipeline tests must pass unchanged.
- `python scripts/audit_jobs_sanitizer.py --input-csv data/jobs-unified.csv --report-json data/jobs-fetch-report.json --limit 50` — verify newly targeted employers appear in drop samples.
- Targeted refresh: `python -m src.jobs.pipeline --output-dir _out/job-sanitization/p2.0-employer-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --force-refresh-all --ignore-circuit-breaker --quiet` — verify output counts stable vs. P1.6 baseline (~6,167 rows), dropped rows attributable to new employer terms.

### Layer 8 — Sector-Gate Output Filter (P2.1, ~2h)

**Concrete specification.**

*Config mechanism:* Environment variable `BALUFFO_STRICT_GAME_ONLY=1`. Not a CLI flag (so it survives multiple pipeline invocations). Not a bridge route (this is an output pipeline concern, not a UI concern). Read once at pipeline startup in `src/jobs/common/config.py`.

*Bridge integration:* The bridge's `_run_child_process()` (line 445) uses `child_env = os.environ.copy()` which preserves the ENTIRE parent environment, then adds bridge-specific overrides on top. `BALUFFO_STRICT_GAME_ONLY=1` set in the bridge's parent process (or startup env) automatically flows through to the pipeline child. **No bridge code changes required.** If the env var is not set, the bridge passes nothing and the pipeline sees it unset → default behavior preserved.

*Insertion point:* `src/jobs/pipeline_finalize.py`, after dedup and before CSV/JSON write. This is after `canonicalize.py` sector labeling and `dedup.py` merge, so the gate sees the final row set with all metadata intact.

*Behavior:*
- When `BALUFFO_STRICT_GAME_ONLY=1`: keep only rows with `sector == "Game"`. All others are dropped with a new drop reason `sector_gate_filtered` recorded in the source-report loss diagnostics.
- When unset or `0`: no change to current behavior.

*UI exposure:* Add a status line in the pipeline report JSON (`source-report`) showing `sectorGateFiltered: N` when the gate is active. No frontend changes required — the frontend's `feed.js` already handles variable row counts and the `sector` field is a first-class frontend filter dimension (see `frontend/jobs/app/runtime/filters-ui.js`). The filtered output simply has fewer rows and the report file carries the evidence.

*Data contract:* `docs/DATA_CONTRACT.md` line 769 documents stable drop reasons including `google_sheets_category_row` and `non_job_static_page`. After P2.1 implementation, add `sector_gate_filtered` to this list as an additive drop reason visible in the report JSON (not in `jobs-unified.csv`/`.json`/`.light.json`).

*Interaction with P2.0:* The sector gate's accuracy depends on P2.0 fixing Category D mislabeling (Google Sheets rows mislabeled `sector: Game`). P2.0 should be implemented before or alongside P2.1. The gate is safe even without P2.0 — it just passes through some non-game rows — because P2.0's employer evidence catches the worst mislabeling.

*Testing:*
- `tests/test_jobs_pipeline_sector_gate.py` (new): unit test the gate function in isolation with mock rows.
- `tests/test_jobs_fetcher_pipeline.py` (extend): integration test with `BALUFFO_STRICT_GAME_ONLY=1`, verify dropped counts and report diagnostics.
- `npm run test:frontend:unit` — confirm no regression; the frontend doesn't need to know about the gate.

*Verification command:* `BALUFFO_STRICT_GAME_ONLY=1 python -m src.jobs.pipeline --output-dir _out/job-sanitization/p2.1-sector-gate-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --no-seed-existing-output --force-refresh-all --ignore-circuit-breaker --quiet`

### Layer 9 — Dedup "Unknown Company" Bug Fix (P2.2, ~3h)

**Problem.** Category P in the inventory (line 768-779): ~50+ rows show `company: "Unknown company"` with category-label titles instead of real job titles. These are REAL game jobs at Scopely, CDPR, ArenaNet, People Can Fly, Insomniac, etc. whose company name and title were corrupted by the google_sheets dedup merge.

**Root cause (validated against source):** `_blocks_google_sheets_generic_role_url_merge()` at `dedup.py:768-779` only guards collisions between TWO Google Sheets rows (line 774: both `current` and `target` must be `_is_google_sheets_row`). When a Google Sheets category-label row collides with a PROVIDER row (Greenhouse, Lever, etc.), the guard returns `False` and the merge proceeds — potentially replacing the provider row's real `title` and `company` with the Google Sheets category label and sheet-context company. Category P rows arise from exactly this cross-source merge scenario.

**Existing code context.**
- `_is_google_sheets_row()` (line 246): identifies Google Sheets rows by source prefix.
- `_has_sheet_role_bucket_title()` (line 268): expanded role-bucket detection covering category labels.
- `_blocks_google_sheets_generic_role_url_merge()` (line 768): blocks merges between two Google Sheets rows with different primary URLs. **Does NOT handle cross-source (Google Sheets↔Provider) merges.**
- `_record_google_sheets_generic_role_guard_sample()` (line 782): records guard impact samples for audit.

**Implementation approach:**
1. Audit the dedup merge path in `src/jobs/dedup.py` to confirm the root cause — specifically how Google Sheets rows interact with provider rows at merge time.
2. Fix the merge preference: when a Google Sheets row has a recognized category-label title (use the same shared category-title predicate from P0/P1.5 in `canonicalize.py`) AND a provider row has a real job title, prefer the provider row's `title` and `company` over the Google Sheets values.
3. The fix applies only when the Google Sheets title is a known category label AND the provider row has a non-empty, non-category-label title.
4. No output schema change. No new drop reason — these rows were never dropped; their metadata was simply wrong.

**Testing:**
- `tests/test_jobs_dedup_google_sheets_guard.py` (extend): add test cases where a category-label Google Sheets row collides with a provider row having a real title; verify the merged row keeps the provider's `title` and `company`.
- `python -m pytest tests/test_jobs_fetcher_google_sheets.py tests/test_jobs_fetcher_pipeline.py` — existing tests must pass unchanged (no regression in output schema or dedup behavior).

**Verification command:** `python -m src.jobs.pipeline --output-dir _out/job-sanitization/p2.2-dedup-audit --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat,greenhouse_sources,lever_sources,workable_sources,gracklehq_sources,smartrecruiters_sources --no-seed-existing-output --force-refresh-all --ignore-circuit-breaker --quiet` — then grep output for `"Unknown company"` to verify count → 0.

**Confidence caveat:** This item touches dedup merge logic, which is central to output stability. The fix surface is narrow (category-label guard in merge preference) but the exact code path needs audit before coding. After the audit, confidence rises to ~90%. The Google Sheets-specific dedup functions already exist in `dedup.py` (lines 246-782), so the integration surface is well-understood.

---

## 6. Priority Actions

### Completed

| Phase | Action | Effort | Key Impact |
|---|---|---|---|
| P0 (L1) | Noise rules: Dorado, Hitica, Baobab, talent-pool | ~1.5h | Eliminates egregious static-source false positives |
| P0 (L2) | Google Sheets conservative category-label detector | ~3h | Drops category-label rows with non-game evidence |
| P0.1 | Extended category labels + ATS link-employer mismatch | ~1.5h | Catches sheet-company/link-employer disagreements |
| P0.2 | Audit command + residual link-employer evidence | ~1h | Catches shine.com/bebee.com false negatives |
| P0.3 | Google Sheets URL-slug title repair | ~1.5h | Category-title count: 3,268 → 1,715 |
| P0.4 | Provider title hydration (Greenhouse, Lever) | ~2h | 695 repaired; 1,013 missed (unsupported providers) |
| P1.0 | Remote OK description-only filter | ~1h | Zero non-job rows after refresh |
| P1.1 | Remote OK generic community-title guard | ~0.5h | Cleans last `Join Our Community` row |
| P1.2 | Workable provider title hydration | ~1h | 56 hydration targets |
| P1.3 | Ashby provider title hydration | ~1h | 95 hydration targets |
| P1.4 | URL-title repair hardening (opaque IDs) | ~1h | 28 Comeet/opaque-ID rows repaired |
| P1.5 | Category-link stale validation + residual labels | ~2h | 1,051 stale drops; remaining category titles → 0 |
| P1.6 | Bootstrap timeout fix | ~1.5h | Bootstrap completes under timeout |

### Remaining — Implementation (P2)

| Phase | Action | Effort | Spec |
|---|---|---|---|
| **P2.0** | Expand known non-game employer/domain evidence | ~3h | See Layer 7 below |
| **P2.1** | Sector-gate filter (`BALUFFO_STRICT_GAME_ONLY`) | ~2h | See Layer 8 below |
| **P2.2** | Fix Category P "Unknown Company" dedup bug | ~3h | See Layer 9 below |

### Deferred — Policy/UX Decision Required (P3)

| Phase | Action | Effort | Status |
|---|---|---|---|
| **P3.0** | Fix google_sheets `company` field | ~4h | Needs product decision: URL extraction vs. blanking vs. upstream column |
| **P3.1** | Corporate/hospitality role policy toggle | ~0.5h | P0 policy: include. Filter/flagging decision needed. |



---

## 7. Key Files

| File | Role |
|---|---|
| `src/jobs/page_gating.py` | Existing noise-filter rules (Layer 1, static source noise) |
| `src/jobs/canonicalize.py` | Canonicalization pipeline, drop-reason gates (Layers 2, 5, 7) |
| `src/jobs/game_detection.py` | Game keyword definitions and detection (Layer 3) |
| `src/jobs/normalizers.py` | Sector labeling (Layer 3 gate insertion point) |
| `src/jobs/common/heuristics.py` | Quality/focus scoring |
| `src/jobs/pipeline_finalize.py` | Output finalization (Layer 8 gate insertion point) |
| `src/jobs/common/config.py` | Config constants, `TARGET_PROFESSIONS`, `BALUFFO_STRICT_GAME_ONLY` |
| `data/defaults/source-registry-active.seed.json` | Source registry entries for problematic static sources |
| `src/jobs/adapters/static_sources.py` | Static source loader builder |
| `src/jobs/adapters/static_detail_heuristics.py` | Detail link classification for static scrapers |
| `src/jobs/adapters/community/__init__.py` | Google Sheets adapter (Layer 4 company field fix) |
| `src/jobs/dedup.py` | Dedup merge logic (Layer 9 "Unknown Company" fix) |
| `tests/test_jobs_dedup_google_sheets_guard.py` | Dedup guard tests (Layer 9) |

---

## 8. Open Questions

1. **Resolved for P0:** Show all real openings at game companies, including corporate/admin roles. Later UX/filtering can revisit corporate-role flagging.
2. **Deferred:** Whether Google Sheets maintainers are aware their category rows emit as job listings is a separate, non-technical upstream coordination task. No code path exists for this; deferred indefinitely.
3. **Resolved:** The sector gate is opt-in via `BALUFFO_STRICT_GAME_ONLY=1` env var. Never on by default. See Layer 8 specification.
4. **Resolved:** For P2.0, parse the normalized host from `jobLink` and match against a hardcoded dict of known non-game employer domains. See Layer 7. For P3.0 (company rewrite), the approach is deferred until a product decision is made; URL extraction from the link host is the most likely approach (~60% coverage), with blanking as fallback.
5. **Resolved:** `remote_ok` is NOT excluded entirely. P1+P1.1 filters removed all non-game rows; the source remains active and produces valid game-job rows when present. If non-game contamination returns through API changes, the existing title/company/tag game-evidence guard catches it.
6. **Resolved for P0:** Use conservative evidence-backed filtering. Do not drop every category label until fresh pipeline evidence quantifies false negatives. P0-P1.6 followed this policy; remaining P2 work continues the same evidence-driven posture.
7. **New — Answered:** The Category F (Healthcare) section appears out of order in the inventory because it was discovered after the initial write-up. Plan text preserved for historical reference; the Layer 7 non-game employer spec includes it at priority tier 6.
