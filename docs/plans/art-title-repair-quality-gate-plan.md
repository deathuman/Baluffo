# Google Sheets Title Column, Redirect Company Repair, and Category Gate Plan

> - **Status:** Active follow-up plan, parser/redirect/company hardening implemented and validated on 2026-05-30; shipped-artifact gate tooling and fresh live-pipeline validation also completed on 2026-05-30, with remaining blockers now isolated to static-source category-page rows
> - **Use this when:** Fixing misleading Google Sheets category titles such as `Art`, Grackle redirect rows with `Unknown company`, or related exact source-category leaks.
> - **Canonical for:** The root cause that the default Google Sheets `gviz` CSV can make `Job Category` look like the title column, the redirect-cache loophole that can preserve unresolved Grackle URLs, and the product policy that likely-live jobs must not be dropped only because title or company extraction failed.
> - **Not canonical for:** Broad company-name inference without redirect/provider evidence, public job payload schema changes, or frontend display changes.
> - **Then inspect:** `docs/scraping-pipeline.md`, `docs/DATA_CONTRACT.md`, `docs/testing.md`, `src/jobs/adapters/community/google_sheets.py`, `src/jobs/canonicalize.py`, `src/jobs/transport.py`, `src/jobs/dedup.py`, `src/jobs/job_link_company.py`
> - **Last updated:** 2026-05-30

## Summary

The misleading `Art` rows are not a frontend fallback and not primarily a title-repair miss. The main root cause is earlier: the default Google Sheets source can be fetched through the `gviz` CSV endpoint, whose header row collapses instruction text and labels into one row. Baluffo's generic column matcher then accepts the first header containing `job`, which is `Job Category`, before it reaches the real `Title` column. The result is that source categories such as `Art`, `Design`, `Animation`, `Product-management`, `Manufacturing`, and `Monetization` are copied into `RawJob.title` and can survive to the shipped feed.

The screenshot row also exposes a related, generalized company/identity issue. Rows using Grackle redirect links can keep `Unknown company` when the sheet company is an untrustworthy label and the redirect target or sibling provider row contains better company evidence. Existing same-Grackle-URL dedup enrichment is not enough when no non-unknown final row with the same Grackle URL survives.

Drop-only cleanup is still rejected. Baluffo is a job-finding app, so a likely-live job must not disappear just because the parser selected the wrong column or failed to recover a title or company. The first fix is to parse the correct source title column. Redirect-backed company repair, downstream title repair, and final quality gates remain necessary as defense in depth.

## Current Evidence

- A local artifact inspection on 2026-05-30 found 194 exact `Art` rows in `data/jobs-unified.csv`.
- Google Sheets dominated the issue: 193 of those exact `Art` rows came from the default `google_sheets` source.
- A broader local artifact comparison found 3,013 default `google_sheets` final rows where the output title equaled the live sheet's `Job Category` cell while the adjacent real `Title` cell was different.
- High-count leaked category values included `Product-management`, `Manufacturing`, `Art`, `Monetization`, `Product`, `Digital-marketing`, `Events`, `Localization`, `Technical-art`, `Animation`, `Vfx`, `Game-production`, `Game-design`, and `Design`.
- In the live default sheet, row `sheet-32` has `Job Category = Art` and real `Title = Senior Design Director`; Baluffo shipped the row as `Art`.
- In the live default sheet, row `sheet-23778` has `Job Category = Design` and real `Title = Senior Systems Designer (12-month Contract)`; Baluffo shipped the row as `Design`.
- Parsing the same live default sheet through `gviz` produced many category titles: `Art: 210`, `Design: 329`, `Animation: 105`, `Product-management: 906`.
- Parsing the direct `export?format=csv` endpoint for the same sheet preserved the real header row and produced `Art: 0`, `Design: 0`, `Animation: 0`, `Product-management: 0`.
- Exact `Animator` is not the same class of bug in the current evidence. Exact `Animator` had 7 rows across mixed sources, and the German Games Industry sheet has at least one real `Job` cell whose title is exactly `Animator`. The broader category leak there is `Animation`, not `Animator`.
- A fresh live pipeline run to `_out/art-title-quality-gate-20260530-live` on 2026-05-30 produced 39,147 final rows. The shipped-artifact gate found zero Google Sheets exact category-title leaks, zero `Unknown company` rows with strong redirect/provider company evidence, 154 weak-evidence `Unknown company` Grackle survivors, and 215 exact category-title blockers that all came from static sources (`214 static_source::...`, `1 scrapy_static_sources`).
- One non-Google-Sheets exact `Art` example came from a static Neon Giant / Teamtailor-shaped row with listing URL `https://www.neongiant.se/en/careers` and application URL `https://jobs.neongiant.se/jobs/1518164-rockstar/applications/new`; the detail page exposes the specific title `Rockstar`.
- The local final artifact has 204 rows with `Unknown company` and Grackle redirect URLs; all 204 are from the default `google_sheets` source.
- Of those 204 Grackle/`Unknown company` rows, 140 also have category-like titles such as `Product-management`, `Game-design`, `Game-production`, `Vfx`, `Technical-art`, `Animation`, `Gameplay`, `Localization`, `Rendering`, `Social-media`, `Live-ops`, `Ui-art`, `Environment-art`, or `Art`.
- Existing Grackle same-URL dedup enrichment did not help these rows: zero of the 204 unknown Grackle rows had a non-unknown final row with the same Grackle URL.
- `data/jobs-source-state.json` currently contains 465 default Google Sheets redirect-cache entries where a Grackle redirect URL maps to itself, for example `https://gracklehq.com/rd/374557 -> https://gracklehq.com/rd/374557`. Those self-mappings can be seeded into future runs as cache hits and prevent fresh redirect resolution.

These counts are evidence from local artifacts and live-source inspection on 2026-05-30, not permanent expected counts. The live sheets and source contents can change.

## Root Cause

The default Google Sheets adapter currently exposes three interacting loopholes:

1. Candidate URL ordering prefers `gviz` before direct `export?format=csv`.
   - `gviz` can flatten multi-row sheet instructions into header cells such as `... Job Category` and `... Title`.
   - Direct export preserves the real header row for the default sheet: `Company Category`, `Company Name`, `Overall Category`, `Job Category`, `Title`, and so on.

2. The title-column matcher treats any header containing `job` as a title candidate.
   - `Job Category` matches before `Title`, so column 3 becomes `RawJob.title`.
   - The actual title column, column 4 in the default sheet, is ignored for the affected `gviz` shape.

3. Canonical title repair is downstream and cannot reliably recover every specific title after the wrong column is selected.
   - Existing Google Sheets category cleanup can drop some category rows, repair some URL-slug/provider cases, and preserve game-adjacent categories.
   - That logic was compensating for a parser-stage column bug. It should remain a guard, but it should not be the first line of defense.

4. Redirect failure can be cached as if it were a useful resolution.
   - `PooledRedirectResolver` can store `original_url -> original_url` when resolution fails or times out.
   - Source-state seeding then treats that self-mapping as a cache hit, so a future run can skip retrying a redirect that is now resolvable.
   - The observed default Google Sheets run had redirect cache hits but no resolved Grackle targets, matching the stale self-cache pattern.

5. Company repair runs before resolved redirect evidence is available.
   - `RawJob.company` is normalized before the Google Sheets redirect target is known.
   - If the sheet company is an untrustworthy label such as `enduring games`, the canonical row can become `Unknown company` even when a resolved provider URL, Grackle listing row, sibling provider row, or trusted detail page can identify the employer.
   - Existing `_enrich_unknown_company_from_gracklehq_redirect` only repairs from another final row with the same Grackle URL. It does not use resolved target URLs, provider posting ids, sibling rows with the same posting id, or direct page metadata.

## Product Policy

- A title miss or title-column mismatch is not a drop reason for a likely-live job.
- Exact source-category titles such as `Art`, `Design`, `Animation`, `Product-management`, and `Technical-art` are not acceptable final `title` values in shipped main-feed artifacts when a more specific source title exists.
- `Unknown company` is not acceptable for supported redirect rows when redirect target, provider URL, sibling provider row, Grackle listing evidence, or trusted page metadata can identify the company.
- Proven dead, removed, unavailable, or no-openings URLs can be dropped through existing dead/no-openings policy.
- A likely-live row that still has only a source-category title after correct source parsing and reasonable repair attempts must fail a quality gate instead of being shipped with a placeholder title.
- No `Title unavailable`, `Unknown title`, or broad category fallback should be introduced into the public main feed for this case.
- Real broad titles can exist. For example, exact `Animator` can be a legitimate source title and must not be banned solely because it is short.
- Do not guess company from weak evidence. Generic hosts such as LinkedIn, broad careers landing pages, or ambiguous marketing pages require structured provider/path evidence, Grackle listing evidence, sibling-row evidence, or trusted page metadata.

## Implementation Strategy

1. Fix Google Sheets CSV candidate selection.
   - Prefer the candidate URL whose parsed headers map to the most trustworthy schema for the sheet.
   - For the default sheet, direct `export?format=csv` should win over `gviz` when it preserves a clean `Title` column.
   - Do not globally remove `gviz`; other sheets may only work through `gviz`. Instead, validate parsed shape per candidate before accepting the first non-empty result.
   - Treat candidate fetch success and candidate parse quality separately. A non-empty parse is not necessarily a correct parse.

2. Harden Google Sheets header scoring.
   - Prefer exact `title`, `job title`, or sheet-specific `job` headers over `job category`.
   - Treat `job category`, `overall category`, `company category`, `category`, `function`, and similar fields as category/sector candidates, not title candidates.
   - Add a parser-level guard for adjacent columns where `Job Category` is followed by `Title`; the title column must select `Title`.
   - Keep support for sheets whose real title header is `Job`, such as the German Games Industry sheet.
   - Add a regression case for multi-row instruction headers flattened into one row by `gviz`.

3. Preserve likely-live jobs by repairing, not dropping.
   - Once the source parser selects the correct title column, most default-sheet category leaks should disappear without dropping rows.
   - Keep URL-slug repair, provider hydration, redirect resolution, and detail-page parsing for rows that still have category or broad placeholder titles.
   - Remove or narrow any early category drop that prevents repair of likely-live rows.
   - Drop only when the row is proven dead, removed, unavailable, no-openings, invalid, or structurally unusable.

4. Fix redirect cache semantics for supported redirect hosts.
   - Do not persist or seed `supported_redirect_url -> same_url` self-mappings as permanent successful cache entries.
   - Track self-resolved or failed redirects separately from successful redirect resolutions.
   - Retry stale self-mapped redirects on later runs, with bounded concurrency and existing timeout controls.
   - Preserve successful redirect cache entries where `resolved_url != original_url`.
   - Keep diagnostics additive. If new redirect-cache counters are added, update `docs/DATA_CONTRACT.md`.

5. Generalize redirect-backed company and identity repair.
   - After resolving a supported redirect, use the resolved target URL as company evidence when `RawJob.company` is missing, unknown, or an untrustworthy label.
   - Prefer structured provider/path company extraction from supported ATS targets such as Ashby, Greenhouse, Lever, SmartRecruiters, Workable, Workday-shaped hosts, Breezy, BambooHR, Jobvite, Personio, and similar existing parser-supported providers.
   - Use Grackle listing rows keyed by redirect URL when the Grackle source provides a non-unknown company for the same redirect link.
   - Use sibling rows that share a provider posting id, UUID, canonical target URL, or redirect target with a non-unknown company.
   - Use trusted target-page metadata only when it clearly identifies the company and does not conflict with stronger provider/sibling evidence.
   - Do not infer company from generic hosts or ambiguous landing pages without stronger evidence.
   - Extend or replace the existing same-Grackle-URL dedup enrichment so it is not limited to final rows that already share the same Grackle URL.
   - Prefer non-unknown company and specific title during dedup when redirect/provider identity proves rows are the same opening.

6. Keep static detail fallback in scope as a secondary fix.
   - If a static listing row contributes only a generic source category such as `Art`, do not let that listing title mask a specific fetched detail-page title.
   - Allow trusted detail pages to recover a specific title from title, heading, JSON-LD, or ATS page payload even when the title is unusual and does not contain conventional job-title tokens.
   - Preserve dead/no-openings detection for static pages before accepting a repaired title.

7. Add final title and company quality gates.
   - After source parsing, canonicalization, repair, and dedup, scan shipped main-feed artifacts for exact source-category titles.
   - The acceptance gate is zero unresolved exact source-category title leaks in shipped main-feed artifacts.
   - The gate must distinguish known legitimate exact role titles from known category/source labels. Exact `Animator` should not fail only because it is short; exact `Animation` should be treated as category-like unless proven to be a real title.
   - Add a companion gate for supported redirect rows that still show `Unknown company` despite resolved target, provider, Grackle-listing, sibling-row, or trusted metadata evidence.
   - If either gate fails, report actionable examples with source, source job id when present, company, location, job link, detected host, parsed source column evidence where available, resolved link when available, and company evidence classification.
   - If new diagnostic fields or report counters are added, update `docs/DATA_CONTRACT.md` and focused tests in the same implementation change.

8. Keep public contracts stable by default.
   - Do not change bridge routes, frontend payload shape, persisted saved-job schema, or public job feed fields unless a later implementation explicitly requires it.
   - Prefer internal parser diagnostics, source report stats, and artifact quality gates over user-facing schema changes.

## Verification

Focused tests should cover:

- Default Google Sheets `gviz`-flattened header shape selects the real `Title` column, not `Job Category`.
- Default Google Sheets direct export shape selects the real `Title` column.
- Candidate URL selection does not accept a non-empty but badly mapped `gviz` parse when a cleaner direct export parse is available.
- The German Games Industry sheet shape still treats the real `Job` column as title and does not break exact legitimate titles such as `Animator`.
- Rows with `Job Category = Art` and `Title = Senior Design Director` canonicalize to `Senior Design Director`.
- Rows with `Job Category = Design` and `Title = Senior Systems Designer (12-month Contract)` canonicalize to the specific title.
- Seeded Grackle redirect self-mappings do not prevent fresh redirect resolution.
- Successful redirect resolutions persist only when the resolved URL differs from the original supported redirect URL.
- Google Sheets rows with unknown or untrustworthy sheet company values repair company from resolved provider targets when the provider URL contains reliable company evidence.
- Google Sheets rows with Grackle redirect URLs repair company from a Grackle listing row, sibling provider row, shared posting id, or trusted page metadata when available.
- The `sheet-32` screenshot class resolves to `Senior Design Director` and a non-unknown Believer company when redirect/sibling evidence is available.
- Google Sheets exact category rows that still cannot be repaired are not silently shipped.
- Redirect links such as Grackle-style links resolve before title repair decisions where redirect resolution is available.
- Static Neon Giant / Teamtailor-shaped data with listing title `Art` and detail title `Rockstar` ships the specific title, not the category.
- Proven dead, removed, unavailable, or no-openings URLs still drop through the existing dead/no-openings policy.

Full verification should include:

- A fresh full pipeline run from live sources, without preserving stale output on empty source failures.
- Inspection of final `data/jobs-unified.*` artifacts and packaged `_out/latest` job data.
- Confirmation that default `google_sheets` no longer has rows where output `title` equals the live sheet `Job Category` while the live sheet `Title` differs.
- Confirmation that exact `Art`, `Design`, `Animation`, `Product-management`, and other source-category leak counts are zero or all explicitly explained as legitimate exact role titles.
- Confirmation that exact legitimate titles such as `Animator` are preserved when they are the real source title.
- Confirmation that persisted Google Sheets redirect cache does not contain Grackle self-mappings as successful cache entries.
- Confirmation that supported redirect rows with company evidence do not ship as `Unknown company`.
- Confirmation that the attached screenshot example ships as `Senior Design Director` with a non-unknown Believer company, or fails the quality gate with the exact missing evidence.

For this documentation-only rewrite, the verification command is:

```powershell
rg -n "Google Sheets Title|Job Category|gviz|Grackle|Unknown company|quality gate|Animator" docs/plans docs/INDEX.md
```

## Acceptance Criteria

- The default Google Sheets parser selects the actual source title column, not `Job Category`, for both direct export and `gviz`-flattened shapes.
- Supported redirect self-mappings are not treated as permanent successful cache entries.
- Shipped main-feed artifacts contain zero unresolved exact source-category title leaks.
- Supported redirect rows do not ship as `Unknown company` when redirect/provider/Grackle/sibling/trusted metadata evidence identifies the company.
- The attached screenshot example is fixed end to end: title `Senior Design Director`, non-unknown Believer company, and no unresolved duplicate caused by Grackle/provider URL aliasing.
- Likely-live jobs are not dropped solely because the title parser could not recover a specific title.
- Legitimate exact short role titles are preserved when source evidence shows they are real titles.
- Dead/no-openings rows remain eligible for dropping through the existing dead-source policy.
- Unrepaired likely-live category-title rows fail the quality gate with enough diagnostic context for follow-up.
- No public job payload schema, persisted saved-job schema, bridge route, or frontend contract changes are made unless explicitly approved in the implementation task.

## Assumptions

- The 194 exact `Art` rows and 3,013 default-sheet category/title mismatches are local evidence from one artifact and one live-source inspection, not stable target counts for tests.
- The default Google Sheets source is the dominant known source of this class of issue, but the final gate should protect all main-feed sources.
- Candidate URL quality scoring can be implemented without adding a new Python dependency.
- Redirect-backed company repair should be evidence-based and limited to supported redirects, provider targets, Grackle listing rows, sibling rows, or trusted metadata.
- Dropping proven dead or no-openings URLs is acceptable and remains separate from title-extraction failure.
- Generic company inference beyond redirect/provider/sibling evidence is out of scope.
- Parser, redirect-cache hardening, redirect-backed provider company repair, and same-opening title-preference implementation landed on 2026-05-30 in `src/jobs/adapters/community/google_sheets.py`, `src/jobs/transport.py`, `src/jobs/canonicalize.py`, and `src/jobs/dedup.py`.
- Final shipped-artifact quality gate tooling and a fresh live-source pipeline validation were completed on 2026-05-30. The remaining follow-up is static-source cleanup for exact category-page titles such as `Legal`, `Community`, `Marketing`, `Account`, `QA`, `Audio`, `Research`, `Security`, `Engineering`, `Finance`, and `Web`.
