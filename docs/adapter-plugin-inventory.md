## Adapter plugin inventory (2026-03-16)

This note captures the initial inventory for the **adapter plugin framework** rollout.

### Source loaders map (jobs pipeline)

All registered sources used by the jobs fetcher are listed in `src/jobs_fetcher_registry.py` (`DEFAULT_SOURCE_LOADER_NAMES`) and have metadata in `SOURCE_REPORT_META`. The pipeline runs each loader and records per-source results in `data/jobs-fetch-report.json`.

| Source name | Adapter | Module / notes | Report key |
|-------------|---------|----------------|------------|
| google_sheets | csv | community ([`adapters/community/google_sheets.py`](src/jobs/adapters/community/google_sheets.py)) | adapter: csv, studio: community_sheet |
| google_sheets_1er2oaxo | csv | community (mirror sheet) | adapter: csv |
| google_sheets_1mvqhxat | csv | community (mirror sheet) | adapter: csv |
| remote_ok | api | community | adapter: api, studio: remote_ok |
| gamesindustry | html | community | adapter: html, studio: gamesindustry |
| epic_games_careers | api | community | adapter: api, studio: epic_games |
| greenhouse_boards | greenhouse | provider_api (registry) | adapter: greenhouse, studio: multiple |
| teamtailor_sources | teamtailor | provider_api (registry) | adapter: teamtailor |
| lever_sources | lever | provider_api (registry) | adapter: lever |
| smartrecruiters_sources | smartrecruiters | provider_api (registry) | adapter: smartrecruiters |
| workable_sources | workable | provider_api (registry) | adapter: workable |
| ashby_sources | ashby | provider_api (registry) | adapter: ashby |
| personio_sources | personio | provider_api (registry) | adapter: personio |
| scrapy_static_sources | scrapy_static | static (Scrapy subprocess) | adapter: scrapy_static |
| social_reddit | social | social (config-driven) | adapter: social, studio: reddit |
| social_x | social | social (config-driven) | adapter: social, studio: x |
| social_mastodon | social | social (config-driven) | adapter: social, studio: mastodon |
| static_studio_pages_* | static | static (registry entries, sharded a_i / j_r / s_z) | adapter: static |

**Excluded by default:** `wellfound` (in `EXCLUDED_DEFAULT_SOURCES`) — disabled due to anti-bot restrictions unless browser fetch is used.

**Health check:** After a run, open `data/jobs-fetch-report.json`. The `sources` array has one entry per attempted source with `name`, `status` (ok/error/excluded), `fetchedCount`, `keptCount`, `error`, and `durationMs`. Use `runtime.selectedSourceCount` and `summary.failedSources` / `summary.excludedSources` for a quick overview.

**Troubleshooting:** To debug a failing source, find its entry in `sources` and check `status`, `error`, and `loss` (e.g. `canonicalDropReasons`). To run only specific sources: `python -m src.jobs_fetcher --only-sources google_sheets,remote_ok`. To force a full run ignoring circuit breaker: `--ignore-circuit-breaker`. To disable a source without code changes, add it to `EXCLUDED_DEFAULT_SOURCES` in `src/jobs_fetcher_registry.py` or remove it from the active registry (`data/source-registry-active.json`) for registry-driven sources.

### Biggest / highest-churn candidates

- **`src/jobs/adapters/static.py` (~550 LOC)**  
  Monolith that includes:
  - static studio page crawling (listing + detail heuristics)
  - detail-link heuristics / filtering
  - per-source concurrency tuning
  - HTML parsing + fallback title synthesis
  - scrapy-runner integration (`scrapy_static`)
  - note: this is now narrower than the initial plugin-rollout snapshot because report/config/fetch/detail internals live in `src/jobs/adapters/static_helpers.py`

### Existing “family-like” clusters (good early plugin families)

- **`src/jobs/adapters/provider_api.py` (~356 LOC)**  
  Already segmented by provider, each with its own registry key and parsing strategy:
  - `greenhouse` (boards JSON)
  - `teamtailor` (listing HTML + detail parsing)
  - `lever`, `smartrecruiters`, `workable` (similar registry-driven flow)
  - `ashby`, `personio` (provider-specific flows)

- **`src/jobs/adapters/social.py` (~269 LOC)**  
  Already segmented by provider:
  - reddit
  - x
  - mastodon

### Initial extraction slices (first wave)

To prove the architecture with minimal risk, start with the provider family that is already naturally sliced:

- **Family**: `provider_api`
  - **Plugins (3–5 slices)**: `greenhouse`, `teamtailor`, `lever`, `workable`, `smartrecruiters`

Then validate framework generality on a second family:

- **Family**: `social`
  - **Plugins**: `reddit`, `x` (optionally `mastodon`)

`static` remains the largest monolith; it becomes a follow-up family once the framework is validated in production paths.

### Static plugins (current)

| Plugin | Host(s) | Purpose |
|--------|--------|---------|
| example_com | example.com | Demo / tests |
| example_org | example.org | Demo / tests |
| supercell | supercell.com, www.supercell.com | Supercell careers (HTML-first; browser escalation when needed) |
| remedy | remedygames.com, www.remedygames.com | Remedy careers (HTML-first; browser escalation when needed) |
| milestone | milestone.it, www.milestone.it | Milestone careers (HTML-first; browser escalation when needed) |
| kojima | kojimaproductions.jp, www.kojimaproductions.jp | Kojima Productions careers (HTML-first; dynamic listing helper + browser escalation) |
| activision | careers.activision.com | Activision careers (HTML-first; browser escalation when needed) |
| blizzard | careers.blizzard.com, www.careers.blizzard.com | Blizzard Entertainment careers (HTML-first; browser escalation when needed) |
| sheet_studios | coolgames.com, gismart.com, aspyr.com, 10chambers.com, careers.10chambers.com, 24bitgames.com, 4jstudios.com, blacksnow.tv, napsteam.com, area35east.com, chubbypixel.com, bonfirestudios.com, bandainamcostudios.my | Sheet-sourced / indie studio career pages (shared heuristics; empty-confirmed or browser fallback when extract fails) |
| littlechicken | littlechicken.nl, www.littlechicken.nl | Little Chicken careers |
| larian | larian.com | Larian Studios static careers (excludes /careers/location/ false positives) |
| static_pilot | (none) | Placeholder; fallback used for all hosts |

To add a new static plugin: (1) Add a module under `src/jobs/adapters/plugins/static/` with `can_handle(ctx)` (e.g. `ctx.source_identity == "example.org"`) and `run(..., pages, source_row, parse_jobpostings_from_html=..., **kwargs)` returning `Sequence[RawJob]`. (2) Register it in `register.py` with `default_registry.register(SimpleAdapterPlugin(...))`. (3) See `docs/architecture-ai-map.md` § Static adapter and this file § Source loaders map.

#### Static source checklist (to avoid extract-zero regressions)

- **Run it alone first**: `py -3 -m src.jobs_fetcher --only-sources static_source::<id> --ignore-circuit-breaker`
- **If it returns 403 / obvious challenge**: ensure it lands in `data/jobs-browser-fallback-queue.json` (browser-required escalation).
- **If it returns 200 but extracts 0**:
  - Add/update a static plugin for that host, or
  - Detect an explicit “no openings” marker (only then is 0 acceptable), or
  - If it’s clearly JS-rendered, classify as browser-required (don’t silently pass as ok).
- **If the page contains ATS outbound links** (Lever/Greenhouse/Workday/etc.): prefer converting the source to the provider adapter family instead of scraping.

#### Repeatable workflow: adding a new static source

Follow this sequence so new static sources rarely end up as silent extract-zero:

1. **Add the source** to the active registry (`data/source-registry-active.json` or Admin → Sources) with `"adapter": "static"` and `pages` (listing URL(s)).
2. **Run it alone**:  
   `py -3 -m src.jobs_fetcher --only-sources static_source::<source_id> --ignore-circuit-breaker`  
   Use the source's `id` or the loader name from the report (e.g. `static_source::static:listing_url:https://...`).
3. **Check the report** in `data/jobs-fetch-report.json`: find the source in `sources` and note `status`, `keptCount`, `classification`, `browserFallbackRecommended`, `error`.
4. **Act on the outcome**:
   - **403 / blocked**: Plugin or default path should set `classification: blocked_or_challenge` and `browserFallbackRecommended: true`; the run will add it to `jobs-browser-fallback-queue.json`. No code change needed if the adapter already does this.
   - **200 but 0 jobs**: Add or update a static plugin for that host. In the plugin: use `_heuristics.detect_no_openings(html)` to set `empty_confirmed` when the page says "no open positions"; otherwise set `classification: fetch_ok_extract_zero` and `browserFallbackRecommended: true`. Use `_heuristics.detect_js_shell(html)` and `detect_outbound_ats_links(...)` to set classification and hints.
   - **ATS links present**: Prefer adding a provider source (e.g. SmartRecruiters) and a `REDUNDANT_STATIC_IF_PROVIDER` rule so the static entry is skipped (see § Redundant static sources).
5. **Verify**: Run the pipeline again (or the same `--only-sources`). Confirm the source no longer appears as a silent success with 0 jobs; it should be either ok with jobs, `empty_confirmed`, or in the browser queue with a clear classification.
6. **Classifications** (canonical values in `src/jobs/adapters/plugins/static/_heuristics.py` and `src/jobs/common.STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE`): `ok_with_jobs`, `ok_no_jobs`, `empty_confirmed`, `fetch_ok_extract_zero`, `blocked_or_challenge`, `timeout`, `rate_limited`, `parse_error`, `error`. Only `fetch_ok_extract_zero`, `blocked_or_challenge`, and `timeout` add the source to the browser fallback queue.

#### Redundant static sources (ATS migration)

When a studio’s jobs are already covered by a provider adapter (e.g. SmartRecruiters, Greenhouse), static entries for that studio’s careers page are skipped so the pipeline doesn’t run extract-zero static scrapes. The mapping is in `src/jobs/common/`: **`REDUNDANT_STATIC_IF_PROVIDER`** (defined in `src/jobs/common/__init__.py`). Each rule lists `hosts` (e.g. `cdprojektred.com`, `www.cdprojektred.com`) and the provider key (`adapter` + `provider_id_field` + `provider_id_value`). If the registry contains that provider source, any static source whose first page URL has that host is excluded from `registry_entries("static")`. To add a new redundant pair: (1) Add the provider source to the default or active registry. (2) Append a rule to `REDUNDANT_STATIC_IF_PROVIDER` with the careers-site hosts and the same provider identifier.

### How to add new sources by family

- **Provider API (Greenhouse, Lever, etc.):** Add the source to the runtime registry (`data/source-registry-active.json` or via Admin → Sources). The fetcher loads registry entries by adapter type; ensure the entry has the required fields (e.g. `slug` for Greenhouse, `api_url` for Lever). No change to `DEFAULT_SOURCE_LOADER_NAMES` needed for registry-driven adapters.
- **Static studio site:** (1) Add a static plugin if the site needs custom parsing (see Static plugins above). (2) Add a registry entry with `"adapter": "static"`, `pages` (listing URL(s)), and `company`/`name`. The pipeline will pick the plugin by host from the first page URL.
- **New CSV/Google Sheet:** Add an entry to `GOOGLE_SHEETS_SOURCES` in `src/jobs/adapters/community/google_sheets.py` (or import from `src.jobs.adapters.community`) with `name`, `sheetId`, `gid`. Add the same `name` to `DEFAULT_SOURCE_LOADER_NAMES` and `SOURCE_REPORT_META` in `src/jobs_fetcher_registry.py`.
- **Social (Reddit/X/Mastodon):** Enable via `--social-enabled` or runtime config. To add a new social provider, implement the loader in `src/jobs/adapters/social.py` and register it in `default_source_loaders` and `SOURCE_REPORT_META`.

