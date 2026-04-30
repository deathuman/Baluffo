# Adapter Plugin Inventory

> - **Status:** Active
> - **Use this when:** adding or changing a source family, adapter plugin, loader path, or plugin ownership boundary
> - **Canonical for:** adapter plugin inventory, source-loader family routing, and future extraction guidance
> - **Not canonical for:** data payload contracts, admin API contracts, or live registry contents
> - **Then inspect:** [`architecture-ai-map.md`](architecture-ai-map.md), [`scraping-pipeline.md`](scraping-pipeline.md), and the owning adapter source files
> - **Last updated:** 2026-04-29

This note captures the current inventory for the **adapter plugin framework** and the stable loader surfaces that still wrap it.

### Source loaders map (jobs pipeline)

All registered sources used by the jobs fetcher are listed in `src/jobs_fetcher_registry.py` (`DEFAULT_SOURCE_LOADER_NAMES`) and have metadata in `SOURCE_REPORT_META`. The pipeline runs each loader and records per-source results in `data/jobs-fetch-report.json`.

| Source name | Adapter | Module / notes | Report key |
|-------------|---------|----------------|------------|
| google_sheets | csv | community ([`adapters/community/google_sheets.py`](../src/jobs/adapters/community/google_sheets.py)) | adapter: csv, studio: community_sheet |
| google_sheets_1er2oaxo | csv | community (mirror sheet) | adapter: csv |
| google_sheets_1mvqhxat | csv | community (mirror sheet) | adapter: csv |
| remote_ok | api | community | adapter: api, studio: remote_ok |
| gamesindustry | html | community | adapter: html, studio: gamesindustry |
| gamejobs | html | community board loader | adapter: html, studio: gamejobs |
| workwithindies | html | community board loader | adapter: html, studio: workwithindies |
| 8bitplay | html | community board loader | adapter: html, studio: 8bitplay |
| gracklehq | html | community board loader | adapter: html, studio: gracklehq |
| epic_games_careers | api | community | adapter: api, studio: epic_games |
| greenhouse_boards | greenhouse | provider_api dispatch -> Greenhouse plugin | adapter: greenhouse, studio: multiple |
| teamtailor_sources | teamtailor | provider_api dispatch -> Teamtailor plugin | adapter: teamtailor |
| lever_sources | lever | provider_api dispatch -> JSON-feed plugin | adapter: lever |
| smartrecruiters_sources | smartrecruiters | provider_api dispatch -> JSON-feed plugin | adapter: smartrecruiters |
| workable_sources | workable | provider_api dispatch -> JSON-feed plugin | adapter: workable |
| recruitee_sources | recruitee | provider_api dispatch -> JSON-feed plugin | adapter: recruitee |
| pinpoint_sources | pinpoint | provider_api dispatch -> JSON-feed plugin | adapter: pinpoint |
| ashby_sources | ashby | provider_api dispatch -> HTML-board plugin | adapter: ashby |
| bamboohr_sources | bamboohr | provider_api dispatch -> structured-listing plugin | adapter: bamboohr |
| breezy_sources | breezy | provider_api dispatch -> HTML-board plugin | adapter: breezy |
| jazzhr_sources | jazzhr | provider_api dispatch -> HTML-board plugin | adapter: jazzhr |
| personio_sources | personio | provider_api entrypoint and provider plugin registration | adapter: personio |
| workday_sources | workday | provider_api dispatch -> structured-listing plugin | adapter: workday |
| scrapy_static_sources | scrapy_static | static (Scrapy subprocess) | adapter: scrapy_static |
| social_reddit | social | social stable loader -> registered Reddit plugin | adapter: social, studio: reddit |
| social_x | social | social stable loader; X plugin is registered for framework path | adapter: social, studio: x |
| social_mastodon | social | social stable loader; Mastodon plugin is registered for framework path | adapter: social, studio: mastodon |
| static_studio_pages_* | static | static (registry entries, sharded a_i / j_r / s_z) | adapter: static |

**Excluded by default:** `wellfound` (in `EXCLUDED_DEFAULT_SOURCES`) — disabled due to anti-bot restrictions unless browser fetch is used.

**Health check:** After a run, open `data/jobs-fetch-report.json`. The `sources` array has one entry per attempted source with `name`, `status` (ok/error/excluded), `fetchedCount`, `keptCount`, `error`, and `durationMs`. Use `runtime.selectedSourceCount` and `summary.failedSources` / `summary.excludedSources` for a quick overview.

**Troubleshooting:** To debug a failing source, find its entry in `sources` and check `status`, `error`, and `loss` (e.g. `canonicalDropReasons`). To run only specific sources: `python src/jobs_fetcher.py --only-sources google_sheets,remote_ok`. To force a full run ignoring circuit breaker: `--ignore-circuit-breaker`. If Playwright/browser fallback is unhealthy, the fetcher will now short-circuit further browser attempts for a short cooldown instead of retrying on every static source. To disable a source without code changes, add it to `EXCLUDED_DEFAULT_SOURCES` in `src/jobs_fetcher_registry.py` or remove it from the active registry (`data/source-registry-active.json`) for registry-driven sources.

### Static adapter ownership

- **`src/jobs/adapters/static.py`**
  Stable root adapter surface only. Keep imports, loader names, and monkeypatch seams stable here.
- **`src/jobs/adapters/static_runtime.py`**
  Run-dependency and per-source context state, including progress, budget, and failure helpers.
- **`src/jobs/adapters/static_listing.py`**
  Plugin fast path, generic listing fetch, rendered-card fallback, listing fingerprint/cache decisions, and detail-traversal planning inputs.
- **`src/jobs/adapters/static_detail.py`**
  Detail traversal batching, adaptive stop behavior, and detail HTML result integration.
- **`src/jobs/adapters/static_sources.py`**
  Shard naming, registry-row dynamic loader naming, and single-source/shard wrapper construction.
- Static helper ownership now lives directly in `static_runtime_support.py` and `static_detail_heuristics.py`; the old `static_helpers.py` facade has been deleted.

### Jobs fetcher compatibility ownership

- **`src/jobs_fetcher.py`**
  Stable CLI facade and test patch surface only. Keep command entrypoints and root monkeypatch seams stable here.
- **`src/jobs/fetcher_compat_exports.py`**
  Lazy compatibility export table for parser, adapter, registry, canonicalize, and dedup symbols surfaced through `src.jobs_fetcher`.
- **`src/jobs/fetcher_compat_runtime.py`**
  Root-backed runtime wrappers for `run_pipeline`, `run_scrapy_static_source`, `registry_entries`, `build_redirect_resolver`, and `maybe_fetch_kojima_job_listing_html`.

### Current provider and social plugin boundaries

- **`src/jobs/adapters/provider_api.py` (~282 LOC)**
  Stable provider loader surface only. It calls `ensure_registered()`, selects a `provider_api` plugin by adapter key, and preserves compatibility exports such as the Personio rate-limit helpers.

- **`src/jobs/adapters/plugins/provider_api/register.py`**
  Registers the current provider plugins:
  - direct runner plugins: `greenhouse_boards`, `teamtailor_sources`
  - JSON-feed plugins: `lever_sources`, `workable_sources`, `smartrecruiters_sources`, `recruitee_sources`, `pinpoint_sources`
  - structured/migration plugins: `personio_sources`, `bamboohr_sources`, `workday_sources`
  - HTML-board plugins: `breezy_sources`, `jazzhr_sources`, `ashby_sources`

- **Provider plugin implementation owners**
  - `greenhouse_runner.py` and `teamtailor_runner.py` own the two direct runners.
  - `json_feed.py` owns shared JSON feed providers.
  - `html_board.py` owns shared HTML board providers.
  - `provider_personio.py` and `provider_structured_listing.py` remain specialized provider owners behind registered plugin entries.

- **`src/jobs/adapters/community/__init__.py`**
  Community-board loaders now include:
  - `google_sheets`
  - `remote_ok`
  - `gamesindustry`
  - `gamejobs`
  - `workwithindies`
  - `8bitplay`
  - `gracklehq`
  - `epic_games_careers`

- **`src/jobs/adapters/social.py` (~620 LOC)**
  Stable social loader surface and compatibility owner. It keeps the public loader functions used by pipeline dispatch, handles cache/progress/diagnostic behavior, and wraps or coordinates registered social plugin paths.

- **`src/jobs/adapters/plugins/social/register.py`**
  Registers `social_reddit`, `social_x`, and `social_mastodon` as social-family plugins. The Reddit loader currently delegates through registry selection; X and Mastodon plugin code is present, but the stable surface still owns compatibility orchestration that should not be changed as cleanup-only work.

- **`src/jobs/adapters/social_parsers.py`**
  Thin parser compatibility facade. Reddit, X, Mastodon, and shared signal parsing now live under `src/jobs/adapters/social_parser/`; keep existing imports through this facade unless behavior work needs the leaf owner directly.

### Future extraction guidance

- Provider plugin extraction is no longer a first-wave task; most provider lanes already dispatch through registered plugins. Future provider work should start in the owning plugin module unless a stable loader compatibility change is required.
- Social behavior work should start in `social.py`, `plugins/social/register.py`, or the relevant `social_parser/` leaf. Audit pipeline loader registration so cache-skip, heartbeat, progress, diagnostics, and fallback behavior remain compatible.
- The root `static.py` surface is split behind focused helper modules. New static-adapter work should start in the helper or static plugin that owns the behavior, not in the root surface.

### Static plugins (current)

| Plugin | Host(s) | Purpose |
|--------|--------|---------|
| activision | careers.activision.com | Activision careers (HTML-first; browser escalation when needed) |
| amanotes | careers.amanotes.com, www.careers.amanotes.com | Amanotes careers |
| ats_wrappers | naughtydog.com, www.naughtydog.com, jobs.zenimax.com | Thin careers pages that mainly point to ATS destinations |
| blizzard | careers.blizzard.com, www.careers.blizzard.com | Blizzard Entertainment careers (HTML-first; browser escalation when needed) |
| cdprojektred | cdprojektred.com, www.cdprojektred.com | CD Projekt RED careers (HTML-first; Playwright fallback when JS shell detected) |
| climax | www.climaxstudios.com | Climax Studios careers |
| embark | careers.embark-studios.com | Embark Studios careers |
| frontier | frontier.co.uk, www.frontier.co.uk | Frontier careers |
| globalstep | globalstep.com | GlobalStep careers |
| hrmos | hrmos.co | HRMOS-powered career pages |
| jobvite | amberstudiocareers (partial) | Jobvite-based studio careers |
| kojima | kojimaproductions.jp, www.kojimaproductions.jp | Kojima Productions careers (HTML-first; dynamic listing helper + browser escalation) |
| lionbridge | careers.lionbridge.com | Lionbridge careers |
| larian | larian.com | Larian Studios static careers (excludes /careers/location/ false positives) |
| littlechicken | littlechicken.nl, www.littlechicken.nl | Little Chicken careers |
| milestone | milestone.it, www.milestone.it | Milestone careers (HTML-first; browser escalation when needed) |
| naconstudiomilan | www.naconstudiomilan.com, naconstudiomilan.com | NACON Studio Milan careers |
| nintendo_csod | jobs.nintendo.de, nintendoeurope.csod.com | Nintendo Europe CSOD careers |
| remedy | remedygames.com, www.remedygames.com | Remedy careers (HTML-first; browser escalation when needed) |
| rendered_cards | workwithindies.com, romerogames.com, starbreeze.com, stepico.com, mobge.net, and similar card/list careers pages | Registered directly from `_rendered_cards.py`; shared rendered-card/list extractor for static pages |
| riot | www.riotgames.com | Riot Games careers |
| sheet_studios | coolgames.com, gismart.com, aspyr.com, 10chambers.com, careers.10chambers.com, 24bitgames.com, 4jstudios.com, blacksnow.tv, napsteam.com, area35east.com, chubbypixel.com, bonfirestudios.com, bandainamcostudios.my | Sheet-sourced / indie studio career pages (shared heuristics; empty-confirmed or browser fallback when extract fails) |
| supercell | supercell.com, www.supercell.com | Supercell careers (HTML-first; browser escalation when needed) |

To add a new static plugin: (1) Add a module under `src/jobs/adapters/plugins/static/` with `can_handle(ctx)` (e.g. `ctx.source_identity == "example.org"`) and `run(..., pages, source_row, parse_jobpostings_from_html=..., **kwargs)` returning `Sequence[RawJob]`. (2) Register it in `register.py` with `default_registry.register(SimpleAdapterPlugin(...))`. (3) See `docs/architecture-ai-map.md` § Static adapter and this file § Source loaders map.

#### Static source checklist (to avoid extract-zero regressions)

- **Run it alone first**: `python src/jobs_fetcher.py --only-sources static_source::<id> --ignore-circuit-breaker`
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
   `python src/jobs_fetcher.py --only-sources static_source::<source_id> --ignore-circuit-breaker`
   Use the source's `id` or the loader name from the report (e.g. `static_source::static:listing_url:https://...`).
3. **Check the report** in `data/jobs-fetch-report.json`: find the source in `sources` and note `status`, `keptCount`, `classification`, `browserFallbackRecommended`, `error`.
4. **Act on the outcome**:
   - **403 / blocked**: Plugin or default path should set `classification: blocked_or_challenge` and `browserFallbackRecommended: true`; the run will add it to `jobs-browser-fallback-queue.json`. No code change needed if the adapter already does this.
   - **200 but 0 jobs**: Add or update a static plugin for that host. In the plugin: use `_heuristics.detect_no_openings(html)` to set `empty_confirmed` when the page says "no open positions"; otherwise set `classification: needs_review` and `browserFallbackRecommended: true`. Use `_heuristics.detect_js_shell(html)`, `detect_outbound_ats_links(...)`, and listing/redirect signals to upgrade to `js_required`, `site_changed`, or `anti_bot_or_challenge` when the evidence is strong enough.
   - **ATS links present**: Prefer adding a provider source (e.g. SmartRecruiters) and a `REDUNDANT_STATIC_IF_PROVIDER` rule so the static entry is skipped (see § Redundant static sources).
5. **Verify**: Run the pipeline again (or the same `--only-sources`). Confirm the source no longer appears as a silent success with 0 jobs; it should be either ok with jobs, `empty_confirmed`, or in the browser queue with a clear classification.
6. **Classifications** (canonical values in `src/jobs/adapters/plugins/static/_heuristics.py` and `src/jobs/common/config.py` via `STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE`): `ok_with_jobs`, `ok_no_jobs`, `empty_confirmed`, `js_required`, `site_changed`, `anti_bot_or_challenge`, `needs_review`, `blocked_or_challenge`, `timeout`, `rate_limited`, `parse_error`, `error`. Only `blocked_or_challenge` and `timeout` add the source to the browser fallback queue automatically; `needs_review` can still be browser-eligible when the adapter explicitly marks it.

#### Redundant static sources (ATS migration)

When a studio’s jobs are already covered by a provider adapter (e.g. SmartRecruiters, Greenhouse), static entries for that studio’s careers page are skipped so the pipeline doesn’t run extract-zero static scrapes. The mapping lives in `src/jobs/common/registry_defaults.py`: **`REDUNDANT_STATIC_IF_PROVIDER`**. Each rule lists `hosts` (e.g. `cdprojektred.com`, `www.cdprojektred.com`) and the provider key (`adapter` + `provider_id_field` + `provider_id_value`). If the registry contains that provider source, any static source whose first page URL has that host is excluded from `registry_entries("static")`. To add a new redundant pair: (1) Add the provider source to the default or active registry. (2) Append a rule to `REDUNDANT_STATIC_IF_PROVIDER` with the careers-site hosts and the same provider identifier.

Admin discovery review now also exposes a read-only provider migration advisory under `candidateReview.providerMigration`. The advisory ranks static/generic candidates using existing provider evidence, supported provider URL patterns, active/pending provider registry rows, explicit ATS links, and `REDUNDANT_STATIC_IF_PROVIDER`. Strong supported evidence may automatically stage a non-destructive provider candidate into the normal discovery/pending path so users benefit without opening Admin. Staging does not promote, hide, reject, tombstone, delete, sync, or migrate the original static row.

Provider coverage validation is recorded only from the staged provider source's own provider fetch evidence. A provider row with `keptCount > 0` may become `providerCoverageStatus="validated_provider"`, which means the provider source is real and usable. Two consecutive successful provider fetches may trigger reversible runtime suppression of the linked static source during normal default fetches. The static source emits an excluded report row with `exclusionReason="dynamic_redundant_provider"` and runs again automatically if provider coverage becomes failed, unstable, or needs review.

Static suppression safety is self-correcting at runtime. The current default fetch reads the latest prior `jobs-fetch-report.json` `staticSuppressionPolicy`, falling back to prior `providerStaticOverlap`, before loader selection. Prior safe or missing evidence allows suppression, prior `insufficient_history` allows suppression with a warning, and prior `needs_review`, `provider_unstable`, static-only jobs, or `static_only_jobs_detected` pauses suppression so the static source runs normally. The current run writes fresh `providerStaticOverlap` and `staticSuppressionPolicy` evidence for the next run. This is not deletion, hiding, demotion, rejection, tombstoning, forced permanent migration, or a permanent redundancy rule, and this workflow does not mutate `REDUNDANT_STATIC_IF_PROVIDER`.

Redundant static proposals are read-only diagnostics layered on top of existing evidence from `staticSuppressionPolicy`, `providerStaticOverlap`, and `providerCoverage`. They summarize evaluated provider/static pairs as safe redundant, keep static, needs more history, needs review/provider unstable, or static-only detected, but they do not change loader selection or runtime suppression eligibility. `keep_static` is only emitted for an evaluated provider/static pair; unlinked static registry rows are not scanned and do not produce proposals. `safe_redundant_static` means keep reversible runtime suppression, not delete, hide, reject, demote, tombstone, or permanently suppress a source.

### How to add new sources by family

- **Provider API (Greenhouse, Lever, Recruitee, Pinpoint, BambooHR, Workday, Breezy, JazzHR, etc.):** Add the source to the runtime registry (`data/source-registry-active.json` or via Admin -> Sources). The fetcher loads registry entries by adapter type; ensure the entry has the required fields (e.g. `slug` for Greenhouse, `api_url` for Lever/Recruitee/Pinpoint, `board_url` for Ashby/Breezy/JazzHR, `feed_url` for Personio, `pages` or `listing_url` for BambooHR/Workday migration sources). No change to `DEFAULT_SOURCE_LOADER_NAMES` is needed once the provider family itself exists. `personio_sources` is now registered through `src/jobs/adapters/plugins/provider_api/register.py` and exposed through the provider_api surface.
- **Static studio site:** (1) Add a static plugin if the site needs custom parsing (see Static plugins above). (2) Add a registry entry with `"adapter": "static"`, `pages` (listing URL(s)), and `company`/`name`. The pipeline will pick the plugin by host from the first page URL.
- **New CSV/Google Sheet:** Add an entry to `GOOGLE_SHEETS_SOURCES` in `src/jobs/adapters/community/google_sheets.py` (or import from `src.jobs.adapters.community`) with `name`, `sheetId`, `gid`. Add the same `name` to `DEFAULT_SOURCE_LOADER_NAMES` and `SOURCE_REPORT_META` in `src/jobs_fetcher_registry.py`.
- **New community board / aggregator:** Add the parser and loader in `src/jobs/adapters/community/__init__.py`, export the parser through `src/jobs/parsers.py`, and only touch `src/jobs_fetcher.py` if a legacy CLI compatibility re-export must stay available. Lazy compatibility export routing lives in `src/jobs/fetcher_compat_exports.py`, and root-backed wrapper seams live in `src/jobs/fetcher_compat_runtime.py`. Then add the loader name to `DEFAULT_SOURCE_LOADER_NAMES` and `SOURCE_REPORT_META` in `src/jobs_fetcher_registry.py`. Recent examples: `gamejobs`, `workwithindies`, `8bitplay`, `gracklehq`.
- **Social (Reddit/X/Mastodon):** Enable via `--social-enabled` or runtime config. To add a new public social source, register the plugin path under `src/jobs/adapters/plugins/social/`, preserve or add the stable loader in `src/jobs/adapters/social.py`, and register the source in `default_source_loaders` and `SOURCE_REPORT_META`.
