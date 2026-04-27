# Scraping Pipeline and Playwright

Overview of the jobs scraping flow, where Playwright is used, and how to compare job counts before and after changes.

## 1) Scraping flow

```text
Discovery (source_discovery)
  -> probe candidates (static/generic_static get optional Playwright fallback on 403/timeout/challenge)
  -> source-discovery-report.json, source-discovery-candidates.json
  -> m5-strategic-backlog.json (derived review snapshot)
  -> pending registry / sync

Pipeline (jobs/pipeline.py -> pipeline_run_setup.py -> pipeline_execution_flow.py -> pipeline_stage_source_execution.py -> pipeline_source_{loop,results,progress}.py -> pipeline_finalize.py)
  -> execute_loader per source (static, scrapy_static, provider APIs, etc.)
  -> Static adapter: listing fetch with optional try_playwright fallback (403/timeout or JS shell + no jobs)
  -> jobs-fetch-report.json, per-source classifications
  -> build_browser_fallback_queue -> jobs-browser-fallback-queue.json

Scrapy path (for scrapy_static sources from browser queue)
  -> registry_entries("scrapy_static") = _scrapy_static_registry_from_browser_queue()
  -> run_scrapy_static_source (static_scrapy.py) invokes scrapers/runner.py with use_browser=True
  -> Runner: Scrapy-Playwright when use_browser and scrapy-playwright installed; else HTTP-only
  -> GenericCareersSpider with meta["playwright"] = True when use_browser
  -> Envelope (jobs, details, stats) back to pipeline; results feed jobs-unified.json
```

- Discovery: Candidates that fail probe (403, timeout, challenge-like HTML) can be retried with Playwright so more sources pass and enter the queue.
- GameDevMap discovery: the default GameDevMap adapter uses the same resumable active-source audit engine as `python src/source_discovery.py --gamedevmap-active-dry-run`. It batches eligible homepages, recovers likely same-site careers URLs in two waves, probes inferred provider/static candidates with bounded concurrency, and writes progress to `data/gamedevmap-active-source-dry-run.json`. Default discovery reuses fresh audit artifacts and feeds validated `jobsFound > 0` rows through the normal candidate/pending/auto-approval flow; static sources do not bypass the admin auto-approve setting. Validated GameDevMap static rows can use the GameDevMap-specific `validatedStaticQueueCap` / `validatedStaticDomainCap` override while still respecting dedupe, tombstones, pending, and auto-approval. The final discovery report includes `gamedevmapAuditSummary` with cache, timing, active split, recovered/browser counts, artifact size, and top failure buckets.
- GameDevMap audit maintenance: the audit artifact includes batch timings plus aggregated failure counts/samples. Use `--gamedevmap-dry-run-batch-size`, `--gamedevmap-dry-run-max-batches`, `--gamedevmap-dry-run-reset`, and `--gamedevmap-dry-run-rerun-reasons` for explicit audit maintenance. Use `--gamedevmap-dry-run-compare-artifact path/to/previous.json` to classify recovered active sources that disappeared between audit runs.
- GameDevMap browser recovery: default GameDevMap discovery stays HTTP-only. Use `--gamedevmap-browser-recovery` to process only saved `browserRecoveryCandidates` from the audit artifact through the existing Playwright fallback helper and merge validated `jobsFound > 0` recoveries back into the same artifact.
- Gameprog/Gamesmap directory audits: enabled Gameprog/Gamesmap adapters use HTTP-only directory audit artifacts by default (`activeAuditEnabled=true`), while `activeAuditEnabled=false` remains the explicit rollback to the legacy cache path. Gameprog is enabled by default; Gamesmap still requires `gamesmap.enabled=true`. The adapters still return the same provider/static/failure rows and route candidates through the normal discovery queue. The final discovery report includes `directoryAuditSummaries` / `summary.directoryAudits` only for audits that ran or reused a fresh artifact in the current process, exposing cache hit, duration, artifact size, timing totals, top failure buckets, and adapter boundary counts.
- Sheet-directory audit: enabled sheet-directory discovery uses the shared directory audit artifact by default. Set `sheetDirectory.activeAuditEnabled=false` to roll back to the legacy direct CSV scan. The audit records CSV fetch/parse/candidate timings, selected CSV URL, row counts, candidate counts, and failure buckets, then feeds the same provider/static/failure rows into the normal discovery queue.
- Seed-careers/web-search audit pilot: set `webSearch.activeAuditEnabled=true` to write/reuse `data/web-search-discovery-audit.json` for the enabled seed-careers and web-search stages. This remains opt-in; the default still calls the direct seed-careers and web-search scanners. The audit records search/page-fetch timings, sub-stage counts, candidate counts, and failure buckets, then feeds the same provider/static/failure rows into the normal discovery queue.
- M5 review snapshot: Discovery also writes `data/m5-strategic-backlog.json` as a derived review artifact. It is built from the canonical discovery ledger and should not be treated as the source of truth for discovery state.
- Static adapter: Only the listing page fetch per source can use Playwright fallback; detail pages stay HTTP. If the pipeline has Playwright available, it injects `try_playwright` into static loaders.
  Static extraction also runs a shared job-page gate so ordinary pages are rejected as `dead_listing_page` instead of becoming synthetic jobs or generic empty misses.
  Static listing/detail HTTP fetches use the shared redirect-aware cache helper. It can follow one same-site redirect, including `www.`/bare-host aliases and HTTP-to-HTTPS upgrades, but rejects unrelated cross-host redirects, redirect chains, credentialed targets, non-HTTP(S) schemes, and HTTPS downgrades.
  Template or malformed detail links, such as `{{...}}`, known placeholder tokens, and listing-page self-links, are skipped as non-job diagnostics before detail fetch so they do not become source-level fetch errors.
- Browser fallback queue: Sources classified as `blocked_or_challenge`, `anti_bot_or_challenge`, `rate_limited`, or `needs_review` with `browserFallbackRecommended: true` (and adapter `scrapy_static`) are written to `jobs-browser-fallback-queue.json`. The next pipeline run uses that list as the scrapy_static registry and runs them with Scrapy-Playwright (`use_browser=True`).

## Static source triage

- Preserve registry rows when a static source is no longer viable. Move unsupported static entries to hidden pending with an explicit `pendingReason` instead of deleting them.
- LinkedIn profile/search/post pages and generic third-party aggregators are not viable static career boards. They should stay out of default active fetches unless a supported provider/social adapter owns them.
- Provider-hosted pages should be migrated to an existing provider adapter only when the repo already supports that provider and the source row has enough local evidence to map it safely. Otherwise, hide the static row and document the unsupported provider family as future work.

## 2) Where Playwright is used

| Point | Location | When |
|-------|----------|------|
| Source check (admin) | `src/bridge/source_check_http.py` (`try_fetch_with_playwright`), `source_check_fetch.py` | Admin source check (`trigger_source_check`); optional browser fallback when HTTP fails or page is challenge-like. |
| Discovery probe | `src/source_discovery/probe.py` | Optional fallback for static / generic_static when probe fails with 403, timeout, or challenge-like response; concurrency limited. |
| Static adapter listing | `src/jobs/adapters/static.py` (root surface) and `src/jobs/adapters/static_{runtime,listing,detail,sources}.py` | Optional fallback for the listing page only: on 403/timeout from `fetch_html_cached`, or when HTML looks like a JS shell and no job data is extracted; `try_playwright` is injected by pipeline when Playwright is available. |
| Scrapy-Playwright | `src/scrapers/runner.py`, `src/scrapers/spiders/generic_careers.py` | When scrapy_static sources are run from the browser queue, runner passes `use_browser=True`; if scrapy-playwright is installed, requests are handled by Playwright and the spider sets `meta["playwright"] = True`. If scrapy-playwright is missing, runner falls back to HTTP-only. |

All Playwright use is optional: discovery and pipeline run without Playwright if it is not installed.

## 3) Before and after job count comparison

To see how much the job count changed after scraping improvements:

1. Total jobs: `data/jobs-unified.json` - count the top-level array length (or `keptCount` / output count from the pipeline summary).
2. Browser fallback queue size: `data/jobs-browser-fallback-queue.json` - number of entries (sources that were recommended for browser/Playwright).
3. Per-source status: `data/jobs-fetch-report.json` - for each source: `status`, `fetchedCount`, `keptCount`, `classification`, `error`. Compare counts of `blocked_or_challenge`, `needs_review`, and `ok` / `ok_with_jobs` before and after.
4. Discovery: `data/source-discovery-report.json` - `summary.lossAccounting`, probe failed / low_evidence_skipped; Playwright probe fallback aims to reduce those.

Suggested comparison:

- Before a run: note `len(jobs-unified.json)`, the size of `jobs-browser-fallback-queue.json`, and the number of sources with `classification` in `{blocked_or_challenge, anti_bot_or_challenge, rate_limited, needs_review}` in the last fetch report.
- After a run (with Playwright fallbacks and Scrapy-Playwright enabled): compare the same metrics. Higher unified count, a smaller browser queue, and fewer blocked / needs_review sources indicate improvement.

See also: `docs/DATA_CONTRACT.md` for report shapes and `docs/architecture-ai-map.md` for static adapter and Scrapy path. For static adapter edits, keep `src/jobs/adapters/static.py` as the compatibility surface and route implementation changes to the focused `static_{runtime,listing,detail,sources}.py` helpers. For pipeline/state edits, keep `src/jobs/pipeline.py` and `src/jobs/state.py` as stable roots and route implementation changes to `src/jobs/pipeline_{run_setup,execution_flow,finalize}.py`, `src/jobs/pipeline_runtime_{writers,summary}.py`, `src/jobs/pipeline_source_{loop,results,progress}.py`, and `src/jobs/state_source_{records,browser,migration}.py`. For payload/report shaping edits, keep `src/jobs/common/contracts.py` and `src/jobs/reporting.py` as stable surfaces and route implementation changes to `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` and `src/jobs/reporting_{summary,queues,breakdowns,social}.py`.

## 4) Running the pipeline

- Recommended (full fetcher): From repo root, set `PYTHONPATH` to the repo root and run the jobs fetcher module:
  `python src/jobs_fetcher.py` (optionally with `--ignore-circuit-breaker`). This runs all default source loaders including static and scrapy_static.
- npm script: `npm run dev:pipeline` runs the stable `src/jobs/pipeline.py` entrypoint with `PYTHONPATH` set to the current directory so that `src` resolves; runtime setup is owned by `pipeline_run_setup.py`, runtime/task-state helpers by `pipeline_runtime_{writers,summary}.py`, source execution by `pipeline_stage_source_execution.py` plus `pipeline_source_{loop,results,progress}.py`, and late-stage report assembly by `pipeline_finalize.py`.
