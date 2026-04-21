# Scraping pipeline and Playwright

Overview of the jobs scraping flow, where Playwright is used, and how to compare job counts before/after changes.

## 1) Scraping flow

```text
Discovery (source_discovery)
  → probe candidates (static/generic_static get optional Playwright fallback on 403/timeout/challenge)
  → source-discovery-report.json, source-discovery-candidates.json
  → m5-strategic-backlog.json (derived review snapshot)
  → pending registry / sync

Pipeline (jobs/pipeline.py)
  → execute_loader per source (static, scrapy_static, provider APIs, etc.)
  → Static adapter: listing fetch with optional try_playwright fallback (403/timeout or JS shell + no jobs)
  → jobs-fetch-report.json, per-source classifications
  → build_browser_fallback_queue → jobs-browser-fallback-queue.json

Scrapy path (for scrapy_static sources from browser queue)
  → registry_entries("scrapy_static") = _scrapy_static_registry_from_browser_queue()
  → run_scrapy_static_source (static_scrapy.py) invokes scrapers/runner.py with use_browser=True
  → Runner: Scrapy-Playwright when use_browser and scrapy-playwright installed; else HTTP-only
  → GenericCareersSpider with meta["playwright"] = True when use_browser
  → Envelope (jobs, details, stats) back to pipeline; results feed jobs-unified.json
```

- **Discovery:** Candidates that fail probe (403, timeout, challenge-like HTML) can be retried with Playwright so more sources pass and enter the queue.
- **M5 review snapshot:** Discovery now also writes `data/m5-strategic-backlog.json` as a derived review artifact. It is built from the canonical discovery ledger and should not be treated as the source of truth for discovery state.
- **Static adapter:** Only the **listing page** fetch per source can use Playwright fallback; detail pages stay HTTP. If the pipeline has Playwright available, it injects `try_playwright` into static loaders.
  Static extraction now also runs a shared job-page gate so ordinary pages are rejected as `dead_listing_page` instead of becoming synthetic jobs or generic empty misses.
- **Browser fallback queue:** Sources classified as `blocked_or_challenge` or `needs_review` with `browserFallbackRecommended: true` (and adapter `scrapy_static`) are written to `jobs-browser-fallback-queue.json`. The next pipeline run uses that list as the scrapy_static registry and runs them with **Scrapy-Playwright** (use_browser=True).

## 2) Where Playwright is used

| Point | Location | When |
|-------|----------|------|
| **Source check (admin)** | `src/bridge/source_check_http.py` (`try_fetch_with_playwright`), `source_check_fetch.py` | Admin “source check” (trigger_source_check); optional browser fallback when HTTP fails or page is challenge-like. |
| **Discovery probe** | `src/source_discovery/probe.py` | Optional fallback for **static** / generic_static when probe fails with 403, timeout, or challenge-like response; concurrency limited (e.g. semaphore 5). |
| **Static adapter listing** | `src/jobs/adapters/static.py` (root surface) and `src/jobs/adapters/static_{runtime,listing,detail,sources}.py` | Optional fallback for the **listing page** only: on 403/timeout from `fetch_html_cached`, or when HTML looks like JS shell and no job data extracted; `try_playwright` injected by pipeline when Playwright is available. |
| **Scrapy-Playwright** | `src/scrapers/runner.py`, `src/scrapers/spiders/generic_careers.py` | When scrapy_static sources are run from the browser queue, runner passes `use_browser=True`; if scrapy-playwright is installed, requests are handled by Playwright and spider sets `meta["playwright"] = True`. If scrapy-playwright is missing, runner falls back to HTTP-only. |

All Playwright use is optional: discovery and pipeline run without Playwright if it is not installed.

## 3) Before/after job count comparison

To see how much the job count changed after scraping improvements:

1. **Total jobs:** `data/jobs-unified.json` — count top-level array length (or `keptCount` / output count from pipeline summary).
2. **Browser fallback queue size:** `data/jobs-browser-fallback-queue.json` — number of entries (sources that were recommended for browser/Playwright).
3. **Per-source status:** `data/jobs-fetch-report.json` — for each source: `status`, `fetchedCount`, `keptCount`, `classification`, `error`. Compare counts of `blocked_or_challenge`, `needs_review`, and `ok`/`ok_with_jobs` before vs after.
4. **Discovery:** `data/source-discovery-report.json` — `summary.lossAccounting`, probe failed / low_evidence_skipped; Playwright probe fallback aims to reduce those.

**Suggested comparison:**

- Before a run: note `len(jobs-unified.json)`, size of `jobs-browser-fallback-queue.json`, and number of sources with `classification` in `{blocked_or_challenge, needs_review}` in the last fetch report.
- After a run (with Playwright fallbacks and Scrapy-Playwright enabled): compare the same metrics. Higher unified count, smaller browser queue, and fewer blocked/needs_review sources indicate improvement.

See also: `docs/DATA_CONTRACT.md` for report shapes; `docs/architecture-ai-map.md` for static adapter and Scrapy path. For static adapter edits, keep `src/jobs/adapters/static.py` as the compatibility surface and route implementation changes to the focused `static_{runtime,listing,detail,sources}.py` helpers.

## 4) Running the pipeline

- **Recommended (full fetcher):** From repo root, set `PYTHONPATH` to the repo root and run the jobs fetcher module:
  `python src/jobs_fetcher.py` (optionally with `--ignore-circuit-breaker`). This runs all default source loaders including static and scrapy_static.
- **npm script:** `npm run dev:pipeline` runs the pipeline script with `PYTHONPATH` set to the current directory so that `src` resolves; it invokes `src/jobs/pipeline.py` (same process as the fetcher’s pipeline, with default loaders when no facade overrides).
