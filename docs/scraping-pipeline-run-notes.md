# Scraping pipeline run notes (Playwright fallback)

> **Status:** Historical run notes / archive-like reference
> - Snapshot from 2026-03-17
> - Useful for context and past observations
> - **Not authoritative** for current implementation, contracts, selectors, queue behavior, or source health
> - Revalidate against current code and `docs/scraping-pipeline.md` before using operationally

**Run date:** 2026-03-17
**Entry point:** `python src/jobs_fetcher.py --ignore-circuit-breaker`
**Output dir:** `data/`

---

## 1) Job count and metrics

| Metric | This run | Note |
|--------|----------|------|
| **Output jobs** | 31,512 | Total entries in `jobs-unified.json` |
| **Failed sources** | 24 | Sources with `status: error` in fetch report |
| **Browser fallback queue size** | 17 | Entries in `jobs-browser-fallback-queue.json` (candidates for next-run Scrapy-Playwright) |
| **Total sources run** | 52 | Selected loaders (excl. scrapy_static_sources which had 0 enabled) |

**Baseline comparison:** A previous run (from existing data) had **32,429** jobs in `jobs-unified.json`. This run produced **31,512** jobs. The difference can be due to: (1) `seed_from_existing_output` was false for this run (full refetch), (2) source availability or rate limits on the day, (3) 24 failed sources not contributing. The Playwright fallback is in place for discovery probe and static listing fetch; Scrapy-Playwright is used when running sources from the browser queue (which was empty at start, so scrapy_static_sources ran with 0 URLs this run).

### Run 2 (Scrapy-Playwright validation)

Second run with the same entry point, after **17** URLs were in the browser fallback queue so that `scrapy_static_sources` would run them with `use_browser=True` (Scrapy-Playwright).

| Metric | Before Run 2 | After Run 2 |
|--------|----------------|-------------|
| **Output jobs** | 31,512 | 31,516 |
| **Browser fallback queue size** | 17 | 17 |
| **Failed sources** | 24 | 24 |

- **Job count:** +4 (31,512 → 31,516). Small increase; some jobs may be from normal variance (e.g. Nintendo 56→57).
- **scrapy_static_sources:** Ran with the 17 browser-queue URLs and reported `fetched=0 kept=0`. Scrapy-Playwright ran but the spider did not extract jobs from those pages (likely DOM/selectors or timing; see Phase 2 wait-for-selector).
- **Failed sources:** Unchanged at 24. None of the 17 queue URLs moved to success in this run.
- **Conclusion:** Proceed with Phase 2 (spider `playwright_page_methods` wait-for-selector) and Phase 3 (static plugins for Larian/CDPR/Supercell) to improve extraction; Scrapy-Playwright is wired correctly but content needs to be waited for and/or parsed with better selectors.

### Why the 17 browser-queue URLs yielded 0 jobs (URL inspection)

After navigating a sample of the 17 queue URLs and checking the spider logic, the main reasons are:

1. **Each queue entry is run as a separate Scrapy run with a single start URL.** The queue has 17 **pages**, so the runner is invoked 17 times, each with `start_urls = [that one page]`. Several of those pages are **not** the main job-listing page:
   - **Supercell:** 6 entries include `supercell.com/en/careers/`, `.../joining-supercell/`, `.../living-helsinki/`, `.../our-offices/`, `.../why-you-might-love-it-here/`. Only the main `/en/careers/` page has the “Open Positions” list with job links. The other five are sub-pages with little or no job links, so those five runs correctly get 0 jobs. The main careers page **does** contain parseable job links when fetched (e.g. `supercell.com/en/careers/senior-3d-character-artist-brawl-stars/<uuid>/`); if that single run also got 0, likely causes are timing (list rendered after “load”) or the runner using a different queue URL for that run.
   - **Milestone:** 4 entries with different paths (`/careers`, `/careers/`, `/it/careers/`, `/news/milestone/why-join-us/`). Only the main careers URL is a real listing; the “why-join-us” one is not.

2. **Activision:** The careers homepage links to `careers.activision.com/search-results` for the actual list. The spider only requests the single URL from the queue (the homepage), which does not contain the job list in the HTML; jobs are on `/search-results`. So the queue (or static adapter) should use the search-results URL as the listing page, or the spider should follow the “Search jobs” link once.

3. **Remedy:** The page has an “Open positions” section and lots of text, but the **job list** may be loaded via Jobylon (embed/iframe or JS). The domain profile already sets `job_provider: "jobylon_v1"`; the runner uses the generic spider, not the Jobylon provider. So for Remedy, either the static adapter (with Jobylon plugin) should handle it before it reaches the browser queue, or the Scrapy path would need to use the Jobylon extractor when the profile says so.

4. **403 / timeout entries (Arkane, Epic, Zenimax, Tequilaworks):** When Scrapy-Playwright runs with a browser, 403s may still occur (e.g. bot detection). Tequilaworks was classified as timeout; Playwright might need a longer timeout or the site may be slow/unreachable.

**Recommendations:** (1) When building the browser queue, prefer the **canonical listing URL** per domain (e.g. one Supercell entry for `supercell.com/en/careers/` only). (2) For Activision, add or normalize to `careers.activision.com/search-results`. (3) Ensure the spider waits for the job list (e.g. wait for selector or `networkidle`) on the main Supercell careers page so the “Open Positions” section is present in the HTML. (4) Keep Remedy on the static/Jobylon path rather than relying on the generic Scrapy run for that domain.

### Browser queue: listing-only and collapse by source (implemented)

The browser fallback queue now enqueues **one canonical listing URL per source** instead of one row per page. Non-listing sub-pages (e.g. “our offices”, “why join us”) are filtered out using domain profiles’ `exclude_listing_path_tokens`; among remaining URLs the shortest path is chosen (typically the main careers page). When building the scrapy_static registry from the queue, rows are **collapsed by sourceId**: one registry row per source with a single URL (shortest path in the group), so Scrapy runs once per source. This reduces wasted browser runs on sub-pages and keeps the queue focused on listing pages.

### URL and flow fixes (implemented)

- **Activision:** Domain profile `careers.activision.com` has `canonical_listing_path: "/search-results"`. The queue and `pick_canonical_listing_url` now resolve the root URL to `careers.activision.com/search-results`; the Activision static plugin also fetches `/search-results` when the source page is the root, so the static adapter hits the real listing page.
- **Remedy / job_provider:** Sources whose canonical URL’s domain has a non-empty `job_provider` (e.g. Remedy with `jobylon_v1`) are no longer added to the browser queue; they stay on the static/Jobylon path.
- **Supercell (timing):** Domain profile for `supercell.com` has `playwright_wait_selector` and `playwright_wait_timeout`; when the spider runs with Playwright it waits for the job list before parsing.
- **403 / timeout (Arkane, Epic, Zenimax, Tequilaworks):** These may still 403 or timeout with Playwright (bot detection or slow/unreachable sites). Consider longer timeouts or manual inspection for specific domains if needed; no automatic change applied.

---

## 2) Failed sources (24) – summary

- **ashby_sources (1):** Ashby board HTML parse yielded no jobs (Jagex, Scopely). Adapter or site layout may have changed.
- **Static / generic static (23):** All reported “no jobs extracted from source pages”. Many are JS-heavy or use non-standard markup; listing fetch may have succeeded (or Playwright fallback used) but extraction heuristics (JSON-LD, link patterns, or plugin logic) found no jobs.

### 2.1 Failing sources (25) – URL inspection and next steps

Summary of URL inspection and fixes applied or deferred:

| Source | Cause | Change |
|--------|--------|--------|
| **Activision** (careers.activision.com/search-results) | JS app shell; job list client-rendered | `playwright_wait_selector` + `playwright_wait_timeout` added in domain profile; queue uses canonical listing URL; Scrapy-Playwright waits before parsing. |
| **Larian** (larian.com/careers) | HTML has job links, no JSON-LD; plugin only used JSON-LD | **Done:** Link-extraction fallback in Larian plugin when JSON-LD returns 0; same-host links matching `/careers/<uuid>` build minimal job rows. |
| **Supercell** (supercell.com/en/careers/) | Full HTML sometimes; production may get 403 or different response | Playwright path + wait selector already in profile; **Done:** Optional link-extraction fallback in Supercell plugin when JSON-LD returns 0. |
| **Remedy** (remedygames.com/careers) | Jobs in Jobylon embed; main HTML has no job links | **Done:** Remedy static plugin calls Jobylon extractor (`extract_jobylon_v1_jobs`) when domain has `job_provider: jobylon_v1`; jobs from Jobylon used instead of HTML parse. |
| **CDPR** (cdprojektred.com/en/jobs) | "OPEN POSITIONS" + SmartRecruiters; list empty or widget | Optional `playwright_wait_selector` in domain profile for browser-queue runs; deeper fix (SmartRecruiters/widget) deferred and documented here. |
| **Others** (Nintendo DE, Insomniac, Rockstar, Quantic Dream, etc.) | Not inspected; likely 403, timeout, JS shell, or different markup | Document and prioritize in follow-up; same pattern: browser queue + Playwright when appropriate, or link-extraction fallback where HTML already has job links. |

---

## 3) Improvement notes by area

### 3.1 Static adapter – “no jobs extracted”

These static sources failed with zero jobs extracted. Improvements to consider:

- **Larian (larian.com/careers):** Known to use custom/JS structure. Consider a dedicated plugin or extended `detailQueryKeys` / link regex; confirm listing HTML after Playwright fallback actually contains job links or JSON-LD.
- **Nacon Studio Milan, Little Chicken (both URLs):** Same “no jobs extracted” pattern. Check if listing returns a JS shell and Playwright fallback is triggered; if so, add or tune selectors for job links / JSON-LD.
- **Supercell, Milestone, Remedy:** Report shows `fetch_ok_extract_zero` in details. Page fetches succeed but extraction returns nothing. Consider site-specific plugins or broader selectors (e.g. more permissive link regex, or JSON-LD in different shapes).
- **CD Projekt Red (cdprojektred.com/en/jobs):** Page shows "OPEN POSITIONS" and "No openings for your field?" with a SmartRecruiters link; list may be empty or loaded via widget. Optional `playwright_wait_selector` added in domain profile for browser-queue runs; deeper fix (SmartRecruiters API or widget handling) deferred.
- **Ubisoft Milan, Quantic Dream, Rockstar, Stormind, Nintendo DE:** Same pattern. Prioritize by traffic; consider Playwright for listing if not already, then adjust selectors or add a small plugin.
- **Kojima Productions, Pixelmafia, Impact Reality, Digital Bros, 34bigthings, Awe Interactive, Workwithindies, Game Jobs (gamejobs.work):** Generic static path; improve either generic link extraction or add domain-specific rules.
- **Insomniac (insomniac.games/careers):** Likely JS-heavy; ensure Playwright fallback is used for listing and that selectors match their structure.
- **Gamesmap-sourced (ahoiii, astragon, andarion-games, articy, augmented-minds):** “No jobs extracted” despite being from gamesmap. Check if these use a different page layout or if the shared gamesmap extraction path needs to handle more variants.

**Generic improvements:**

- In [src/jobs/adapters/static.py](../src/jobs/adapters/static.py), ensure `try_playwright` is used for listing when `detect_js_shell` or 403/timeout occurs, and that the same HTML is then passed to `parse_jobpostings_from_html` and link extraction.
- Consider logging when Playwright fallback is used and whether it yielded more links/jobs, to see which domains benefit.
- In [src/jobs/adapters/plugins/static/](../src/jobs/adapters/plugins/static/), add or extend plugins for high-value domains (e.g. Larian, Supercell, CDPR) with custom selectors or JSON-LD handling.

### 3.2 Scrapy path and browser queue

- **scrapy_static_sources** this run: “No enabled scrapy_static sources” because the browser fallback queue was empty at start. After this run, **17** entries were written to `jobs-browser-fallback-queue.json` (sources with `blocked_or_challenge` or `fetch_ok_extract_zero` and adapter `scrapy_static`). The **next** pipeline run will run those 17 URLs via Scrapy with `use_browser=True` (Scrapy-Playwright).
- **Browser queue entries** include:
  - **blocked_or_challenge:** e.g. Arkane Studios (403), Epic Games careers (403). Scrapy-Playwright should help on the next run.
  - **fetch_ok_extract_zero:** e.g. Activision, Kojima Productions, Milestone (multiple URLs), Remedy, Supercell, Zenimax. Next run will retry with browser; if still zero jobs, consider site-specific selectors or `playwright_page_methods` (e.g. wait for a specific selector before parsing).

**Spider improvements (GenericCareersSpider):**

- In [src/scrapers/spiders/generic_careers.py](../src/scrapers/spiders/generic_careers.py), when `use_browser=True`, consider adding `playwright_page_methods` (e.g. wait for a common job-list selector) on listing requests so JS-rendered content is fully loaded before parsing.
- Optionally add a small per-domain config (e.g. in [src/scrapers/domain_profiles.py](../src/scrapers/domain_profiles.py)) to set “wait for selector” or longer timeouts for known heavy-JS career pages.

### 3.3 Ashby adapter

- **ashby_sources:** “no jobs extracted from ashby board html” for Jagex and Scopely. Check [src/jobs/adapters/](../src/jobs/adapters/) or provider-specific code for Ashby; confirm board URL and HTML structure haven’t changed and that the parser still matches the current markup. **Deferred:** Fix or deeper investigation left for a follow-up; if the board HTML or API has changed, a small parser update may be enough.

---

## 4) Suggested next steps

1. **Run the pipeline again** (no need to clear browser queue) so that the **17** browser-queue URLs run with Scrapy-Playwright and see how many of them yield jobs.
2. **Add observability** for Playwright usage: e.g. log when static listing fallback or discovery probe uses Playwright and whether it increased link/job count for that URL.
3. **Prioritize 2–3 high-value static sources** (e.g. Larian, CDPR, Supercell) and add or tune plugins/selectors so “no jobs extracted” turns into at least one successful extraction path.
4. **Summarize the latest run first** with `python tools/measurements/pipeline/latest_run_report.py` so you can see discovery/fetch counts, `site_changed` reconciliation, and parser-regression queue presence without opening the raw JSON.
5. **Re-check job count** after the next run: compare `data/jobs-unified.json` length and `data/jobs-browser-fallback-queue.json` size again to see if Scrapy-Playwright and any selector tweaks increase the total job count.

---

## 5) How to reproduce and re-check

```bash
# From repo root, with PYTHONPATH set
cd c:\Users\Andrea\Documents\GitHubRepository\Baluffo
$env:PYTHONPATH = (Get-Location).Path
python src/jobs_fetcher.py --ignore-circuit-breaker
```

Then:

- **Total jobs:** `Get-Content data\jobs-unified.json | ConvertFrom-Json | Measure-Object | Select-Object -ExpandProperty Count` (or count top-level array length).
- **Browser queue size:** `(Get-Content data\jobs-browser-fallback-queue.json | ConvertFrom-Json).Count`
- **Failed sources:** In `data/jobs-fetch-report.json`, count `sources[]` with `status == "error"`.

See [docs/scraping-pipeline.md](scraping-pipeline.md) for full flow and before/after comparison steps.
