# Browser Fallback Pool (single Chromium per pipeline run)

> - **Status:** Implemented 2026-08-11, commit `025b4522`
> - **Problem:** every browser fallback launched a fresh Chromium (`try_fetch_with_playwright`); measured 41–44 launches per 50-source bench, 2–5 s each on constrained containers
> - **Fix:** one lazy Chromium per fetch stage; fresh `BrowserContext` per call
> - **Working draft:** `.opencode/plans/browser-fallback-pool-2026-08-11.md` (superseded)

## Design (as implemented)

`src/jobs/browser_fallback_pool.py` — `BrowserFallbackPool`:

- **One Chromium per fetch stage**, launched lazily on first acquisition.
- **Dedicated dispatcher thread** running an asyncio loop. Sync Playwright is
  greenlet-bound to its creating thread, so a naive shared pool is impossible;
  worker threads submit fetches via `asyncio.run_coroutine_threadsafe`. One
  browser serves concurrent per-call contexts (concurrency preserved at
  `browser_fallback_max_workers`).
- **Fresh `BrowserContext` per call** — cookies/localStorage never bleed
  across servers (matches launch-per-call semantics).
- **API:** `pool.fetch(url, timeout_s) -> (html, error)`; never raises;
  errors normalized through `normalize_browser_fallback_error` so the
  existing `BrowserFallbackCircuitBreaker` token list still matches
  (`target closed`, `browser has been closed`, …).
- **Kill switch:** `BALUFFO_BROWSER_POOL=0` → legacy `try_fetch_with_playwright`
  launch-per-call path (checked in `run_source_execution_stage`).
- **Metrics:** `pool_startup_ms`, `pool_acquisitions`, `pool_relaunch_count`
  emitted as a progress line at stage end
  (`[jobs_fetcher] INFO browserPool acquisitions=N startupMs=M relaunchCount=R`).

### Wiring

- `src/jobs/pipeline_stage_source_execution.py` — `_build_capped_try_playwright`
  takes optional `pool=`; uses `pool.fetch` as the semaphore-capped target.
- `src/jobs/pipeline_source_loop.py` — pool created per
  `run_source_execution_stage` when the kill switch is off; closed in `finally`
  (browser process killed even on abort).
- `try_fetch_with_playwright` (`src/bridge/source_check_http.py`) untouched —
  legacy un-pooled path for admin source checks and tests.
- `BrowserFallbackCircuitBreaker` (`src/jobs/browser_fallback.py`) untouched —
  still owns cooldown + failure counting. Browser death → error token match →
  30-min cooldown. No inline retry added.

## Bench evidence (subset-50, mw=4, pi4-tight)

Artifacts: `_out/perf-pipeline/subset50-pool/` incl. `SUMMARY_POOL.md`.

| Metric | Value |
|---|---:|
| `playwright_fallback_used` triggers | 44 |
| Pool acquisitions | 43 |
| **Chromium launches** | **1** (startup 559 ms) |
| relaunches | 0 |
| Fetch stage wall (50 sources, mw=4) | 7.5 min |
| Memory during fetch | 1.00 GiB (66.9%), 0 OOM |

Legacy equivalent: 43 launches × 2–5 s ≈ 90–215 s of launch overhead removed
before any page time. 43 acquisitions through one resident browser.

## Tests

`tests/test_browser_fallback_pool.py` (7 tests, stdlib-only, real headless
Chromium against a localhost `http.server`): html fetch, 4-thread concurrency,
cookie isolation, double-close idempotency, circuit-breaker token match,
bad-URL error path, kill-switch parsing. Plus updated
`test_pipeline_source_loop.py` / `test_pipeline_stage_source_execution.py`
(stage tests stub the helper; autouse fixture disables the pool).

## Rollback

- `BALUFFO_BROWSER_POOL=0` on the container → legacy path, immediate, no redeploy.
- Code removal: drop the `pool` kwarg + kill-switch guard; restore
  `_build_capped_try_playwright` body. No persisted state.

## Not done (deliberate)

- **Watchdog thread / crash auto-relaunch** — cooldown via existing circuit
  breaker; add if crashes slip through.
- **Obscura engine swap** — the pool gets ~85% of the win with zero new deps;
  A/B Obscura only if still blocked-source-bound.
- **Pool for discovery / admin checks** — fetch-only scope per signoff.
- **Sidecar Chromium container** — bigger architecture, not justified at
  pi4-tight scale.

## Related finding (separate, open)

Finalize-phase grind on this seed: `fetch/applying_lifecycle` ran CPU-hot for
20+ min after fetch without completing; same phase never completed in H1/H2
(see `SUMMARY_POOL.md`). Not pool-related; lifecycle functions are O(n)
dict-indexed on 42k × 71k rows, so the hot loop is unexplained by visible
code. Track separately.
