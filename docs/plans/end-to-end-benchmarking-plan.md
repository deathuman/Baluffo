# End-to-End Benchmarking &amp; Responsiveness Plan

> - **Status:** Active plan
> - **Use this when:** adding frontend/backend instrumentation, profiling pipeline operations, fixing UI stalling (especially Admin page), or setting up CI performance regression detection
> - **Canonical for:** frontend User Timing instrumentation, Long Task detection, Playwright performance traces, profiling hooks (BALUFFO_PROFILE=1), sync operation timing, NDJSON perf trend tracking, CI smoke benchmark, startup profile regression gate, and Admin-page boot‑sequence optimizations
> - **Not canonical for:** pipeline/discovery timing payload shapes (use `pipeline_timing.py`, `runtime_metrics.py`), fetcher metrics contracts (use `fetcher_metrics.py`), startup probe event schema (use `startup_profile.py`, `startup_telemetry.py`), Admin task/progress UX (use `plans/task-progress-operational-console-plan.md`), or source‑sync production hardening (use `plans/source-sync-production-readiness-plan.md`)
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../startup-probe-architecture.md`](../startup-probe-architecture.md), [`../testing.md`](../testing.md), and the source files listed per phase below
> - **Last updated:** 2026-05-04

## Verdict

Baluffo already has **strong backend aggregate timing** — pipeline stage totals, discovery stage/adapter breakdowns, fetcher run‑history statistics, and a 9‑stage cold/warm startup profile with explicit ms thresholds. Three standalone benchmark scripts exist for discovery, fetch incremental, and startup probe pairs. However:

1. **Frontend rendering is a blind spot** — only 3 `performance.now()` calls across the entire frontend. No User Timing marks/measures, no Long Task observer, no Web Vitals.
2. **No profiling** — no `cProfile`/`py-spy`/`pyinstrument` integration. Adapter‑level timing exists but cannot reveal *which function inside* a slow adapter burns time.
3. **No CI performance regression detection** — benchmark steps are absent from all workflows.
4. **No performance history trends** — every benchmark run is a standalone snapshot with no cross‑run comparison.
5. **Sync operations have no timing instrumentation** — unlike discovery and fetch which have full stage‑timing systems.
6. **No reusable lightweight instrumentation pattern exists** — the codebase exclusively uses ad‑hoc `time.perf_counter()` scattered across individual functions; no decorators, context managers, or middleware.

### Admin‑page boot is the primary stalling surface

A deep‑dive of the Admin page boot sequence (`frontend/admin/app/runtime.js:249-317`) revealed:

| Issue | Location | Impact |
|-------|----------|--------|
| ~18 HTTP requests launched at boot | `auth.js:31-67` | Browser connection queuing (HTTP/1.1 6‑per‑origin limit) |
| `cacheAdminDom()` — ~50 synchronous DOM queries | `dom.js:3-103` | Blocks main thread before any async work |
| Redundant `loadSyncStatus` double‑call | `health.js:551` + `auth.js:65` | Duplicate bridge round‑trip |
| `loadOpsHealthData` double‑fire within 900ms | `auth.js:52` (setTimeout 900ms) + `auth.js:62` (immediate) | Duplicate ops health fetch |
| `buildDiscoveryRegistrySignature` — `JSON.stringify` on entire registry | `registry/load.js:63-86` | CPU block proportional to registry size on every load |
| 12+ sequential `innerHTML` assignments (ops section) | `health.js:494-550` | Each triggers DOM layout recalculation |
| 5+ sequential `innerHTML` assignments (discovery section) | `registry/load.js:150-170` | Same layout thrashing pattern |
| No `requestIdleCallback` or deferred rendering | All boot render paths | All heavy DOM work runs synchronously on data‑fetch resolution |
| Tight 500ms polling during active tasks | `fetcher/watch.js:116-126`, `discovery/watch.js:184-194` | Main‑thread churn on each tick |

This plan addresses the observational gap first (Phase 1), then exploits the new data to apply optimisations (Phase 3), and finally prevents regression (Phase 4).

---

## Conflict &amp; Compatibility Notes

| Existing plan | Compatibility |
|---|---|
| **`plans/task-progress-operational-console-plan.md`** | No conflict. That plan owns the shared task‑run presenter, compact Current Runs rows, stalled/orphaned display states, and Admin task/progress UX. This plan does not change task‑run rendering or progress bar logic. The lightweight counter‑based instrumentation (1e) and Long Task observer (1b) will *feed data into* the task‑run view model without altering its interface. |
| **`plans/source-sync-production-readiness-plan.md`** | No conflict. This plan adds sync stage‑timing instrumentation (1f) that slots into the existing sync‑service structure. The sync plan owns snapshot hardening, conflict handling, and BaluffoSync governance. Timing instrumentation is additive and orthogonal. |
| **`plans/saved-job-tracker-improvements-plan.md`** | No conflict. Saved page is out of scope for this plan. |
| **`../startup-probe-architecture.md`** | No conflict. This plan extends the existing startup‑probe JSONL pipeline with additional frontend User Timing marks and Long Task events. The event schema and storage path stay unchanged. |
| **`../archive/admin-health-dashboard-console-closeout.md`** | No conflict. The health‑dashboard owns compact Discovery/Fetch/Sync lanes and tabbed review surfaces. This plan's Admin quick wins (1e‑bonus) optimise boot timing without changing layout or UX. |

---

## Main gaps

### 1) No frontend User Timing marks

The startup‑probe system already captures desktop‑boot events (launch → site ready → window created → window shown → page loaded → local data ready → auth ready → first render → first interactive). But within each page, no `performance.mark()` / `performance.measure()` calls exist. We cannot answer "how long did the Admin discovery section take to render?" or "which of the 12+ innerHTML writes is the slowest?"

### 2) No main‑thread stall detection

Without a `PerformanceObserver` for `"longtask"` entries, every UI stutter is invisible. The Admin page's synchronous CPU work (`cacheAdminDom`, `JSON.stringify`, 12+ sequential innerHTML writes) is the most likely stalling source, but we lack hard evidence.

### 3) No per‑operation profiling

Aggregate `time.perf_counter()` calls track how long each source takes but not *why*. Without `cProfile`/`py-spy`, hotspots inside adapters (e.g., slow `BeautifulSoup` parsing, excessive HTTP redirects, inefficient list comprehensions) are invisible until a human reads the code.

### 4) No Client‑side fetch timing

The bridge has no request‑timing middleware. The frontend has no fetch‑timing wrapper. We cannot measure "how long did `GET /ops/fetcher-metrics` take on the wire?" vs "how long did the frontend spend parsing its response?"

### 5) No CI benchmark gate

Benchmarks run manually. PRs can silently regress discovery duration, fetch duration, or startup time without any signal.

### 6) No cross‑run trend data

Every benchmark overwrites its `_out/` output. No persistent history file allows comparing "was this week's fetch faster or slower than last week's?"

### 7) No sync timing

Discovery and fetch have `runtime_metrics.py` and `pipeline_timing.py`. Sync has no equivalent — no stage durations, no per‑phase timing, no structured runtime payload.

---

## Phase 1 – Instrument the blind spots

### 1a – Frontend User Timing marks

Add `performance.mark()` / `performance.measure()` at every identifiable lifecycle step in each page's boot sequence, wiring into the existing startup‑probe JSONL pipeline (`POST /desktop-local-data/startup-metric`).

**Target Admin page boot steps** (`frontend/admin/app/runtime.js:249-317`, `frontend/admin/app/auth.js:31-67`):

| Step | Mark name | Location |
|------|-----------|----------|
| cacheAdminDom start/end | `admin_dom_cache_start` / `admin_dom_cache_end` | `runtime.js:251` → `dom.js:3-103` |
| Auth init start/end | `admin_auth_init_start` / `admin_auth_init_end` | `auth.js:31-67` wrapper |
| Bridge status fetch start/end | `admin_bridge_status_start` / `admin_bridge_status_end` | `bridge-status.js:55` |
| Overview data fetch start/resolve | `admin_overview_fetch_start` / `admin_overview_fetch_done` | `overview.js:61-80` |
| Discovery data fetch start/resolve | `admin_discovery_fetch_start` / `admin_discovery_fetch_done` | `registry/load.js:88-211` |
| Discovery render start/end | `admin_discovery_render_start` / `admin_discovery_render_end` | `registry/load.js:150-170` |
| Ops health fetch start/resolve | `admin_ops_health_fetch_start` / `admin_ops_health_fetch_done` | `health.js:388-572` |
| Ops alerts render start/end | `admin_ops_alerts_render_start` / `admin_ops_alerts_render_end` | `health.js:494-550` each section |
| Sync fetch start/resolve | `admin_sync_fetch_start` / `admin_sync_fetch_done` | `sync.js:120-138` |
| First render timestamp | `admin_first_render` | Existing `markAdminFirstInteractive` |

**Target Jobs page** (`frontend/jobs/app/feed.js`):

| Step | Mark name |
|------|-----------|
| Page boot start | `jobs_boot_start` (already `jobs_init_start` on line 49) |
| Start-up preview resolve | `jobs_preview_ready` |
| Full feed fetch start/resolve | `jobs_feed_fetch_start` / `jobs_feed_fetch_done` |
| Render start/end | `jobs_render_start` / `jobs_render_end` |
| First interactive | `jobs_first_interactive` (already exists) |

**Target Saved page** (`frontend/saved/app/runtime/boot.js`):

| Step | Mark name |
|------|-----------|
| Boot start | `saved_boot_start` |
| Local data init ready | `saved_local_data_init_ready` (already exists) |
| Auth ready | `saved_auth_ready` (already exists) |
| First render | `saved_first_render` (already exists) |
| First interactive | `saved_first_interactive` (already exists) |

**Implementation approach:**
- Add a `markStep(name)` helper in a new shared module `frontend/shared/perf-marks.js` that calls `performance.mark(name)` and, when `BALUFFO_STARTUP_PROBE` is set, emits the metric to the bridge via the existing `emitMetric` pattern
- In each page's boot composition, wrap existing function calls with `markStep('section_start')` / `markStep('section_end')` and create `performance.measure('section', 'section_start', 'section_end')`
- Keep the helper zero-cost when not in probe mode (no-op function)

### 1b – Long Task observer

Add a `PerformanceObserver` for `"longtask"` entries on all three pages to detect and report main‑thread blocking.

**Implementation** (`probes/long-task-observer.js` — new shared module):

```js
export function observeLongTasks(emitMetric) {
  if (typeof PerformanceObserver === "undefined") return;
  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      emitMetric("long_task", {
        duration: entry.duration,
        startTime: entry.startTime,
        name: entry.name,
        entryType: entry.entryType,
        attribution: entry.attribution?.map(a => ({
          name: a.name,
          duration: a.duration,
          containerType: a.containerType,
          containerName: a.containerName,
          containerId: a.containerId,
        })),
      });
    }
  });
  observer.observe({ type: "longtask", buffered: true });
}
```

- Wire into each page's boot composition (`runtime.js` level for admin, jobs, saved)
- Emit through existing `POST /desktop-local-data/startup-metric` path when `BALUFFO_STARTUP_PROBE` is set
- The `attribution` array identifies which script/container caused the long task, directly answering "what is stalling?"

### 1c – Playwright performance traces

Extend existing Playwright E2E tests (`tests/frontend/*.spec.mjs`) to capture Chrome DevTools Protocol traces.

**Implementation:**

```js
import { chromium } from "@playwright/test";

const browser = await chromium.launch();
const context = await browser.newContext();
const page = await context.newPage();

await context.tracing.start({ screenshots: true, snapshots: true });
await page.goto("http://localhost:PORT/admin/index.html");
await context.tracing.stop({ path: "_out/perf-traces/admin-boot-trace.zip" });
```

Add per‑test assertions:
- `page.evaluate(() => performance.getEntriesByType("paint"))` → report First Paint, First Contentful Paint
- `page.evaluate(() => { const l = new PerformanceObserver(() => {}); ... })` → capture LCP if available
- Capture total navigation timing via `performance.getEntriesByType("navigation")[0].domContentLoadedEventEnd`

Create a new smoke test variant `test:frontend:perf` in `package.json` that runs these tracer tests against each page (admin, jobs, saved). Perf tests are excluded from the main smoke suite — run manually or in the CI perf workflow (Phase 4a).

### 1d – Profiling hooks (`BALUFFO_PROFILE=1`)

Add an opt-in profiling path using `cProfile` and `py-spy` around adapter execution and discovery probe stages.

**Implementation:**

New module `src/shared/profile_utils.py`:

```python
import os
import cProfile
import io
import pstats

PROFILE_ENABLED = os.environ.get("BALUFFO_PROFILE", "").strip().lower() in ("1", "true", "yes")


def run_profiled(fn, *args, profile_name="default", **kwargs):
    if not PROFILE_ENABLED:
        return fn(*args, **kwargs)
    profiler = cProfile.Profile()
    try:
        return profiler.runcall(fn, *args, **kwargs)
    finally:
        s = io.StringIO()
        pstats.Stats(profiler, stream=s).sort_stats("cumulative").print_stats(30)
        out_dir = Path(os.environ.get("BALUFFO_DATA_DIR", "_out")) / "perf-profiles"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{profile_name}.prof.txt").write_text(s.getvalue())
        profiler.dump_stats(str(out_dir / f"{profile_name}.prof"))
```

Wrap these call sites:

| Call site | Profile name | File |
|-----------|-------------|------|
| Each adapter `fetch()` in pipeline loop | `adapter_{name}_{source_id}` | `src/jobs/pipeline_stage_source_execution.py` |
| `run_discovery()` main entry | `discovery_full_run` | `src/source_discovery/orchestrator.py` |
| Each probe batch in discovery | `discovery_probe_batch_{adapter}` | `src/source_discovery/orchestrator_probe.py` |
| `sync_service.pull_and_merge_sources()` | `sync_pull_merge` | `src/bridge/sync_service.py` |

`py-spy` is an alternative for production‑like runs (no instrumentation overhead). Document in the plan but implement `cProfile` first since it works without extra dependencies.

### 1e – Lightweight always‑on counter‑based instrumentation

Add minimal reusable instrumentation that runs always (zero‑config) to accumulate structured timing counters.

**Implementation:**

New shared module `src/shared/timing_counters.py`:

```python
from __future__ import annotations
import time
from collections import defaultdict
from typing import Any

_timers: dict[str, list[float]] = defaultdict(list)


def record_duration(category: str, duration_ms: int) -> None:
    _timers[category].append(duration_ms)


class Timer:
    def __init__(self, category: str):
        self.category = category
        self._start: float | None = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args):
        if self._start is not None:
            record_duration(self.category, int((time.perf_counter() - self._start) * 1000))
```

Where to integrate:

| Location | Category | File |
|----------|----------|------|
| Bridge `do_GET`/`do_POST` handler | `bridge_request_{method}_{path}` | `src/bridge/server/handler.py:131-147` |
| Frontend fetch wrapper | `frontend_fetch_{endpoint}` | `frontend/admin/app/data-source.js` (new wrapper) |
| Frontend render helper | `frontend_render_{section}` | `frontend/shared/dom-utils.js` (new wrapper around innerHTML) |
| Pipeline adapter call | `adapter_run_{name}` | `src/jobs/pipeline_stage_source_execution.py` |
| Discovery stage | `discovery_stage_{stage}` | `src/source_discovery/runtime_metrics.py` |

Expose accumulated counters via a new bridge endpoint `GET /ops/perf-counters` (admin‑only, returns JSON of `{category: [p50, p95, count, sum]}`). This gives real‑time insight into per‑request latencies without external monitoring.

### 1e‑bonus – Admin page quick wins (low‑risk fixes from boot analysis)

These are direct fixes for waste identified in the boot sequence analysis. Implement before or alongside Phase 1 — they remove known overhead without changing architecture.

| # | Fix | File | Detail |
|---|-----|------|--------|
| Q1 | Remove redundant `loadSyncStatus` call | `health.js:551` | `loadSyncStatus` is already called at `auth.js:65`. The call inside `loadOpsHealthData`'s success handler is a duplicate. Remove it — sync data is already loaded by then. |
| Q2 | Cancel the 900ms setTimeout if immediate load fires first | `auth.js:52` | The `setTimeout(loadOpsHealthData, 900)` at boot creates a guaranteed second fetch. Save the timer id; clear it in `loadOpsHealthData`'s busy‑guard (`health.js:389-391`). |
| Q3 | Replace `JSON.stringify` registry hash with incremental digest | `registry/load.js:63-86` | Current `buildDiscoveryRegistrySignature` serialises the full pending/active/rejected set to compute a change‑detection string. Replace with a rolling hash (`adler32` or `fnv1a`) that iterates rows once. |
| Q4 | Defer `cacheAdminDom()` to lazy on‑access | `dom.js:3-103` | Replace the upfront ~50 `querySelector`/`querySelectorAll` calls with a `getDomRef(name)` function that caches the result on first access. The initial boot path only touches ~10 of the 50+ references — the rest pay cost only when first used. |
| Q5 | Wrap heavy renders in `requestIdleCallback` | `health.js:494-550`, `registry/load.js:150-170` | Non‑critical renders (trends, history, dedup lists, source tables) can be deferred with `requestIdleCallback` or a simple 50ms `setTimeout` chain. Critical renders (KPIs, alerts, bridge status) stay inline. |
| Q6 | Progressive section reveal | All boot renders | Render each section's placeholder immediately, then fill as its promise resolves. Currently all 6 discovery fetches must resolve before *any* discovery content appears. |

### 1f – Sync operation timing

Add the same stage‑timing pattern used by discovery (`runtime_metrics.py`) and fetch (`pipeline_timing.py`) to sync operations.

**Target:** `src/bridge/sync_service.py` and `src/source_sync_runtime.py`.

Add these stage keys:

| Stage | Measured from | Measured to |
|-------|---------------|-------------|
| `pullRemote` | Start of `pull_and_merge_sources` | Remote JSON fetched |
| `mergeLocalRegistry` | Remote JSON parsed | Local registry loaded and merged |
| `resolveConflicts` | Merge complete | Conflict resolution done |
| `applyLocal` | Local registry written | File write confirmed |
| `pushRemote` | Start of `push_sources_snapshot` | GitHub Contents API PUT complete |
| `totalSync` | Sync operation start | All stages complete |

**Runtime payload shape** (new module `src/bridge/sync_timing.py`):

```python
{
    "totalDurationMs": int,
    "stageTotalsMs": {"pullRemote": int, "mergeLocalRegistry": int, ...},
    "conflictCount": int,
    "tombstonesSuppressed": int,
    "rejectedSuppressed": int,
    "pushed": bool,
    "noOp": bool,
}
```

Wire into the existing `GET /sync/status` response under a new `"timing"` key. Store the last 20 sync timing records in `data/sync-timing-history.json` (NDJSON compatible).

---

## Phase 2 – Focused benchmarks

### 2a – Baseline data collection

Run each benchmark against the current state *before* any Phase 3 optimisations to establish a baseline.

| Benchmark | Command | Output |
|-----------|---------|--------|
| Discovery sanity | `npm run perf:discovery:benchmark` | `_out/perf-baseline/discovery-baseline.json` |
| Discovery + web search | `npm run perf:discovery:benchmark -- --include-web-search` | `_out/perf-baseline/discovery-web-baseline.json` |
| Fetch incremental | `python src/fetch_incremental_sanity_benchmark.py` | `_out/perf-baseline/fetch-baseline.json` |
| Startup cold | `npm run perf:startup:cold` | `_out/perf-baseline/startup-cold-baseline.json` |
| Startup warm | `npm run perf:startup:warm` | `_out/perf-baseline/startup-warm-baseline.json` |
| Startup pair | `npm run perf:startup:pair` | `_out/perf-baseline/startup-pair-baseline.json` |
| pytest timing | `npm run perf:py:timing` | Terminal output (not persisted) |

Each baseline run appends an NDJSON row to `_out/perf-trend.ndjson` (see Phase 4b).

### 2b – Adapter deep profiling

- Run `BALUFFO_PROFILE=1 python src/fetch_incremental_sanity_benchmark.py` to produce `.prof` files for each adapter
- Identify the top‑3 `cumulative` time sinks per adapter using `pstats`
- Cross‑reference with `pipeline_timing.py`'s `highCostLowYieldSources` and `detailHeavySources` metrics to prioritise which adapters to optimise in Phase 3

---

## Phase 3 – Optimisation (evidence‑dependent)

Ordered by expected impact based on Phase 1 &amp; 2 data. All items are conditional — measure first, optimise second.

### P0 – Admin boot parallelisation + lazy rendering

Based on the boot analysis (Phase 1e‑bonus Q5–Q6), apply:

- `requestIdleCallback` wrappers for: ops trends render (`health.js` ~line 540), dedup lists render (`health.js` ~line 533), source tables render (`registry/load.js:150-170`), ops history render (`health.js` ~line 522)
- Parallelise discovery registry fetches: currently `GET /discovery/report`, `GET /discovery/candidates`, `GET /registry/pending`, `GET /registry/active`, `GET /registry/rejected` fire simultaneously — move the static `data/jobs-fetch-report.json` fetch into the same `Promise.all` instead of the separate `resolveLatestFetchReport` call that follows
- Show each discovery section (pending, active, rejected) as its promise resolves rather than waiting for all 6 fetches

### P1 – Web Worker for CSV parsing

`frontend/jobs/parsing-utils.js:178` parses CSV on the main thread using `performance.now()` timing that already shows this is a tracked concern.

- Move `parseCSVRecords()`, `findCompanyColumnIndex()`, `findColumnIndex()` into a Web Worker (`frontend/jobs/parsing-worker.js`)
- Post CSV text to worker, receive parsed rows via `postMessage`
- Show a "Parsing N sources..." progress indicator during worker execution

### P1 – Virtual scrolling / pagination for jobs feed

With 40k entries growing, the jobs feed DOM becomes expensive.

- Evaluate current DOM node count under realistic filter conditions (pagination already exists at `currentPage` in feed state)
- If DOM exceeds ~5000 nodes, implement a virtual scroller that only renders visible + overscan rows (~30 DOM nodes regardless of dataset size)
- Prioritise only if Phase 1b Long Task data shows render‑time stalls

### P2 – Adapter‑level fixes

Based on Phase 2b profiling results:

- Reduce HTTP redirect chains in slow adapters (use `allow_redirects=False` + manual follow for redirect chains > 2 hops)
- Add `lru_cache` to adapter functions that parse the same URL pattern repeatedly
- Increase `max_per_domain` or `adapter_http_concurrency` for adapters with long idle times
- Address specific hotspots revealed by `cProfile` output

### P3 – Startup sequence dependency flattening

Based on startup probe data (Phase 2a):

- If `auth_ready_to_first_render` exceeds thresholds, evaluate lazy auth initialisation — defer auth check until first user interaction with auth‑gated features
- If `page_loaded_to_local_data_ready` is a bottleneck, add streaming JSON parsing for large local data files instead of loading the entire file into memory
- If `window_created_to_window_shown` is slow, investigate native window creation delay (likely Chromium launch flags or preload optimisation)

---

## Phase 4 – CI + Trends

### 4a – CI smoke benchmark

New GitHub Actions workflow `.github/workflows/benchmark.yml`:

```yaml
name: benchmark
on:
  pull_request:
    paths:
      - "src/jobs/**"
      - "src/source_discovery/**"
      - "src/bridge/**"
      - "src/fetcher_metrics.py"
      - "src/discovery_sanity_benchmark.py"
on:
  workflow_dispatch: {}
```

Steps:

1. Checkout + setup Python
2. Run discovery smoke benchmark: `python src/discovery_sanity_benchmark.py --timeout 30 --top 5` (quick mode: 5 sources, not 20)
3. Run fetch smoke benchmark: `python src/fetch_incremental_sanity_benchmark.py --timeout 30 --sources greenhouse_boards lever_sources` (2 adapters only)
4. Load baseline from `_out/perf-baseline/latest.json` if it exists
5. Compare `totalDurationMs`:
   - `>15%` regression → ❌ fail
   - `>5%` regression → ⚠️ warning annotation on PR
   - `<=5%` → ✅ pass
6. If baseline does not exist, create it (informational run, no pass/fail)

The smoke benchmark must stay under 60s total wall‑clock to avoid delaying PR merges.

### 4b – NDJSON trend tracking

Each benchmark run (manual or CI) appends a single NDJSON row to `_out/perf-trend.ndjson`:

```jsonl
{"ts":"2026-05-04T12:00:00Z","mode":"discovery","totalDurationMs":45200,"sourceCount":18,"adapterCount":9,"wallClockMs":48200,"commitSha":"abc123","status":"pass"}
{"ts":"2026-05-04T12:05:00Z","mode":"fetch","totalDurationMs":128000,"sourceCount":12,"adapterCount":8,"wallClockMs":135000,"commitSha":"abc123","status":"pass"}
```

New npm script `perf:trend` that reads the last 20 entries and prints a delta table:

```
mode        date           duration  vs prev  vs baseline
discovery   2026-05-04     45.2s     --       --
discovery   2026-05-11     42.1s     -6.9%    -6.9%
fetch       2026-05-04     128.0s    --       --
fetch       2026-05-11     131.2s    +2.5%    +2.5%
```

Implementation: simple Python script `scripts/perf_trend.py` that reads `_out/perf-trend.ndjson` and outputs formatted text.

### 4c – Startup profile regression gate

Add threshold enforcement to the existing startup probe flow.

**Target:** `src/ship/startup_profile.py:summarize_startup_metrics` and `src/packaged_desktop_smoke.py`.

- After `summarize_startup_metrics()` computes stage durations, compare each against `PROFILE_THRESHOLDS_MS`
- If `total_launch_to_first_usable_ui > 18s` (cold) or `>12s` (warm), emit a `"perf_regression"` entry in the startup metric with `{stage: "total_launch_to_first_usable_ui", durationMs: N, thresholdMs: 18000, severity: "critical"}`
- In `packaged_desktop_smoke.py` (the `--startup-probe` path), check the summarised status. If status is `"failed"` and the `--profile-only` flag is set, exit with code 1 and print the bottleneck classification
- The CI perf workflow (4a) can optionally call `npm run perf:startup:cold -- --fail-on-threshold` to gate on startup time

---

## Implementation order (recommended)

```
Quick wins (highest confidence, lowest effort)
├── 1e-bonus Q1 (rm loadSyncStatus double-call)       ~15 min
├── 1e-bonus Q2 (cancel redundant opsHealth timeout)   ~15 min
├── 1e-bonus Q4 (lazy cacheAdminDom)                  ~30 min
├── 1a (User Timing marks – Admin first)               ~1.5 hr
├── 1b (Long Task observer)                            ~1 hr
├── 1e (lightweight counters – bridge handler only)    ~1 hr
├── 1c (Playwright traces – single page)               ~1.5 hr

Deep instrumentation
├── 1a (User Timing marks – jobs, saved)               ~1 hr
├── 1e (counters – frontend fetch, render wrappers)    ~1 hr
├── 1d (BALUFFO_PROFILE=1 hooks)                       ~2 hr
├── 1f (sync timing)                                   ~2 hr

Baseline measurement
├── 2a (run all benchmarks, create baseline)           ~1 hr
├── 2b (profile top adapters)                          ~2 hr
├── 4b (NDJSON trend script)                           ~1 hr

Optimisation (evidence‑dependent)
├── P0 (Admin boot parallelisation)                    ~2 hr
├── P1 (Web Worker / virtual scroll)                   ~4 hr
├── P2 (adapter fixes)                                 ~4 hr
├── P3 (startup flattening)                            ~2 hr

CI + regression gates
├── 4a (CI smoke benchmark workflow)                   ~2 hr
├── 4c (startup profile regression gate)               ~1 hr
├── 1e-bonus Q3 (incremental digest)                   ~1 hr
├── 1e-bonus Q5/Q6 (idleCallback + progressive)        ~1.5 hr

Total estimate: ~30 hr (spread across the above items)
```

---

## Verification commands

```powershell
# Phase 1a – User Timing marks present in each page
node --test "tests/frontend/unit/perf-marks.test.mjs"

# Phase 1b – Long Task observer works
node --test "tests/frontend/unit/long-task-observer.test.mjs"

# Phase 1d – Profiling produces output
$env:BALUFFO_PROFILE="1"; python src/discovery_sanity_benchmark.py --timeout 5 --top 3
Test-Path "_out/perf-profiles/discovery_full_run.prof"

# Phase 1e – Counter instrumentation zero‑cost when disabled
python -m pytest tests/test_pipeline_execution.py -q --durations=5

# Phase 1f – Sync timing payload shape
python -m pytest tests/test_bridge_sync_service.py -q -k "timing"

# Phase 2 – Baseline benchmarks complete without error
npm run perf:discovery:benchmark -- --timeout 10 --top 5
npm run perf:startup:cold

# Phase 4a – CI smoke benchmark (dry run)
python src/discovery_sanity_benchmark.py --timeout 30 --top 5
python src/fetch_incremental_sanity_benchmark.py --timeout 30 --sources greenhouse_boards lever_sources

# Phase 4b – Trend script parses existing data
python scripts/perf_trend.py

# Phase 4c – Startup threshold gate
python src/packaged_desktop_smoke.py --startup-probe --profile-only --profile-mode cold

# General – no lint regressions from any instrumentation
npm run lint:precommit
```

---

## Success criteria

1. Every page (jobs, saved, admin) emits structured `performance.mark()` entries at each boot lifecycle step, visible via `performance.getEntriesByType("mark")` and persisted in startup‑probe JSONL when `BALUFFO_STARTUP_PROBE` is set.
2. Long Task entries are captured and persisted for all three pages, identifying which scripts/containers cause main‑thread blocking.
3. Playwright traces capture FCP and LCP for each page during E2E perf tests.
4. `BALUFFO_PROFILE=1` produces `.prof` files in `_out/perf-profiles/` for adapter, discovery, and sync operations without breaking normal execution.
5. Lightweight timing counters accumulate per‑operation durations with negligible overhead (no measurable wall‑clock impact on baseline benchmarks).
6. Sync operations report stage‑level timing through `GET /sync/status`.
7. CI smoke benchmark runs under 60s and rejects >15% regressions in discovery/fetch duration.
8. `_out/perf-trend.ndjson` accumulates cross‑run data; `npm run perf:trend` prints meaningful deltas.
9. Startup probe exits non‑zero when thresholds are exceeded and `--fail-on-threshold` is set.
10. Admin boot analysis quick wins (Q1–Q6) are implemented, measurably reducing the number of redundant HTTP requests and synchronous DOM work at boot.
11. All instrumentation is zero‑cost when disabled (no measurable overhead in normal operation).
12. No lint or test regressions from any instrumentation or optimisation change.
