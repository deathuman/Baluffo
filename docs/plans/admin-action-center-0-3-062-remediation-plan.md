# Action Center remediation plan — umbrel container 0.3.062

> - **Class:** shipped-defect
> - **Trigger:** an operating umbrel container on `0.3.062` whose Admin Action Center raised a
>   critical storage signal and a browser-fallback signal after the 2026-11-15 10:54 UTC fetch;
>   its "Copy all diagnostics" payload is the evidence below
> - **Verified against:** `215f970d` (source, tests, and docs read from this tree; the deployed
>   0.3.062 image predates `0.3.063`, so every claim here is about behaviour the tree still has)
> - **Status:** Active follow-up — S1.1–S1.4 and S2.1–S2.4 implemented (uncommitted); S3 measured
>   on a full WSL Linux replay. S2.5 container evidence remains; nothing here has been re-tested
>   against the live container.

## What the report shows

Three signals are live in that payload (the other two, `stale_fetch` and `sync_status`, are not:
the last successful fetch is 8 h old against a 12 h threshold, and `lastResult` is `ok`).

| Signal | Payload evidence |
|---|---|
| `storage_health` (critical) | route `ok: false`; `storage.healthy: false`; `quickCheck: "deferred-large-database"`, `quickCheckDeferred: true`, `quickCheckMaxDatabaseBytes: 67108864` against `databaseBytes: 286019584`; `lastWriteError: ""`; `writeLock.held: false`; `walMode: "wal"`; `foreignKeys: 1`; `migrationVersion: "009"`; all 13 `diagnostics` `ok: true` |
| `browser_fallback_health` (warning) | probe `ok: false`, `stage: "navigation"`, `durationMs: 1332`, `errorType: "TargetClosedError"`, `error: "BrowserContext.new_page: Target page, context or browser has been closed"`, `lastFailureAt: 2026-11-15T10:51:49Z`; `browserFallbackAttemptedSources: 264`, `browserFallbackFailedSources: 264`, `browserFallbackRecommendedSources: 65` |
| `failed_sources` (warning) | `failedSources: 436` of `totalSources: 2465` (`failedSourceRatioLatest: 0.1769`); `zeroKeptSources: 1945`, `zeroKeptNeedsReviewSources: 1938`, `zeroKeptNeedsReviewNonFailedSources: 1502`; `providerChildFailureCount: 126`; `pendingSourcesCount: 864` |

The same storage payload also carries, under `walMaintenance`:
`status: "failed"`, `reason: "write"`, `mode: "TRUNCATE"`, `durationMs: 5118`,
`error: "SQLite WAL checkpoint failed after busy retry attempts: (1, -1, -1)"`, and the sizes it
recorded **before** that attempt — `databaseBytes: 555212800`, `walBytes: 997831072`,
`freelistBytes: 257609728`. The live sizes in the same payload are 286 019 584 and 0.

`health` reports `status: "healthy"` with `alerts: []`, `alertsEvaluated: true`,
`alertBasis: "history"` — so the summary surface and the Action Center disagree about the same
container, and `_meta.partial: false` says the Action Center had all three payloads.

## Issue 1 — the critical storage signal is a latched flag, not an integrity failure

### What the operator is told

`evaluateStorageHealth` (`frontend/admin/app/action-center.js`) renders
**"SQLite integrity check failed"** for any `storage.healthy === false`. In this payload no
integrity check ran: the database is 286 MB against a 64 MiB quick-check cap, so
`quick_check_for_health` returns `deferred-large-database` and the store counts a deferred check
as acceptable (`quick_check_ok = quick_check == "ok" or quick_check_deferred` in `health()`).

### What actually set the flag

1. `healthy` is `self._healthy and quick_check_ok` (`src/storage/baluffo_store.py`, `health()`).
   Deferred counts as ok, so `false` here can only be `_healthy`.
2. `_healthy` is initialised `True` exactly once (`__init__`, `:244`) and set `False` by failure
   paths in `_initialize_writer` (`:391`, `:401`), `write` (write-lock timeout `:500`, terminal
   busy `:532`, other `:543`), `_read_with_retry` (`:784`), and `checkpoint_required` (`:961`).
   **No path sets it back.** A successful write clears `_last_write_error` (`write`, `:525`) but
   not the flag, which is exactly why this payload has `healthy: false` with an empty
   `lastWriteError`.
3. The path that fired is `_run_wal_maintenance` → `checkpoint_required("TRUNCATE")`, scheduled
   from `reason: "write"` because the WAL was at or above the 256 MiB truncate threshold. The
   result `(1, -1, -1)` is a **busy** checkpoint: nothing was checkpointed. TRUNCATE needs the WAL
   quiescent; the store's own persistent read connection plus the pipeline's reads make that
   unattainable during a run, and ten busy retries (`DEFAULT_BUSY_RETRY_ATTEMPTS = 10`) cannot
   outlast a reader.
4. `_run_wal_maintenance` records `status: failed` with the pre-attempt sizes and `_healthy = False`,
   then raises. The store now reports unhealthy for the rest of the process's life, and
   `wal_maintenance_status()` carries no timestamp, so no surface can say when a ~1 GB WAL was
   observed. The sizes it captured (555 212 800 / 997 831 072 / freelist 257 609 728) match
   neither the rebuild journal's staging size (255 619 072) nor the live database (286 019 584),
   so this payload cannot even date the attempt.

Two costs beyond the misleading label: a 1.0 GB WAL beside a 555 MB database is a disk-usage risk
the panel describes as "integrity check failed", and a genuine corruption would now look identical
to this.

### Remediation

**S1.1 — make health attributable and recoverable.** Record
`unhealthyReason: {code, message, at}` at each `_healthy = False` site with a stable code
(`write_lock_timeout`, `busy_exhausted_write`, `busy_exhausted_read`, `wal_checkpoint_busy`,
`write_error`, `startup_probe`, `pragma`). Clear it and restore `_healthy = True` after a
successful write transaction (the same place that clears `lastWriteError`), a successful
checkpoint, and a successful VACUUM. The existing
`tests/storage/test_baluffo_store_write_path.py::test_required_checkpoint_failure_marks_store_unhealthy`
is rewritten to pin a **non-busy** failure: it still raises and reads `healthy: false` immediately
after the failure, while a busy result is a maintenance state (S1.2) rather than a latch.

**S1.2 — stop treating a busy checkpoint as an integrity event.** In `checkpoint_required`, run
PASSIVE first (it checkpoints everything no reader is pinning), escalate to FULL/TRUNCATE only when
the passive result reports `logFrames == checkpointedFrames`, and on busy record a new
`status: "busy"` — not `failed` — keeping `_healthy` true and rescheduling on the existing 60 s
cooldown. Add `attemptedAt` to the status so the operator can date it. Keep the disk-pressure
signal: `walBytes`/`databaseBytes` in the status are the actionable numbers, as the docs already
expect (`docs/TROUBLESHOOTING.md`, *Long gap between `desktop_bridge_configured` and
`desktop_bridge_registry_ready`*). Two callers already do this correctly and are the shape to
copy: the bulk-delete final checkpoint logs a failure as a warning instead of raising
(`baluffo_store.py:630`), and `install_rebuilt_database` runs its own
`PRAGMA wal_checkpoint(TRUNCATE)` and returns `reason: "checkpoint-busy"` rather than latching
health. `backup_to` (`:2448`) is the outlier: it calls `checkpoint_required("TRUNCATE")`
unguarded, so one busy backup also marks the store unhealthy for good.

**S1.3 — attribute the copy.** `evaluateStorageHealth` chooses its summary by reason: claim
"integrity check failed" only when `quickCheck` is a real non-`ok` value; for
`wal_checkpoint_busy` say e.g. "WAL checkpoint blocked — 1.0 GB WAL on a 555 MB database
(busy)"; for `healthy: false` with a deferred check, name the recorded reason and state that the
integrity check was deferred, not failed. Include the attempt time.

**S1.4 — one verdict, two surfaces.** *(Decided: option A.)* `/ops/fetch-kpis?view=summary` said
`status: healthy`, `alerts: []` while the Action Center showed a critical storage signal. The
summary now appends a `storage_unhealthy` alert built from the same bounded, cached storage-health
read the storage route serves: `critical` for a real integrity failure, `warning` for a busy or
errored checkpoint (including the S1.2 healthy-but-busy shape, with WAL/database sizes and the
attempt time), and no alert when clean. `alertBasis` becomes `history+storage`
(`history-active-run+storage` during a run) so the field names what the alert set covers. Pinned by
`tests/admin/test_admin_bridge_ops_health.py`.

**Acceptance.** With a `wal_checkpoint_busy` reason the panel shows a dated warning naming the WAL
size and makes no integrity claim; `healthy` returns to true after the next successful commit; a
real `quickCheck != "ok"` still renders critical; no existing storage or Action Center test is
weakened.

## Issue 2 — the browser lane delivered nothing, and one death can end it for a run

### What the numbers can and cannot prove

`browserFallbackAttemptedSources == browserFallbackFailedSources == 264` proves the lane produced
no HTML for any source it was asked about. It does **not** prove 264 crashes: `attempted` is set
whenever the lane is budget-eligible (`src/jobs/adapters/static_listing_runner.py:430`), and that
includes a call the circuit breaker then refuses during its 30-minute cooldown
(`src/jobs/browser_fallback.py`). One environment failure plus 263 refusals yields the same pair
of numbers. `runtime.browserFallbackDemand` (`attempts`, `refused`, `servedEmptyEnvironment`,
`servedEmptyPage`, written by `pipeline_source_loop`) is the only field that separates them, and it
is not in the summary payload the Action Center reads.

### What the pool does with a dead browser

- `_fetch_captured` calls `self._browser.new_context()` and `context.new_page()` outside any
  death-marker handling; only `page.goto` failures call `_mark_unavailable`
  (`src/jobs/browser_fallback_pool.py`). The observed error is at `new_page`.
- `_ensure_started` returns early whenever `self._browser is not None`, so a closed browser handle
  is reused by every later call instead of being relaunched.
- `_available = False` (set only from the `goto` and probe-navigation paths) is itself a latch: `_ensure_started` then
  raises "browser fallback unavailable" until `health_probe()` or `close()` calls
  `_hard_close_locked`. `_maybe_recycle_browser` counts only successful acquisitions, so a dead
  browser never reaches the 20-acquisition recycle.
- The end-of-run probe does reset and relaunch — and it *also* failed at `new_page` after 1 332 ms.
  That is the part that points at the environment rather than the pool state: a freshly launched
  Chromium was unusable too, 3 minutes before the run finished successfully.

### Remediation

**S2.1 — detect death and relaunch, boundedly.** In `fetch_captured`'s `except`, treat the death
marker (extend to `TargetClosedError` by type as well as text) as "the browser died": reset the
pool through `_hard_close_locked`, retry the fetch once on a fresh browser, cap at two relaunches
per death episode (a successful acquisition resets the budget), count them
(`pool_relaunch_on_death`), and spend at most one immediate recovery probe per run.

**S2.2 — check liveness, not identity.** `_ensure_started` calls `self._browser.is_connected()`
and relaunches when it is false, instead of trusting a non-`None` handle. This is the one-line
version of the same fix, and it should land with S2.1 rather than instead of it.

**S2.3 — replace the sticky `_available` latch** with consecutive-launch-failure accounting bounded
by the breaker's cooldown, so one death cannot disable the lane for the rest of the run.

**S2.4 — put the demand split in the summary.** Carry the five `browserFallbackDemand` integers
into `compute_ops_fetch_kpis_summary` (`src/bridge/ops_api_health.py`) and render
"refused in cooldown / empty-environment / empty-page" in the Action Center row. Without it the
next report repeats this one's ambiguity.

**S2.5 — settle the container-side cause.** The probe dying 1.3 s after launch is not explained by
any pool-state bug. Read-only checks on the host: the container's cgroup `memory.max` and
`memory.events` (`oom`/`oom_kill`), `docker logs` for `chrome_crashpad`/`GPU process launch
failed`, and `/dev/shm` size. The flags already shipped
(`--disable-dev-shm-usage`, `--in-process-gpu` on Linux, `--renderer-process-limit=2`,
`--js-flags=--max-old-space-size=512`, `src/shared/browser_launch_args.py`) cover the documented
container failures; a cgroup OOM kill is the remaining candidate. Do not add flags before that
evidence exists.

**Acceptance.** A killed browser is relaunched once and later fallbacks in the same run still run;
a run whose browser cannot start reports a `launch`-stage failure, not `navigation`; the summary
distinguishes refused from crashed; `tests/test_browser_fallback_pool.py` gains the
death-then-relaunch and the refused-not-crashed cases.

## Issue 3 — 1 502 zero-kept sources cannot be judged until the reader works

This report must not be read as "1 502 dead boards":

- With 264 of 264 fallbacks failing, every JS-rendered board necessarily kept zero. Those rows are
  a reader failure, not a board finding — the exact shape the coverage skill's sanity battery exists
  to prevent.
- `failedSources` (436) and `zeroKeptNeedsReviewNonFailedSources` (1 502) are counts only; per-row
  `failureBucket`/`classification` is what separates 404s and redirects from client-rendered boards.
- `pendingApprovalsCount` is not a second queue: `compute_ops_fetch_kpis_summary` mirrors
  `pendingSourcesCount` into it.

**Order of work.** Fix S2 first, then run one real fetch and the offline half of
`tools/coverage_drain.py` with `--verify-collected` under an isolated `--data-dir`, bucket by
`failureBucket`, and only then consider retiring or repointing a board. Drain the 864 pending
candidates through the existing registry approval path, not by bulk state edits. No board is
retired from this payload alone.

**Measured 2026-11-15 (S3, full).** Full isolated replay on WSL2 Linux (2,557 source loaders
from the 3,019-row live registry; 41,801 jobs; 489 failed): **1,466 zero-kept**. Buckets:
`needs_review` 1,099, `site_changed` 286, `js_required` 55, `details_broken` 14, `seed_invalid`
5, `anti_bot_or_challenge` 5, `no_openings` **2**. Reader-side **360 (24.6%)**, explicit board
findings **2 (0.1%)**, other undecided 1,003 (68.4%), harness 5. Within `needs_review`, 96 rows
carry "no jobs extracted" and are the only board candidates; 335 of the 432 such errors are
already reader-bucketed. Browser lane: 777 attempts, 0 refused, 751 served HTML, 26 empty pages,
37 relaunches. One board held the run for 66 minutes despite the 90 s budget: a 7.8 MB detail page
(6,065 anchors) passed a 6.58 MB block to `html_fragment_lines` once per anchor; the 1,000,000
stage values are the report normalizer's cap, not 1,000 s. A follow-up probe of the 96 candidates
found **21 reader misses** (11 direct, 10 one-hop), **3 explicit empty**, **29 blocked/dead**, and
**43 no job evidence** (six probable-empty Teamtailor sites, 37 unverified). The earlier bounded
Windows sample (120 previously zero-kept boards) had 37 collected (30.8%) and 0 board findings.
Artifacts: `_out/coverage-062-s3-full/` and `_out/coverage-062-s3-sample/`. The replay ran on
WSL Linux, not the umbrel container; S2.5 container evidence remains open.

## Secondary observations in the same payload

Not Action Center signals; recorded so the next reader does not re-derive them.

| Observation | Evidence | State |
|---|---|---|
| Summary vs Action Center verdict disagree | `status: "healthy"`, `alerts: []` with a critical storage signal in the same poll | Covered by S1.4 |
| The rebuild journal's ~210 GiB is **not** a metric bug | `backupDeleteFreedBytes: 226491760640` is the real size of the deleted pre-rebuild copy, and `reclaimedBytes: 226236141568` is exactly that minus the installed database (226 491 760 640 − 255 619 072) | **Resolved** against the journal (`stagingBytes: 255619072`); do not "fix" these numbers |
| `tableSizes` leaves 113 MB (39%) unattributed | `otherBytes: 113027309`, `truncated: true` after 12 listed tables | The breakdown cannot show what grew; `job_sources` (197 083 rows, 95.9 MB) is the largest measured table |
| `job_sources` holds ~4.9 rows per live job | 197 083 rows vs 40 472 jobs, with `jobGenerations.totalGenerationCount: 0`, `generationRowCountKnown: false`, `status: "nothing-to-prune"` | Candidate stale-generation retention; needs a generation row count before proposing a purge |
| `avgFetchDurationMs7d` was a plain mean | `collect_fetch_history_metrics` divided the sum by run count, so 6 667 427 ms (1 h 51 m) over 11 runs was dominated by the longest | **Done:** the payload carries `medianFetchDurationMs7d`, `p90FetchDurationMs7d` and `fetchDurationSampleCount7d` over finished runs with a measured duration, and the Admin card headlines the median |
| `sync.runtime.lastPull`/`lastPush` are empty objects | `lastPullAt` is set and `lastResult: "ok"`, but `result`/`finishedAt`/`error` are all `""` in the summary view | Cosmetic for the Action Center (it reads `lastResult`/`lastError`); trace the runtime state before touching the projection |
| `appVersion` is empty | `sync.appVersion: ""` | Expected on `0.3.062`; `0.3.063` stamps container image identity |

## Slices

| # | Change | Files | Pinned by |
|---|---|---|---|
| S1.1 | `unhealthyReason` plus recovery on success | `src/storage/baluffo_store.py` | `tests/storage/test_baluffo_store_write_path.py`, `test_baluffo_store_exception_ratchet.py` |
| S1.2 | PASSIVE-first checkpoint, `status: "busy"`, `attemptedAt` | `src/storage/baluffo_store.py` | `tests/storage/test_baluffo_store_checkpoint.py` (split out; the maintenance module is at its 400-line budget) |
| S1.3 | Reason-aware Action Center storage copy | `frontend/admin/app/action-center.js`, `frontend/admin/render/action-center.js` | `tests/frontend/unit/admin-action-center-signals.test.mjs` |
| S1.4 | Storage verdict on the summary surface | `src/bridge/ops_api_health.py`, `src/bridge/ops_health.py` | `tests/admin/test_admin_bridge_ops_health.py` |
| S2.1–S2.3 | Death detection, liveness check, bounded relaunch | `src/jobs/browser_fallback_pool.py` | `tests/test_browser_fallback_pool.py` |
| S2.4 | Demand split in the fetch KPIs | `src/bridge/ops_api_health.py`, `src/bridge/fetch_report_summary.py`, `frontend/admin/app/action-center.js` | `tests/test_fetch_report_success_metadata.py`, `tests/bridge/test_ops_summary_route_caches.py` |
| S3 | Isolated re-measure and bucketing | no code | `tools/coverage_drain.py --verify-collected` output |

Each slice is independently reversible. S1.2 and S2.1–S2.3 are the two that change what the
container does; S1.1 and S1.3 only change what it reports.

**Verification before any commit:** `python -m pytest tests/storage tests/test_browser_fallback_pool.py
tests/admin tests/bridge -q`, `npm run test:py`, and `npm run lint:repo-guardrails`. Live validation
needs the target release deployed, because `/ops/health.appVersion` must equal it before a browser
observation counts.

## Non-goals

- No change to public job text, locations, or persisted contracts.
- No board retirement, registry bulk edit, or coverage mutation from this payload.
- No new route and no new polling; S1.3/S2.4 extend payloads that already exist.
- No Chromium flags, memory limits, or Playwright upgrades before the S2.5 host evidence lands.
- No rewriting of the rebuild or retention numbers in this pass.

## Open questions

1. ~~Should `storage_unhealthy` join the history-based alert set (S1.4 option A), or should the
   summary only expose `kpis.storageHealthy` and say storage is not covered (option B)?~~
   **Resolved: option A.** One verdict beats two surfaces disagreeing; `alertBasis` now says
   `history+storage` rather than claiming the alert set is history-only.
2. Should a busy WAL checkpoint stay a warning, or critical once `walBytes` exceeds a size the
   container's disk cannot absorb? The numbers to decide it are already in the status.
3. Keep `_healthy` as an "this store has failed at least once" diagnostic after S1.1, or delete it
   and let `unhealthyReason` carry the whole story?
