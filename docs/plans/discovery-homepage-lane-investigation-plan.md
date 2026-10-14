# Discovery homepage lane investigation plan

> - **Class:** performance-tooling
> - **Trigger:** The homepage fetch is volume-bound, and the existing 64-lane arm is faster but not yet separated from live-host and 429 variance. A decision needs a fresh baseline after the probe-permit release.
> - **Verified against:** 9b8901f8 plus the 2026-10-12 discovery lane snapshots
> - **Status:** Deferred until receipt coverage and lifecycle-safety gates P1–P2 in [the end-to-end follow-up plan](discovery-performance-follow-up-plan.md) pass, followed by a pinned three-batch baseline and a healthy matching deployment.

## Question

Does raising activeAuditHomepageFetchConcurrency from 32 to 64 reduce actual homepage-fetch and full-audit wall time on the same 3,000 studios without increasing rate limiting or losing successful pages? This phase measures only; it changes no production configuration.

## Architecture and evidence

The GameDevMap audit batches 1,000 homepage rows at a time. [active_audit_batch_runtime.py](../../src/source_discovery/active_audit_batch_runtime.py) calls its fetch_homepages strategy; [gamedevmap_active_dry_run.py](../../src/source_discovery/gamedevmap_active_dry_run.py) routes that through [directory_fetch.py](../../src/source_discovery/directory_fetch.py) and the shared [http_batch.py](../../src/shared/http_batch.py). Keep `perHostConcurrency` at 3 while varying the total homepage lanes. The default fetcher selects the async HTTPX branch by identity; a timing wrapper can select the sync branch instead, so each arm must record and preserve transport mode.

The [homepage-stage snapshot](../snapshots/discovery-homepage-stage-floor-2026-10-12.md) classifies this phase as volume-bound: batches fed lanes for 82–83% of wall, and the tail above the 6-second request budget was only 0.8%. One 1,000-page comparison reported 21.2–22.5 seconds at 32 lanes versus 16.2 seconds at 64, but the 32-lane repeats differed by 1.3 seconds, successful pages varied by 45, and 429 counts were noisy (9 at one 32-lane run, 56 at its repeat, 57 at 64). These are promising counterfactuals, not a safe config change.

Do not shorten homepage timeouts: 57 of 80 recorded requests over 3 seconds were successful pages, including two HTTP 200 responses over 7 seconds. The same snapshot records 39.7 seconds of homepage analysis, which this lane experiment cannot improve. The trust-anchor repair also changes which homepages are readable, so the post-release baseline must include that code path.

## Investigation steps

1. **Preflight after deployment.** Verify `/ops/health` and the pipeline status on the target Umbrel, confirm `appVersion` matches the pushed build, and ensure no other discovery/fetch task is active. At the last inspection the host was 0.3.014 while this checkout was 0.3.018, /ops/health returned 504, and no task was active; those measurements are not acceptance evidence.
2. **Capture a fresh 32-lane baseline.** Use the same pinned 3,000-homepage population, 1,000-row batch size, `perHostConcurrency=3`, `timeout=6`, TLS behavior, and probe-permit build. Save run artifacts under _out/perf-runs/discovery; do not use repo data as scratch output.
3. **Run paired lane arms.** Reuse `_out/perf-runs/discovery/homepage_floor_bench.py` and `verify_homepage_floor.py`. Compare 32 and 64 lanes with counterbalanced order over at least three pairs. Keep transport mode, host population, retries, per-host limit, and all other settings constant. Only add a 48-lane arm if 64 is noisy or shows a throttling tradeoff that a middle setting could resolve.
4. **Attribute coverage and politeness.** Record per-batch wall and occupancy, per-host request outcomes, 200/429/5xx/timeouts, retries, readable pages, response bytes, and resulting candidates. Compare board identity by host+tenant, not studio label or id. Inspect which hosts account for changed 429s; an aggregate count alone cannot separate noise from harm. Keep homepage-analysis time separate from fetch time.
5. **Make a go/no-go recommendation.** Accept a lane increase only if paired fetch-wall gains recur beyond the 32-lane run-to-run spread, page and downstream candidate outcomes do not regress, and host-level throttling does not rise materially. Then run one full three-batch audit on the matching build before proposing a separate config implementation. If the signal fails, keep 32 and profile homepage analysis as a separate investigation.

## Boundaries

- No production config, timeout, retry, parser, or per-host limit changes in this investigation.
- No benchmark framework or shipped scripts; keep measurement helpers and artifacts under `_out/perf-runs/discovery` and reuse the existing verifier.
- No claims about Admin/Jobs page rendering. This is backend audit runtime only; UI work needs fresh Browser evidence on the matching build.
- Recovery-wave deadlines and search-query concurrency are separate: previous snapshots show different coverage/rate-limit tradeoffs, so do not combine them with homepage lane tuning.

## Closeout

Record the paired result in a dated evidence snapshot. If the lane increase is supported, write a separate implementation change with the chosen value and acceptance checks; otherwise retain 32 and route the next investigation to homepage-analysis cost. Delete this temporary plan when the investigation closes.
