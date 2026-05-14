# Post-0.2.0 Desktop Runtime RAM Reduction Plan

> - **Status:** Active plan, deferred post-v0.2.0
> - **Use this when:** revisiting desktop runtime RAM reduction, packaged startup memory, or static site process consolidation
> - **Canonical for:** deferred proposal, risks, validation plan, and known loopholes
> - **Not canonical for:** current runtime behavior, release requirements, or benchmark baselines
> - **Then inspect:** [`../startup-probe-architecture.md`](../startup-probe-architecture.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../testing.md`](../testing.md), [`../../src/ship/desktop_app/`](../../src/ship/desktop_app/), and [`../../src/ship/runtime_launcher.py`](../../src/ship/runtime_launcher.py)
> - **Last updated:** 2026-05-14

## Summary

This plan defers the next meaningful desktop RAM reduction until after v0.2.0. The conservative Chromium flag pass was mostly neutral, and the next credible lever is reducing the packaged desktop process count by folding the static site server into the launcher process.

That change is intentionally not v0.2.0 scope. The expected win is useful but not release-critical, and the work crosses launcher cleanup, stale-runtime reclaim, startup traces, retry behavior, packaged rehearsals, and import-weight boundaries.

## Current Evidence

The latest complete benchmark evidence came from `_out/perf-complete/20260514-122901-214828/summary.json` after the conservative Chromium flag pass:

| Section | Browser RAM | Baluffo RAM | Notes |
|---------|-------------|-------------|-------|
| Startup cold | about 595 MiB | about 232 MiB | full packaged desktop startup |
| Startup warm | about 564 MiB | about 192 MiB | full packaged desktop startup |
| Sync | 0 MiB | about 195 MiB | full packaged no-browser runtime |

The removable-looking site process footprint is about 42-44 MiB. More Chromium flags are unlikely to be the safest next step because the prior low-risk flag set produced only a neutral cold result and a small warm improvement.

## Deferred Strategy

The future implementation should preserve browser-visible behavior while removing one packaged `Baluffo.exe` process from the normal desktop runtime:

1. Extract static site serving from [`../../src/ship/runtime_launcher.py`](../../src/ship/runtime_launcher.py) into a focused leaf helper.
2. Run the site server inside the desktop launcher process by default.
3. Keep the existing `__child_site__` path for compatibility, orphan-reclaim tests, stale sessions, and rollback.
4. Add an escape hatch that forces the old child-process site mode.
5. Preserve the site URL, site port, startup metrics, packaged sync contract, and release smoke coverage.

## Known Loopholes

Any implementation must explicitly close these before it is considered safe:

- Cleanup must distinguish process termination from in-process server shutdown.
- Retry logic needs `.poll()`-like semantics for the in-process site handle.
- Startup probe metrics must receive explicit `data_dir` and `startup_probe` inputs instead of relying on process environment.
- Stale reclaim must handle both old child-owned `sitePid` and future launcher-owned `sitePid`.
- Import weight must be measured so moving site serving into the launcher does not absorb most of the saved memory.
- Orphan-reclaim, update rehearsal, sync rehearsal, startup probes, and packaged smoke must remain covered.

## Validation Plan

Future implementation should run:

```powershell
python -m py_compile src/ship/runtime_launcher.py src/ship/desktop_app/launcher_flow.py
python -m pytest tests/test_runtime_launcher.py tests/desktop_app tests/packaged_desktop/test_rehearsal_flows.py tests/packaged_desktop/test_runtime_wait_and_reports.py -q
npm run perf:complete
```

Acceptance criteria:

- `perf:complete` report shape and sections stay unchanged.
- Startup cold/warm and sync still pass full packaged runtime paths.
- Startup timing does not materially regress.
- Baluffo category RAM drops by a measurable site-process footprint.
- Default back to child-process site mode if RAM does not improve or launcher stability regresses.
