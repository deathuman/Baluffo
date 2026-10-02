# Desktop Runtime RAM Reduction

> - **Status:** Active (deferred, lever un-landed) — the one remaining credible desktop RAM lever, still pending
> - **Class:** cleanup
> - **Trigger:** no date. Deferred on risk, not on a condition: ~42-44 MiB measured from 2026-05-14 against a cold-start budget with 24% headroom, against seven unenumerated loopholes that all fail silently. Reopen only if `npm run perf:complete` shows the site process still near 42 MiB.
> - **Verified against:** 722c3be3
> - **Use this when:** revisiting desktop runtime RAM reduction, packaged startup memory, or static site process consolidation
> - **Canonical for:** the site-process fold-in proposal, its known loopholes, and its validation plan
> - **Not canonical for:** current runtime behavior, release requirements, or benchmark baselines
> - **Then inspect:** [`../startup-probe-architecture.md`](../startup-probe-architecture.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../testing.md`](../testing.md), [`../../src/ship/desktop_app/`](../../src/ship/desktop_app/), and [`../../src/ship/runtime_launcher.py`](../../src/ship/runtime_launcher.py)
> - **Last updated:** 2026-10-02 (declaration block added; site-process symbols re-verified as still live in process.py, launcher.py and runtime_launcher.py)

## The lever

One packaged `Baluffo.exe` still serves the static site as a **child process**. That process
costs about 42–44 MiB, which makes folding it into the launcher the next credible lever after a
Chromium flag pass that came out mostly neutral.

**Verified still pending 2026-10-01.** The process boundary has not moved:

- `src/ship/desktop_app/process.py:25` `build_child_command()` still emits
  `["__child_site__", "--root", …, "--port", …]` for `mode == "site"`.
- `src/ship/desktop_app/launcher.py:75-78` dispatches that mode to `run_site_server` in the child.

`_DesktopSiteServer` living in `src/ship/runtime_launcher.py:128` does **not** mean the server
runs in-process — it means the code is importable from there. Do not read that class as the lever
already landing; it was mistaken for that once during this review.

Nothing in `src/ship`'s history since the plan was parked reduces process count. The 19
RAM-adjacent commits in that window are launcher/CLI narrowing, updater recovery, and startup
regression fixes.

## Evidence, and how stale it is

Last complete benchmark: `_out/perf-complete/20260514-122901-214828/summary.json`, after the
conservative Chromium flag pass.

| Section | Browser RAM | Baluffo RAM | Notes |
|---------|-------------|-------------|-------|
| Startup cold | about 595 MiB | about 232 MiB | full packaged desktop startup |
| Startup warm | about 564 MiB | about 192 MiB | full packaged desktop startup |
| Sync | 0 MiB | about 195 MiB | full packaged no-browser runtime |

**Re-measure before implementing.** These figures are from 2026-05-14. The 42–44 MiB site
process is the plan's own sizing of the removable footprint, not a current measurement — and per
[`../measurement-methods.md`](../measurement-methods.md), a number this old is a hypothesis until
`npm run perf:complete` says otherwise. More Chromium flags are unlikely to be the safest next
step: the prior low-risk flag set produced a neutral cold result and a small warm improvement.

## Strategy

Remove one packaged `Baluffo.exe` from the normal desktop runtime without changing
browser-visible behavior:

1. Extract static site serving from [`../../src/ship/runtime_launcher.py`](../../src/ship/runtime_launcher.py) into a focused leaf helper.
2. Run the site server inside the desktop launcher process by default.
3. Keep the existing `__child_site__` path for compatibility, orphan-reclaim tests, stale sessions, and rollback.
4. Add an escape hatch that forces the old child-process site mode.
5. Preserve the site URL, site port, startup metrics, packaged sync contract, and release smoke coverage.

## Known loopholes to close first

- Cleanup must distinguish process termination from in-process server shutdown.
- Retry logic needs `.poll()`-like semantics for the in-process site handle.
- Startup probe metrics must receive explicit `data_dir` and `startup_probe` inputs instead of relying on process environment.
- Stale reclaim must handle both old child-owned `sitePid` and future launcher-owned `sitePid`.
  Today both `src/ship/desktop_app/_linux.py:617` and `_windows.py:700` read `sitePid` from stale state, and `src/dev_admin_supervisor.py` writes it from the site process.
- Import weight must be measured, so moving site serving into the launcher does not absorb most of the saved memory.
- Orphan-reclaim, update rehearsal, sync rehearsal, startup probes, and packaged smoke must remain covered.

## Validation

```powershell
python -m py_compile src/ship/runtime_launcher.py src/ship/desktop_app/launcher_flow.py
python -m pytest tests/test_runtime_launcher.py tests/desktop_app tests/packaged_desktop -q
npm run perf:complete
```

The rehearsal tests are named by directory here, not by file. An earlier revision of this plan
cited `tests/packaged_desktop/test_rehearsal_flows.py` and
`tests/packaged_desktop/test_runtime_wait_and_reports.py`; **neither exists**. The suite was
split into `test_rehearsal_browser_job.py`, `test_rehearsal_browser_launch.py`,
`test_rehearsal_completion.py`, `test_rehearsal_lifecycle.py`, and others.

Acceptance criteria:

- `perf:complete` report shape and sections stay unchanged.
- Startup cold/warm and sync still pass full packaged runtime paths.
- Startup timing does not materially regress.
- Baluffo category RAM drops by a measurable site-process footprint.
- Default back to child-process site mode if RAM does not improve or launcher stability regresses.
