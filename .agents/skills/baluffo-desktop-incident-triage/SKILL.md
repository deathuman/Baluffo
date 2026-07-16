---
name: baluffo-desktop-incident-triage
description: Diagnose Baluffo packaged desktop incidents, crashes, sudden exits, lingering Baluffo.exe processes, bridge timeouts, first-run startup bugs, updater or data-root problems, portable ZIP behavior, packaged smoke failures, Windows AppData migration issues, Linux desktop dispatch problems, and runtime evidence collection.
---

# Baluffo Desktop Incident Triage

## Overview

Use this skill when a packaged or desktop-local Baluffo runtime behaves incorrectly. Collect evidence before fixing; many desktop failures are caused by stale runtime artifacts, process lifecycle, bridge readiness, or packaged-smoke environment differences.

## Workflow

1. Preserve evidence first.
   - Ask for or identify the exact runtime path, version, time of failure, and whether the app was packaged, dev bridge, portable, updater-launched, or CI smoke.
   - Before killing processes or deleting files, inspect process state, relevant logs, smoke reports, startup metrics, bridge ports, and runtime data roots.
   - Use non-destructive commands first; request explicit approval for destructive cleanup.

2. Load the smallest authoritative docs.
   - Read `AGENTS.md`, `docs/INDEX.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/TROUBLESHOOTING.md`, `docs/LOCAL_SETUP.md`, `docs/RELEASE.md`, `docs/testing.md`, and `docs/startup-probe-architecture.md` when startup timing is involved.
   - Read release or desktop gotcha memories only as continuity, then verify against source, tests, and artifacts.

3. Classify the incident.
   - Bridge timeout or empty first-run feed: inspect bridge readiness, `/tasks/run-jobs-pipeline-status`, live task lifecycle, bootstrap state, and runtime feed/report artifacts.
   - Lingering process or sudden exit: inspect launcher, managed browser PID, bridge child ownership, shutdown lifecycle, updater relaunch state, and startup metrics. Distinguish regular close (`desktop_regular_close_shutdown_requested`), confirmed active-work close (`desktop_confirmed_active_work_shutdown_requested`), active-work close attempts, and browser-loss fallback; regular close depends on the page delivering beacon plus keepalive-fetch lifecycle signals before launcher cleanup can use the fast path.
   - Reload shutdown: distinguish F5, Ctrl+R, and Ctrl+Shift+R navigation from real desktop window close. Check whether a new-page `alive` signal clears any pending regular-close shutdown within the bridge grace window.
   - False idle shutdown: inspect page lifecycle heartbeats, non-health page traffic, owner-session state, browser PID liveness, and regular-close timers before assuming the user closed the app.
   - Stale Admin Sync row: compare `/ops/task-state?view=summary`, `/ops/task-live/sync?view=summary`, and `/sync/status?view=summary`; a terminal successful sync must clear Current Runs without requiring page reload.
   - False first-run Jobs modal or missing feed: inspect existing local feed files, first-run state markers, feed route responses, and bootstrap task state before starting or deleting anything.
   - Portable build-cache bloat or stale build: inspect `_out/portable-build-cache`, cache retention settings, the build fingerprint, and `_out/latest/build/portable` before rebuilding repeatedly.
   - Packaged smoke failure: inspect generated smoke JSON/artifacts before relying on console output.
   - Data-root or updater issue: confirm `%APPDATA%/Baluffo`, legacy portable data markers, update manifest behavior, and platform-specific dispatch.
   - Live pipeline watchdog: for long-running or stuck pipelines, inspect task lifecycle state, active run id, bridge health/live task endpoints, process CPU/IO movement, and artifact mtimes/sizes for `jobs-fetch-report`, `jobs-fetch-tasks`, `jobs-unified`, and ship/runtime output. Compare the current phase with the last meaningful progress timestamp before waiting longer.

4. Implement a narrow fix only after root cause.
   - Keep Windows behavior stable when adding Linux or cross-platform dispatch.
   - Do not expand root monkeypatch or compatibility seams.
   - Add deterministic tests or packaged smoke coverage for first-run, shutdown, updater, bridge, or data-root behavior when relevant.
   - Update troubleshooting, release, or setup docs only when workflow or behavior changes.

5. Validate the user-visible path.
   - Run focused Python tests for bridge, desktop app, ship, packaged smoke, or startup probe code.
   - Run the relevant packaged smoke or startup probe when the issue is packaged-only.
   - If a local packaged app must be launched, keep track of process IDs and close only the processes you started unless the user approves cleanup.
   - For Baluffo local UI or visual QA, use the bridge-backed dev runtime or packaged runtime. A bare static server is not valid evidence for desktop-local behavior, task state, bridge routes, packaged data roots, or live pipeline UI.
   - Close out with root cause, evidence inspected, commands run, changed paths, and any remaining manual verification.

## Guardrails

- Do not delete runtime data, app state, or logs as a first response.
- Do not assume dev-server behavior matches packaged behavior.
- Do not rely on a bare static server for desktop-local visual QA; use the bridge-backed dev or packaged runtime when visual inspection is needed.
- If a pipeline shows no progress changes across two checks separated by a meaningful interval, stop blind waiting and diagnose the stuck phase; if full live validation was only for data-quality confidence, capture partial evidence and switch to a targeted validation slice.
- Do not create Linux behavior by patching Windows tests through global `os.name` assumptions.
