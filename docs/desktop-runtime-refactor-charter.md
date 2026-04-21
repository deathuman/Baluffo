# Desktop Runtime Refactor Charter

> Historical/planning record for a focused cleanup lane. Start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and [`architecture-ai-map.md`](architecture-ai-map.md) before using this document for lane-specific compatibility context.

Use this tracker for the `src/ship/desktop_app` modularization campaign.

## Title

Desktop Runtime Package Modularization

## Goal

Turn `src/ship/desktop_app/__init__.py` into a thin compatibility facade over focused package modules so desktop runtime work is easier to reason about, safer to change, and cheaper for AI tools to edit. Preserve packaged desktop behavior as closely as possible while improving module ownership, searchability, and edit boundaries.

## Target Boundary

- Primary subsystem: Desktop runtime / packaged launcher flow
- Entry file(s): `src/ship/desktop_app/__init__.py`, `src/ship/desktop_app/cli.py`, `src/ship/desktop_app/__main__.py`
- Ownership boundary being clarified: config/process, browser launch, session/lock, Windows-specific helpers, startup supervision, launcher orchestration
- What becomes easier after this change: targeted bug fixes, tighter test scope, smaller AI context windows, safer packaged-app work

## Why Now

- Current pain: `__init__.py` mixes config, Windows internals, browser supervision, session state, updater handoff, and CLI entry logic in one 3k+ line file
- Why this is worth doing now: desktop behavior is special, heavily tested, and high-risk; keeping it monolithic increases drift risk and token waste
- Why this should stay narrow: behavior preservation matters more than redesign; this is a structural extraction, not a product change

## In Scope

- Create a repo-tracked execution checklist for the full campaign
- Freeze and document the current compatibility surface
- Extract focused package modules behind the existing `src.ship.desktop_app` facade
- Keep tests and packaged smoke flows behaviorally stable
- Remove safe redundancies and legacy leftovers encountered inside touched areas

## Out of Scope

- Changing desktop CLI flags or packaged runtime contracts
- Redesigning startup, updater, or browser supervision behavior
- Adding dependencies
- Broad test-suite reorganization before the production split is stable

## Stability Impact

- Runtime behavior touched: desktop startup, browser launch/attach, session state, lock reclaim, health polling, updater handoff
- Persisted state touched: desktop session JSON, desktop instance lock, startup metrics reads, updater success-marker path
- Packaging or desktop behavior touched: yes, throughout the launcher path
- Compatibility concern: tests and internal tools patch `src.ship.desktop_app` directly, so facade compatibility is part of the effective contract
- Rollback trigger: any regression in `tests/desktop_app/` or packaged smoke coverage tied to startup, reclaim, updater handoff, or Chromium supervision

## Compatibility Surface Audit

- Stable import surface to preserve:
  - `DesktopRuntimeConfig`
  - `InstanceLock`
  - `main`
  - `launch_desktop_app`
  - `launch_browser_for_url`
  - `watch_browser_session`
  - `resolve_chromium_browser_candidates`
  - `terminate_process`
  - current externally used constants such as `WINDOW_TITLE`, `PREFERRED_BROWSER_PATH_ENV`, `STARTUP_PROFILE_MODE_ENV`
- Direct internal callers outside the package:
  - `src/dev_admin_supervisor.py`
  - `src/packaged_desktop_smoke.py`
  - `tests/desktop_app/`
  - `tests/packaged_desktop/`
- Transitional constraint:
  - moved functions must continue resolving key dependencies through `src.ship.desktop_app` where tests patch the package root directly

## AI Accessibility Impact

- Source-of-truth files after refactor:
  - `src/ship/desktop_app/config.py`
  - `src/ship/desktop_app/process.py`
  - `src/ship/desktop_app/browser.py`
  - `src/ship/desktop_app/session.py`
  - `src/ship/desktop_app/_windows.py`
  - `src/ship/desktop_app/startup.py`
  - `src/ship/desktop_app/launcher.py`
- Expected search path for future edits:
  - config and paths -> `config.py`
  - child processes -> `process.py`
  - browser attach/reveal -> `browser.py`
  - session and lock handling -> `session.py`
  - Win32/process reclaim -> `_windows.py`
  - startup readiness and watchdogs -> `startup.py`
  - launcher orchestration -> `launcher.py`
- Docs or registry to update:
  - `docs/AI_ASSISTANT_GUIDE.md`
  - `docs/architecture-ai-map.md`
  - `docs/startup-probe-architecture.md`
- Transitional seam being kept temporarily:
  - lazy package-root compatibility access from extracted modules so package-root monkeypatches keep working

## Target Module Map

- `config.py`
  - constants, runtime config, path resolution, port resolution
- `process.py`
  - child command building, child process launch/termination, temporary cwd/sys.path isolation
- `browser.py`
  - browser discovery, Chromium app-mode launch, browser-ready polling, browser fallback launch
- `session.py`
  - session-state file I/O, instance lock lifecycle, session validation, conflict diagnosis
- `_windows.py`
  - Win32-only process/window helpers, job object handling, stale runtime reclaim
- `startup.py`
  - startup readiness, heartbeat/handoff logic, browser watchdogs, success-marker publication
- `launcher.py`
  - launcher diagnostics, fatal messaging, updater handoff snapshotting, main desktop orchestration, CLI entry

## Verification

- Cheapest syntax/check step:
  - `python -m pytest tests/desktop_app/ -q`
- Cheapest focused packaged step:
  - `python -m pytest tests/packaged_desktop/ -q`
- Broader verification required only if:
  - packaged startup probe behavior, updater handoff flow, or release-facing launcher wiring changes semantically

## Acceptance Criteria

- `src/ship/desktop_app/__init__.py` becomes a thin compatibility facade
- no product-facing desktop behavior regression
- docs point future editors to focused modules instead of a single behemoth file
- safe duplications/legacy leftovers are removed where touched or explicitly deferred
- future AI or human editors can find the right desktop implementation file in one or two searches

## Progress Checklist

1. [x] Create the repo-tracked charter and keep it current
2. [x] Freeze and document the package compatibility surface
3. [x] Extract `config.py`, `process.py`, and `browser.py`
4. [x] Extract `session.py` and `_windows.py`
5. [x] Extract `startup.py` and `launcher.py`
6. [x] Reduce `__init__.py` to a thin facade
7. [x] Clean transitional seams that are no longer needed
8. [x] Update desktop ownership docs
9. [x] Run targeted desktop verification suites

## Redundancy / Legacy Findings

- Confirmed legacy risk: current desktop module relies on an implicit `ROOT` symbol; make the repository root explicit during extraction.
- Transitional seam required: package-root monkeypatch compatibility is effectively part of the test contract, so extracted functions should keep resolving patched helpers through `src.ship.desktop_app` until the split is fully stable.
- Removed during implementation:
  - direct packaged smoke import from `src.ship.desktop_app.__init__`
  - implicit desktop `ROOT` lookup
- Verification completed:
  - `python -m pytest tests/desktop_app/ -q`
  - `python -m pytest tests/packaged_desktop/ -q`

## Notes

- This refactor is phased and behavior-preserving by default.
- Opportunistic cleanup is allowed only inside touched boundaries with test proof.
