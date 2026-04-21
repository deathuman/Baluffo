# Startup Probe Architecture

> **AI usage**
> - **Use this when:** changing packaged startup timing, probe orchestration, launcher timing, or startup metrics
> - **Canonical for:** startup measurement ownership, event flow, command surface, and runtime-vs-probe boundaries
> - **Not canonical for:** general testing guidance or release sequencing
> - **Then inspect:** `src/ship/startup_telemetry.py`, `src/ship/desktop_app/launcher.py`, `src/ship/desktop_app/startup.py`, `src/ship/desktop_app/browser.py`, `src/ship/startup_probe_policy.py`, `src/packaged_desktop_smoke.py`, and `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`

This document is the single source of truth for Baluffo's packaged startup measurement architecture. Keep probe policy and startup-flow explanations here instead of repeating them in command docs or release notes.

## Canonical Commands

Use these repo-native entrypoints for packaged startup timing:

- `npm run perf:startup:cold`
- `npm run perf:startup:warm`
- `npm run perf:startup:pair`

The `perf:*` aliases are the contributor-facing surface. The lower-level `probe:desktop:startup:*` commands exist, but docs should prefer the `perf:*` names unless a release checklist specifically needs the probe form.

## Ownership Boundaries

Baluffo's startup measurement stack has three layers:

1. **Runtime telemetry**
   - Lives in shipped code.
   - Owns startup event append/read helpers, URL readiness probes, and browser/page startup metric transport.
   - Must stay small and dormant unless startup measurement is enabled.
   - Primary code: `src/ship/startup_telemetry.py`, bridge startup-metric storage, browser-side startup metric hooks.

2. **Desktop launcher runtime**
   - Lives in shipped code.
   - Owns site/bridge launch ordering, browser launch acceptance, handoff, reveal detection, and runtime readiness.
   - Should depend on small telemetry helpers, not probe-policy decisions.
   - Primary code: `src/ship/desktop_app/launcher.py`, `src/ship/desktop_app/startup.py`, `src/ship/desktop_app/browser.py`.

3. **Startup probe policy + analysis**
   - Probe-only logic.
   - Owns required event sets, strict managed-browser policy, probe failure categorization, and startup summary refinement.
   - Should not leak into normal desktop behavior beyond simple config flags passed into the launcher/runtime.
   - Primary code: `src/ship/startup_probe_policy.py`, `src/ship/startup_profile.py`, `src/packaged_desktop_smoke.py`, and `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`.

## What Stays in the Shipped Package

Do **not** remove startup telemetry from the regular package entirely.

The packaged startup probes are only trustworthy because they measure the real shipped app path. That means the following stay in product code:

- launcher startup trace events
- browser/page startup metric hooks
- bridge persistence for startup metrics
- runtime URL readiness probing

What should stay out of the normal launcher path:

- strict startup-probe browser selection policy
- startup-probe required event lists
- probe-only pass/fail classification
- packaged-smoke artifact/report orchestration

Keep `src/packaged_desktop_smoke.py` as the CLI and root patch surface. Put concrete packaged-smoke helper changes in `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py` unless the command surface itself needs to change.

## Event Flow

The packaged startup measurement path is:

1. The desktop launcher emits runtime startup events.
2. Browser pages emit startup metrics once `startupProbe=1` enables the browser-side transport.
3. The bridge stores those browser-origin metrics under the runtime data directory.
4. Packaged smoke collects startup metrics from the runtime data and bridge endpoints.
5. Startup profile analysis renders the stage timings and summary report.

Important event families:

- launcher/runtime events: `desktop_*`
- browser/page events: `jobs_*`, `saved_*`, `admin_*`
- report outputs: `startup-profile-summary.json`, packaged smoke `report.json`

## Artifact and Reporting Rules

- Use repo-local artifact roots only:
  - `.tmp/packaged-desktop-smoke/`
  - `.tmp/packaged-desktop-smoke-pair/`
  - `data/packaged-desktop-smoke-report.json`
- Do not introduce `%LOCALAPPDATA%\Temp` dependencies for startup perf workflows.
- Keep report shape and event names stable unless compatibility work is intentional and verified across runtime, smoke runner, and tests.

## Drift Control Rules

When changing startup measurement code:

- Update this document first if the ownership boundary changes.
- Prefer importing shared startup helpers over copying event groups or probe classifications into another file.
- Prefer one canonical doc link from `docs/testing.md`, `CONTRIBUTING.md`, and release/process docs over repeated prose.
- Keep helper names aligned with the concepts above:
  - `startup_telemetry`
  - `desktop launcher runtime`
  - `startup probe policy`
