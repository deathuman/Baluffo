---
name: baluffo-umbrel-performance-triage
description: Diagnose Baluffo Umbrel Admin, Jobs, Sync, Discovery, and page-load slowness with Codex in-app Browser and Developer-mode evidence before proposing code changes. Use when the user reports slow Umbrel UI, high LCP or CLS, blank or false-empty Admin panels, excessive polling, source or log DOM pressure, or mismatch between backend profiling and browser-visible behavior.
---

# Baluffo Umbrel Performance Triage

Use this skill to keep Umbrel performance work Browser-evidence-first and bounded. Treat repo docs, source, tests, and `AGENTS.md` as canonical; use Basic Memory only for continuity and recent live gotchas.

## Evidence Order

1. Confirm live state without mutation.
   - Default target is `http://192.168.50.61:8877/` unless the user gives another URL.
   - Probe one compact endpoint batch: `/ops/health`, `/ops/task-state?view=summary`, `/tasks/run-jobs-pipeline-status`, `/data/jobs-unified-startup.json`, `/sync/status?view=summary`, and `/discovery/report?view=summary`.
   - Record app version, startup readiness, active task state, HTTP status, elapsed time, and obvious route outliers.
   - If port `8877` is refused or timing out, classify reachability before changing Baluffo code.

2. Capture browser-perceived evidence.
   - Prefer Codex in-app Browser with Developer mode/CDP access for traces, console, network, DOM/style inspection, screenshots, and interaction.
   - Always create a fresh Browser-managed tab and navigate it explicitly for proof work; do not rely on the currently selected tab reported in the prompt.
   - Use the Browser plugin/in-app browser path; do not use Chrome DevTools MCP, external Playwright MCP, or standalone Playwright automation unless the user explicitly requests that fallback.
   - For broad Admin/Jobs performance work, capture six browser traces where available: Admin cold, Admin warm, Jobs cold, Jobs warm, Jobs to Admin, and Admin to Jobs.
   - Save raw traces and summaries under `_out/` with a timestamped directory.
   - Extract LCP element and time, CLS, long tasks, slow resources, request waterfall, repeated 404s, console errors, DOM-size insights, and routes that run before first useful render.

3. Interpret browser evidence first.
   - Prioritize user-visible blockers: blank or false-empty states, wrong LCP element, CLS, large initial DOM, full diagnostics before user action, overlapping polls, repeated fallback probes, and slow first-byte routes that block first paint.
   - Use `/ops/performance-profile`, `/ops/storage-metrics`, `perf:complete`, and route timings only as supporting evidence after Browser/Developer-mode evidence identifies a suspect route or workload.
   - Do not optimize sync, provider fetch, source policy, or backend internals unless browser-visible evidence shows the page is waiting on them.
   - For active Discovery/Fetch/Sync work, sample compact hot-state routes with fixed attempts only: `/tasks/run-jobs-pipeline-status`, `/ops/task-state?view=summary`, `/ops/task-live/fetch?view=summary`, and `/app/ready`.
   - Treat repeated `/registry/sources`, `/registry/conflicts`, `/admin/ops-tab-counts`, `/ops/dashboard-health`, full task-live, or fetch-log timeout spam during active work as evidence of an active-polling regression.
   - Live Umbrel Browser proof only counts for a patch after `/ops/health.appVersion` equals the target version; older live installs are diagnostic evidence only.

4. Produce one compact triage report.
   - Include current live facts, trace artifact paths, the top bottlenecks in priority order, the smallest fix direction, acceptance criteria, and what is deliberately out of scope.
   - If Codex Browser/Developer mode, browser access, or Umbrel reachability is blocked, stop with a bounded blocker report instead of guessing.

## Defaults And Acceptance

- Admin shell visible under 2 seconds; useful Admin content under 5 seconds.
- Jobs first useful table or startup preview under 5 seconds.
- Admin and Jobs CLS below 0.1.
- No Fetcher or Discovery log text should become the LCP element.
- No full diagnostics routes should run during initial Admin boot unless an active task requires live tailing.
- No repeated startup 404 probes or overlapping idle status-poll bursts.
- During active runs, compact task-state/task-live summary routes should have zero 504s under the sampling window and meet the p95 target named in the release plan.
- Admin source tables, KPI cards, current rows, and fetch logs must remain non-empty or explicitly delayed; blank containers are not acceptable active-run proof.

## Token And Loop Guardrails

- Do one Basic Memory search, one live endpoint batch, and one trace set before planning a fix.
- Do not create a new benchmark layer while Browser/Developer-mode traces are missing.
- If the same failure or unclear trace result repeats twice, stop and diagnose from docs/source/tests/traces instead of rerunning.
- Keep closeouts short: facts, trace paths, top blocker, next action.
