# Task Lifecycle Ledger Plan

> - **Status:** Lifecycle core implemented; closeout required before legacy lifecycle authority is removed
> - **Use this when:** fixing Admin Current Runs / Recent Runs contradictions, pipeline child-task ownership, task heartbeat/orphan behavior, or long-running discovery/fetch lifecycle bugs
> - **Canonical for:** the planned single-source-of-truth task lifecycle refactor
> - **Not canonical for:** current route payload contracts, report schemas, fetch/discovery output contracts, or release procedures
> - **Then inspect:** [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and the bridge task lifecycle modules
> - **Last updated:** 2026-05-08

## Summary

Baluffo now has a backend-owned lifecycle ledger for Admin/Ops task state. The original problem was that Admin decided whether work was running, done, failed, or orphaned by combining `admin-task-state.json`, `admin-run-history.json`, discovery/fetch reports, pipeline runtime state, file mtimes, heartbeats, and frontend merge logic.

That split authority has produced repeated bugs:

- Discovery shown as `Failed` while the parent pipeline is still running discovery.
- Pipeline failing its discovery wait while discovery is still alive.
- Active rows showing misleading `Finished` timestamps.
- Report staleness and file-lock issues halting or misclassifying otherwise healthy work.

The permanent model is a backend-owned task lifecycle ledger. Reports describe work and outputs. The lifecycle ledger decides whether work is alive, terminal, failed, canceled, or orphaned.

## Target model

Create a canonical `TaskLifecycleService` backed by a persisted lifecycle ledger, likely `data/admin-task-lifecycle.json`.

Canonical task row fields:

- `schemaVersion`
- `runId`
- `taskType`
- `parentRunId`
- `parentTaskType`
- `status`
- `stage`
- `startedAt`
- `heartbeatAt`
- `finishedAt`
- `terminalReason`
- `ownerKind`
- `ownerPid`
- `progress`
- `summary`

Allowed statuses:

- `queued`
- `running`
- `succeeded`
- `failed`
- `canceled`
- `orphaned`

Required invariants:

- `queued` and `running` rows must have empty `finishedAt`.
- `succeeded`, `failed`, `canceled`, and `orphaned` rows must have non-empty `finishedAt`.
- Reports may enrich `progress` and `summary`, but reports must not independently decide lifecycle state.
- A running parent pipeline can own child `discovery`, `fetch`, and `sync` rows.
- A child task can become `orphaned` only after its direct owner and parent owner are both inactive.

## Implementation progress

Implemented:

- Added `TaskLifecycleService` backed by `data/admin-task-lifecycle.json`.
- Added bridge facade/path wiring and cleanup reset support.
- Wired fetch, discovery, sync, and pipeline launch/terminal paths into the lifecycle ledger.
- Added explicit pipeline child attachment through `parentRunId`.
- Replaced fixed pipeline child wait deadlines with quiet-evidence timeout logic and a distinct absolute safety cap terminal reason.
- Switched `/ops/task-state` and `/ops/history` to lifecycle-first rows while reducing legacy read authority.
- Added bridge-startup cleanup that orphans stale lifecycle rows whose owner cannot survive a desktop bridge restart.
- Simplified Admin Current Runs so the frontend trusts backend lifecycle rows and does not retain missing active rows for one extra sample.
- Added bridge-startup reconciliation during the transition, then removed it from normal bridge startup so legacy lifecycle import is explicit migration/test tooling only.
- Kept legacy report/history/task-state helpers only for explicit migration, tests, and maintenance tooling; production lifecycle routes no longer use them as authority.
- Made terminal report evidence win over stale active task-state/progress metadata during child lifecycle projection.
- Removed task-state liveness authority from live payload projection; lifecycle snapshots now decide active/terminal state before task-state compatibility evidence is considered.
- Removed fetch task launch writes/prunes against `admin-run-history.json`; fetch launch now records lifecycle rows plus fetch report evidence.
- Removed discovery, sync, and pipeline lifecycle writes/prunes against `admin-run-history.json`; their lifecycle state now lands in `admin-task-lifecycle.json` with task-specific report/live evidence.
- Removed production duplicate-start checks, task-live projection, discovery heartbeat, fetch launch, and pipeline child heartbeat/liveness dependencies on `admin-task-state.json`; lifecycle rows now own those decisions.

Remaining closeout:

- Run a long real discovery/fetch smoke beyond the old nominal timeout window before the next packaged release.
- Keep explicit migration/maintenance helpers isolated from production routes; remove those helpers only when the repo no longer needs old-run recovery tooling.
- Continue hardening progress projection tests as new task evidence fields are added; lifecycle rows must remain identity/liveness authority, while current report/task evidence supplies display progress.
- Validate packaged `/ops/fetch-report?view=live` against a real portable run before the next packaged release.
- Fix the observed packaged fetch drift where Current Runs / Selected Run Analysis can lag behind Fetcher Output. The table row is currently served by `/ops/task-state`, which still projects raw lifecycle current rows in `OpsApi.get_current_task_state_payload()`; Fetcher Output is served by `/fetcher/log`, so it can show fresher worker progress than the row.
- Fix packaged `/ops/fetch-report` and `/ops/fetch-report?view=live` returning a zero-count shell report while the runtime `jobs-fetch-report.json` contains current counts. The observed cause is stale `jobs-fetch-report.jsonl` journal overlay data taking precedence through `source_registry.load_json_object()`.
- Make active fetch display progress evidence-first on every request: merge `jobs-fetch-tasks.json`, `jobs-fetch-report.json`, and lifecycle identity, then recompute `ratio` from counts instead of trusting stale copied lifecycle `progress`.

## Observed fetch progress drift - 2026-05-08 packaged run

During a real packaged pipeline run, the UI showed this split:

- Fetcher Output: `1,211/2,102 sources resolved` and `58%`.
- Current Runs / Selected Run Analysis: `1,201/2,102 sources resolved` and `50%`.
- Later endpoint checks showed `/ops/task-state` catching up in jumps, while `/fetcher/log` continued to show fresher progress lines.

Root cause findings:

- `/fetcher/log` is a text/event output stream and polls frequently. It reflects fetcher-emitted progress lines quickly.
- `/ops/task-state` currently returns `OpsApi.get_current_task_state_payload()`, which builds rows from lifecycle current rows and pipeline status. That path does not use the richer `ops_task_fetch_live.build_fetch_live_payload()` projection that merges live task/report evidence.
- Lifecycle rows are correct for identity and liveness, but their copied `progress` can be stale between child heartbeat refreshes.
- `/ops/fetch-report` can return the initial shell report because `source_registry.load_json_object()` checks adjacent JSON journal data. A stale `jobs-fetch-report.jsonl` journal created at task start can shadow the newer `jobs-fetch-report.json`.
- The real packaged `jobs-fetch-report.json` and `jobs-fetch-tasks.json` files had fresher counts, so the worker was not stalled; the display path was reading or prioritizing the wrong evidence.

Required fixes:

- Change `/ops/task-state` active fetch/discovery/sync rows to use the shared live projection path, not raw lifecycle row progress. Lifecycle selects the active run; task/report evidence supplies display progress.
- For fetch rows, prefer matching-current-run `jobs-fetch-tasks.json` when its heartbeat or mtime is recent, then merge in `jobs-fetch-report.json`, then use lifecycle snapshot only as fallback metadata.
- Recompute fetch `ratio` as `resolvedSources / sourceCount` whenever `sourceCount > 0`. Do not trust stored `ratio` from lifecycle rows, reports, task files, or logs when counts are present.
- Ensure `resolvedSources`, `completedTasks`, `outputCount`, `failedSources`, and `excludedSources` are monotonic across same-run evidence during projection. Use live task/report counts to prevent older lifecycle snapshots from moving the row backward.
- Stop using registry-style JSON journal overlay reads for runtime evidence artifacts: `jobs-fetch-report.json`, `jobs-fetch-tasks.json`, `source-discovery-report.json`, live task files, lifecycle rows, and bridge event/output files should be plain current-file reads unless a specific artifact explicitly opts into journaling.
- Delete, ignore, or namespace stale runtime journals such as `jobs-fetch-report.jsonl` so they cannot shadow current report JSON. Registry journaling may remain for registry files if still needed.
- Keep `/ops/fetch-report?view=live` compacting heavy source detail, but it must compact the current normalized report rather than a stale shell report.
- Frontend polling can still differ by cadence, but backend responses must make the table row and Fetcher Output converge on the same counts within one task-state poll interval.

## Observed stuck pipeline after fetch completion - 2026-05-08 packaged run

A second real packaged pipeline reproduced the more severe blocker: fetch completed, but the parent pipeline stayed in Current Runs at `stage=fetch`.

Observed live state:

- `/tasks/run-jobs-pipeline-status` returned `active=true`, `stage=fetch`, and `Running fetch... (67%)`.
- `/ops/history` showed the child fetch run as `failed` with `terminalReason="owner_inactive_without_terminal_report"`.
- The actual `jobs-fetch-report.json` was terminal and correct: `finishedAt=2026-05-08T09:56:02+02:00`, `taskProgress.active=false`, `phaseKey="completed"`, `ratio=1.0`, `sourceCount=2102`, and `failedSources=315`.
- The fetch owner PID was no longer running, and the fetcher log ended with `Jobs fetch completed`.
- `/ops/fetch-report?view=live` still returned the initial zero-count shell report because `jobs-fetch-report.jsonl` was older than `jobs-fetch-report.json` but still won in `source_registry.load_json_object()`.

Root cause:

- Runtime evidence reads used registry journal overlay semantics. A stale startup `jobs-fetch-report.jsonl` shadowed the newer terminal `jobs-fetch-report.json`.
- The fetch lifecycle watcher read the stale report, failed to close the child as completed, and later marked it `owner_inactive_without_terminal_report`.
- The pipeline wait loop read the same stale report, so it never observed `finishedAt` and stayed in the fetch wait path.
- Pipeline child liveness also trusted a running lifecycle row without checking whether its recorded `ownerPid` was still alive, which could keep extending the quiet-evidence window after the fetch worker had exited.
- A follow-up live run showed a separate terminal-classification bug: `failedSources > 0` in a completed fetch summary was treated as a task-level fetch failure. Source failures are operational evidence and retry input; they are not lifecycle failure unless the report has an explicit task error or failed status.

Required fixes:

- `load_json_object()` must not let an older adjacent journal override a newer canonical JSON object. If both exist and the canonical JSON parses, the journal can only overlay when its mtime is newer than the canonical artifact.
- Pipeline child liveness must treat a row with a non-running `ownerPid` as not live unless newer terminal or progress evidence is observed from the task/report artifact.
- Fetch lifecycle closeout must read the terminal report evidence and finish the child row before owner-inactive cleanup can misclassify a completed fetch.
- Fetch lifecycle closeout must classify a completed report with failed source counts as `succeeded/completed`; only explicit report `status=failed|error|failure` or `summary.error` may fail the fetch task lifecycle.
- Add packaged-regression coverage where `jobs-fetch-report.jsonl` contains an unfinished shell, `jobs-fetch-report.json` contains a terminal report, and both `/ops/fetch-report?view=live` and the pipeline wait path observe the terminal JSON.
- Manual validation must include a real packaged pipeline run that reaches fetch completion, records the fetch child as completed even with normal source-level failures, and advances the parent pipeline out of `stage=fetch` without requiring the quiet timeout.

## Implementation plan

1. Add `TaskLifecycleService`.
   - Implement `start_run`, `heartbeat_run`, `finish_run`, `fail_run`, `orphan_run`, `attach_child`, `get_current_runs`, and `get_recent_runs`.
   - Enforce lifecycle invariants at write time.
   - Keep rows JSON-serializable and stable for bridge payloads.

2. Wire all task launch paths into the service.
   - Discovery, fetch, sync, and jobs pipeline create lifecycle rows at launch.
   - Background process spawns write `ownerKind="process"` and `ownerPid`.
   - Pipeline writes `ownerKind="pipeline"` for child tasks it owns or waits on.

3. Make parent-child ownership explicit.
   - Pipeline rows own child discovery/fetch/sync rows through `parentRunId`.
   - Pipeline stage updates heartbeat both parent and currently waited child.
   - Child quiet periods stay running while parent ownership is active.

4. Replace fixed pipeline child wait deadlines with quiet-evidence timeouts.
   - Pipeline discovery/fetch waits fail only after a configured quiet window with no live child evidence.
   - Live evidence includes lifecycle heartbeat, parent-owned child heartbeat, report progress timestamp, recent report/log writes, or validated process ownership.
   - A child running longer than the nominal timeout must not fail if live evidence is still advancing.
   - Keep any absolute safety cap separate from the quiet timeout and report it with a distinct terminal reason.

5. Demote reports to evidence.
   - Discovery/fetch reports continue to write progress, counts, failures, and outputs.
   - Lifecycle projection reads reports only to enrich lifecycle rows.
   - Report `finishedAt` can request a terminal transition, but lifecycle service validates ownership and ordering first.

6. Replace heuristic run projection.
   - `/ops/task-state` reads current rows from the lifecycle service.
   - `/ops/history` reads terminal rows from the lifecycle service.
   - Existing `run_history_api` report-staleness heuristics become migration/reconciliation helpers, not the primary source of truth.

7. Keep compatibility during migration.
   - Continue writing `admin-task-state.json` and `admin-run-history.json` for one transition release if needed.
   - Add a reconciliation path from existing history/report files into the lifecycle ledger.
   - Preserve current route shapes unless a separate compatibility decision changes them.

8. Simplify frontend lifecycle logic.
   - Admin treats backend lifecycle rows as authoritative.
   - Frontend no longer decides liveness by merging `active`, `finishedAt`, report fields, and mtimes.
   - Frontend may keep defensive display hardening, such as blanking `Finished` for active rows.

9. Route active task-state display through shared live projection.
   - `/ops/task-state` keeps lifecycle rows as identity/liveness authority.
   - Active fetch/discovery/sync rows call the same task-specific live projection used by `/ops/task-live/{type}`.
   - Pipeline rows continue to expose parent stage, but active child summaries are enriched from the active child projection.
   - Selected Run Analysis and Current Runs must receive identical task-progress counts for the same fetch run.

10. Fix runtime report IO so evidence files are not shadowed by registry journals.
   - Introduce or use a runtime-evidence JSON reader that reads the requested artifact directly.
   - Do not call `source_registry.load_json_object()` for fetch/discovery reports, fetch task files, live task files, or lifecycle rows.
   - Ignore adjacent `.jsonl` journals for runtime evidence artifacts.
   - Add cleanup/migration handling for stale `jobs-fetch-report.jsonl` files generated by prior packaged runs.

## Test plan

Backend lifecycle tests:

- Starting a pipeline creates one running pipeline row.
- Starting discovery/fetch/sync creates rows with correct `taskType`, `runId`, and owner fields.
- Pipeline-owned discovery gets `parentRunId` and remains running while the pipeline is in discovery stage.
- Pipeline-owned fetch gets `parentRunId` and remains running while the pipeline is in fetch stage.
- Running rows never contain `finishedAt`.
- Terminal rows always contain `finishedAt`.
- A report with stale or contradictory `finishedAt` cannot fail a parent-owned child.
- A quiet child becomes `orphaned` only after process owner and parent owner are both inactive.
- Pipeline timeout remains possible only when the child has no live owner evidence.
- Discovery/fetch waits exceeding the nominal timeout continue while live evidence is recent.
- PID liveness false but report/log progress recent still counts as live child evidence.
- A child wait fails only after the quiet-evidence window expires.
- Absolute safety-cap failure, if kept, uses a distinct terminal reason from quiet timeout.
- Active fetch task-state rows recompute percentage from fresh evidence counts. Given lifecycle progress at `50%` and `jobs-fetch-tasks.json` / `jobs-fetch-report.json` at `58%`, `/ops/task-state` must render the count-based `58%`.
- `/ops/fetch-report` and `/ops/fetch-report?view=live` ignore stale `jobs-fetch-report.jsonl` journal overlays and return the current `jobs-fetch-report.json` counts.
- If live task evidence and fetch report evidence differ for the same run, projection keeps count fields monotonic and never prefers an older lifecycle snapshot for display progress.
- Runtime evidence readers do not use registry journal overlay semantics for non-registry artifacts.

Frontend tests:

- Current Runs only renders lifecycle `running` rows.
- Recent Runs only renders terminal rows.
- Active rows never show a Finished timestamp.
- Selected Run Analysis omits Finished for active rows.
- Parent and child rows stay consistent through discovery, fetch, and sync stages.
- Current Runs and Selected Run Analysis show the same fetch percentage and counts for the selected active run.
- Fetcher Output and Current Runs converge to the same fetch counts after one `/ops/task-state` poll when backend evidence has advanced.

Integration smoke:

- Run jobs pipeline with discovery longer than 15 minutes.
- Confirm pipeline remains Current Running while discovery is active.
- Confirm discovery child remains Current Running or parent-owned, not Failed.
- Confirm discovery completion advances to fetch.
- Confirm active fetch row percentage matches the latest current-run task/report evidence and does not lag behind Fetcher Output due to stale lifecycle `progress`.
- Confirm `/ops/fetch-report?view=live` reads the current packaged report even if a stale `jobs-fetch-report.jsonl` exists beside it.
- Confirm registry metadata file locks do not halt discovery finalization.
- Confirm successful pipeline completion moves all rows to Recent with terminal timestamps.

## Acceptance criteria

This refactor is complete when:

- There is exactly one backend lifecycle authority for task status.
- Reports cannot independently mark live tasks failed or orphaned.
- Parent-child ownership is explicit in persisted lifecycle state.
- `/ops/task-state` and `/ops/history` no longer rely on report mtime/staleness guessing for active-vs-failed decisions.
- Admin UI displays no contradictory rows such as `Running` with `Finished`, or parent pipeline running with child discovery failed.
- Lifecycle invariants are covered by backend tests and frontend display tests.

Current closure note:

- `data/admin-task-lifecycle.json` is the lifecycle ledger authority. Legacy state/history helpers are reserved for explicit migration, tests, and maintenance tooling rather than production lifecycle projection.
- Fetch/discovery reports, fetch task files, logs, and live event/output files remain valid operational evidence artifacts; they are not lifecycle authority.
- Lifecycle rows must not be treated as display-progress authority. Stale copied `progress`, `summary`, or `taskProgress` values should be ignored or stripped by projection code once the closeout work lands.
- Open gap from the 2026-05-08 packaged run: active fetch rows can still display stale lifecycle progress because `/ops/task-state` is not yet fully routed through the shared fetch live projection, and `/ops/fetch-report` can be shadowed by stale runtime `.jsonl` journal data. These are release-blocking closeout items for the lifecycle ledger authority work.

## Suggested implementation order

1. Introduce `TaskLifecycleService` and invariant tests.
2. Wire discovery/fetch/sync launch and terminal paths.
3. Wire pipeline parent/child ownership.
4. Switch ops projection routes to lifecycle-backed rows.
5. Simplify frontend run model and renderer assumptions.
6. Run focused bridge/Admin/frontend validation.
7. Build a portable executable and perform one long-running pipeline smoke.
