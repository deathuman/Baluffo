# Task Lifecycle Ledger Plan

> - **Status:** Lifecycle core implemented; targeted hardening ongoing
> - **Use this when:** fixing Admin Current Runs / Recent Runs contradictions, pipeline child-task ownership, task heartbeat/orphan behavior, or long-running discovery/fetch lifecycle bugs
> - **Canonical for:** the planned single-source-of-truth task lifecycle refactor
> - **Not canonical for:** current route payload contracts, report schemas, fetch/discovery output contracts, or release procedures
> - **Then inspect:** [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and the bridge task lifecycle modules
> - **Last updated:** 2026-05-07

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
- Switched `/ops/task-state` and `/ops/history` to lifecycle-first rows while preserving legacy terminal fallback for the transition release.
- Added bridge-startup cleanup that orphans stale lifecycle rows whose owner cannot survive a desktop bridge restart.
- Simplified Admin Current Runs so the frontend trusts backend lifecycle rows and does not retain missing active rows for one extra sample.

Remaining hardening:

- Run long real discovery/fetch smoke beyond the old nominal timeout window.
- Remove legacy fallback after one transition release.

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

Frontend tests:

- Current Runs only renders lifecycle `running` rows.
- Recent Runs only renders terminal rows.
- Active rows never show a Finished timestamp.
- Selected Run Analysis omits Finished for active rows.
- Parent and child rows stay consistent through discovery, fetch, and sync stages.

Integration smoke:

- Run jobs pipeline with discovery longer than 15 minutes.
- Confirm pipeline remains Current Running while discovery is active.
- Confirm discovery child remains Current Running or parent-owned, not Failed.
- Confirm discovery completion advances to fetch.
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

## Suggested implementation order

1. Introduce `TaskLifecycleService` and invariant tests.
2. Wire discovery/fetch/sync launch and terminal paths.
3. Wire pipeline parent/child ownership.
4. Switch ops projection routes to lifecycle-backed rows.
5. Simplify frontend run model and renderer assumptions.
6. Run focused bridge/Admin/frontend validation.
7. Build a portable executable and perform one long-running pipeline smoke.
