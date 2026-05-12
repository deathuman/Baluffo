# Task Lifecycle Ledger Closeout

> - **Status:** Archived
> - **Use this when:** reviewing historical closeout evidence for Admin task lifecycle authority, Current Runs / Recent Runs consistency, task progress projection, pipeline child ownership, or packaged lifecycle smoke validation
> - **Canonical for:** historical lifecycle ledger closeout evidence only
> - **Not canonical for:** route payload contracts, report schemas, fetch/discovery output contracts, or release procedures
> - **Then inspect:** [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`storage-contract.md`](../storage-contract.md), [`testing.md`](../testing.md), `src/bridge/task_lifecycle.py`, `src/bridge/ops_api.py`, and `src/bridge/pipeline_service.py`
> - **Last updated:** 2026-05-12

## Implementation Status

All code changes complete and verified by 161 backend + 10 frontend tests,
including an end-to-end smoke pipeline test that exercises the full
discovery→fetch→completion flow using the stub-success mode.

| Step | Description | Status |
|------|------------|--------|
| 1 | Runtime evidence IO hardening (`load_runtime_evidence`) | Done |
| 2 | Regression coverage (5 test scenarios + frontend + e2e smoke) | Done |
| 3 | Task-state projection parity (discovery + sync enrichment) | Done |
| 4 | Legacy lifecycle helper exposure (6 call sites) | Done |
| 5 | `load_json_array` mtime guard + runtime evidence guard | Done |
| 6 | Final packaged validation (e2e smoke test) | Done |
| 7 | Close docs | Done |

## Summary

This page is the archived tracker for the completed Admin task/progress lifecycle closeout.

The older task/progress operational console tracker is closed: Admin task/progress logs now use the shared task-run presenter for run-state labels, compact history remains compact, and detailed operator evidence stays behind analysis/disclosure surfaces. That work has no active follow-up here except where it depends on correct lifecycle rows and progress evidence.

The lifecycle ledger work is mostly implemented. In the current JSON-backed runtime, `data/admin-task-lifecycle.json` is the backend authority for task identity, liveness, parent/child ownership, start/end timestamps, and terminal state. Reports, live task artifacts, logs, and sync runtime files are evidence sources for display progress and operator detail; they do not own lifecycle state.

The final goal was to remove the remaining ambiguity between lifecycle authority and operational evidence, add the last regression coverage for the 2026-05-08 packaged findings, and perform one real packaged pipeline validation before this plan is archived. Current SQLite/WAL runtime authority behavior is owned by [`storage-contract.md`](../storage-contract.md).

## Current State

Done:

- `TaskLifecycleService` exists in `src/bridge/task_lifecycle.py` and persists lifecycle rows to `data/admin-task-lifecycle.json`.
- Fetch, discovery, sync, and pipeline launch/terminal paths create or finish lifecycle rows instead of using `admin-run-history.json` as lifecycle authority.
- Pipeline child ownership is explicit through `parentRunId` / `parentTaskType`, with child rows attached to the parent pipeline.
- Pipeline discovery/fetch waits use quiet-evidence timeouts plus a distinct absolute safety-cap terminal reason instead of failing only because a nominal duration elapsed.
- Bridge startup no longer imports legacy history/state rows into the lifecycle ledger as normal runtime behavior; legacy import is explicit migration/test tooling.
- `/ops/history` reads recent lifecycle rows, and `/ops/task-state` starts from current lifecycle rows.
- Admin Current Runs no longer keeps missing backend active rows alive for an extra frontend sample.
- Active fetch rows in `/ops/task-state` use the shared fetch live projection, so Current Runs and Selected Run Analysis receive the same fetch progress shape as `/ops/task-live/fetch`.
- Fetch live projection merges same-run `jobs-fetch-tasks.json`, `jobs-fetch-report.json`, and lifecycle identity; it recomputes `ratio` from counts and keeps key count fields monotonic across evidence sources.
- Older adjacent JSON journals no longer shadow newer canonical JSON files in `load_json_object()`. A journal overlay only wins when the journal file is newer than the canonical JSON.
- Pipeline child liveness rejects lifecycle rows whose recorded `ownerPid` is no longer running, unless newer report/task evidence is observed through the normal evidence path.
- Fetch lifecycle closeout treats source-level `failedSources` as operational evidence, not task failure. Only explicit failed/error status or `summary.error` fails the fetch lifecycle row.
- The shared task-run presenter adoption from the operational console tracker is complete through `frontend/shared/task-run-view-model.js`, `frontend/admin/app/fetcher/report.js`, `frontend/admin/app/fetcher/watch.js`, and `frontend/admin/app/discovery/progress.js`.

Still intentionally present:

- `admin-task-state.json` and `admin-run-history.json` helpers remain for explicit migration, tests, and maintenance compatibility. They must not become normal lifecycle authority again.
- Fetch/discovery reports, fetch task files, logs, sync live task files, and bridge events remain valid operational evidence artifacts.
- Downloadable diagnostics export remains outside this lifecycle closeout unless a separate support workflow asks for it.

## 2026-05-08 Packaged Findings

The packaged run exposed two concrete failure classes.

Fetch progress drift:

- Fetcher Output showed fresher counts than Current Runs / Selected Run Analysis.
- The row path was using stale lifecycle progress while the fetcher log and report/task artifacts had newer evidence.
- Code inspection now shows the fetch path has been moved through `ops_task_fetch_live.build_fetch_live_payload()`, with count-based ratio recomputation.
- Remaining work is route-level regression coverage and packaged validation, not another frontend merge workaround.

Pipeline stuck after fetch completion:

- A fetch child completed, but stale `jobs-fetch-report.jsonl` evidence caused the bridge to miss terminal `jobs-fetch-report.json` and later mark the child `owner_inactive_without_terminal_report`.
- Code inspection now shows older journals cannot override newer canonical JSON, pipeline child liveness checks owner PID, and fetch completion with source-level failures closes as completed.
- Remaining work is a regression that exercises the route/pipeline path with stale journal plus terminal JSON, then a real packaged run proving the parent pipeline advances out of `stage=fetch`.

## 2026-05-11 Deep Audit Findings

A full stress-test of every code path listed in the "Done" section found seven concrete gaps the plan understated or missed. The 2026-05-08 mtime fix was partial — it prevents older journals from winning, but does not prevent a journal with newer mtime and stale content from shadowing a canonical terminal JSON.

### Finding 1: Journal overlay is still active on all runtime evidence (not just registry)

`load_json_object()` in `src/source_registry_io.py:260-284` applies journal overlay on every read. The 2026-05-08 fix added mtime comparison (`_json_journal_should_overlay_base`, line 398-407), but this is a temporal heuristic: if the `.jsonl` journal has a newer mtime than the canonical `.json`, the journal wins — even if the journal content is stale. A late progress update appending to the JSONL *after* the terminal JSON write would produce exactly this condition.

Affected critical paths:

| Code path | File:line | Impact if shadowed |
|-----------|----------|-------------------|
| Pipeline wait loop | `pipeline_service.py:721` | Stale report → pipeline never sees terminal → stuck at `stage=fetch` |
| Inactive-worker recovery | `pipeline_service.py:381` | Stale report without `finishedAt` → recovery can't detect terminal child |
| Heartbeat refresh | `admin_entrypoint_services.py:123` | Stale progress written into lifecycle ledger rows |
| Snapshot building | `run_history_api.py:594-597,631-632` | Stale `ChildTaskSnapshot` → wrong liveness/terminal state for all downstream consumers |
| Fetch live projection | `ops_task_fetch_live.py:131,142` | Stale fetch counts in Current Runs / Fetcher Output |
| Discovery live projection | `ops_task_discovery_live.py:372-373` | Stale discovery phase progress |
| Sync live projection | `ops_task_projection.py:120` | Stale sync task state |

**The plan's original "minimum acceptable state" (keep journal overlay, document as intentional) is rejected.** Runtime evidence files must bypass journal overlay entirely. A journal should only shadow the canonical JSON for registry artifacts that explicitly opt into journal semantics.

### Finding 2: `load_json_array` has no mtime guard at all

`load_json_array()` at `src/source_registry_io.py:246-257` unconditionally prefers the journal if it exists as a list. No mtime comparison exists. While `load_json_array` is not currently called for runtime evidence files, it is a sibling function in the same IO module and a future landmine if any runtime evidence ever switches to array format.

### Finding 3: `_build_child_task_snapshot` uses `admin-task-state.json` for PID liveness, not the lifecycle ledger

At `run_history_api.py:341`, `_load_task_state_entry(task_type, load_json_object, task_state_path)` reads `admin-task-state.json` to determine `state_active`. The lifecycle ledger's `ownerPid` is NOT consulted here. This creates a split-liveness regime:

- `pipeline_child_run_is_live` at `admin_entrypoint_services.py:569-593` checks lifecycle ledger `ownerPid` — correct
- `_child_task_is_active` (fallback in `pipeline_service.py:197`) checks `ChildTaskSnapshot.active` — built from `admin-task-state.json` PID — stale source

The two can disagree: lifecycle says alive, snapshot says dead; or vice versa. The fallback path at `pipeline_service.py:185-197` will use the wrong source when `_child_run_is_live` returns `False`.

### Finding 4: `sync_task_running()` is a production path, not migration — it reads AND writes `admin-run-history.json`

At `admin_task_runtime.py:190-220`, `sync_task_running()` calls `reconcile_sync_history_locked()` which at `run_history_api.py:579-581` both `load_run_history()` and `save_run_history()`. This is called every time sync state is checked, not only at migration. It is a live production dependency, not a stale constructor parameter to remove.

### Finding 5: No test for "journal newer mtime but stale content" scenario

The two existing tests at `test_source_registry_io.py:46-65,68-87` only cover:
- Stale journal with OLDER mtime + newer canonical JSON → canonical wins
- Newer journal with stale canonical JSON → journal wins (normal case)

The dangerous case — newer journal mtime with stale content shadowing a canonical terminal JSON — has zero coverage. The mtime comparison is a temporal heuristic, not a content-freshness guarantee, but no test validates what happens when the two diverge.

### Finding 6: No end-to-end pipeline or frontend regression test

All existing tests are Python backend unit tests. The packaged failure manifested as Admin UI divergence (Current Runs vs Fetcher Output). No test verifies that `frontend/shared/task-run-view-model.js` correctly receives and renders enriched lifecycle data after the regression fixes. Frontend unit test file `tests/frontend/unit/task-run-view-model.test.mjs` covers per-type dispatch but doesn't exercise the drift scenario end-to-end.

### Finding 7: Discovery/sync live projections also read through journal overlay

The plan's Step 3 (projection parity) focuses on adding enrichment for discovery/sync rows in `/ops/task-state`, but does not address the fact that even the existing discovery/sync live projections at `ops_task_discovery_live.py:372-373` and `ops_task_projection.py:120` read their evidence through `load_json_object()` with journal overlay. If journal overlay is fixed for fetch but left active for discovery/sync, the inconsistency simply moves to a different task type.

## Remaining Closeout

Only these steps should block final closure.

### 1. Runtime evidence IO hardening — create a separate reader, reject journal overlay on evidence

**Decision:** The "minimum acceptable state" (keep journal overlay, document as intentional) is rejected. Runtime evidence files must bypass journal overlay entirely.

Implementation:
- Add `load_runtime_evidence(path, default)` to `src/source_registry_io.py` that reads the canonical JSON file directly — no journal check, no mtime comparison, no `.jsonl` fallback. If the canonical file is absent or corrupt, return the default.
- Route all runtime evidence reads through it: fetch reports, fetch tasks, discovery reports, sync live task files, lifecycle rows, logs, bridge events.
- Keep `load_json_object()` with journal overlay only for registry artifacts that explicitly opt into journal semantics (`source-registry-active.json`, `source-registry-pending.json`, `source-registry-metadata.json`).
- Remove journal overlay from the following call sites (map to `load_runtime_evidence`):

| File:line | Artifact |
|-----------|----------|
| `ops_task_fetch_live.py:131,142` | `jobs-fetch-report.json`, `jobs-fetch-tasks.json` |
| `ops_task_discovery_live.py:372-373` | `source-discovery-report.json` |
| `ops_task_projection.py:120` | `sync-live-task.json` |
| `run_history_api.py:594-597,631-632` | `jobs-fetch-report.json`, `jobs-fetch-tasks.json`, `source-discovery-report.json` |
| `pipeline_service.py:381,579,721` | `jobs-fetch-report.json`, `source-discovery-report.json` |
| `admin_entrypoint_services.py:123` | `jobs-fetch-report.json`, `jobs-fetch-tasks.json` |
| `ops_api.py:309,405` | `jobs-fetch-report.json`, `source-policy-soak-report.json` |

- Cover the change with a test proving that a stale `.jsonl` journal with ANY mtime does not affect `load_runtime_evidence` results.

### 2. Regression coverage — add 5 specific test scenarios

Replace the original 3 scenarios with this expanded set:

1. **Combined stale-journal + terminal-JSON through routes.** Test `GET /ops/fetch-report?view=live` when a `jobs-fetch-report.jsonl` (newer mtime, stale content) exists alongside a `jobs-fetch-report.json` (older mtime, terminal content). Assert the route returns the canonical terminal report, not the journal payload. Same for the pipeline `wait_for_report_completion` path.

2. **Combined stale-journal + terminal-JSON with newer journal mtime.** Test the dangerous case where the `.jsonl` journal has a newer mtime but stale content (no `finishedAt`) while the canonical `.json` has an older mtime with terminal data. Assert the canonical JSON wins via `load_runtime_evidence`. Before Step 1 is implemented, this test will FAIL — proving the gap exists.

3. **`/ops/task-state` fetch progress parity.** Assert that when the lifecycle ledger has stale progress, the active fetch row in `GET /ops/task-state` still matches the shared live fetch projection (`ops_task_fetch_live_mod.build_fetch_live_payload()`). The enrichment at `ops_api.py:555-578` must fire correctly.

4. **Fetch completion with `failedSources > 0` closes as `succeeded/completed`.** Already partially covered at `test_admin_bridge_task_launch.py:346-378`. Add route-level coverage through `/ops/task-state` and `/ops/history` asserting the terminal row has `lifecycleStatus=succeeded/completed` when `report.status` is not `error/failed/failure` and `summary.error` is absent.

5. **Frontend regression test for progress convergence.** Add a test to `tests/frontend/unit/task-run-view-model.test.mjs` that verifies `buildTaskRunView()` does not show stale progress when lifecycle snapshot lags behind live evidence. Assert `progressStale` is `false` when live evidence is fresher.

### 3. Active task-state projection parity — extend enrichment to discovery and sync

~~"Either route or document why fetch-only"~~ → **Route them.** The enrichment is already implemented for discovery and sync in their respective `build_*_live_payload` functions. The gap is only in the route handler `ops_api.py:524-544`.

Implementation:
- In `ops_api.py:get_current_task_state_payload()`, after `task_type == "fetch"` enrichment (line 555-578), add equivalent blocks for `task_type == "discovery"` and `task_type == "sync"`.
- Discovery enrichment: call `ops_task_discovery_live_mod.build_discovery_live_payload()` (already imported at `ops_task_live.py:9`).
- Sync enrichment: call `ops_task_projection_mod.build_sync_live_payload()` (already at `ops_task_projection.py:113`).
- Pipeline child summaries at line 501-519 must also prefer fresher child evidence from live projections over stale copied lifecycle progress.
- Verify that after Step 1 is implemented, these discovery/sync live projections no longer read through journal overlay.

### 4. Narrow legacy lifecycle helper exposure — specific targets

The audit identified these concrete production call sites. Each must be addressed:

| File:line | Function | Reads/Writes | Action |
|-----------|----------|-------------|--------|
| `run_history_api.py:341` | `_build_child_task_snapshot` → `_load_task_state_entry` | Reads `admin-task-state.json` | Switch PID liveness to lifecycle ledger `ownerPid` via `get_lifecycle_current_runs()`. Remove `task_state_path` parameter from `_build_child_task_snapshot` and `SyncHistoryDeps`. |
| `run_history_api.py:579-581` | `reconcile_sync_history_locked` | Reads AND writes `admin-run-history.json` | Stop writing. The read may still be needed until lifecycle history fully replaces run-history projection, but writes must stop. Called from `sync_task_running()` at `admin_task_runtime.py:193`. |
| `run_history_api.py:677-681` | `sync_history_from_reports` | Writes `admin-run-history.json` via `deps.save_run_history(projection.rows)` | Stop writing. Called on bridge startup from `ops_history_projection.py:47-54` → `ops_api.py:348-349` → `admin_bridge.py:504-505`. |
| `admin_task_runtime.py:248` | `reconcile_lifecycle_legacy_state` | Reads `admin-task-state.json` | OK — this is an explicit migration function. Keep but rename to make the boundary obvious (e.g., `migrate_legacy_task_state_to_lifecycle`). |
| `lifecycle_cleanup.py:148` | `close_stale_task_rows` | Reads `admin-task-state.json` | Switch dead-PID check to lifecycle ledger `ownerPid`. This is startup maintenance, not migration, so it needs the correct authority source. |
| `task_history.py:130-141` | `_clear_task_state_locked` / `clear_task_state` | Writes `admin-task-state.json` | Remove writes after lifecycle start/finish paths no longer depend on this file. Clear the task state entry via lifecycle ledger APIs instead. |

After all call sites are addressed, remove `task_state_path` from `SyncHistoryDeps` (`run_history_api.py:213`) and any remaining constructor parameters that pipe the legacy path through the production call graph.

### 5. `load_json_array` mtime guard

`load_json_array` at `source_registry_io.py:246-257` has no mtime comparison — it unconditionally prefers the journal. Add mtime comparison matching `load_json_object`'s behavior (`_json_journal_should_overlay_base`). Add a test proving a stale journal with older mtime does not shadow a newer canonical JSON array.

Additionally, add an assertion or type-check that `load_json_array` is only used for registry artifacts (active registry, pending registry), never for runtime evidence files. If a future call site passes a runtime evidence path, it should raise a clear error.

### 6. Final packaged validation — 6 assertions

Build the portable app and run a full jobs pipeline long enough to exceed the old nominal discovery/fetch timeout window. Confirm:

1. Discovery remains current while live (no stale progress in Current Runs).
2. Fetch progress converges across Current Runs / Selected Run Analysis / Fetcher Output within one poll of `GET /ops/task-state`.
3. `GET /ops/fetch-report?view=live` returns the current terminal report even if a stale `.jsonl` file exists on disk.
4. The parent pipeline advances out of `stage=fetch` after its fetch child produces a terminal report.
5. All child/parent rows land in Recent Runs with non-null terminal timestamps.
6. No stale `.jsonl` runtime evidence files remain on disk after fetch completion that could shadow the canonical JSON on the next run.

### 7. Close the docs after validation

- Update [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`testing.md`](../testing.md) only if the final IO/projection changes alter their contracts or verification commands.
- Archive this page after the packaged validation passes and no production lifecycle authority remains outside the current lifecycle backend. Current SQLite/WAL runtime authority behavior is owned by [`../storage-contract.md`](../storage-contract.md).

## Closure Criteria

This work is done when:

- In the current JSON-backed runtime, `data/admin-task-lifecycle.json` is the only production authority for task identity, liveness, parent/child ownership, timestamps, and terminal state.
- Reports and task artifacts can enrich progress and summaries but cannot independently mark a task live, failed, canceled, or orphaned.
- `/ops/task-state`, `/ops/history`, `/ops/task-live/<taskType>`, and selected-run analysis agree on the same active run identity and terminal state.
- Active fetch, discovery, and sync progress is computed from same-run evidence counts and cannot move backward because of stale lifecycle snapshots.
- Runtime evidence files (fetch reports, fetch tasks, discovery reports, sync live task files) are read through `load_runtime_evidence()` and are never subject to journal overlay. No stale `.jsonl` journal — regardless of mtime — can shadow a canonical runtime JSON file.
- `load_json_array` has mtime comparison parity with `load_json_object` and is guarded against use for runtime evidence files.
- Normal source-level fetch failures remain operator evidence and retry input, not task-level lifecycle failure.
- Legacy `admin-task-state.json` and `admin-run-history.json` behavior is either removed from production paths or explicitly isolated as migration/test/maintenance tooling. All PID liveness checks use lifecycle ledger `ownerPid`, not `admin-task-state.json` entries.
- `_build_child_task_snapshot` in `run_history_api.py` no longer depends on `task_state_path` for liveness.
- `reconcile_sync_history_locked` and `sync_history_from_reports` no longer write `admin-run-history.json`.
- Focused backend and frontend regression tests plus one packaged pipeline smoke cover the 2026-05-08 failures and the 2026-05-11 audit findings.
- No stale `.jsonl` runtime evidence files remain on disk after fetch completion.

## Historical Routing

- The old operational console closeout is now a short historical pointer at [`../archive/task-progress-operational-console-closeout.md`](../archive/task-progress-operational-console-closeout.md).
- The unindexed underscore note [`task_lifecycle_ledger_closeout_plan.md`](task_lifecycle_ledger_closeout_plan.md) is superseded by this archived closeout and should not be extended.
