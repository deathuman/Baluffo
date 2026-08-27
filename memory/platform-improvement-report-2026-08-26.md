# Baluffo Platform Improvement Report — 2026-08-26

Prioritized general improvements beyond the Jobs boot-perf work. The cross-cutting P0 tech-debt tracker is **closed** (see its non-goals: BridgeApi field deletion, updater facade removal, macOS, full CSS minification).

## Status
- **Tier-1 #1 — pause UI interval polls when tab hidden**: DONE (`ade7d9de`). New `frontend/shared/visibility-poll.js` (`createVisibilityPausedInterval`); wired into `admin-bridge-button.js`, `action-center.js`, `bridge-status.js`. Desktop lifecycle heartbeat intentionally left running (app-liveness). Unit tested.
- **Tier-1 #2 — wiring-contract tests**: DONE (`8b0763db`). Boot.js `init()` got an injection seam `(deps.initJobsFeed ?? initJobsFeed)`; new behavioral test `jobs-runtime-wiring.test.mjs` asserts runtime mode forwards into `initJobsFeed` in both modes. Updated the brittle source-text guard in `jobs-container-boot-waterfall.test.mjs` to tolerate the seam. Prevents the v0.2.139 silent-dep-drop class.
- **Tier-1 #3 — boot-perf regression gate in CI**: DONE (`f4ee3161`). `npm run measure:container-jobs-boot` (cold/warm/nav) + new `.github/workflows/jobs-boot-perf.yml` builds+runs the container and fails on regression. Deliberately NOT in local `release:preflight` (no live container there).
- **Tier-2 #4 — version-bump automation**: DONE (`55911ce4`). `scripts/bump_version.py` syncs `app_version.py`, `umbrel-app.yml`, `docker-compose.yml`, `CHANGELOG.md` (rolls [Unreleased] into a dated section), `dist/baluffo-ship/app/current.txt`, then regenerates `release-notes.md`. Note: the docs guardrail `test_release_docs_cover_the_current_public_release_line` requires the rolled-up section to contain deployment-coverage prose (same-origin Linux container / Umbrel raw-LAN / GHCR multi-arch) — that prose must be authored in [Unreleased] *before* bumping, so the guardrail is expected to fail on a bare bump until prose exists.
- **Tier-2 #5 — retire shadow dual-write**: DONE (`078cb29a`). `src/bridge/admin_task_lifecycle.py` shadow mode read JSON + compared to SQLite on every request; SQLite was already the seeded authority for taskRuns/taskEvents and shadow was unreachable in prod. Removed the comparison + dead `_compare_route_projection`/`_route_key` helpers; shadow now behaves as pure SQLite (JSON kept only as a fallback when the store is missing/errors). Regression test `tests/test_admin_task_lifecycle_shadow_retire.py` locks it. `set_authority_mode` still lists "shadow" as an accepted mode (now a no-op alias for sqlite) — left intentionally to avoid contract churn.
- **Tier-2 #6 — replace `?v=N` cache-busting**: DONE (`bae048bc`). Root cause: `static_files.py` served JS/CSS with `max-age=3600` and no validator, so `?v=N` on ~100 import sites + HTML tags was the only bust. Fix: server emits `ETag` (sha1 of body) + `Cache-Control: public, no-cache`, with 304 when `If-None-Match` matches — caching stays but auto-busts on content change. Removed every `?v=N` from `frontend/**/*.js` and root `admin.html`/`jobs.html`/`saved.html`; updated `tests/frontend/unit/jobs-html.test.mjs`; added `tests/bridge/test_static_files_cache_headers.py` (ETag + 304). Container bundle uses content-hashed immutable assets, unaffected; esbuild `strip-import-query` now strips nothing.
- **Tier-2 #9 — dedupe admin-bridge watcher wiring (jobs/saved)**: DONE (`c5ecdc96`). The Jobs and Saved pages built `createAdminBridgeButtonWatcher` with ~identical desktop-readiness/degrade/status-path config. Extracted `createAdminBridgeButtonWatcherForPage` into `frontend/shared/admin-bridge-button.js`; both boot files now pass only page-specific inputs (buttonEl + applyState + runtime-mode fns). Behavior unchanged. Added 2 unit tests (derived status path + bootstrap delegation). Note: the actual auth controllers (`authController` vs `savedAuthController`) were intentionally NOT merged — that is a separate, security-sensitive change not implied by this item.

## Closed (no code change)
- **Tier-3 #7 — guardrail assert-message terseness**: Closed. Inspected `release_docs_policy.py` and the line-budget checks: they use bare `assert <bool>` with no failure message (cryptic on failure). Decided NOT to sweep — it is a subjective style-only change across dozens of asserts, broad churn, low value.
- **Tier-3 #8 — verify desktop update-rehearsal against real published manifests**: Closed as a release-checklist item (not code). The rehearsal (`src/ship/packaged_smoke/rehearsal_update.py:535`) is self-contained: it generates its own Ed25519 keypair, signs a manifest locally, and runs the full update flow against a local release server — already exercising the real signing/verification code path. "Real published manifest" verification belongs at release time. Recorded in `docs/RELEASE.md` Portable EXE Verification: once the GitHub release is live, validate the updater end-to-end against the real release-key-signed manifest + assets. (commit `0e9ed683`)
- **Tier-3 #8** — verify desktop update-rehearsal (`packaged_desktop_smoke.py --desktop-update-rehearsal`) against real published manifests.
- **Tier-3 #9** — dedupe shared auth/guest wiring between jobs/saved pages.

## Excluded (per closed tracker / guardrails)
BridgeApi interface splitting, updater facade deletion, admin_bridge.py deletion, macOS, full CSS minification.

## Unexplored-area audit + P0 correctness fixes (2026-08-26, after the report)
Investigated areas NOT covered by the original report: desktop app runtime/packaging,
discovery/source-policy, pipeline/sync/scheduling/registry. Each candidate was validated
against the code before fixing (this caught two false positives).

### Implemented (commit 1c012f93)
- **P0-1 sync wedge**: `src/bridge/sync_task_flow.py` — `_run_sync_task` cleanup
  (`remove_active_sync_run/thread`) now lives in an outer `finally`, so an unexpected
  exception can't leave the run id in `ACTIVE_SYNC_RUNS` (which blocked all syncs + the
  pipeline sync stage forever).
- **P0-2 sync double-exec**: `sync_task_flow.py` — `except (TypeError, ValueError)` no longer
  re-invokes `action_func`; only the signature inspection is guarded, so a pull/push raising
  those types runs exactly once.
- **P0-3 pipeline refused restart**: `pipeline_service_stages.py` `_run_worker` now catches broad
  `Exception`, marks the run errored (clears `active`) and re-raises — an unexpected fault no
  longer leaves `active=True` and blocks future starts.
- **P0-4 discovery finalization timeout**: `pipeline_service_stages.py` `_wait_for_discovery_auto_approval`
  now raises `RuntimeError` on the 10-min timeout instead of silently proceeding into fetch.
- **P0-6 sync start TOCTOU**: `sync_service.py` `start_sync_task` does check+register atomically
  under the (reentrant RLock) `_ops_state_lock`.

### Validated and dropped
- **P0-5 sync counters lock**: `sync_state.SyncState.update_sync_counters` has NO callers (dead);
  the real counter path `source_sync_runtime.update_sync_counters` is already lock-guarded. No change.

### Implemented (commit bac22c54) — P1 perf batch
- lifecycle heartbeat amplification (mirror single changed row instead of ALL rows + full parity every ~1s);
  registry `load_tombstones()` hoisted out of the per-bucket `normalize_state` loop; status lock released
  before control-status IO in `start_task`; `heartbeatAt` now populated so pipeline stall detection works;
  discovery watch reuses the already-read report; source-policy backfill uses a per-call identity index
  (was O(N^2)); source-checker caps per-page detail fetches. Dropped as false positives: discovery
  identity-signature "3x" (distinct states, O(1)) and registry "JSON twice / authority rebuilt" (mutually
  exclusive branches). Verified: 144 targeted tests + ruff + repo guardrails green.

### Implemented (commit e66282b4) — P2 desktop parity + P3 dead-code cleanup
- **P2-2 port fallback**: `src/ship/desktop_app/_linux.py` — added `_pids_listening_on_tcp_port_via_proc`
  parsing `/proc/net/tcp`+`/proc/net/tcp6` (state 0A) and mapping socket inodes → PIDs via `/proc/*/fd`
  readlinks; used when psutil is absent and as a fallback if `net_connections` raises AccessDenied.
- **P2-4 poll-exit**: `src/ship/desktop_app/_linux.py` — `_poll_process_exit_until_timeout` now re-checks
  liveness after the loop (keeps the 0.0 floor for instant checks).
- **P3-1a dead params**: `pipeline_service.py` + root wiring dropped `append_run_history`,
  `upsert_run_history`, `clear_task_state` (never stored/used); updated 6 PipelineService test
  constructors.
- **P3-1b RunHistoryFuncs**: removed unused protocol from `sync_service.py` + `__init__.py` re-export +
  `whitelist.py` entry.
- **P3-1c _reconcile_sync_history**: removed the no-op method and its call in `sync_task_running`.
- **P3-2 Event().wait**: replaced 4 one-shot sleeps with `time.sleep` (admin_task_runtime,
  discovery_service_watch, pipeline_service_stages, pipeline_service_children).
- **P3-3 unlocked status reads**: wrapped two `self._status` reads under `self._lock`
  (pipeline_service_status, pipeline_service_lifecycle).

### Validated and dropped (false positives)
- **P2-1** `.resolve()` already applied on Linux (stored exePath resolves via `api._current_exe_path()`).
- **P2-3** rehearsal rollback/version-floor already unit-tested in the symlink/source-watch suites.
- **P3-1d** `prune_started_rows_for_type` is live (injected dep, called at sync_task_flow.py:380).
- **P3-4** non-serializable status silently dropped is false: `TypeError` propagates.

### Also fixed
- Pre-existing P1 regression: `test_pipeline_service_control_files.py` now supplies enough `now_iso`
  timestamps for the `heartbeatAt` population added in bac22c54 (3 calls: child-attach write, heartbeatAt,
  snapshotAt).

### Test gate
bridge 628 + admin 246 tests pass; ruff + repo guardrails green. One pre-existing unrelated failure
remains: `tests/bridge/test_container_runtime.py::test_container_handler_serves_static_data_and_runtime_config`
(CSS Cache-Control expected `public, max-age=3600`, got `public, no-cache`) — outside P0–P3 scope.

### Follow-up batch (2026-08-27): CI gate alignment — mypy + knip wired into the lint workflow
- `.github/workflows/lint.yml` now runs `npm run typecheck:py` (mypy) and `npm run lint:deadcode:js`
  (knip) on every push/PR.
- Findings cleared so the new gates land green: mypy interface stubs in `src/bridge/task_lifecycle_core.py`
  (`_write_rows_json_locked`, `_mirror_row_to_storage`), `str | None` annotation in `server/handler.py`
  `_etag_matches`, and `cast(Path, ...)` in the /proc port-detection tests; knip unused-export fix in
  `frontend/admin/render/source-policy-review.js` (`renderSourcePolicyBulkToolbar` un-exported — internal
  call site and the bulk-bar render test preserved); `measure_container_jobs_boot.mjs` and
  `page_load_audit.mjs` import chromium from `@playwright/test` (declared devDependency) instead of the
  transitively-hoisted `playwright`.
- `docs/RELEASE.md`: new "Local Preflight vs CI Gate Coverage" section (preflight passed ≠ all gates;
  the container Jobs boot-perf gate and eslint stay outside local preflight) plus the rule to author the
  `[Unreleased]` compatibility sentence BEFORE running `scripts/bump_version.py`.
- `docs/CHANGELOG.md`: compatibility sentence authored in `[Unreleased]` — 0.2.140 bump prep for the
  auto-hydrate full-feed change (`61084862` + `fb05d1c0`, still unreleased).
- Gates at closeout: mypy 1210 source files clean; knip clean; eslint clean on touched file; frontend
  unit suite exit 0; targeted pytest 21 passed (proc port detection, static-file cache headers, shadow
  retire); repo guardrails all groups green. Extended suite runs at pre-push.
