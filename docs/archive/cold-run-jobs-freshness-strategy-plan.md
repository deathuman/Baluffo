# Cold-Run Jobs Freshness Strategy Plan

> - **Status:** Archived — fully implemented 2026-05-28
> - **Use this when:** verifying the cold-run bootstrap behavior, inspecting the pre-implementation strategy and loophole audit
> - **Canonical for:** archived cold-run UX strategy record
> - **Not canonical for:** current route contracts; see active docs instead
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-28

## Summary

The first cold-run Jobs experience should remain useful without pretending packaged listings are fresh. With the staged Google Sheets bootstrap as the first-run data path, the package should stop shipping row-bearing jobs snapshots, automatically retrieve sheet jobs on first run, and keep limited-coverage messaging visible until the app has real full-pipeline evidence.

The original strategy should not be treated as 100% safe as written. The loophole pass found stale carryover, empty-feed overwrites, false `Last updated: just now` freshness, weak full-pipeline detection, and an ignored `allowSheetsFallback` flag. The revised strategy closes those known gaps.

The "no bundled jobs" state is acceptable only as a short progress or retry state. A stripped package must not land users on a silent empty feed; it must immediately enter bootstrap-in-progress UI when the bridge can start the bootstrap, or show a clear retryable no-data state when offline or blocked.

This plan is documentation only. It records the intended strategy and guardrails; it does not make the proposed route names, report metadata, or frontend behavior canonical until implementation and contract docs are updated.

## Repo-Grounded Audit

- `scripts/build_ship_bundle.py` currently copies `jobs-unified-light.json`, `jobs-unified.json`, `jobs-unified.csv`, and `jobs-fetch-report.json` through `APP_RUNTIME_DATA_FILES`.
- The same build script currently generates `jobs-unified-startup.json` from the first rows of `jobs-unified-light.json`.
- `src/ship/runtime_launcher.py` serves runtime data first and static package data second for `jobs-unified-startup.json`, `jobs-unified-light.json`, `jobs-unified.json`, and `jobs-unified.csv`.
- `frontend/jobs/app/feed.js` already attempts to disable Sheets fallback on first load with `allowSheetsFallback: !firstLoad`, but `frontend/jobs/app/runtime/feed-controller.js` and `frontend/jobs/app/sources.js` do not currently pass that option through.
- `src/jobs/pipeline_run_setup.py` can still seed from existing output when `BALUFFO_FETCH_SEED_EXISTING_OUTPUT` is set, even if the caller passes `seed_from_existing_output=False`.
- `src/bridge/ops_health.py` currently treats only `fetch_never_run` as non-dismissible and derives successful fetch history from fetch rows, not full pipeline completion rows.

## Loophole Audit

- Automatic versus manual bootstrap: after row-bearing packaged artifacts are stripped, first-run sheet retrieval cannot remain purely user-chosen without reintroducing the harsh empty-start problem. Bootstrap must auto-start once for an empty runtime data directory, with user-controlled retry after a recorded failure.
- Existing install residue: stripping files from a new package does not guarantee old `data/jobs-unified*` files disappear from an already-installed portable/runtime directory. Startup must quarantine stale seed-era row artifacts when there is no successful runtime report, while preserving legitimate user runtime feeds.
- Stale carryover: the existing `onlySources` fetch path can seed from existing output, so a sheet-only run could keep stale non-sheet rows unless bootstrap explicitly disables existing-output seeding.
- Environment override carryover: `BALUFFO_FETCH_SEED_EXISTING_OUTPUT` can force seeding in the pipeline setup. Bootstrap must unset or ignore that override and fail promotion if the staged report says it seeded existing output.
- Empty-feed overwrite: normal fetch output is written directly to the served runtime data directory, so a failed or zero-output bootstrap could otherwise make the next Jobs load look like there are no jobs.
- Partial promotion: copying feed files one by one can expose mismatched report/feed state if the app reloads mid-promotion. Promotion must be staged and atomic at the artifact-set level.
- Storage parity: normal fetch completion mirrors source runs and jobs feed rows into SQLite/shadow storage. Bootstrap promotion must reuse or share that mirroring path so JSON, gzip exports, CSV, and storage authority stay consistent.
- False freshness: the startup preview path currently labels preview data with the current browser time, which makes packaged data look freshly updated.
- Weak full-pipeline detection: `fetch_never_run` can clear after any successful fetch, including a sheet bootstrap. It does not prove discovery, fetch, registry conflict adjudication, and sync have all run.
- Limited-scope alert contamination: a successful bootstrap can have far fewer rows than a full fetch. Ops health must not use `coverageScope: "bootstrap_sheets"` runs as full-fetch baselines for output-drop or reliability alerts.
- Post-pipeline downgrade: a bootstrap route that remains available after a successful full pipeline could replace a full-coverage runtime feed with sheet-only rows. Bootstrap must be first-run/retry behavior, not the general refresh path after full pipeline success.
- Browser fallback ambiguity: the Jobs feed wrapper currently accepts only `timeoutMs`, so `allowSheetsFallback: false` is not actually forwarded to the data-source layer.
- Packaged feed staleness: loading the full packaged feed on first run can display many stale listings, even if the capped startup preview is made fresher. Once sheet bootstrap owns first-run data retrieval, the row-bearing packaged jobs artifacts should be stripped instead of refreshed.
- Bridge availability: limited-coverage messaging must survive reloads and still be derivable from local report metadata if `/ops/dashboard-health` is temporarily unavailable.
- Retry loop risk: failed bootstrap attempts must record terminal failure state for the UI so refreshes do not repeatedly auto-start bootstrap forever.

## Key Changes

- On first cold start, read the local `jobs-fetch-report.json` before trying any local row feed. If it has no successful runtime run and no promoted runtime feed, auto-start bootstrap once and show bootstrap progress instead of an empty jobs list.
- Returning users still auto-load runtime data when the report has `finishedAt`, no terminal error, and `summary.outputCount > 0`.
- If bootstrap fails or the bridge is unavailable, show a retryable no-data state and do not auto-loop in the same recorded failure state. The retry CTA may start the same bootstrap route manually.
- Stop showing startup data as `Last updated: just now`. Use the runtime report `finishedAt` when available; otherwise show no freshness timestamp or clear unknown-freshness copy.
- Strip packaged row snapshots only after first-run sheet bootstrap, missing-artifact UI, and upgrade cleanup are implemented; do not generate `jobs-unified-startup.json` from packaged data in that mode.
- Once source data row snapshots are no longer package inputs, remove the `.gitignore` exceptions for `data/jobs-unified.json` and `data/jobs-unified-light.json`, and remove those tracked generated files from git in the same implementation slice.
- Add startup cleanup that quarantines stale `jobs-unified-startup.json`, `jobs-unified-light.json(.gz)`, `jobs-unified.json(.gz)`, and `jobs-unified.csv` when there is no successful runtime report. Do not quarantine row files when the local report proves a successful runtime feed.
- Add a staged bootstrap endpoint, `POST /tasks/run-jobs-bootstrap`, that runs only the three canonical Google Sheets sources into a private staging directory.
- Gate bootstrap admission so it cannot downgrade a full-pipeline feed. After a successful full pipeline exists, the UI should hide bootstrap controls and the route should reject or no-op instead of promoting sheet-only output over full coverage.
- Promote `jobs-unified.json`, `jobs-unified-light.json`, `jobs-unified.csv`, and the fetch report into runtime data only after at least one sheet succeeds and the staged output count is non-zero.
- Do not implement bootstrap by calling the normal fetch route with `onlySources` unless that path explicitly forces the bootstrap invariants. The current `onlySources` flow is not enough because selected-source runs can seed existing output.
- Prevent stale carryover by running bootstrap with a clean per-run staging directory, `seed_from_existing_output=False`, `preserve_previous_on_empty=False`, `force_refresh_all=True`, `ignore_circuit_breaker=True`, and `social_enabled=False`.
- Prevent environment carryover by clearing or bypassing `BALUFFO_FETCH_SEED_EXISTING_OUTPUT` for bootstrap and rejecting promotion when staged report/runtime metadata indicates existing-output seeding.
- Prevent empty-feed overwrite by leaving the currently served runtime feed untouched on bootstrap zero-output or failure.
- Promote the artifact set atomically: write promoted files through temporary names or a promoted staging directory, then swap only after all required artifacts and report metadata validate.
- Merge only sheet source-state and lifecycle entries during promotion; never replace non-sheet source state or lifecycle data wholesale.
- Mirror promoted bootstrap source runs and jobs feed rows through the same JSON/SQLite storage-authority path used by normal fetch completion.
- Add `coverageScope: "bootstrap_sheets"` to the promoted report summary, runtime metadata, and lifecycle task summary.
- Keep a persistent, non-dismissible `pipeline_never_run` warning until a successful full Jobs pipeline lifecycle row exists.
- Treat `coverageScope` and `pipeline_never_run` as separate signals. `pipeline_never_run` clears after a successful full pipeline; the Jobs page still shows sheet-limited current-feed messaging whenever the currently served report has `coverageScope: "bootstrap_sheets"`.
- Ensure full pipeline success overwrites or clears bootstrap `coverageScope`; do not let a later full run inherit `"bootstrap_sheets"` metadata.
- Update ops health so `coverageScope: "bootstrap_sheets"` rows do not drive full-fetch output-drop or reliability baselines.
- Update the Jobs page warning/banner to use `fetch_never_run`, `pipeline_never_run`, and local report `coverageScope` so limited-coverage messaging survives reloads and still appears if bridge health is temporarily unavailable.
- Fix the Jobs feed wrapper so `allowSheetsFallback: false` is forwarded and first-load local-feed behavior cannot silently use browser Sheets fallback.
- Update canonical docs during implementation closeout: `admin-bridge-api.md`, `fetcher-runtime-contracts.md`, `DATA_CONTRACT.md`, and `testing.md`.

## Packaged Artifact Stripping

Strip stale row-bearing jobs artifacts from the top-level ship `data/` directory after the first-run Google Sheets bootstrap is available:

- Remove `jobs-unified.json.gz`.
- Remove `jobs-unified-light.json.gz`.
- Remove `jobs-unified.csv`.
- Remove `jobs-unified-startup.json`.

Keep tiny runtime/control shells because they carry first-run state, task-state compatibility, and health signal with negligible size cost:

- Keep the `jobs-fetch-report.json` shell with empty `runId`, `startedAt`, `finishedAt`, and zero output summary.
- Keep the empty `jobs-fetch-tasks.json` shell.
- Keep the empty `jobs-source-state.json.gz` shell.
- Keep the empty `jobs-success-cache.json` shell.
- Keep version contract files and non-jobs runtime seed/config files unchanged.

Implementation notes:

- Remove `_generate_startup_preview` from the ship-build path or make it a no-op when no packaged light feed exists.
- Keep `.gitignore` broad enough that generated top-level jobs row artifacts stay untracked after removal. The current ignore rules already cover `data/*.json`, `data/*.csv`, and `data/*.json.gz`; the rollout should remove only the row-feed unignore exceptions, not add broad `data/jobs-*` exceptions.
- Preserve tracked non-row inputs such as `data/source-discovery-config.json`, `data/defaults/*.json`, and `data/contracts/*.json`.
- Treat stripped row artifacts as absent, not as empty feeds. Missing `jobs-unified*` before bootstrap should produce first-run bootstrap UI, not an empty jobs list.
- Keep runtime serving support for the same filenames because bootstrap and full pipeline still write those runtime artifacts.
- Add an upgrade cleanup/quarantine path for old row-bearing artifacts already present in `data/`. Quarantine only when the report is the empty shell or otherwise lacks successful runtime output; preserve files when the report has a successful runtime `finishedAt` and positive output count.
- Record cleanup in a migration report or structured log so support can distinguish "stripped stale seed" from data loss.
- Saved Jobs lifecycle overlay must tolerate missing `jobs-unified-light.json`, `jobs-unified.json`, and `jobs-lifecycle-state.json` by returning an empty overlay.
- Data Sources UI must tolerate missing unified feed artifacts before bootstrap.

## Interfaces

- New bridge route: `POST /tasks/run-jobs-bootstrap`.
- Bootstrap response shape: `{ started, runId, task: "jobs_bootstrap", taskType: "fetch", preset: "bootstrap_sheets", coverageScope: "bootstrap_sheets" }`.
- Bootstrap source scope: `google_sheets`, `google_sheets_1er2oaxo`, and `google_sheets_1mvqhxat`.
- Bootstrap report metadata: `summary.coverageScope` and `runtime.coverageScope`, initially `"bootstrap_sheets"` for promoted bootstrap output.
- Bootstrap failure metadata: a terminal lifecycle/report state that marks the bootstrap attempt failed without promoting or emptying jobs feed files.
- Bootstrap admission: allowed for first-run or bootstrap-retry states before full pipeline success; rejected or no-op after a successful full pipeline exists.
- Bootstrap lifecycle summary metadata: `coverageScope: "bootstrap_sheets"`.
- New health alert id: `pipeline_never_run`, non-dismissible, based on successful lifecycle rows for `taskType: "pipeline"`.
- Successful full pipeline definition: a lifecycle/history row whose normalized task type is `pipeline`, terminal status is successful, and `finishedAt` is present.

## Test Plan

- Python: bootstrap does not seed existing output, does not preserve stale output on empty, promotes only on successful non-empty sheet output, and leaves the existing feed untouched on failure.
- Python: bootstrap ignores or clears `BALUFFO_FETCH_SEED_EXISTING_OUTPUT` and refuses promotion if staged runtime metadata says existing output was seeded.
- Python: bootstrap uses a clean per-run staging directory and cannot promote stale files left by an older failed staging run.
- Python: promotion is atomic enough that a simulated mid-promotion failure leaves the previously served report/feed pair intact.
- Python: sheet source-state and lifecycle merge preserves non-sheet state.
- Python: promoted bootstrap output mirrors source runs and jobs feed rows through JSON/SQLite storage modes with parity diagnostics.
- Python: `pipeline_never_run` appears before a full pipeline, remains after bootstrap, clears after successful full pipeline, and cannot be acknowledged away.
- Python: bootstrap-scoped runs do not count toward full-fetch output-drop or degraded-reliability baselines.
- Python: bootstrap admission rejects or no-ops after successful full pipeline evidence exists and never overwrites a full-coverage feed with sheet-only output.
- Python: startup cleanup quarantines stale seed-era row artifacts when no successful runtime report exists and preserves row artifacts when the report proves a successful runtime feed.
- Frontend unit: first cold start skips full packaged feed refresh, does not show `just now`, shows the limited-coverage warning, and honors `allowSheetsFallback: false`.
- Frontend unit: first cold start with missing `jobs-unified*` does not show `Unable to load jobs` prematurely; it auto-starts bootstrap once or presents retryable bootstrap state.
- Frontend unit: failed bootstrap records UI state and does not auto-loop on reload without user retry.
- Frontend unit: Saved Jobs lifecycle overlay and Data Sources UI tolerate missing row-bearing jobs artifacts before bootstrap.
- Frontend unit: bootstrap CTA starts bootstrap, polls task state, reloads jobs after promoted success, and keeps the full-pipeline warning visible.
- Frontend unit: bootstrap controls are hidden or disabled after successful full pipeline evidence exists.
- Frontend unit: current-feed limited messaging remains when `coverageScope` is `"bootstrap_sheets"` and clears only when a full pipeline report replaces that scope.
- Packaging: built ship data excludes row-bearing jobs artifacts but still includes report/task/state shells.
- Packaging/repo hygiene: `data/jobs-unified.json` and `data/jobs-unified-light.json` are no longer tracked, `.gitignore` no longer unignores them, and source config/default/contract data remains tracked.
- Packaged smoke: fresh package starts without bundled jobs, retrieves sheet jobs once, promotes the runtime feed, and keeps the full-pipeline warning visible.
- Packaged smoke: offline fresh start shows retryable no-data/bootstrap-failed UI and never falls back to stale bundled jobs.
- Packaged upgrade smoke: an install with old packaged row artifacts but no successful runtime report quarantines those artifacts before the Jobs page can serve them.
- Regression: returning user with promoted runtime feed loads normally without rerunning first-time bootstrap.
- Regression: returning user with a successful full-pipeline report loads normally, preserves row artifacts, and does not show first-pipeline warnings.

## Assumptions

- Bootstrap improves freshness only for sheet-exposed jobs; it does not verify every company listing page.
- First-run bootstrap is automatic once per empty jobs-runtime state after packaged row artifacts are stripped, meaning no successful report and no promoted runtime feed. The user controls retry after a recorded bootstrap failure, but bootstrap is not the post-pipeline refresh path.
- A successful full Jobs pipeline that writes a full-coverage report remains the only state that clears first-pipeline and bootstrap-scope limited-coverage messaging.
- "Strip jobs artifacts" means strip packaged row snapshots, not tiny runtime/control shells.
- Offline first run can show a retryable no-data state; the package no longer contains stale fallback jobs.
- Upgrading users with a successful runtime feed keep that feed; cleanup targets only stale seed-era artifacts with no successful runtime report.
- No Python or Node dependencies are added.
- Saved jobs, tracking, and existing job row contracts remain unchanged.
