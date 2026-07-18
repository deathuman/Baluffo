# Admin Bridge API Reference

> - **Status:** Active
> - **Use this when:** editing frontend bridge consumers, route handlers, or task launch/status flows
> - **Canonical for:** endpoint surface, route naming, and high-level request intent
> - **Not canonical for:** backend business logic internals or service ownership
> - **Then inspect:** `src/bridge/routes/get_*.py`, `src/bridge/routes/post_routes_{admin,local_data,update}.py`, `src/bridge/*.py`, `frontend/*/services.js`
> - **Last updated:** 2026-07-14
> - **Ownership note:** ops/task-state internals now compose through `src/bridge/ops_api.py`, `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, and `src/bridge/ops_live_payload.py`
> - **Local-data ownership note:** desktop local-data storage now routes through `src/local_data_store.py` as a thin facade over `src/local_data_store_{shared,profiles,saved_jobs,attachments,availability,backup}.py`, while the shared desktop runtime stays rooted at `frontend/shared/local-data/desktop-client.js` over `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`
> - **Desktop update ownership note:** the helper executable stays rooted at `src/ship/desktop_updater.py` over `src/ship/desktop_updater_{ui,release,install}.py`, while the Jobs desktop update UI stays rooted at `frontend/jobs/app/desktop-update.js` over `frontend/jobs/app/desktop-update-{model,dom,controller}.js`
> - **POST-route ownership note:** `src/bridge/routes/post_routes.py` is now the thin registration surface over `src/bridge/routes/post_routes_{admin,local_data,update}.py`
> - **GET-route ownership note:** `src/bridge/routes/get_routes.py` is now the thin public delegator. Route behavior belongs in the domain leaves `get_admin_bootstrap.py`, `get_admin_ops_tab_counts.py`, `get_app.py`, `get_discovery.py`, `get_fetch_report.py`, `get_fetch_report_sources.py`, `get_local_data.py`, `get_ops_diagnostics.py`, `get_ops_status.py`, `get_pipeline_tasks.py`, `get_registry.py`, `get_registry_conflicts.py`, `get_source_policy.py`, and `get_sync.py`.

Compact reference for AI coders. Desktop endpoints are local-only on localhost; container deployments serve the same API paths same-origin behind the combined UI/API HTTP service.

## Desktop Local Data

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/admin/bootstrap` | Bounded Admin control-plane startup payload: app/session readiness, local overview summary, current tasks, 2 recent runs, and sync readiness |
| GET | `/admin/ops-tab-counts?view=summary` | Bounded Admin tab badge counts for Overview alerts, Discovery Review, Source Policy Review, Registry Conflicts, and pending/unavailable deep diagnostics |
| GET | `/desktop-local-data/session` | Current user session |
| GET | `/desktop-local-data/profiles` | List existing local desktop profiles |
| POST | `/desktop-local-data/sign-in` | Create/sign-in profile |
| POST | `/desktop-local-data/sign-out` | Sign out current profile |
| GET | `/desktop-local-data/saved-jobs?uid=` | List saved jobs for user |
| GET | `/desktop-local-data/saved-job-keys?uid=` | List job keys only |
| GET | `/desktop-local-data/availability-attention?uid=` | Current profile unread availability transition summary |
| GET | `/desktop-local-data/availability-overlay?uid=` | Bounded exact-identity Saved availability projection |
| POST | `/desktop-local-data/saved-jobs/save` | Bookmark a job |
| POST | `/desktop-local-data/saved-jobs/remove` | Remove saved job |
| POST | `/desktop-local-data/saved-jobs/status` | Update application status |
| POST | `/desktop-local-data/saved-jobs/tracking` | Update split phase/outcome tracking |
| POST | `/desktop-local-data/saved-jobs/notes` | Update job notes |
| POST | `/desktop-local-data/availability-attention/acknowledge` | Acknowledge one availability transition or all current transitions |
| POST | `/desktop-local-data/availability/report` | Report or clear a profile-local unavailable job state and queue independent validation |
| GET | `/desktop-local-data/attachments?uid=&jobKey=` | List attachments |
| GET | `/desktop-local-data/attachments/content?uid=&jobKey=&attachmentId=` | Download attachment |
| POST | `/desktop-local-data/attachments/add` | Add attachment |
| POST | `/desktop-local-data/attachments/delete` | Delete attachment |
| GET | `/desktop-local-data/activity?uid=&limit=` | Activity log |
| GET | `/desktop-local-data/backup/export-file?uid=&includeFiles=` | Export backup (JSON or ZIP) |
| POST | `/desktop-local-data/backup/export` | Export backup (JSON payload) |
| POST | `/desktop-local-data/backup/import` | Import backup |
| POST | `/desktop-local-data/admin/overview` | Admin overview |
| POST | `/desktop-local-data/admin/wipe` | Wipe account |
| POST | `/desktop-local-data/open-url` | Open a job/application URL in the default browser |

In container mode, `/desktop-local-data/open-url` is disabled because there is no host desktop browser to control. It returns HTTP 409 with `{ "ok": false, "error": "not available in container mode" }`. Other local-data routes remain available and store profiles, saved jobs, attachments, backup data, and activity under the configured data directory, `/data` for the container runtime.

`POST /desktop-local-data/admin/overview` accepts optional `{ "detail": "summary" | "full" }`. The default is `full` for backward compatibility. `summary` returns the same overview shape with metadata-backed attachment sizes and skips filesystem stat work; `full` returns exact filesystem-backed attachment sizes when files are present.

Container mode serves UI and API from the same origin and does not emit browser CORS allow headers. Desktop/non-container bridge serving keeps its existing localhost split-origin CORS behavior.

## App / Desktop Runtime

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/app/update-status` | Desktop updater/install status; `currentVersion` is the installed app version |
| POST | `/app/check-for-update` | Check GitHub release/update manifest state; `{force: true}` bypasses the manifest cache |
| POST | `/app/download-update` | Start desktop update download |
| POST | `/app/install-update` | Start install-and-restart handoff |
| POST | `/app/desktop-session-lifecycle` | Desktop session heartbeat / closing lifecycle |

These `/app/*` routes are desktop-runtime routes. In container mode, `/app/update-status`, `/app/check-for-update`, `/app/download-update`, `/app/install-update`, and `/app/desktop-session-lifecycle` return HTTP 409 with `{ "ok": false, "error": "not available in container mode" }`.

## Source Registry

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/registry/active` | List active sources |
| GET | `/registry/pending` | List pending sources |
| GET | `/registry/rejected` | List rejected sources |
| GET | `/registry/summary` | Lightweight registry summary counts without source rows |
| GET | `/registry/summary?view=exact` | Normalized registry summary counts without source rows; slower than the default summary and intended for diagnostics |
| GET | `/registry/sources?view=table&buckets=pending,active,rejected&includeHiddenPending=0` | Compact Admin source-table payload for selected buckets from one backend state load |
| GET | `/registry/sources?buckets=pending,active,rejected&includeHiddenPending=0` | Full diagnostic source payload for selected buckets from one backend state load |
| GET | `/registry/conflicts` | Full duplicate-family conflict report with triage buckets, ranked review queues, advisory winners, row diffs, evidence cards, and lifecycle actions |
| GET | `/registry/conflicts?view=summary` | Cheap Admin startup conflict summary. It must not build the full conflict queue; it returns cached exact counts when available, otherwise `summaryStatus: "pending"` with registry counts and `detailRoute` |
| POST | `/registry/approve` | Approve pending sources (`{ids: []}`) |
| POST | `/registry/reject` | Reject pending sources (`{ids: []}`) |
| POST | `/registry/rollback` | Rollback active to pending (`{ids: []}`) |
| POST | `/registry/demote-active` | Demote active sources back to pending for reversible operator triage (`{ids: []}`) |
| POST | `/registry/conflicts/auto-demote-safe` | Re-check and apply guarded duplicate-family safe demotions/replacements (`{action: "", ids: []}`), including same-provider aliases, weaker provider/static pairs, higher-yield pending providers, and exact normalized static URL aliases |
| POST | `/registry/conflicts/check-sources` | Check only active sources currently in duplicate conflict cards and persist comparison evidence (`{familyKeys: [], sourceIds: [], applyAutopilot: false}`); writes compact running progress to `registry-conflict-adjudication.json`; with `applyAutopilot: true`, high-confidence losers are demoted to pending |
| POST | `/registry/restore-rejected` | Restore rejected to pending (`{ids: []}`) |
| POST | `/registry/restore-deleted` | Restore locally deleted sources from tombstones (`{ids: [], urls: []}`) |
| POST | `/registry/delete` | Local-only delete; writes tombstones and removes sources from the registry (`{ids: [], urls: []}`) |
| POST | `/sources/manual` | Add manual source (`{url: ""}`) |

The default `/registry/summary` response is a cheap storage snapshot and returns `summaryExact: false`, `countBasis: "storage"`. Use `/registry/summary?view=exact` only when diagnostics need normalized counts from the same path as the full registry state. The combined `/registry/sources` response also marks its summary as `summaryExact: true`, `countBasis: "normalized"` because it already performed one full state load. Admin source tables use `/registry/sources?view=table`, which keeps the same envelope and source-table/action fields but omits heavy diagnostic fields such as full `pages`, `detailPagesSample`, raw source-directory details, and nested evidence payloads. Table view keeps direct source URL fields and includes `pages[0]` only as a fallback when the direct table URL fields are absent. The default `/registry/sources` view remains full-fidelity for diagnostics.

When `sourceRegistry=sqlite`, the registry GET routes and POST mutations read and publish through the SQLite source-registry generation before regenerating active/pending/rejected/tombstone compatibility exports. Payload shapes stay unchanged. Storage, busy-timeout, missing-generation, parity, export, or direct-JSON-drift failures persist `sourceRegistry=json` and return the JSON artifact path while leaving SQLite rows available for diagnostics.

## Discovery

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/discovery/report` | Full last discovery run report for manual diagnostics |
| GET | `/discovery/report?view=summary` | Bounded discovery status/counter/log-tail summary for lightweight status surfaces. It does not return full candidate or failure arrays |
| GET | `/discovery/candidates` | Persisted discovery review candidates, including queued and deferred rows |
| GET | `/discovery/config` | Saved Source Discovery admin preferences |
| GET | `/discovery/log` | Discovery log (supports `?offset=`) |
| POST | `/discovery/check-source` | Check specific source (`{sourceId: ""}`) |
| POST | `/discovery/config` | Update Source Discovery admin preferences (`{autoApproveHealthyPendingOnComplete: true|false}`) |
| POST | `/tasks/run-discovery` | Trigger discovery task (`{preset: "default"|"uncapped"}`); `default` now uses the former uncapped-lite behavior, while `uncapped` is the stronger exploration preset with higher queue caps, and both keep evidence/probe safety guardrails |

## Jobs Pipeline

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/fetcher/log` | Fetcher log (supports `?offset=`) |
| GET | `/ops/fetch-report` | Last fetch report |
| GET | `/ops/fetch-report/sources?runId=&limit=&offset=&status=` | Bounded terminal fetch source rows |
| GET | `/ops/fetcher-metrics?windowRuns=` | Fetcher performance metrics |
| POST | `/tasks/run-jobs-bootstrap` | First-run/retry sheet-limited bootstrap fetch. Returns `{started, runId, task: "jobs_bootstrap", taskType: "fetch", preset: "bootstrap_sheets", coverageScope: "bootstrap_sheets"}` and no-ops/rejects after an existing runtime feed or full pipeline success |
| POST | `/tasks/run-fetcher` | Run fetcher with presets (`{preset: "default"|"incremental"|"retry_failed"|"force_full"|"uncapped", ...}`) |
| POST | `/tasks/run-jobs-pipeline` | Run jobs pipeline task |
| POST | `/tasks/job-availability-check` | Start a bounded background direct-link check by exact `availabilityId` |
| GET | `/tasks/job-availability-check-status?runId=` | Return bounded progress and result for a direct-link check |
| GET | `/tasks/jobs-pipeline-schedule` | Return persisted recurring Jobs pipeline schedule config and runtime due/pending status |
| POST | `/tasks/jobs-pipeline-schedule` | Update recurring Jobs pipeline schedule config (`{enabled, intervalHours}`; whole-hour interval `1`-`168`) and immediately re-evaluate due state |
| POST | `/tasks/abort` | Abort active `fetch`, `discovery`, or `pipeline` run by `{taskType, runId, reason?}`. Standalone `sync` abort is rejected; pipeline sync-stage abort is deferred until sync completes |
| GET | `/tasks/run-jobs-pipeline-status` | Pipeline task status |

## Source Policy / Review State

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/source-policy/recommendations` | Source-policy review recommendations and migration-link candidates for the Admin Ops source-policy panel |
| POST | `/source-policy/review-action` | Persist local source-policy review decisions for recommendation rows |
| POST | `/source-policy/migration-link-action` | Persist local source-policy migration-link review decisions |
| POST | `/dedup/review-action` | Persist local dedup review-state decisions for fetch-report dedup evidence rows |

## Sync

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/sync/status` | Full sync status and config |
| GET | `/sync/status?view=summary` | Lightweight sync config/runtime summary for Admin startup and Action Center. It avoids full sync history/timing hydration |
| POST | `/sync/config` | Update sync settings |
| POST | `/sync/test` | Test sync configuration |
| POST | `/sync/pull` | Pull sources (sync) |
| POST | `/sync/push` | Push sources (sync) |

`/sync/status` includes additive sync timing diagnostics: latest `timing`, bounded `timingHistory`, `stageTotalsMs`, and sorted `stageTop` rows for pull/push operations. Completed sync task summaries also carry the operation `timing` payload for Ops/run-history diagnostics.
| POST | `/tasks/run-sync-pull` | Async pull with task tracking |
| POST | `/tasks/run-sync-push` | Async push with task tracking |

## Operations

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/ops/health` | Full bridge health check |
| GET | `/app/ready` | Minimal readiness check for UI liveness/status badges. In container mode this is served by the public gateway; it does not read lifecycle, schedule, registry, dashboard, history, reports, or SQLite-backed data |
| GET | `/ops/health?view=ready` | Lightweight bridge readiness check for startup/status badges. It avoids dashboard/support projections |
| GET | `/ops/dashboard-health` | Full Admin dashboard health payload with alerts, KPIs, schedule state, and source-policy/dedup review indicators |
| GET | `/ops/dashboard-health?view=summary` | Lightweight Admin first-paint dashboard summary. Avoids full fetch report, run history, discovery report, registry sources, fetcher metrics, storage-health detail, audit artifacts, and performance-profile hydration |
| GET | `/ops/fetch-kpis?view=summary` | Bounded user-facing fetch KPI summary for Admin cards. It omits source-health arrays, provider coverage, dedup diagnostics, performance profile, audit artifacts, full history, and full fetch report bodies |
| GET | `/ops/history?limit=` | Run history (sync/fetcher/discovery) |
| GET | `/ops/task-live/<taskType>` | Detailed live task payload for `fetch`, `discovery`, or `sync` |
| GET | `/ops/task-live/<taskType>?view=summary` | Compact active-polling live task payload preserving run identity, status, task progress, summary, timestamps, and bounded recent events while omitting full work items |
| GET | `/ops/task-state` | Full current task projection for diagnostics, including task work-item/event detail when available |
| GET | `/ops/task-state?view=summary` | Compact hot-path task projection preserving active task identity/progress while omitting full work items and bounding recent events |
| GET | `/ops/fetch-report` | Last fetch summary |
| GET | `/ops/fetch-report/sources?runId=&limit=&offset=&status=` | Bounded terminal fetch source rows |
| GET | `/ops/fetcher-metrics?windowRuns=` | Fetcher metrics |
| GET | `/ops/performance-profile` | Bounded in-memory bridge route and backend operation timing aggregates; route labels redact query strings and dynamic path segments, and no raw samples or payload bodies are returned |
| GET | `/ops/storage-metrics` | Runtime storage write, registry journal, source-sync size, and route timing diagnostics |
| GET | `/ops/storage-health` | SQLite runtime storage health, migration version, authority modes, WAL mode, busy counters, and quick_check status |
| GET | `/ops/discovery-audit-artifacts` | Bounded diagnostics for known discovery audit artifacts under the active data dir; returns existence, size, hash, top-level keys, compact summary, and warnings without exposing full JSON bodies |
| GET | `/ops/task-failure-attempts` | Bounded diagnostics for latest fetch/discovery failure attempts; classifies expected cache/dedupe/queue skips separately from hard failures and actionable discovery diagnostics without exposing raw artifact bodies or URLs |
| POST | `/ops/alerts/ack` | Acknowledge alert (`{id: ""}`); active non-dismissible alerts return `{ok: true, ignored: true}` |
| GET | `/desktop-local-data/startup-metrics?limit=` | Startup performance data |
| POST | `/desktop-local-data/startup-metric` | Record startup event |
| POST | `/desktop-local-data/startup-metrics/batch` | Record a bounded batch of startup metric events after first usable render |

## Diagnostic Artifacts

The bridge retains support-oriented diagnostic events in `data/admin-bridge-events.jsonl`, under the configured data directory. This artifact is append-only JSONL with bounded retention and is separate from console logging; `--log-format human|jsonl` and `BALUFFO_BRIDGE_LOG_FORMAT` only affect stdout.

Retained bridge event rows use this shape:

```json
{
  "schemaVersion": 1,
  "ts": "2026-04-25T12:34:56+00:00",
  "level": "info",
  "event": "task_started",
  "message": "task_started",
  "fields": {
    "runId": "fetch_abc123"
  }
}
```

Known sensitive field names such as tokens, passwords, secrets, API keys, and authorization values are redacted recursively in the retained artifact. Session IDs remain available as support correlation IDs.

## Query Params

- `uid`: User profile ID
- `jobKey`: Job key (e.g. `job_abc123`)
- `offset`: Log byte offset for pagination
- `limit`: Result limit (default varies)
- `includeFiles`: Include attachment files in backup (`0`/`1`)

## Notes

- Desktop sign-in UI should call `/desktop-local-data/profiles` first and prefer existing-profile selection. If that load fails, the current desktop flow is explicit `Retry` / `Create new profile` / `Cancel`, not blind text entry for existing profiles.
- `/app/update-status` is the desktop source of truth for installed app version and updater state. Jobs/Saved/Admin desktop chrome reads `currentVersion` from this payload.
- `/ops/alerts/ack` does not persist acknowledgement for active non-dismissible alerts. The first-run `fetch_never_run` guidance remains visible until a successful fetch clears the condition; `pipeline_never_run` remains visible until a successful full Jobs pipeline lifecycle row exists, including after sheet bootstrap.
- `/ops/task-live/<taskType>` is the detailed live surface for fetch/discovery/sync. It emits `workItems`, `recentEvents`, `taskProgress`, and lifecycle fields; it does not emit a detailed `tasks` alias anymore. Active Admin and Jobs polling should use `/ops/task-live/<taskType>?view=summary`; the summary view preserves the live task envelope, `taskProgress`, `summary`, identity, status, timestamps, and bounded recent events, and omits full `workItems` while exposing `workItemCount`/`workItemsTruncated`.
- Fetch may emit pre-source setup progress on the same live task contract before work items exist. Valid setup `taskProgress.phaseKey` values include `loading_state`, `seeding_existing_output`, `selecting_sources`, `applying_exclusions`, and `initializing_runtime`; counts are bounded setup diagnostics, not source-row progress.
- During active fetch or pipeline fetch, Admin must not add storage-health polling, full fetch-report diagnostics, or hidden retry storms to explain startup. Hot progress comes from `/ops/task-live/fetch?view=summary`, `jobs-fetch-tasks.json`, and the active-task snapshot; `admin-task-lifecycle.json` is for run identity, scalar summaries, coarse phase heartbeat, and terminal state. Active fetch summaries may expose bounded execution rate, ETA, and capped running source names, but full source rows and detailed reports remain manual/detail surfaces. Historical lifecycle rows self-compact on the next lifecycle write; read-only startup/bootstrap routes must not rewrite lifecycle solely to shrink old files.
- `recentEvents` rows on `/ops/task-live/<taskType>` are normalized by `src/shared/live_task.py` and use the shared live task event envelope:
  - `schemaVersion`: currently `1`.
  - `event`: stable event token, preferring an explicit `event`, then `phaseKey`, then `live_task_event`.
  - `timestamp`, `level`, `taskType`, `runId`, `workItemId`, `phaseKey`, and `message`: compatibility fields used by Admin Ops and support diagnostics.
- `taskProgress`, `workItems`, and `recentEvents` are the support-ready live task contract for fetch/discovery/sync. They should be extended through the shared normalizers rather than by adding task-specific parallel event formats. Discovery uses these fields for wave-level progress, including current stage, stage index/total, generated/survived counts, probe counts, and bounded stage events.
- `/desktop-local-data/startup-metrics?limit=` returns retained startup diagnostic rows from `data/desktop-startup-metrics.jsonl`. Rows use `schemaVersion: 1`, `ts`, `event`, `category`, and either `fields` for runtime traces or `payload` for browser/page metrics; `browserTsMs` is preserved when browser-created timing is available. Browser startup code should batch/defer page metric writes through `/desktop-local-data/startup-metrics/batch` until after first usable render so metric writes do not compete with first paint.
- `/ops/storage-metrics` is read-only diagnostics. It returns additive `storageMetrics` for JSON/gzip write counts, serialization and replace durations, compressed/uncompressed byte sizes, registry `.jsonl` journal bytes/rows, and source-sync snapshot size pressure, plus existing route timing counters under `routeCounters`.
- `/ops/storage-health` is read-only diagnostics for the SQLite runtime store. It returns `{ok, storage}` with migration version, WAL mode, foreign-key state, quick_check status, busy counters, last write error, diagnostics, and current per-surface authority modes. After M6, new stores seed `taskRuns`, `taskEvents`, `syncRuns`, `sourceRuns`, `jobsFeed`, and `sourceRegistry` as SQLite-backed unless a persisted rollback returns the affected surface to JSON.
- `/ops/discovery-audit-artifacts` is a fixed allowlist diagnostic route for the sheet-directory, web-search, GameDevMap, Gameprog, and Gamesmap discovery audit artifacts. It does not accept path query input, does not expose artifact bodies, and does not widen static `/data` serving.
- `/ops/task-failure-attempts` reads only the latest fetch and discovery reports from the active data directory and returns bounded counts/examples. Fetch `excluded/cache_within_freshness_window` rows and discovery `dedupe_skipped`, `queue_filtered`, and `suppressed_static` rows are classified as expected skips; permanent discovery DNS and 404/410 misses for generated recovery, homepage, website, and static probe checks are classified as expected negatives. Raw URLs, raw errors, full source rows, and artifact bodies are not returned.
- `/ops/fetch-report` keeps its report payload shape for manual/detail diagnostics. Admin hot paths must use `/ops/fetch-report?view=summary`, `/ops/fetch-report?view=live`, `/ops/task-live/fetch?view=summary`, or `/ops/task-state?view=summary`; compact views must prefer `jobs-fetch-report-summary.json`, `jobs-fetch-tasks.json`, and the active snapshot before any full report hydration. Summary recovery may inspect only bounded top-level metadata from a newer terminal full report when a worker left an active sidecar stale. `?view=live` may return capped `sources` with `sourcesTruncated`/`sourceCount`; detailed rows belong behind `/ops/fetch-report/sources`.
- Full and summary fetch-report views preserve bounded availability identity diagnostics, including accepted/rejected counts, capped rejection-reason counts, quarantine/truncation counts, and post-filter integrity counts. Rejected candidates set degraded availability coverage without entering the feed. Finalization failures are terminal `status="error"` reports with inactive failed progress and a stable bounded `summary.errorCode`; pipeline parents propagate that code instead of waiting for orphan cleanup.
- `/ops/fetch-report/sources` is additive and bounded. It returns `{ok, runId, sources, count, limit, offset, source, warning}` and uses SQLite only while `sourceRuns=sqlite`; otherwise it falls back to the JSON report rows.
- `/ops/task-state` remains backward-compatible for full diagnostics. Admin startup and other hot paths must use `/ops/task-state?view=summary`; its top-level `tasks` array remains the current-run contract, but rows omit `workItems`, expose `workItemCount`, and bound `recentEvents`.
- Admin startup uses `/admin/bootstrap` for first-use control-plane data and may query `/tasks/run-jobs-pipeline-status` immediately as the canonical running-pipeline fallback when bootstrap or Ops routes are delayed. After first bootstrap render, Admin may issue one compact `/registry/sources?view=table&buckets=pending,active,rejected&includeHiddenPending=0&limitPerBucket=250` source-table request so source containers hydrate without blank startup states. That compact source-table request must be queued before fallback schedule/history refreshes and before idle summary routes such as `/registry/conflicts?view=summary`, `/ops/fetch-kpis?view=summary`, and `/admin/ops-tab-counts?view=summary`. Those idle summary routes may hydrate after source tables, but must run sequentially and must not overlap with the source-table request. Full diagnostics such as `/ops/health`, `/ops/dashboard-health`, full `/ops/task-state`, full `/discovery/report`, diagnostic `/registry/sources`, full logs, fetch reports, storage health, fetcher metrics, discovery audit artifacts, task failure attempts, performance profile, and `/ops/task-live/*` are tab-open or manual-refresh work only, except task-live routes may attach after bootstrap or pipeline status confirms an active task.
- After active pipeline/fetch work transitions to idle, Admin must run one bounded sequential final-state recovery for task status, pipeline schedule, recent activity, sync summary, and Ops summary badges before attempting compact source-table lazy hydration. Source-table hydration may stay visibly delayed during active work, but degraded bootstrap or source-table delays must not blank the next trigger date, sync readiness, control-panel final state, or terminal pipeline status.
- Active/final Admin state must come from bounded hot summaries and gateway fallbacks. Full fetch reports, storage-health, full diagnostics, registry summaries, and source-table loads are manual/tab-open or post-active detail work and must not be required for pipeline final-state, schedule, sync, or control-panel recovery.
- Full pipeline fetch on Umbrel may use the bounded container-only pipeline throughput profile, but Admin route budgets remain unchanged: the speedup must not introduce storage-health polling, full diagnostics, source-table fan-out, or full fetch-report auto-polling during active work.
- Pipeline rows in `/ops/history` and Admin Operations Activity are orchestration parent runs. Stage-level diagnostic detail is derived from child Discovery, Fetch, and Sync rows linked by `parentRunId`; the child rows remain visible as normal runs. Admin may load a two-run startup slice and later an 80-run detail slice; subsequent two-run refreshes merge by `runId`, dedupe, and cap the cached history at 80 so older rows and open-run state are not discarded.
- `/tasks/run-jobs-pipeline-status` keeps the Jobs UI flat progress payload (`currentStep`, `totalSteps`, `percent`, `label`). Ops-facing lifecycle rows use normalized `taskProgress` semantics with `phaseLabel`, `ratio`, and `counts` so Admin diagnostics can render pipeline progress consistently.
- Source-sync failures during the Jobs pipeline are non-blocking. The pipeline status may finish with `stage: "completed_with_warnings"`, `completedWithWarnings: true`, `syncStatus: "warning"`, and a bounded `syncWarning` object while still preserving `updatesFound` / `refreshRecommended` for the completed discovery/fetch output.
- `/tasks/jobs-pipeline-schedule` is bridge-runtime only. It persists `jobs-pipeline-schedule-config.json` under the active data dir, is disabled by default, supports only whole-hour intervals from `1` through `168`, computes cadence from terminal pipeline lifecycle rows, collapses missed intervals into one pending run, and never aborts an active pipeline when disabled. `/ops/health.schedule.pipeline` mirrors its current enabled/pending/due/next-run status while preserving the existing `fetcher` and `discovery` schedule keys.
- `POST /tasks/job-availability-check` accepts one exact `availabilityId` and returns a `runId` promptly; lifecycle parsing, custom-manifest lookup, and target preparation run in the background worker. Canonical identities resolve from lifecycle state; custom Saved identities resolve only from the bridge-written scoped priority manifest, never from a caller URL. A duplicate request for the same identity reuses the active `runId`. Invalid or failed worker preparation reaches a terminal failed status and always clears the active identity. `GET /tasks/job-availability-check-status?runId=` returns only task identity, timestamps, terminal state, compact classification, bounded evidence, and additive `applied`; evidence older than the current lifecycle checkpoint is diagnostic-only. The direct classifier is shadow-only unless `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1`; transient HTTP/network/anti-bot evidence never closes a row, and shadow evidence never restores a profile report.
- `GET /desktop-local-data/availability-attention?uid=` returns the current profile's unread transition summary. `GET /desktop-local-data/availability-overlay?uid=` returns at most 2,000 rows containing exact `jobKey` / `availabilityId`, current status/timestamps, and compact evidence; it never returns custom URLs or private-ledger records. `POST /desktop-local-data/availability-attention/acknowledge` accepts `transitionId` or `allCurrent=true`. `POST /desktop-local-data/availability/report` accepts `{uid, jobKey, action: "report"|"clear"}` and persists the profile-local `localReport` plus legacy-compatible `hiddenByReport` flag. Reported rows stay visible in All and Availability attention with a `Reported unavailable` state; the Saved UI confirms using the job title/company, offers Clear, and provides an Undo toast. A report queues an independent check when an `availabilityId` exists.
- Before pipeline launch, the bridge writes narrow schema-v2 `jobs-availability-priority.json` rows containing only `availabilityId`, canonical public `jobLink`, priority, and `canonical` / `custom_saved` scope. Custom rows participate in the private Saved rotation without entering public lifecycle/history or coverage. After every successful or warning pipeline publication, the shared bridge service projects lifecycle transitions idempotently into profile data and consumes the bounded sweep plan; unavailable saved rows remain eligible so a definitive live result can reopen them. A profile-projection failure is reported but does not suppress the sweep. Failed and canceled pipelines do neither. Container/Umbrel proxies these same routes; static browser mode has no check/report mutation capability and hides those actions.
- `/tasks/abort` is runId-owned and does not support abort-by-type. In-progress abort remains lifecycle `running` with `stage: "aborting"` or `stage: "abort_pending_sync"`, `summary.abortRequestedAt`, and active `taskProgress.phaseKey: "aborting"`. Terminal abort writes lifecycle `canceled` with `terminalReason: "user_abort_requested"`. If non-canceled terminal evidence already exists before abort intent is recorded, the route rejects the request instead of converting that completed run; pipeline parent cancellation may still proceed while already-terminal children finalize normally.
- Saved-page bridge consumers should keep route calls inside slice-local `frontend/saved/services.js`; page behavior now fans out through `frontend/saved/app/runtime/*.js` and `frontend/saved/app/admin-bridge-state.js`, not through new root facades.
- Long-running admin tasks are now **runId-owned**. `runId` is the only lifecycle identity for fetch, discovery, sync, and pipeline rows. Timestamp-only matching is not part of the runtime lifecycle model anymore.
- Current Runs and `/ops/history` are projected from SQLite task runtime rows when their authority modes are SQLite, with JSON lifecycle exports retained for rollback/debug.
- Authoritative owners by task type:
  - `fetch`: lifecycle row identity/liveness, `source_runs` for terminal source rows after M4, `data/jobs-fetch-tasks.json` for active progress, and compact `data/jobs-fetch-report.json`/evidence archives as compatibility/debug evidence
  - `discovery`: lifecycle row identity/liveness plus `data/source-discovery-report.json` as progress/evidence
  - `sync`: `SyncState` in the bridge runtime
  - `pipeline`: bridge pipeline runtime state
- GET routes are read-only for lifecycle state. Loading `/discovery/report`, `/ops/history`, `/ops/task-state`, or `/ops/fetch-report` must not auto-finish or prune tasks.
- `data/admin-task-lifecycle.json` is the source of truth for task liveness and terminal state.
- `data/admin-task-state.json` and `data/admin-run-history.json` are legacy compatibility/migration artifacts. Normal bridge startup, task launch, task completion, task-state projection, and history projection must not read or write them as lifecycle authority.
- Bridge startup does not reconcile legacy history/state rows into `data/admin-task-lifecycle.json`. Legacy lifecycle import belongs in explicit migration or test tooling, not normal bridge startup.
- To reset current lifecycle/debug artifacts before a clean debugging session, run:
  - `python scripts/reset_admin_task_lifecycle.py --data-dir data`

- Bridge-started fetch runs enable social by default unless the request payload explicitly sets `socialEnabled: false`.
- Discovery `default` now maps to the former uncapped-lite behavior. Discovery `uncapped` is the stronger exploration preset and is distinct from `force_full`.
- `/discovery/log` is designed for live tailing via byte offsets, and the Admin UI now re-attaches to active discovery runs after page refresh.
- Source Discovery keeps a bridge-persisted admin preference, enabled by default, that auto-approves healthy pending sources with jobs after discovery completes and before any follow-on auto-sync push decision. Pending rows demoted by registry conflict automation (`registry_conflict_safe_auto_demote` or `registry_conflict_adjudication_auto_demote`) are excluded so discovery cannot re-promote conflict losers during the next pipeline run.
- `weakSignal` remains an advisory hint for ranking and re-probe heuristics, and weak pending/deferred rows stay in review instead of being auto-approved.
- Report-side queue throttles like `domain_cap` do not veto a clean pending registry row.
- Source registry deletes are tombstone-backed and local-only; the restore path is explicit, and manual add will not silently clear a tombstone.
- `/registry/conflicts` is report-only. Its additive `triage`, `review`, `automation`, and `adjudication` objects classify duplicate-family cards for operator review, while existing row lifecycle actions still use the normal registry routes. Admin startup must not call the full conflict route. The compact summary route may run only after the startup source-table request has finished and must not overlap with other startup-heavy routes. The summary route is a compute-time hot path, not just a small response, and must not call the full conflict queue builder. It returns cached exact counts when a cache key matches the registry/source/adjudication evidence, otherwise it returns `summaryStatus: "pending"` and `summaryExact: false` so Admin can render the section while full details remain available from `/registry/conflicts`. The separate `/registry/conflicts/auto-demote-safe` route re-checks strict eligibility before moving safe active duplicates back to pending or applying guarded active/pending replacements. Supported guarded actions are `auto_demote_same_adapter_provider_alias`, `auto_demote_provider_static_weaker_source`, `auto_promote_pending_provider_higher_jobs`, `auto_demote_static_normalized_url_alias`, and `auto_demote_static_same_host_listing_variant`; all actions are reversible registry state moves and never delete or reject rows. Same-adapter provider aliases can be treated as safe when the winner has stronger evidence and the rows share the same display name, endpoint shape, and positive job count, which covers provider-host redirects that expose identical live jobs. Active provider/static cards can demote one or more static rows when the provider winner has positive `jobsFound` evidence and each static loser has a known jobs-found count that is equal to or lower than the provider count. Pending provider versus active-source pairs can promote the pending provider and demote the active row when the pending provider has a known higher `jobsFound` count; if the active source is static and the pending source is a provider or recognized provider-host static URL, equal job counts still prefer the provider-backed source, and a zero-job provider-backed source can replace a one-job active static source when that static count is only weak evidence. Other active sources with equal-or-higher known jobs counts still suppress the pending provider loser into `automation.audit.safePendingProviderLowerJobs`. Registry service load also applies these guarded demotions automatically during the normal registry normalization pass, and reports the result under `registryAutoHeal.safeAutomation`. Pending rows whose `pendingReason` or `stateChangedBy` is `registry_conflict_safe_auto_demote` are omitted from unresolved conflict cards and exposed under `automation.audit.safeAutoDemotedPending` so reversible safe-demotion history remains visible without inflating the active operator worklist. Already-pending static rows can also be omitted from unresolved cards under `automation.audit.safePendingStaticAlias` when they share a studio-specific host with the active static row, the active path is career/job related, the pending path is either career/job related or the site homepage, and the active row has stronger job evidence. `/registry/conflicts/check-sources` writes live comparison evidence to `data/registry-conflict-adjudication.json`; running payloads keep `families` empty and expose compact `heartbeatAt`, `taskProgress`, and `progress` diagnostics for the Admin/Ops panel. Manual calls default to evidence-only, while explicit autopilot calls demote only high-confidence losers to pending with reason `registry_conflict_adjudication_auto_demote`.
