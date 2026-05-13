# Admin Bridge API Reference

> - **Status:** Active
> - **Use this when:** editing frontend bridge consumers, route handlers, or task launch/status flows
> - **Canonical for:** endpoint surface, route naming, and high-level request intent
> - **Not canonical for:** backend business logic internals or service ownership
> - **Then inspect:** `src/bridge/routes/{get_routes,post_routes,post_routes_admin,post_routes_local_data,post_routes_update}.py`, `src/bridge/*.py`, `frontend/*/services.js`
> - **Last updated:** 2026-05-13
> - **Ownership note:** ops/task-state internals now compose through `src/bridge/ops_api.py`, `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, and `src/bridge/ops_live_payload.py`
> - **Local-data ownership note:** desktop local-data storage now routes through `src/local_data_store.py` as a thin facade over `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`, while the shared desktop runtime stays rooted at `frontend/shared/local-data/desktop-client.js` over `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`
> - **Desktop update ownership note:** the helper executable stays rooted at `src/ship/desktop_updater.py` over `src/ship/desktop_updater_{ui,release,install}.py`, while the Jobs desktop update UI stays rooted at `frontend/jobs/app/desktop-update.js` over `frontend/jobs/app/desktop-update-{model,dom,controller}.js`
> - **POST-route ownership note:** `src/bridge/routes/post_routes.py` is now the thin registration surface over `src/bridge/routes/post_routes_{admin,local_data,update}.py`

Compact reference for AI coders. Endpoints are local-only (localhost).

## Desktop Local Data

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/desktop-local-data/session` | Current user session |
| GET | `/desktop-local-data/profiles` | List existing local desktop profiles |
| POST | `/desktop-local-data/sign-in` | Create/sign-in profile |
| POST | `/desktop-local-data/sign-out` | Sign out current profile |
| GET | `/desktop-local-data/saved-jobs?uid=` | List saved jobs for user |
| GET | `/desktop-local-data/saved-job-keys?uid=` | List job keys only |
| POST | `/desktop-local-data/saved-jobs/save` | Bookmark a job |
| POST | `/desktop-local-data/saved-jobs/remove` | Remove saved job |
| POST | `/desktop-local-data/saved-jobs/status` | Update application status |
| POST | `/desktop-local-data/saved-jobs/notes` | Update job notes |
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

## App / Desktop Runtime

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/app/update-status` | Desktop updater/install status; `currentVersion` is the installed app version |
| POST | `/app/check-for-update` | Check GitHub release/update manifest state |
| POST | `/app/download-update` | Start desktop update download |
| POST | `/app/install-update` | Start install-and-restart handoff |
| POST | `/app/desktop-session-lifecycle` | Desktop session heartbeat / closing lifecycle |

## Source Registry

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/registry/active` | List active sources |
| GET | `/registry/pending` | List pending sources |
| GET | `/registry/rejected` | List rejected sources |
| GET | `/registry/summary` | Registry summary counts |
| GET | `/registry/conflicts` | Duplicate-family conflict report with triage buckets, ranked review queues, advisory winners, row diffs, and lifecycle actions |
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

When `sourceRegistry=sqlite`, the registry GET routes and POST mutations read and publish through the SQLite source-registry generation before regenerating active/pending/rejected/tombstone compatibility exports. Payload shapes stay unchanged. Storage, busy-timeout, missing-generation, parity, export, or direct-JSON-drift failures persist `sourceRegistry=json` and return the JSON artifact path while leaving SQLite rows available for diagnostics.

## Discovery

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/discovery/report` | Last discovery run report |
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
| POST | `/tasks/run-fetcher` | Run fetcher with presets (`{preset: "default"|"incremental"|"retry_failed"|"force_full"|"uncapped", ...}`) |
| POST | `/tasks/run-jobs-pipeline` | Run jobs pipeline task |
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
| GET | `/sync/status` | Sync status and config |
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
| GET | `/ops/health` | Bridge health check |
| GET | `/ops/dashboard-health` | Admin dashboard health summary with alerts, KPIs, schedule state, and source-policy/dedup review indicators |
| GET | `/ops/history?limit=` | Run history (sync/fetcher/discovery) |
| GET | `/ops/task-live/<taskType>` | Detailed live task payload for `fetch`, `discovery`, or `sync` |
| GET | `/ops/task-state` | Current summary task projection; top-level `tasks` array remains the current-run contract |
| GET | `/ops/fetch-report` | Last fetch summary |
| GET | `/ops/fetch-report/sources?runId=&limit=&offset=&status=` | Bounded terminal fetch source rows |
| GET | `/ops/fetcher-metrics?windowRuns=` | Fetcher metrics |
| GET | `/ops/storage-metrics` | Runtime storage write, registry journal, source-sync size, and route timing diagnostics |
| GET | `/ops/storage-health` | SQLite runtime storage health, migration version, authority modes, WAL mode, busy counters, and quick_check status |
| POST | `/ops/alerts/ack` | Acknowledge alert (`{id: ""}`); active non-dismissible alerts return `{ok: true, ignored: true}` |
| GET | `/desktop-local-data/startup-metrics?limit=` | Startup performance data |
| POST | `/desktop-local-data/startup-metric` | Record startup event |

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
- `/ops/alerts/ack` does not persist acknowledgement for active non-dismissible alerts. The first-run `fetch_never_run` guidance remains visible until a successful fetch clears the condition.
- `/ops/task-live/<taskType>` is the detailed live surface for fetch/discovery/sync. It emits `workItems`, `recentEvents`, `taskProgress`, and lifecycle fields; it does not emit a detailed `tasks` alias anymore.
- `recentEvents` rows on `/ops/task-live/<taskType>` are normalized by `src/shared/live_task.py` and use the shared live task event envelope:
  - `schemaVersion`: currently `1`.
  - `event`: stable event token, preferring an explicit `event`, then `phaseKey`, then `live_task_event`.
  - `timestamp`, `level`, `taskType`, `runId`, `workItemId`, `phaseKey`, and `message`: compatibility fields used by Admin Ops and support diagnostics.
- `taskProgress`, `workItems`, and `recentEvents` are the support-ready live task contract for fetch/discovery/sync. They should be extended through the shared normalizers rather than by adding task-specific parallel event formats. Discovery uses these fields for wave-level progress, including current stage, stage index/total, generated/survived counts, probe counts, and bounded stage events.
- `/desktop-local-data/startup-metrics?limit=` returns retained startup diagnostic rows from `data/desktop-startup-metrics.jsonl`. Rows use `schemaVersion: 1`, `ts`, `event`, `category`, and either `fields` for runtime traces or `payload` for browser/page metrics; `browserTsMs` is preserved when browser-created timing is available.
- `/ops/storage-metrics` is read-only diagnostics. It returns additive `storageMetrics` for JSON/gzip write counts, serialization and replace durations, compressed/uncompressed byte sizes, registry `.jsonl` journal bytes/rows, and source-sync snapshot size pressure, plus existing route timing counters under `routeCounters`.
- `/ops/storage-health` is read-only diagnostics for the SQLite runtime store. It returns `{ok, storage}` with migration version, WAL mode, foreign-key state, quick_check status, busy counters, last write error, diagnostics, and current per-surface authority modes. After M6, new stores seed `taskRuns`, `taskEvents`, `syncRuns`, `sourceRuns`, `jobsFeed`, and `sourceRegistry` as SQLite-backed unless a persisted rollback returns the affected surface to JSON.
- `/ops/fetch-report` keeps its report payload shape. With `sourceRuns=sqlite`, terminal source rows are hydrated from SQLite/archive while `?view=live` remains compact and omits bulky `details`.
- `/ops/fetch-report/sources` is additive and bounded. It returns `{ok, runId, sources, count, limit, offset, source, warning}` and uses SQLite only while `sourceRuns=sqlite`; otherwise it falls back to the JSON report rows.
- `/ops/task-state` is unchanged. Its top-level `tasks` array remains the compact current-task summary contract used by Ops and Jobs.
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
- `/registry/conflicts` is report-only. Its additive `triage`, `review`, `automation`, and `adjudication` objects classify duplicate-family cards for operator review, while existing row lifecycle actions still use the normal registry routes. The separate `/registry/conflicts/auto-demote-safe` route re-checks strict eligibility before moving safe active duplicates back to pending or applying guarded active/pending replacements. Supported guarded actions are `auto_demote_same_adapter_provider_alias`, `auto_demote_provider_static_weaker_source`, `auto_promote_pending_provider_higher_jobs`, `auto_demote_static_normalized_url_alias`, and `auto_demote_static_same_host_listing_variant`; all actions are reversible registry state moves and never delete or reject rows. Same-adapter provider aliases can be treated as safe when the winner has stronger evidence and the rows share the same display name, endpoint shape, and positive job count, which covers provider-host redirects that expose identical live jobs. Active provider/static cards can demote one or more static rows when the provider winner has positive `jobsFound` evidence and each static loser has a known jobs-found count that is equal to or lower than the provider count. Pending provider versus active-source pairs can promote the pending provider and demote the active row when the pending provider has a known higher `jobsFound` count; if the active source is static and the pending source is a provider or recognized provider-host static URL, equal job counts still prefer the provider-backed source, and a zero-job provider-backed source can replace a one-job active static source when that static count is only weak evidence. Other active sources with equal-or-higher known jobs counts still suppress the pending provider loser into `automation.audit.safePendingProviderLowerJobs`. Registry service load also applies these guarded demotions automatically during the normal registry normalization pass, and reports the result under `registryAutoHeal.safeAutomation`. Pending rows whose `pendingReason` or `stateChangedBy` is `registry_conflict_safe_auto_demote` are omitted from unresolved conflict cards and exposed under `automation.audit.safeAutoDemotedPending` so reversible safe-demotion history remains visible without inflating the active operator worklist. Already-pending static rows can also be omitted from unresolved cards under `automation.audit.safePendingStaticAlias` when they share a studio-specific host with the active static row, the active path is career/job related, the pending path is either career/job related or the site homepage, and the active row has stronger job evidence. `/registry/conflicts/check-sources` writes live comparison evidence to `data/registry-conflict-adjudication.json`; running payloads keep `families` empty and expose compact `heartbeatAt`, `taskProgress`, and `progress` diagnostics for the Admin/Ops panel. Manual calls default to evidence-only, while explicit autopilot calls demote only high-confidence active losers to pending with reason `registry_conflict_adjudication_auto_demote`.
