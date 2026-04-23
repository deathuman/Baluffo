# Admin Bridge API Reference

> **AI usage**
> - **Use this when:** editing frontend bridge consumers, route handlers, or task launch/status flows
> - **Canonical for:** endpoint surface, route naming, and high-level request intent
> - **Not canonical for:** backend business logic internals or service ownership
> - **Then inspect:** `src/bridge/routes/*`, `src/bridge/*.py`, `frontend/*/services.js`
> - **Ownership note:** ops/task-state internals now compose through `src/bridge/ops_api.py`, `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, and `src/bridge/ops_live_payload.py`

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
| POST | `/registry/approve` | Approve pending sources (`{ids: []}`) |
| POST | `/registry/reject` | Reject pending sources (`{ids: []}`) |
| POST | `/registry/rollback` | Rollback active to pending (`{ids: []}`) |
| POST | `/registry/restore-rejected` | Restore rejected to pending (`{ids: []}`) |
| POST | `/registry/restore-deleted` | Restore locally deleted sources from tombstones (`{ids: [], urls: []}`) |
| POST | `/registry/delete` | Local-only delete; writes tombstones and removes sources from the registry (`{ids: [], urls: []}`) |
| POST | `/sources/manual` | Add manual source (`{url: ""}`) |

## Discovery

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/discovery/report` | Last discovery run report |
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
| GET | `/ops/fetcher-metrics?windowRuns=` | Fetcher performance metrics |
| POST | `/tasks/run-fetcher` | Run fetcher with presets (`{preset: "default"|"incremental"|"retry_failed"|"force_full"|"uncapped", ...}`) |
| POST | `/tasks/run-jobs-pipeline` | Run jobs pipeline task |
| GET | `/tasks/run-jobs-pipeline-status` | Pipeline task status |

## Sync

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/sync/status` | Sync status and config |
| POST | `/sync/config` | Update sync settings |
| POST | `/sync/test` | Test sync configuration |
| POST | `/sync/pull` | Pull sources (sync) |
| POST | `/sync/push` | Push sources (sync) |
| POST | `/tasks/run-sync-pull` | Async pull with task tracking |
| POST | `/tasks/run-sync-push` | Async push with task tracking |

## Operations

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/ops/health` | Bridge health check |
| GET | `/ops/history?limit=` | Run history (sync/fetcher/discovery) |
| GET | `/ops/task-live/<taskType>` | Detailed live task payload for `fetch`, `discovery`, or `sync` |
| GET | `/ops/task-state` | Current summary task projection; top-level `tasks` array remains the current-run contract |
| GET | `/ops/fetch-report` | Last fetch summary |
| GET | `/ops/fetcher-metrics?windowRuns=` | Fetcher metrics |
| POST | `/ops/alerts/ack` | Acknowledge alert (`{id: ""}`); active non-dismissible alerts return `{ok: true, ignored: true}` |
| GET | `/desktop-local-data/startup-metrics?limit=` | Startup performance data |
| POST | `/desktop-local-data/startup-metric` | Record startup event |

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
- `/ops/task-state` is unchanged. Its top-level `tasks` array remains the compact current-task summary contract used by Ops and Jobs.
- Saved-page bridge consumers should keep route calls inside slice-local `frontend/saved/services.js`; page behavior now fans out through `frontend/saved/app/runtime/*.js` and `frontend/saved/app/admin-bridge-state.js`, not through new root facades.
- Long-running admin tasks are now **runId-owned**. `runId` is the only lifecycle identity for fetch, discovery, sync, and pipeline rows. Timestamp-only matching is not part of the runtime lifecycle model anymore.
- Current Runs and `/ops/history` are projected from task owners, not from `admin-run-history.json` alone.
- Authoritative owners by task type:
  - `fetch`: `data/jobs-fetch-report.json`, `data/jobs-fetch-tasks.json`, and the matching `data/admin-task-state.json` entry
  - `discovery`: `data/source-discovery-report.json` and the matching `data/admin-task-state.json` entry
  - `sync`: `SyncState` in the bridge runtime
  - `pipeline`: bridge pipeline runtime state
- GET routes are read-only for lifecycle state. Loading `/discovery/report`, `/ops/history`, `/ops/task-state`, or `/ops/fetch-report` must not auto-finish or prune tasks.
- `data/admin-run-history.json` is a derived summary surface. It records starts and completions, but it is not the source of truth for liveness.
- To reset current lifecycle/debug artifacts before a clean debugging session, run:
  - `python scripts/reset_admin_task_lifecycle.py --data-dir data`

- Bridge-started fetch runs enable social by default unless the request payload explicitly sets `socialEnabled: false`.
- Discovery `default` now maps to the former uncapped-lite behavior. Discovery `uncapped` is the stronger exploration preset and is distinct from `force_full`.
- `/discovery/log` is designed for live tailing via byte offsets, and the Admin UI now re-attaches to active discovery runs after page refresh.
- Source Discovery keeps a bridge-persisted admin preference, enabled by default, that auto-approves healthy pending sources with jobs after discovery completes and before any follow-on auto-sync push decision.
- `weakSignal` remains an advisory hint for ranking, review, and re-probe heuristics only; it does not block auto-approval.
- Report-side queue throttles like `domain_cap` do not veto a clean pending registry row.
- Source registry deletes are tombstone-backed and local-only; the restore path is explicit, and manual add will not silently clear a tombstone.
