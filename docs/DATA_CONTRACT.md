# Baluffo Data Contracts

> - **Status:** Active
> - **Use this when:** changing payload shape, schema fields, saved-job structure, discovery output shape, or UI interaction handles
> - **Canonical for:** data contracts between pipeline, bridge, frontend, and local user data flows
> - **Not canonical for:** subsystem ownership or route wiring
> - **Then inspect:** `src/core/schemas.py`, `src/core/contracts.py`, `src/jobs/common/contracts.py`, relevant tests, and the owning runtime docs
> - **Last updated:** 2026-04-26
> - **Also update when changing contract shape:** `src/core/schemas.py`, `src/core/contracts.py`, `src/jobs/common/contracts.py`, the owning `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` leaves, relevant tests, and any affected UI/runtime docs

This document serves as the absolute boundary and source of truth for data structures passed between the Python pipeline (`src/jobs/`) and the Vanilla JS frontend (`frontend/`).

**CRITICAL:** The frontend expects `camelCase` keys in all `data/*.json` files. The Python backend maps these explicitly through the stable `src/jobs/common/contracts.py` surface and its owning `contracts_{runtime,source_reports,task_state,fetch_report}.py` leaves.

**Runtime source of truth:** `src/jobs/common/contracts.py` remains the stable contract surface used by the jobs pipeline, with implementation ownership split across `src/jobs/common/contracts_runtime.py`, `contracts_source_reports.py`, `contracts_task_state.py`, and `contracts_fetch_report.py`. `src/core/schemas.py` defines the Pydantic validation models used at pipeline, bridge, and local-data boundaries, including `CanonicalJobSchema`, `SavedJobSchema`, `LocalSavedJobRowSchema`, `LocalDataActivityRowSchema`, `LocalDataAttachmentRowSchema`, `LocalDataBackupPayloadSchema`, and `ManifestSchema`. `src/core/contracts.py` uses these schemas to validate pipeline payloads before writing `jobs-unified.json`, while bridge local-data routes keep save-input validation compatibility-lenient and validate persisted/output rows separately. New fields or contract changes require updating this doc and the Pydantic schemas in `src/core/schemas.py`.

## 1. CanonicalJob
Represents a single job posting retrieved from the external sources.

| Field | Type | Description |
|---|---|---|
| `id` | `string` / `number` | A unique identifier, often auto-incremented by the pipeline or derived. |
| `title` | `string` | The title of the job opening. |
| `company` | `string` | The studio or employer name. |
| `city` | `string` | The geographic city or empty if purely remote. |
| `country` | `string` | The localized or ISO country name. |
| `workType` | `string` | One of `Remote`, `Hybrid`, `Onsite`. |
| `contractType` | `string` | One of `Full-time`, `Internship`, `Temporary`, `Unknown`. |
| `jobLink` | `string` | The canonical URL to apply for the job. |
| `sector` | `string` | The industry sector, e.g., `Game` or `Tech`. |
| `profession` | `string` | The normalized profession key (e.g. `3d-artist`, `gameplay`, `tools`). |
| `companyType` | `string` | The type of company (e.g. `Game`, `Tech`). |
| `description` | `string` | Fallback description text. |
| `source` | `string` | Name of the scraper/board (e.g. `google_sheets_tech`, `greenhouse_static`). |
| `sourceJobId` | `string` | The ID of the job according to the originating ATS / board. |
| `fetchedAt` | `string` (ISO 8601) | When the pipeline successfully fetched this row. |
| `postedAt` | `string` (ISO 8601) | When the employer originally posted the role (if available). |
| `status` | `string` | The job status (e.g. `active`, `likely_removed`, `archived`). |
| `firstSeenAt` | `string` (ISO 8601) | When this pipeline first discovered the job. |
| `lastSeenAt` | `string` (ISO 8601) | The last pipeline run this job was detected as active. |
| `removedAt` | `string` (ISO 8601) | When the pipeline detected a 404 or removal. |
| `dedupKey` | `string` | A unique content hash used for deduplication. |
| `qualityScore` | `number` | The heuristic health of the job details [0-100]. |
| `focusScore` | `number` | Deprecated/internal score [0-100]. |
| `sourceBundleCount` | `number` | The number of exact duplicates collapsed into this canonical row. |
| `sourceBundle` | `Array<Object>` | Raw ATS payload of the duplicate rows. |
| `adapter` | `string` | The Python adapter module used (e.g., `static`, `social`, `csv`). |
| `studio` | `string` | The underlying pipeline configuration studio group. |

## 2. Desktop Local Data

Desktop local data is file-backed under `data/local-user-data/`. `src/local_data_store.py` remains the stable import surface, while persisted row shapes are defined by the dedicated local-data schemas in `src/core/schemas.py`.

### 2.1 Persisted saved-job row

This is the canonical stored/output row shape returned by desktop local-data GET routes and used inside backup payloads.

| Field | Type | Description |
|---|---|---|
| `profileId` | `string` | Owning local profile ID. |
| `jobKey` | `string` | Primary key `job_<hash>` derived from normalized job identity. |
| `title` | `string` | Job title. |
| `company` | `string` | Company or studio name. |
| `sector` | `string` | Normalized sector value. |
| `companyType` | `string` | Company type such as `Game` or `Tech`. |
| `city` | `string` | City display text. |
| `country` | `string` | Country display text. |
| `workType` | `string` | `Remote`, `Hybrid`, `Onsite`, or similar display value. |
| `contractType` | `string` | Contract label such as `Full-time`, `Internship`, or `Unknown`. |
| `jobLink` | `string` | Sanitized application URL. |
| `profession` | `string` | Profession key or display label. |
| `isCustom` | `boolean` | True for user-created local rows. |
| `customSourceLabel` | `string` | User-facing badge/source label for custom rows. |
| `reminderAt` | `string` (ISO 8601) | Optional reminder timestamp. |
| `contactedAt` | `string` (ISO 8601) | Optional contacted timestamp. |
| `updatedBy` | `string` | Optional user/editor marker. |
| `applicationStatus` | `string` | Canonical local status such as `bookmark`, `applied`, or interview/offer phases. |
| `phaseTimestamps` | `object<string, string>` | Phase-to-timestamp map; `bookmark` is always present after normalization. |
| `notes` | `string` | Freeform local notes. |
| `attachmentsCount` | `number` | Current count of persisted local attachments for the row. |
| `savedAt` | `string` (ISO 8601) | Bookmark/create timestamp. |
| `updatedAt` | `string` (ISO 8601) | Last mutation timestamp. |

`/desktop-local-data/saved-jobs/save` remains compatibility-lenient: the POST input validator still accepts legacy fields such as `snapshot`, `status`, `attachments`, `signature`, and `keySalt` through `SavedJobSchema`. Those fields are accepted for input compatibility, but they are not the canonical persisted/output row contract.

### 2.2 Persisted activity row

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Activity row ID, currently `log_<random>`. |
| `profileId` | `string` | Owning local profile ID. |
| `type` | `string` | Event type such as `job_saved`, `status_changed`, or attachment events. |
| `jobKey` | `string` | Related saved-job key when present. |
| `title` | `string` | Job title snapshot for history display. |
| `company` | `string` | Company snapshot for history display. |
| `createdAt` | `string` (ISO 8601) | Event timestamp. |
| `details` | `object` | Event-specific metadata. |

### 2.3 Persisted attachment row

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Attachment ID, currently `att_<random>` or imported ID. |
| `profileId` | `string` | Owning local profile ID. |
| `jobKey` | `string` | Related saved-job key. |
| `name` | `string` | Original attachment filename. |
| `type` | `string` | MIME type. |
| `size` | `number` | Attachment size in bytes. |
| `createdAt` | `string` (ISO 8601) | Attachment creation/import timestamp. |
| `path` | `string` | Relative on-disk filename under the user attachment directory. |

### 2.4 Backup export/import payload v2

Desktop backup export/import uses schema version `2` and is profile-scoped.

| Field | Type | Description |
|---|---|---|
| `version` | `number` | Always `2` for the current writer. |
| `schemaVersion` | `number` | Always `2` for the current writer. |
| `exportedAt` | `string` (ISO 8601) | Export timestamp. |
| `includesFiles` | `boolean` | Whether attachment file contents were embedded. |
| `counts.savedJobs` | `number` | Count of exported saved-job rows. |
| `counts.customJobs` | `number` | Count of exported custom saved-job rows. |
| `counts.historyEvents` | `number` | Count of exported activity rows. |
| `counts.attachments` | `number` | Count of exported attachment rows. |
| `profile` | `object` | Profile metadata with `id`, `name`, and `email`. |
| `savedJobs` | `Array<LocalSavedJobRow>` | Saved-job rows using the canonical persisted shape above. |
| `attachments` | `Array<LocalDataBackupAttachment>` | Attachment metadata rows; each row may include `blobDataUrl` when `includesFiles` is true. |
| `activityLog` | `Array<LocalDataActivityRow>` | Activity/history rows. |

`blobDataUrl` is additive inside backup attachment rows. It is only populated when the caller exports with files included, and importers must remain tolerant of metadata-only backups without file contents.

### 2.5 Stable JS runtime contract

The canonical browser/desktop local-data runtime surface is defined by `frontend/local-data/runtime-contract.js`. `window.JobAppLocalData` in desktop mode must satisfy `LOCAL_DATA_RUNTIME_METHODS` exactly.

Stable method groups:

- Auth/session: `isReady`, `getCurrentUser`, `onAuthStateChanged`, `signIn`, `signOut`
- Saved jobs and status: `saveJobForUser`, `removeSavedJobForUser`, `getSavedJobKeys`, `subscribeSavedJobs`, `generateJobKey`, `canTransitionPhase`, `updateApplicationStatus`, `updateJobNotes`
- Attachments: `buildAttachmentPath`, `listAttachmentsForJob`, `addAttachmentForJob`, `getAttachmentBlob`, `getAttachmentOpenUrl`, `getAttachmentDownloadUrl`, `deleteAttachmentForJob`
- Activity/backup/admin: `listActivityForUser`, `exportProfileData`, `getBackupExportUrl`, `importProfileData`, `getAdminOverview`, `wipeAccountAdmin`

---

## 3. UI Interaction Contract (`data-ui`)

To decouple UI logic from presentation/styling, Baluffo uses `data-ui` attributes as the "canonical handles" for all interactive elements.

### The Strategy
1. **Registry**: All `data-ui` tokens MUST be registered in `frontend/shared/ui/selectors.js`.
2. **HTML Implementation**: Elements in `.html` templates should include the attribute: `data-ui="token-name"`.
3. **JS selection**: Use the `ui(token)` helper from `selectors.js` to query elements: `document.querySelector(ui(G_TOKENS.myToken))`.

### Guidelines for AI Agents
- **NEVER** use class names or IDs for querying if a `data-ui` attribute is available.
- If you add a new interactive element, add a corresponding token to `selectors.js` and apply it via `data-ui`.

---

## 4. Workspace HUD Contract (`LATEST_MANIFEST.json`)

The orchestrator generates a machine-readable HUD in `_out/LATEST_MANIFEST.json` after every run. AI agents SHOULD read this file first to understand the current workspace state.

### Schema (v1)
```json
{
  "last_run_id": "string (YYYYMMDD_HHMMSS)",
  "last_run_time": "string (timestamp)",
  "status": "string (success|failure)",
  "summary": "string (human-readable status)",
  "src_hash": "string (SHA256 of src/)",
  "artifacts_root": "string (path to run directory)",
  "artifacts": {
    "exe": "string (path to portable exe artifact)",
    "ship": "string (path to ship bundle artifact)",
    "smoke_report": "string (relative path to report.json)",
    "py_tests_status": "string (not_run|passed|failed)",
    "node_tests_status": "string (not_run|passed|failed)",
    "py_tests_ok": "boolean",
    "node_tests_ok": "boolean"
  }
}
```

`py_tests_ok` and `node_tests_ok` remain legacy HUD booleans. New code and AI agents should prefer `py_tests_status` and `node_tests_status` when present so `not_run` is distinguishable from `failed`.

---

## 5. Source Registry and Sync

Baluffo's source registry is split into three local bucket files and one local-only tombstone ledger under `data/`:

| File | Purpose |
|---|---|
| `data/source-registry-active.json` | Active sources ready for fetch/sync |
| `data/source-registry-pending.json` | Pending sources in the probe/standby pool |
| `data/source-registry-rejected.json` | Local rejected sources; a normal registry bucket, not a delete sentinel |
| `data/source-registry-tombstones.json` | Local delete ledger keyed by `source_identity()` |

### Canonical registry row

Registry rows are normalized around these canonical fields:

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Canonical source identity derived from `source_identity()` |
| `registryState` | `string` | One of `active`, `pending`, or `rejected` |
| `pendingReason` | `string` | Why the row is pending or rejected; empty for active rows |
| `stateChangedAt` | `string` (ISO 8601) | Timestamp of the last transition into the current registry state |
| `stateChangedBy` | `string` | Actor or route that performed the transition |
| `lastPromotedAt` | `string` (ISO 8601) | Last time the row was promoted into `active` |
| `lastDemotedAt` | `string` (ISO 8601) | Last time the row was demoted into `pending` or `rejected` |
| `hiddenFromDefault` | `boolean` | Optional pending-row flag for recoverable rows hidden from default review views |
| `duplicateOfSourceId` | `string` | Optional pointer to the active winner when a duplicate-family row is demoted |

Legacy lifecycle fields such as `candidateState`, `approvedAt`, `approvedBy`, `liveAt`, `quarantinedAt`, and `quarantineReason` remain populated for compatibility, but they should be treated as compatibility mirrors rather than the canonical source of truth.

### Sync snapshot v2

Remote sync snapshots now use schema version `2` and are built from canonical per-source rows.

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `number` | Always `2` for the current writer |
| `generatedAt` | `string` (ISO 8601) | Snapshot build time |
| `source` | `object` | Snapshot origin metadata |
| `active` | `Array<Object>` | Active registry rows |
| `pending` | `Array<Object>` | Pending registry rows |

`rejected` rows are intentionally excluded from remote snapshots. Snapshot readers still accept legacy v1 input and infer the transition metadata needed to merge into the canonical model. Tombstones are never included in remote snapshots.

---

## 6. Runtime Configuration

### Frontend Config
The `frontend-runtime-config.js` is generated by `scripts/build_frontend_runtime_config.py`. It bridges the gap between Python-side configuration and the browser.

### Bridge Config
The Admin Bridge (`src/admin_bridge.py`) follows a strict precedence:
1. CLI Arguments
2. Environment Variables (`BALUFFO_*`)
3. `baluffo.config.local.json` (machine-local overrides)
4. `baluffo.config.json` (committed defaults)

`baluffo.config.json` is the committed app/runtime configuration owner. Tooling-specific
configuration such as `opencode.json` remains separate; do not move MCP/editor keys into
the runtime config or runtime keys into tool config.

---

## 7. Configuration Schema

### baluffo.config.json

| Key | Default | Description |
|-----|---------|-------------|
| **bridge.host** | `"127.0.0.1"` | Admin bridge listen host |
| **bridge.port** | `8877` | Admin bridge listen port |
| **bridge.log_format** | `"human"` | Log format (`human` or `jsonl`) |
| **bridge.log_level** | `"info"` | Log level (`info` or `debug`) |
| **bridge.quiet_requests** | `false` | Suppress request logging |
| **storage.data_dir** | `"data"` | Runtime data directory |
| **storage.source_discovery_config_path** | `"data/source-discovery-config.json"` | Source discovery settings path |
| **storage.source_discovery_log_path** | `"data/source-discovery.log"` | Source discovery log path |
| **storage.social_sources_config_path** | `"data/social-sources-config.json"` | Social source settings path |
| **security.github_app_enabled_default** | `true` | GitHub App sync enabled by default |
| **sync.packaged_config_path** | `"packaging/github-app-sync-config.json"` | GitHub App sync config |
| **sync.local_enabled_default** | `true` | Source sync enabled by default |
| **sync.default_repo** | `""` | Default sync repo (e.g. `owner/repo`) |
| **sync.default_branch** | `"main"` | Default sync branch |
| **sync.default_path** | `"baluffo/source-sync.json"` | Default sync file path |
| **sync.build_key_derivation_default** | `"embedded"` | Key derivation mode |
| **desktop.site_port** | `8080` | Local site port |
| **desktop.bridge_port** | `8877` | Desktop mode bridge port |
| **desktop.bridge_host** | `"127.0.0.1"` | Desktop mode bridge host |
| **desktop.open_path** | `"jobs.html"` | Desktop startup page |
| **desktop.title** | `"Baluffo"` | Desktop window title |

**Config precedence:** CLI args → Environment (`BALUFFO_*`) → `baluffo.config.local.json` → `baluffo.config.json` → code defaults

**Machine-local overrides:** Use `baluffo.config.local.json` for settings that must not be committed.

---

## 8. Source discovery contract

Source discovery writes `data/source-discovery-report.json` and `data/source-discovery-candidates.json`. Treat the following as stable until a dedicated plan.

**Pydantic validation:** Report summary shape is defined and validated at the discovery output boundary. See **src/source_discovery/schemas.py** for `DiscoveryReportSummarySchema` and `DiscoveryReportSchema`. The orchestrator validates the summary with `DiscoveryReportSummarySchema.model_validate(report["summary"])` before writing the report; invalid shape raises `ValidationError`. The snapshot test `test_discovery_report_snapshot_contract` also validates the summary so the contract is enforced in CI.

### Stable public APIs (`src/source_discovery`)

Do not change signatures or remove without a dedicated plan:

- `run_discovery(...)`
- `discover_gamesmap_candidates(...)`
- `probe_candidate(...)`, `async_probe_candidate(...)`, `validate_candidate_for_probe(...)`
- `parse_gamesmap_detail_page(...)`, `parse_gamesmap_index_entries(...)`, `build_static_candidate_from_page(...)`

`src/source_discovery/orchestrator.py` remains the public run surface and test patch seam for `run_discovery(...)`; helper modules such as `orchestrator_runtime.py`, `orchestrator_generation.py`, `orchestrator_probe.py`, and `orchestrator_finalize.py` are implementation detail behind that contract. Gamesmap helper ownership now lives in `gamesmap_{cache,parsing,candidates}.py`, discovery reporting/helper ownership lives in `reporting_{progress,candidates,backlog}.py`, and web-search helper ownership lives in `web_search_{fetch,extract,candidates}.py`, with `gamesmap.py`, `reporting.py`, and `web_search.py` kept as stable import surfaces.

### Data contracts

- **source-discovery-report.json** and **source-discovery-candidates.json** must remain shape-compatible.
- **source-discovery-report.json** now includes top-level `runId` for lifecycle ownership. The same `runId` must also appear in the matching `data/admin-task-state.json` discovery entry while the task is active.
- **Report summary** must retain: counts, stage maps (`generatedCountByStage`, `survivedDedupeCountByStage`, `probedCountByStage`, `queuedCountByStage`), `lossAccounting`, `adapterCounts`, `methodCounts`.
- **Runtime lifecycle metadata:** discovery runtime may include `runtime.lifecycle.owner` and `runtime.lifecycle.heartbeatAt`. These fields are additive and used by the bridge to project Current Runs without mutating the report.
- **Candidates file semantics:** `data/source-discovery-candidates.json` is the persisted discovery review queue. It may contain both queued candidates and deferred review rows; consumers must use `deferred` / `deferReason` instead of assuming every row is queue-ready.
- **M5 review snapshot:** `data/m5-strategic-backlog.json` is a derived review artifact built from discovery output. It is additive and must not replace `data/source-discovery-candidates.json` as the canonical discovery ledger.
- **Additive candidate metadata** may include lifecycle and ranking fields such as `candidateState`, `rankScore`, `rankReasons`, `promotionLane`, `approvedAt`, `approvedBy`, `liveAt`, `quarantinedAt`, `quarantineReason`, `deferCount`, `firstDeferredAt`, and `lastDeferredAt`.
- **Hidden pending rows** remain recoverable pending rows. `candidateState="hidden"` / `hiddenFromDefault=true` means default review views may omit them; explicit review views may request them.
- **Candidates** and **failures** objects must retain the fields asserted in `test_discovery_report_snapshot_contract`.
- Any contract change requires: updated snapshot fixture (`tests/fixtures/source_discovery_report_snapshot.json`), doc update, and a focused PR.

---

## 9. Admin task progress contract

Fetcher and discovery reports may include a shared `taskProgress` object for the admin loading bars. This is the preferred progress contract for the frontend.

### Stable fields

| Field | Type | Description |
|---|---|---|
| `active` | `boolean` | True while the task should render as in progress. |
| `phaseKey` | `string` | Stable machine-readable phase token such as `executing_sources` or `probing_candidates`. |
| `phaseLabel` | `string` | Human-readable phase label shown in the admin UI. |
| `mode` | `string` | Either `indeterminate` or `determinate`. |
| `ratio` | `number` | `0..1` progress ratio when `mode` is `determinate`; ignored otherwise. |
| `counts` | `object` | Display-only task metrics used to enrich the label, not to redefine primary progress semantics. |

### Frontend contract

- Controllers consume `taskProgress` and pass raw report state plus optional log-derived phase hints into the domain layer.
- The domain layer is responsible for mapping `taskProgress` into the rendered progress view.
- The shared progress renderer only renders the derived view model; it must not infer phases or ratios from raw report counters.
- Raw report counters remain useful for details, but the primary loading-bar state comes from `taskProgress`.
- `/ops/task-live/<taskType>` is the detailed Admin Ops live task contract for fetch, discovery, and sync. Its `recentEvents` rows are normalized by `src/shared/live_task.py` and include `schemaVersion`, `event`, `timestamp`, `level`, `taskType`, `runId`, `workItemId`, `phaseKey`, and `message`.
- The live task `event` token is additive and stable for diagnostics. It uses an explicit row `event` when present, then `phaseKey`, then `live_task_event`; existing consumers should continue to rely on the compatibility fields they already read.
- Do not introduce task-specific parallel live event formats for fetch, discovery, or sync. Extend `taskProgress`, `workItems`, and `recentEvents` through the shared normalizers.

### Lifecycle identity contract

- `runId` is the only lifecycle identity for long-running admin tasks.
- Fetch lifecycle surfaces:
  - `data/jobs-fetch-report.json`
  - `data/jobs-fetch-tasks.json`
  - `data/admin-task-state.json` entry `fetch`
- Discovery lifecycle surfaces:
  - `data/source-discovery-report.json`
  - `data/admin-task-state.json` entry `discovery`
- `data/admin-run-history.json` is a derived history surface keyed by `runId`. It is not authoritative for whether a run is still active.
- `data/jobs-fetch-tasks.json` now carries top-level `runId`, `startedAt`, `finishedAt`, and `heartbeatAt`.
- Fetch report runtime may include `runtime.lifecycle.owner` and `runtime.lifecycle.heartbeatAt`.
- Any new task-lifecycle artifact must preserve `runId` end to end instead of relying on timestamps.

### Lifecycle cleanup

- For a clean post-migration debug baseline, use:
  - `python scripts/reset_admin_task_lifecycle.py --data-dir data`
- This command resets only current lifecycle/debug artifacts and keeps `admin-run-history.json` in the current runId-only shape.

---

## 9. Fetch report diagnostic breakdowns

`summary.needsReviewBreakdown` is the shaped zero-kept static diagnostic view. It does not promise to equal every raw `needs_review` marker in `sources`.

`summary.okCleanSources` and `summary.okWithWarningSources` are additive counters over source-report rows whose `status` remains `ok`. They distinguish clean successful sources from successful sources carrying warning/error diagnostic text without changing source status semantics.

`summary.sizeGuardrails` is an additive output-size diagnostic. It does not change the output file contract: `jobs-unified.json`, `jobs-unified-light.json`, and `jobs-unified.csv` are still written with the same row fields. Unified JSON files are compact serialized; report/debug JSON remains pretty-printed.

| Field | Type | Description |
|---|---|---|
| `json.bytes` | `number` | Current `jobs-unified.json` byte size. |
| `json.limitBytes` | `number` | Full JSON warning limit, currently `80_000_000`. |
| `json.exceeded` | `boolean` | True when the full JSON byte size is over its limit. |
| `lightJson.bytes` | `number` | Current `jobs-unified-light.json` byte size. |
| `lightJson.limitBytes` | `number` | Light JSON warning limit, currently `60_000_000`. |
| `lightJson.exceeded` | `boolean` | True when the light JSON byte size is over its limit. |
| `csv.bytes` | `number` | Current `jobs-unified.csv` byte size. |
| `csv.limitBytes` | `number` | CSV warning limit, currently `50_000_000`. |
| `csv.exceeded` | `boolean` | True when the CSV byte size is over its limit. |

`summary.sizeGuardrailExceeded` remains the aggregate compatibility flag and is true when any `summary.sizeGuardrails.*.exceeded` value is true.

| Field | Type | Description |
|---|---|---|
| `byShape` | `object` | Counts and examples by diagnostic shape. |
| `topByWallTime` | `array` | Slowest included static rows. |
| `topByFrequency` | `array` | Shapes ordered by frequency and duration. |
| `rawMarkerCount` | `number` | Count of source rows where `classification`, `failureBucket`, or `zeroKeptClassification` is `needs_review`. |
| `includedCount` | `number` | Count of rows included in the shaped zero-kept static breakdown. |

---

## 10. Social experiment report contract

The fetch report may include a top-level `socialSummary` block for the M6 social/community pilot.

### Purpose

This block is additive and exists so the jobs fetch report, bridge ops health, and manual review artifact can describe the same measured experiment.

### Definitions

- `official-board origin` means first-party company boards and structured ATS/company-page ingestion only.
- `not official-board origin` means community sheets, aggregators, repost feeds, and social sources.
- `uniqueKeptCount` is measured post-dedup on final canonical output rows.
- `officialBoardOverlapCount` means a canonical job appears in both social and official-board origin paths.

### Fetch report shape

| Field | Type | Description |
|---|---|---|
| `pilotWindowStartAt` | `string` | Start of the measured pilot window. |
| `pilotWindowEndAt` | `string` | End of the measured pilot window. |
| `scheduledRunCount` | `number` | Number of scheduled fetch runs included in the pilot window. |
| `keptCount` | `number` | Total social rows kept in the run window. |
| `uniqueKeptCount` | `number` | Kept social jobs whose final canonical row is unique to social after dedup. |
| `officialBoardOverlapCount` | `number` | Kept social jobs that also appear in official-board ingestion paths. |
| `duplicateCount` | `number` | Kept social rows removed as duplicates. |
| `duplicateRate` | `number` | `duplicateCount / keptCount` for the run window. |
| `lowConfidenceDropped` | `number` | Social rows dropped because confidence was below threshold. |
| `sampleSize` | `number` | Reviewed social sample size, or `0` when no review data exists. |
| `reviewedCount` | `number` | Rows in the review artifact that have a true/false positive judgment. |
| `falsePositiveCount` | `number` | Reviewed rows marked as false positive. |
| `falsePositiveRate` | `number` | `falsePositiveCount / reviewedCount` for the reviewed sample. |
| `reviewArtifactPath` | `string` | Path to `data/social-experiment-review.json` or the run-local equivalent. |
| `channels` | `object` | Per-channel summaries for `reddit` and `mastodon`. |

### Review artifact

- The pipeline writes a deterministic candidate sample to `data/social-experiment-review.json`.
- The sample is stable-sorted by canonical job id or dedup key, then truncated to the first 50 eligible social-kept rows.
- Human review fills in `reviewDecision` and `reviewNotes` for the candidate rows.
- If no review data exists yet, the report should emit `sampleSize = 0`, `reviewedCount = 0`, `falsePositiveCount = 0`, and `falsePositiveRate = 0`.

### Bridge visibility

The bridge ops health payload mirrors a compact `kpis.socialExperiment` view from the fetch report so operators can review the experiment without reading the raw report file.

---

## 11. Fetch regression reconciliation contract

The fetch report may include a top-level `healthSummary` reconciliation pair for the parser-regression lane.

### Fields

| Field | Type | Description |
|---|---|---|
| `siteChangedDiagnosedCount` | `number` | Number of top-level source rows diagnosed as `site_changed`. |
| `parserRegressionQueueCount` | `number` | Number of rows written to `jobs-parser-regression-queue.json`. |

### Queue artifact

- The canonical parser-regression artifact path is exposed at `outputs.parserRegressionQueue`.
- `listingChanged` remains the source/report field; the queue artifact projects it as `listingFingerprintChanged` for review readability.
- For normalized fetch-report rows, static sources diagnosed as `site_changed` preserve `listingUrl`, `pages`, and `sourceId` so the regression lane can recover `oldUrl` even when detail payloads are empty.
- For normalized fetch-report rows, aggregate provider sources diagnosed as `site_changed` preserve `providerUrl` so `greenhouse_boards` and `workable_sources` can still enter the regression lane when no listing URL surface exists.
- Admission to the lane is diagnosis-driven from the top-level source row. URL/status/fingerprint fields are enrichment and ordering data only.
