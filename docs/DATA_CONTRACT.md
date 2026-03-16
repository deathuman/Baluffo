# Baluffo Data Contracts

This document serves as the absolute boundary and source of truth for data structures passed between the Python pipeline (`src/jobs/`) and the Vanilla JS frontend (`frontend/`).

**CRITICAL:** The frontend expects `camelCase` keys in all `data/*.json` files. The Python backend maps these explicitly in `src/jobs/models.py`.

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

## 2. SavedJob
Represents a job the user has locally bookmarked or created entirely offline in their browser/desktop client.

| Field | Type | Description |
|---|---|---|
| `jobKey` | `string` | Primary key `job_<hash>` combining title/company/city/country. |
| `snapshot` | `CanonicalJob_Partial` | A flattened subset of `CanonicalJob` containing purely display data (Title, Company, Location, WorkType). |
| `createdAt` | `string` (ISO 8601) | When the bookmark/custom row was created. |
| `updatedAt` | `string` (ISO 8601) | When the local user state was last modified. |
| `status` | `string` | The user's active stage (e.g., `saved`, `applied`, `interviewing_1`, `offer`). |
| `notes` | `string` | User notepad text. |
| `isCustom` | `boolean` | True if the user manually created this row rather than bookmarking an ATS row. |
| `customSourceLabel` | `string` | A visual badge name (e.g. `Custom`, `LinkedIn`). |
| `reminderAt` | `string` (ISO 8601) | User's local alarm/reminder target. |
| `attachments` | `number` | Count of local files attached. |
| `signature` | `string` | A unique signature based on core job fields. |

---

## 3. UI Interaction Contract (`data-ui`)

To decouple UI logic from presentation/styling, Baluffo uses `data-ui` attributes as the "canonical handles" for all interactive elements.

### The Strategy
1. **Registry**: All `data-ui` tokens MUST be registered in [frontend/shared/ui/selectors.js](file:///c:/Users/Andrea/Documents/GitHubRepository/Baluffo/frontend/shared/ui/selectors.js).
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
    "py_tests_ok": "boolean",
    "node_tests_ok": "boolean"
  }
}
```

---

## 5. Runtime Configuration

### Frontend Config
The `frontend-runtime-config.js` is generated by `scripts/build_frontend_runtime_config.py`. It bridges the gap between Python-side configuration and the browser.

### Bridge Config
The Admin Bridge ([admin_bridge.py](file:///c:/Users/Andrea/Documents/GitHubRepository/Baluffo/src/admin_bridge.py)) follows a strict precedence:
1. CLI Arguments
2. Environment Variables (`BALUFFO_*`)
3. `baluffo.config.local.json` (machine-local overrides)
4. `baluffo.config.json` (committed defaults)
