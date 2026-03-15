# Baluffo Data Contracts

This document serves as the absolute boundary and source of truth for data structures passed between the Python pipeline (`scripts/jobs/`) and the Vanilla JS frontend (`frontend/`).

**CRITICAL:** The frontend expects `camelCase` keys in all `data/*.json` files. The Python backend maps these explicitly in `scripts/jobs/models.py`.

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
