# Local Mode Setup

This app runs in local-first mode (no backend required).

## Storage model
- Profiles are stored in `localStorage`.
- Saved jobs and attachment metadata are stored in IndexedDB database `baluffo_jobs_local`.
- Active session profile id is stored in `localStorage` key `baluffo_current_profile_id`.

## Sign-in behavior
- Clicking `Sign in` prompts for a profile name.
- Existing profile names sign into that local profile.
- New names create a profile and sign in immediately.

## Backup and restore
- `Saved Jobs` page includes `Export Backup` and `Import Backup`.
- Backup file format is JSON and profile-scoped.
- Import merges jobs by deterministic `jobKey`.

## Data contract
Saved job record fields:
- `jobKey`, `title`, `company`, `companyType`, `city`, `country`, `workType`, `contractType`, `jobLink`
- `savedAt`, `updatedAt`
- `applicationStatus` (default `bookmarked`)
- `notes` (default empty string)
- `attachmentsCount` (default `0`)

## Future migration note
`local-data-client.js` exposes a provider-like interface (`window.JobAppLocalData`) so this local implementation can later be swapped to another backend without rewriting page-level UI logic.
