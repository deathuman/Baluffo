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
- Export supports `Include files` toggle:
  - off: notes + attachment metadata only
  - on: includes attachment file contents

## Data contract
Saved job record fields:
- `jobKey`, `title`, `company`, `companyType`, `city`, `country`, `workType`, `contractType`, `jobLink`
- `savedAt`, `updatedAt`
- `applicationStatus` (default `bookmark`)
- `notes` (default empty string)
- `attachmentsCount` (default `0`)

## Administration
- `Admin` page (`admin.html`) shows local profiles and storage usage totals.
- Access is protected by a local hardcoded PIN: `1234`.
- Wiping an account removes profile, saved jobs, notes, and attachments for that user.

## Future migration note
`local-data-client.js` exposes a provider-like interface (`window.JobAppLocalData`) so this local implementation can later be swapped to another backend without rewriting page-level UI logic.
