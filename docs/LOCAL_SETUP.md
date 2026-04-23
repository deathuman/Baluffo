# Local Setup

> - **Status:** Active
> - **Use this when:** you need the current local-first storage model, sign-in behavior, backup flow, or the smallest command set
> - **Canonical for:** browser-local vs desktop-local behavior, local-data routing, and local runtime setup
> - **Not canonical for:** release sequencing or the full verification matrix
> - **Then inspect:** [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md), [`testing.md`](testing.md), and [`RELEASE.md`](RELEASE.md) as needed
> - **Last updated:** 2026-04-23

## Local-first modes

| Mode | Runtime | Storage owner |
|------|---------|---------------|
| Browser pages | Static site only | Browser `localStorage` + IndexedDB |
| Desktop / launcher | Local site + local bridge | Bridge-backed file store under `data/local-user-data/` |

## Storage model

### Browser-local mode
- Profiles/session live in `localStorage`.
- Saved jobs, notes, and attachment metadata live in IndexedDB database `baluffo_jobs_local`.
- Browser mode is local-only and does not require the bridge.

### Desktop-local mode
- Profiles live in `data/local-user-data/profiles.json`.
- Current session lives in `data/local-user-data/session.json`.
- Per-user data lives under `data/local-user-data/users/<uid>/`.
- Desktop pages read/write local data through `/desktop-local-data/*`, not browser IndexedDB/localStorage.

## Code routing

- `src/local_data_store.py` is the stable desktop local-data store surface; implementation belongs in `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`.
- `frontend/shared/local-data/desktop-client.js` is the stable shared desktop-local runtime root; implementation belongs in `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`.
- `frontend/local-data/services.js` remains a transitional local-data boundary. Feature slices should continue to call their own slice `services.js`, not import the shared desktop runtime directly.

## Sign-in behavior

- Guest browsing works in Jobs, but seen/saved persistence requires signing in.
- Browser mode can create or re-use a local profile in-browser.
- Desktop mode loads existing local profiles first through `GET /desktop-local-data/profiles`.
- If desktop profile loading fails, the flow is explicit `Retry` / `Create new profile` / `Cancel`; it does not silently guess an existing profile from blind text entry.

## Backup and restore

- `Saved Jobs` supports `Export Backup` and `Import Backup`.
- Backups are profile-scoped.
- `Include files` off exports notes + attachment metadata only.
- `Include files` on includes attachment file contents.

## Local admin behavior

- `Admin` shows local profiles and storage totals from the bridge-backed local data store.
- Admin overview and wipe actions are local bridge actions; there is no separate Admin PIN flow.
- On a first desktop run, Admin may show non-dismissible guidance to run Jobs Fetcher until the first successful fetch completes.

## Minimum commands

| Goal | Command |
|------|---------|
| Start local launcher (site + bridge + owned browser) | `npm run dev:bridge` |
| Run jobs pipeline locally | `npm run dev:pipeline` |
| Full build | `npm run build` |
| Developer Python lane | `npm run test:py` |
| Frontend smoke lane | `npm run test:smoke` |
| Full verification | `npm run verify` |

## Related docs

- [`DATA_CONTRACT.md`](DATA_CONTRACT.md) - canonical local-data row, backup, and runtime contract shapes
- [`admin-bridge-api.md`](admin-bridge-api.md) — current localhost route surface
- [`testing.md`](testing.md) — canonical verification matrix
- [`RELEASE.md`](RELEASE.md) — packaging, updater, and release workflow
- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) — task routing and edit boundaries
