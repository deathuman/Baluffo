# 1. OBJECTIVE

Fix remaining ESLint warnings using the detailed action plan provided.

# 2. CURRENT STATE

- **Ruff**: ✅ Clean
- **ESLint**: ✅ 0 errors, ~40 warnings
- **Previous commits**: f26a634 (lint fixes), dbf04d1 (docs), readiness report done

# 3. DETAILED FIX PLAN (From User Input)

## Priority 1: Quick Wins (Do First)

### frontend/shared/state-hub.js
- Rename catch vars to `_err` or `_ignored`

### theme.js
- Rename bare `_` vars to `_ignored`, `_theme`, `_err`

### tests/frontend/packaged-desktop-smoke.mjs
- Delete `trigger` var (unused test local)

### frontend/admin/app/runtime.js
- Delete `adminPageService` var (dead local)

### frontend/admin/domain.js
- Delete `finishedAt` var (likely extracted, no longer used)
- Delete `status` var

## Priority 2: Dead Locals (Do Second)

### frontend/admin/app/fetcher.js
- Delete `formatFetcherRuntimeOptions` var
- Delete `formatLifecycleSummary` var
- Rename callback args: `startOpsHealthPolling`, `jobsFetchReportUrl`

### frontend/jobs/app/runtime.js
- Delete `writeAutoRefreshSignal`, `markSeenJobsBulk`, `lastFilterOptionsSignature`
- Delete/rename `clearJobsPipelinePolling`
- Delete `showLoading` var

### frontend/saved/app/runtime.js
- Delete most unused helpers/constants

## Priority 3: Callback Shape Params (Rename)

### frontend/admin/app/auth.js
- Rename: `renderUsersEmpty`, `stopBridgeStatusWatch`, `stopOpsHealthPolling`, `showToast`

### frontend/admin/app/discovery.js
- Rename: `loadDiscoveryData` arg

# 4. RECOMMENDED COMMITS

## Commit 1: "chore: silence intentional unused callback params"
- Rename intentionally unused args to `_name`
- Rename catch vars to `_ignored`

## Commit 2: "chore: remove dead frontend locals and helpers"
- Delete dead vars/helpers

# 5. VALIDATION

- [ ] Run `npx eslint .` after each commit
- [ ] Verify no new errors introduced
