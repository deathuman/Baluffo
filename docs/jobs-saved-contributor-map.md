# Jobs + Saved + Admin Contributor Map

Use with `docs/architecture-ai-map.md` for full system context.

## Minimal context entrypoints
- Jobs page:
  - `frontend/jobs/app.js`
  - `frontend/jobs/app/runtime.js`
  - then only the needed feature module under `frontend/jobs/app/*.js`
- Saved page:
  - `frontend/saved/app.js`
  - `frontend/saved/app/runtime.js`
  - then only the needed feature module under `frontend/saved/app/*.js`
- Admin page:
  - `frontend/admin/app.js`
  - `frontend/admin/app/runtime.js`
  - then only the needed feature module under `frontend/admin/app/*.js`

## Where to change what (current)
- Page orchestration: `frontend/{jobs|saved|admin}/app/runtime.js`
- Page-specific feature modules: `frontend/{jobs|saved|admin}/app/*.js`
- Rendering markup/classes: `frontend/{jobs|saved|admin}/render.js`
- Pure rules/transforms: `frontend/{jobs|saved|admin}/domain.js`
- Fetch/parsing adapters: `frontend/{jobs|saved|admin}/data-source.js`
- Bridge/local-data service calls: `frontend/{jobs|saved|admin}/services.js`
- URL/local/session view-state sync: `frontend/{jobs|saved|admin}/state-sync/index.js`
- Shared helpers:
  - `frontend/shared/ui` for DOM/UI helper primitives
  - `frontend/shared/data` for pure data/value helpers (no DOM)

## Dependency direction (current)
```text
index -> app
app -> app/runtime
app/runtime -> app/* | actions | domain | data-source | render | services | state-sync
app/* -> shared helpers or page-layer primitives (no cross-page imports)
actions/domain/data-source/render/services/state-sync -> shared or root utilities only
```

## Quick edit guidance
- Add Jobs filter/page interaction: `frontend/jobs/app/filters.js` (+ `app/runtime.js` wiring if needed)
- Add Jobs fetch/refresh behavior: `frontend/jobs/app/feed.js`
- Add Jobs country/region filter behavior: `frontend/jobs/app/countries.js`
- Add Jobs source preset / startup feed-source behavior: `frontend/jobs/app/sources.js`
- Add Saved notes behavior: `frontend/saved/app/notes.js`
- Add Saved attachments behavior: `frontend/saved/app/attachments.js`
- Add Saved filter/sort rules: `frontend/saved/app/view-state.js`
- Add Admin unlock/ops/fetch/discovery/sync behavior: `frontend/admin/app/{auth,ops,fetcher,discovery,sync}.js`
- Add render-only visual changes: `frontend/{jobs|saved|admin}/render.js`
- Add bridge/local-data transport behavior: `frontend/{jobs|saved|admin}/services.js`
- Add URL/session state sync behavior: `frontend/{jobs|saved|admin}/state-sync/index.js`

## Transitional seams
- Jobs adapter compatibility: `_runtime.facade()` is still intentionally limited to the jobs adapter compatibility layer.
- Local data compatibility: `window.JobAppLocalData` remains the temporary runtime boundary in `frontend/local-data/services.js`.
- Bridge runtime state: mutable server-adjacent bridge state should live in `src/bridge/server/runtime_state.py` or `src/bridge/sync_state.py`, not new module globals elsewhere.
