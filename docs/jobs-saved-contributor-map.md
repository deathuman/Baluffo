# Jobs + Saved + Admin Contributor Map

## Where to change what
- Jobs/Saved/Admin UI behavior or DOM events: `frontend/{jobs|saved|admin}/app.js`
- Rendering markup/classes: `frontend/{jobs|saved|admin}/render.js`
- Pure rules/transforms: `frontend/{jobs|saved|admin}/domain.js`
- Fetch/parsing adapters: `frontend/{jobs|saved|admin}/data-source.js`
- Bridge/local-data service calls: `frontend/{jobs|saved|admin}/services.js`
- URL/local/session view-state sync: `frontend/{jobs|saved|admin}/state-sync/index.js`
- Shared cross-page helpers only:
  - `frontend/shared/ui` for DOM/UI helper primitives
  - `frontend/shared/data` for pure data/value helpers (no DOM)

## Dependency direction (all feature slices)
```text
index -> app
app -> actions | domain | data-source | render | services | state-sync
actions/domain/data-source/render/services/state-sync -> shared or root utilities only
```

## Quick edit guidance
- Add a filter or page interaction: edit `app.js` (+ `domain.js` only if rule logic is needed)
- Add a new row/detail visual: edit `render.js` (+ `app.js` wiring)
- Add backup/fetch parsing behavior: edit `data-source.js`
- Add auth/profile/local bridge behavior: edit `services.js` and call from `app.js`
- Add local/session persistence or URL sync behavior: edit `state-sync/index.js`
