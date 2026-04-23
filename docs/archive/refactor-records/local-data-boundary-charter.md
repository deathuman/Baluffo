# Local Data Boundary Cleanup

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/local_data_store.py` stayed as the stable desktop local-data store surface while implementation ownership moved into `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`.
- `frontend/shared/local-data/desktop-client.js` stayed as the stable desktop-local runtime root while implementation ownership moved into `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`.
- Route signatures, saved-job behavior, backup/import-export semantics, and packaged rehearsal imports remained stable.

## Final Owning Surfaces

- Stable roots: `src/local_data_store.py`, `frontend/shared/local-data/desktop-client.js`
- Owning leaves: `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py` and `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`

## Current Routing

Current routing lives in the active wiki, especially [`../../DATA_CONTRACT.md`](../../DATA_CONTRACT.md), [`../../LOCAL_SETUP.md`](../../LOCAL_SETUP.md), [`../../admin-bridge-api.md`](../../admin-bridge-api.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## Historical Notes

- `frontend/local-data/services.js` remained intentionally transitional so page slices did not need to import the shared desktop runtime directly.
- The root surfaces stayed stable for bridge, packaged smoke, and test callers.
