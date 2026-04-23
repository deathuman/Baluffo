# Jobs Fetcher Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/jobs_fetcher.py` stayed as the stable CLI and test patch surface.
- Lazy export bookkeeping moved into `src/jobs/fetcher_compat_exports.py` and root-backed runtime wrappers moved into `src/jobs/fetcher_compat_runtime.py`.
- Fetcher behavior, loader naming, source registry shape, admin launch wiring, and static adapter compatibility remained unchanged.

## Final Owning Surfaces

- Stable root: `src/jobs_fetcher.py`
- Owning leaves: `src/jobs/fetcher_compat_exports.py`, `src/jobs/fetcher_compat_runtime.py`, and the package-owned pipeline/adapter leaves under `src/jobs/*`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../adapter-plugin-inventory.md`](../../adapter-plugin-inventory.md).

## Historical Notes

- Root-backed monkeypatch seams for transport and diagnostics stayed intact on purpose.
- Later follow-up state is summarized in [`../history/runtime-first-cleanup-handoff.md`](../history/runtime-first-cleanup-handoff.md).
