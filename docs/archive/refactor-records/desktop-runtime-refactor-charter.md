# Desktop Runtime Refactor Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/ship/desktop_app/__init__.py` was reduced to a thin compatibility facade over focused desktop runtime modules.
- The runtime split extracted `config.py`, `process.py`, `browser.py`, `session.py`, and `_windows.py`, then later split launcher/startup ownership into focused modules.
- The later closeout wave further thinned runtime ownership into `launcher_{flow,diagnostics,recovery}.py` and `startup_{ready,watchdog}.py` behind the stable `launcher.py` and `startup.py` roots.

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md) and [`../../architecture-ai-map.md`](../../architecture-ai-map.md). The later end-state for this cleanup line is captured in [`../history/final-leaf-closeout-program.md`](../history/final-leaf-closeout-program.md).

## Completed Closeout

- The package compatibility surface was frozen before the split.
- `config.py`, `process.py`, `browser.py`, `session.py`, and `_windows.py` were extracted.
- `launcher.py` and `startup.py` were extracted and kept as compatibility surfaces.
- `__init__.py` was reduced to a thin facade, ownership docs were updated, and desktop-targeted verification was completed.

## Legacy Findings

- The old runtime shape depended on an implicit `ROOT` symbol; making the repository root explicit was part of the cleanup.
- Package-root monkeypatch compatibility was effectively part of the test contract, so extracted helpers continued to resolve patched behavior through `src.ship.desktop_app` until the split stabilized.
- The pass removed a direct packaged smoke import from `src.ship.desktop_app.__init__` and removed the implicit desktop `ROOT` lookup.

## Historical Notes

- Targeted verification completed with `python -m pytest tests/desktop_app/ -q` and `python -m pytest tests/packaged_desktop/ -q`.
- This record is intentionally slightly richer than the other archived charters because it preserves the main transitional findings from the earlier package split.
