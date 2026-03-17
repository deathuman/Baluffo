# Submit to main — PR / merge description

Use the text below as the PR description (or merge commit body) when submitting this branch to `main`.

---

## Summary

Adapter plugin rollout: community package, static plugin family, source discovery and jobs pipeline updates.

## Changes

### Adapter layer
- **Community adapter**: Replaced single-file `community.py` with a `community/` package; Google Sheets logic moved to `community/google_sheets.py`, `__init__.py` re-exports and runs remote_ok, gamesindustry, epic, wellfound.
- **Static plugins**: New plugin family under `adapters/plugins/static/` with per-studio modules (activision, blizzard, kojima, milestone, remedy, sheet_studios, supercell), shared `_heuristics.py`, and `register.py` updates; static adapter and runtime wiring updated to use plugins.
- **Runtime**: `_runtime.py` and `provider_api.py` small fixes; adapter registry and `static.py` refactor for plugin-based loading.

### Jobs pipeline
- **Core**: Updates in `common.py`, `canonicalize.py`, `parsers.py`, `pipeline.py`, `registry.py`, `reporting.py`, `transport.py`, `jobs_fetcher.py`, `pipeline_io.py`; Python version guard tweaks.
- **Source discovery**: `source_discovery.py` improvements; tests and fixture snapshot updates for discovery and jobs fetcher.

### Frontend & docs
- **Jobs UI**: Minor fixes in `feed.js` and `runtime.js`.
- **Docs**: `adapter-plugin-inventory.md` updated with source loaders map and static plugin inventory; `LOCAL_SETUP.md` updates.
- **Scripts**: New `scripts/benchmark_discovery_probe.py` for discovery benchmarking.

### Tests
- `test_jobs_fetcher.py` and `test_source_discovery.py` extended; `source_discovery_report_snapshot.json` updated.

---

## Suggested commit message (if squashing)

```
Adapter plugin rollout: community package, static plugin family, discovery and pipeline updates

- Replace community.py with community/ package; extract Google Sheets to community/google_sheets.py
- Add static plugin family (activision, blizzard, kojima, milestone, remedy, sheet_studios, supercell) with _heuristics and register
- Update source_discovery, jobs pipeline (common, reporting, transport, jobs_fetcher, pipeline_io), and Python version guard
- Frontend: feed.js and runtime.js tweaks
- Docs: adapter-plugin-inventory and LOCAL_SETUP
- Add benchmark_discovery_probe.py; extend jobs_fetcher and source_discovery tests
```

---

## Before you push

- **Data files**: The repo tracks some `data/*` files. Consider leaving large runtime artifacts (e.g. `jobs-unified*.json`, `jobs-lifecycle-state.json`, `jobs-fetch-report.json`) unstaged so the commit stays focused on code and config. To commit only source, docs, tests, and scripts:

  ```powershell
  git add LOCAL_SETUP.md docs/ frontend/ src/ tests/ scripts/ .vscode/
  git status   # then add any specific data/config you want
  ```

- **Already committed**: You are ahead of `origin/main` by 1 commit (removal of `requirements-desktop.txt`). This PR can include that commit or you can push it separately.
