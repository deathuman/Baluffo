# Repository Health Completed Tasks

> Historical repo-health completion record preserved for archive/reference use. Start with [`../../repo-health-action-tracker.md`](../../repo-health-action-tracker.md) for current priorities before using this record.

This page stores completed items moved out of the active repository-health tracker so [`../../repo-health-action-tracker.md`](../../repo-health-action-tracker.md) can stay focused on work that still needs attention.

## Completed P0 Items

1. **Completed: finish the broad mypy sweep.**
   This consolidated the staged rollout across bridge/admin live-payload and JSON helpers, source/discovery audit lanes, jobs runtime/report contract helpers, static adapter/detail and listing clusters, source-sync and desktop app boundaries, and the final full-tree enforcement. The sweep reduced the broad audit from `835` errors to `0`, and `mypy.ini` now enforces the full `src/` tree instead of a staged file list without changing runtime behavior or persisted payload contracts.
   **Done when:** complete.

2. **Completed: add a Python dependency lock strategy for reproducible builds.**
   `requirements-lock.txt` is now the canonical Python lock artifact, and CI/release install surfaces consume it instead of floating `requirements.txt`.
   **Done when:** complete.

3. **Completed: stop generated-file newline churn in `data/source-approval-state.json`.**
   `save_json_atomic` now writes newline-terminated JSON, and targeted regression coverage protects the writer behavior used by the approval-state file.
   **Done when:** complete.

4. **Completed: resolve GitHub Dependabot high-severity vulnerabilities.**
   The Scrapy remediation updated the direct dependency to the latest released `Scrapy==2.15.0`, raised the `scrapy-playwright` source requirement floor to `>=0.0.46`, and regenerated `requirements-lock.txt`. The remediation was validated with the dependency import/version check, `python -m pip check`, focused Scrapy/runtime tests, refactor and lint gates, and a forced orchestrator build; the remaining `pip-audit` Scrapy advisory had no fixed version and affected an unused Scrapy file-download storage path.
   **Done when:** complete.

## Completed P1 Items

1. **Completed: reduce JS hygiene noise before the next broad frontend refactor.**
   ESLint now reports `0 warnings, 0 errors`, and `knip` reports `0` unused JS exports. This pass removed production discovery/controller warning sources, pruned unused test imports, removed dead loading-render code, and narrowed internal-only frontend/test helper exports without changing runtime or UI behavior.
   **Done when:** complete.

2. **Completed: raise coverage in the weakest validated modules.**
   Added focused coverage shards for `src/source_sync_crypto.py`, `src/source_discovery/stage_control.py`, `src/source_discovery/probe.py`, and `src/source_discovery/url_patches.py` without changing runtime behavior. The targeted coverage lane now reports `97%`, `100%`, `93%`, and `99%` respectively, clearing the agreed `80%` per-module threshold.
   **Done when:** complete.
