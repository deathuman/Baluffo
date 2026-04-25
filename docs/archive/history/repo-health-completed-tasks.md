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

3. **Completed: clear the remaining ESLint boolean-cast noise.**
   Removed the redundant `Boolean(...)` wrappers in `frontend/admin/domain/sources.js`, added regression coverage for the weak-signal approval branch, and restored a clean `npm run lint:js` run without changing the admin approval ladder behavior.
   **Done when:** complete.

4. **Completed: add maintained CI status badges to README.**
   `README.md` now includes workflow status badges for `tests`, `lint`, and `build-portable-exe`, closing the P1-10 repo-health gap around CI visibility without changing runtime behavior.
   **Done when:** complete.

5. **Completed: add Python dependency security scanning to CI.**
   `npm run security:python` now runs `pip-audit` against `requirements-lock.txt`, writes the JSON report under `.tmp/security/`, and the CI lint workflow runs it after the pre-commit guardrails. Known non-actionable findings must be recorded in `tools/security/pip-audit-allowlist.json` with advisory id, package, reason, owner, and review date; malformed or expired allowlist entries fail the gate.
   **Done when:** complete.

6. **Completed: move structural guardrails out of pytest and frontend unit collection.**
   Repository-policy guardrails now run through `npm run lint:repo-guardrails` and `tools/repo_health/repo_guardrails.py`, with grouped checks for docs, workflow, compatibility surfaces, frontend structure, repo-root layout, test shape, and test line budgets. The old pytest and frontend unit guard files were removed from collection, `scripts/precommit_gate.py` runs the repo-health guardrails before the complexity baseline, and `scripts/refactor_changed_gate.py` routes docs/workflow/compatibility checks through the repo-health entrypoint.
   **Done when:** complete.

## Completed P2 Items

1. **Completed: make Python import sorting explicit in the gate.**
   Confirmed `ruff.toml` selects both `F` and `I`, CI routes through `npm run lint:precommit:ci`, and the pre-commit configuration runs `ruff-check`. A workflow regression test now protects that wiring so import sorting cannot silently drop out of the enforced gate.
   **Done when:** complete.

2. **Completed: add a source complexity baseline gate.**
   Ruff `C901` now runs through `scripts/check_complexity_baseline.py` in the CI pre-commit lane with an explicit `src/` baseline, rule, threshold, and Ruff version. The gate allows current complexity hotspots to improve but fails new or worsened source hotspots without adding a new complexity-specific dependency.
   **Done when:** complete.

3. **Completed: standardize structured diagnostics for support and ops.**
   P2-14 closed as a phased structured diagnostics effort. Phase 1 added retained admin bridge diagnostic events at `data/admin-bridge-events.jsonl` with bounded retention and redaction while preserving console logs. Phase 2 hardened `/ops/task-live/<taskType>` so fetch, discovery, and sync `recentEvents` share the versioned `src/shared/live_task.py` envelope. Phase 3 versioned `data/desktop-startup-metrics.jsonl` rows with support categories while preserving existing startup metric event names, bridge routes, `payload` / `fields` compatibility, and packaged smoke startup snapshots.
   **Done when:** complete.
