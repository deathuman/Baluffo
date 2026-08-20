# Archived Scripts

Scripts moved here are no longer referenced by tests, CI, package.json, or any
`src/` module, and no runbook documents their use. They are preserved for
history and recovery — full content and history remain in git:

```bash
git log --oneline -- scripts/archive/<name>.py
git show <commit>:scripts/archive/<name>.py
```

| Script | Lines | What it did | Last touched |
|--------|-------|-------------|--------------|
| `repro_discovery_spawn.py` | 259 | Reproduce admin-bridge style discovery spawn and detect "stuck on Initializing" (mirrors `TaskLaunchApi.run_background_script` env for discovery) | 2026-03-30 |
| `generate_report.py` | 213 | Generate a readiness report (pillar scores / maturity level) from `analyze_repo.py` JSON analysis | 2026-03-24 |
| `game_studios_sheet_funnel.py` | 144 | Report funnel: game-studios sheet → registry → pipeline outcome counts from data-dir artifacts | 2026-03-24 |
| `refresh_url_patches.py` | 77 | Refresh URL patches from a discovery report using shared discovery helpers | 2026-05-20 |

## Why archived

Audited 2026-08-19 (plan item 13 re-audit): none of these four are imported by
any `src/` or `tests/` module, invoked from `package.json` or GitHub Actions,
enumerated by repo guardrails, or documented in a runbook. They were one-off
developer/debug tools with no current reference path.

## Kept in `scripts/` (not dead)

The re-audit also confirmed the plan's "16 unreferenced scripts" claim is
largely stale — 12 of the 16 are live or documented and stay in place:

- **Live via tests**: `source_policy_soak_report.py` (8 dedicated test files),
  `jobs_yield_gate.py`, `backup_e2e_validate.py`, `audit_diff.py`,
  `check_complexity_baseline.py` (also run by `precommit_gate.py`),
  `audit_json_artifacts.py`, `gitleaks_precommit.py`
- **Live via CI/config**: `serve_static_site.py` (run by all three Playwright
  configs), `location_unknown_country_manifest.py` (pinned by
  `tools/repo_health/workflow_policy.py` and `whitelist.py`)
- **Documented manual tools**: `benchmark_discovery_probe.py` (docs/testing.md),
  `reset_admin_task_lifecycle.py` (admin-bridge-api + DATA_CONTRACT CLI docs),
  `source_audit_sweep.py` (runbook CLI wrapper for the live `src/` module)
