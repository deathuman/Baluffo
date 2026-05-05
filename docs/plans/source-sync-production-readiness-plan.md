# Source Sync Production-Readiness Plan

> **Status:** Final closeout tracker.
> **Objective:** keep only remaining work required to mark production readiness as complete.

## Scope

- In-repo sync runtime, snapshot governance, storage, schema, rate-limit telemetry, tests, and admin runtime payloads are already in place.
- Open work is now GitHub-side governance and rollback policy only.

## Remaining Work (Closeout)

1. **Commit signing + required checks**
   - Enable signed commits for BaluffoSync sync writes.
   - Register `validate-source-sync.yml` as a required status check on `main`.

2. **Repository policy hardening**
   - Enforce repository rules for:
     - linear history
     - signed commit requirement
     - no force-push
     - restricted branch deletion
     - restricted bypass

3. **Environment separation**
   - Define GitHub Environments for staging/production sync paths and required reviewers for production path writes.

4. **Rollback checkpoint**
   - Ensure `last-known-good` tag exists on `main` and points to the last validated write.
   - Keep date-stamped rollback tags for operational hygiene.

5. **Plan closeout bookkeeping**
   - Keep task-progress operational console ownership in [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md).
   - Keep this file until all items in section 2 are complete.

## Close criteria

The plan is complete when all of the following are satisfied in-repo and via GitHub governance:

1. all bot sync commits on `main` are signed (or equivalent verification policy for the selected actor)
2. `validate-source-sync.yml` is required before merge to `main`
3. repository rulesets enforce linear history and prevent force-push
4. `last-known-good` exists on `main` and points to the last validated sync write

## Pending validation commands

```powershell
python -m pytest tests/test_source_sync.py tests/test_source_sync_runtime.py tests/test_source_policy_soak_report.py
npm run lint:precommit
```
