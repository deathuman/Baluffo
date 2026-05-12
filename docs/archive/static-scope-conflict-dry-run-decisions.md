# Static Scope Conflict Dry-Run Decisions

> - **Status:** Archived operator decision record
> - **Use this when:** reviewing historical static scope conflict dry-run decisions and the Arrowhead apply-safe exercise
> - **Canonical for:** historical operator intent and dry-run/apply-safe evidence only
> - **Not canonical for:** current registry state, runtime source-policy behavior, persisted-job behavior, source-sync behavior, or future Admin apply actions
> - **Then inspect:** [`../source-policy-runbook.md`](../source-policy-runbook.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and current source-policy reports if fresh behavior matters
> - **Last updated:** 2026-05-12

This record documents operator review intent only. It does not authorize registry edits, source suppression, timeout tuning, source-sync changes, fetch behavior changes, persisted-job changes, or Admin apply actions.

## Summary

The generic source-policy soak report now produces `sections.staticRegistryScopeConflicts` and dry-run-only `patchProposals` for `shadowed_cross_host` rows. The latest local evidence found ten dry-run patch proposals and kept all other conflicts review-only.

Evidence source:

- `_out/source-policy-soak/source-policy-soak-report.json`
- `_out/source-policy-soak/source-policy-soak-report.md`
- Command: `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out/source-policy-soak --format both`
- Latest run status: generated successfully

## Latest Scope Conflict Summary

- Static rows scanned: `2190`
- Total scope conflicts: `99`
- `shadowed_cross_host`: `10`
- `needs_split_source`: `14`
- `manual_scope_review`: `39`
- `zero_kept_review`: `36`
- Dry-run patch proposals: `10`

Guardrails confirmed:

- `destructiveActionAllowed=false`
- `requiresExplicitAdminAction=true`
- `behaviorChangeAllowed=false`
- `applyAllowed=false` on patch proposals

## Proposed Future Apply-Safe Candidate

- Source: `Arrowhead Game Studios (GameDevMap)`
- Proposed action: `narrow_static_scope`
- Classification: `shadowed_cross_host`
- Remove page: `https://jobs.arrowheadgamestudios.com/`
- Remove page: `https://jobs.arrowheadgamestudios.com`
- Keep pages:
  - `https://arrowheadgamestudios.com`
- Decision status: `proposed`
- Required next implementation: `validate whether apply path changed conflict surface`

Rationale:

The off-listing jobs host entries on Arrowhead are covered by another active registry row, while the Arrowhead listing page remains the likely preferred root. This is a conservative dry-run candidate for `shadowed_cross_host`, and its command path has now been exercised to validate the write surface.

## Applied Locally

- Source: `Arrowhead Game Studios (GameDevMap)`
- Applied command: `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out/source-policy-soak --format both --apply-static-scope-proposal "static:listing_url:https://arrowheadgamestudios.com"`
- Audit artifact: `_out/source-policy-soak/static-scope-apply-audit.json`
- Removed page: `https://jobs.arrowheadgamestudios.com/`
- Removed page: `https://jobs.arrowheadgamestudios.com`
- Kept pages:
  - `https://arrowheadgamestudios.com`
- Post-apply verification: rerun showed `conflictCount=99`, `patchProposalCount=10`.

The apply-safe run updated only the local runtime active registry row and preserved `id`, `listing_url`, `careersUrl`, and unrelated metadata. Seed defaults were not edited.

## Review-Only Rows

- Super Lucky: absent from the generic conflicts after the seed narrowing.
- Koei: remains `zero_kept_review`; keep decision-gated and do not include in automatic cleanup.
- Remaining `zero_kept_review` rows: review-only until separate evidence shows they should be split, narrowed, or left as-is.
- New evidence also shows non-zero `needs_split_source` and `manual_scope_review` buckets.

### Immediate Next Closure Step

1. Keep running scoped soak reports without changing source-policy behavior.
2. Use `needs_split_source` and `manual_scope_review` rows as explicit decision checkpoints.
3. Before any further apply-safe run, add a decision trace entry in `source-decision-log-template.md` with source ID, keep/remove list rationale, and review-state state.

## Next Implementation Boundary

The guarded CLI apply-safe mechanism has now been exercised once for Arrowhead. Any future apply-safe expansion should preserve review/audit evidence, require an explicit operator action, and avoid applying to `zero_kept_review`, `needs_split_source`, or `manual_scope_review` rows.
