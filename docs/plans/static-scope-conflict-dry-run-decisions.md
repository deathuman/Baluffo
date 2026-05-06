# Static Scope Conflict Dry-Run Decisions

This record documents operator review intent only. It does not authorize registry edits, source suppression, timeout tuning, source-sync changes, fetch behavior changes, persisted-job changes, or Admin apply actions.

## Summary

The generic source-policy soak report now produces `sections.staticRegistryScopeConflicts` and dry-run-only `patchProposals` for `shadowed_cross_host` rows. The latest local evidence found one dry-run patch proposal and kept all other conflicts review-only.

Evidence source:

- `_out/source-policy-soak/source-policy-soak-report.json`
- `_out/source-policy-soak/source-policy-soak-report.md`
- Command: `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out/source-policy-soak --format both`
- Latest run status: generated successfully

## Latest Scope Conflict Summary

- Static rows scanned: `1922`
- Total scope conflicts: `73`
- `shadowed_cross_host`: `1`
- `needs_split_source`: `0`
- `manual_scope_review`: `0`
- `zero_kept_review`: `72`
- Dry-run patch proposals: `1`

Guardrails confirmed:

- `destructiveActionAllowed=false`
- `requiresExplicitAdminAction=true`
- `behaviorChangeAllowed=false`
- `applyAllowed=false` on patch proposals

## Proposed Future Apply-Safe Candidate

- Source: `Capcom (Manual Website)`
- Proposed action: `narrow_static_scope`
- Classification: `shadowed_cross_host`
- Remove page: `https://job.axol.jp/bx/s/capcom_27/entry/agreement`
- Keep pages:
  - `https://www.capcom.co.jp/recruit/`
  - `https://www.capcom.co.jp/recruit/mid-career/index.html`
  - `https://www.capcom.co.jp/recruit/job_culture/`
  - `https://www.capcom.co.jp/recruit/mid-career/`
- Decision status: `proposed`
- Required next implementation: `none in this pass`

Rationale:

The off-listing host `job.axol.jp` is covered by another active registry row, while the Capcom row still has Capcom-owned recruit pages to keep. This is a good first candidate for a future generic apply-safe mode, but the current implementation remains dry-run only.

## Applied Locally

- Source: `Capcom (Manual Website)`
- Applied command: `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out/source-policy-soak --format both --apply-static-scope-proposal "static:listing_url:https://www.capcom.co.jp/recruit/"`
- Audit artifact: `_out/source-policy-soak/static-scope-apply-audit.json`
- Removed page: `https://job.axol.jp/bx/s/capcom_27/entry/agreement`
- Kept pages:
  - `https://www.capcom.co.jp/recruit/`
  - `https://www.capcom.co.jp/recruit/mid-career/index.html`
  - `https://www.capcom.co.jp/recruit/job_culture/`
  - `https://www.capcom.co.jp/recruit/mid-career/`
- Post-apply verification: `conflictCount=0`, `patchProposalCount=0`

The apply-safe run updated only the local runtime active registry row and preserved `id`, `listing_url`, `careersUrl`, and unrelated metadata. Seed defaults were not edited.

## Review-Only Rows

- Super Lucky: absent from the generic conflicts after the seed narrowing.
- Koei: remains `zero_kept_review`; keep decision-gated and do not include in automatic cleanup.
- Remaining `zero_kept_review` rows: review-only until separate evidence shows they should be split, narrowed, or left as-is.

## Next Implementation Boundary

The guarded CLI apply-safe mechanism has now been exercised once for Capcom. Any future apply-safe expansion should preserve review/audit evidence, require an explicit operator action, and avoid applying to `zero_kept_review`, `needs_split_source`, or `manual_scope_review` rows.
