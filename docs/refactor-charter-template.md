# Narrow Refactor Charter

Use this template before starting any structural refactor that is meant to improve stability or AI accessibility.

## Title

Short name for the refactor.

## Goal

State the exact problem being solved in one paragraph.

## Target Boundary

- Primary subsystem:
- Entry file(s):
- Ownership boundary being clarified:
- What becomes easier after this change:

## Why Now

- Current pain:
- Why this is worth doing now:
- Why this should stay narrow:

## In Scope

- Item
- Item
- Item

## Out of Scope

- Item
- Item
- Item

## Stability Impact

- Runtime behavior touched:
- Persisted state touched:
- Packaging or desktop behavior touched:
- Compatibility concern:
- Rollback trigger:

## AI Accessibility Impact

- Source-of-truth file after refactor:
- Expected search path for future edits:
- Docs or registry to update:
- Any transitional seam being kept temporarily:

## Implementation Shape

- Modules to shrink, split, or simplify:
- Interfaces or contracts to formalize:
- Existing abstractions to reuse:
- New abstraction to avoid unless proven necessary:

## Verification

- Cheapest syntax/check step:
- Cheapest focused test step:
- Broader verification required only if:

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

Add any migration sequencing, follow-up tickets, or explicit assumptions here.
