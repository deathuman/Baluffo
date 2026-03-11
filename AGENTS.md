# Repo-Local Fast Iteration Guardrails

This file defines repo-specific working defaults for Baluffo. These instructions are meant to reduce friction for routine coding while preserving stricter behavior for release-grade and high-risk work.

## Working Lanes

### Routine Changes

Use this as the default lane for:
- UI edits
- small refactors
- targeted bug fixes
- isolated test updates
- narrow changes in one file or one subsystem

Routine defaults:
- Inspect only the named file or the nearest relevant code before editing.
- Prefer direct, local understanding over broad repo exploration.
- Send progress updates only at milestones:
  - when starting exploration
  - before editing files
  - after verification
  - when blocked
- Use best-effort verification by default.
- Prefer one cheap, relevant check for the touched area.
- Skip verification when it is disproportionately expensive relative to the change, but say so in the final response.
- Keep final responses concise and focused on:
  - what changed
  - what was verified
  - any remaining risk or unverified area

Routine work should not default to:
- broad smoke runs
- full build pipelines
- desktop packaging checks
- release validation
- wide repo sweeps before editing

### High-Risk Changes

Use the stricter lane for:
- release tags, release assets, publish workflows, or versioning work
- packaging or installer changes
- sync/auth/config/secrets handling
- desktop runtime or WebView/packaged-app behavior
- destructive file operations
- migrations or compatibility-sensitive changes
- changes that affect multiple subsystems
- changes that modify user data or persisted runtime state

High-risk defaults:
- Inspect broadly enough to understand the affected path end to end.
- Verify the risky path explicitly.
- Be cautious about release state, secrets, packaging assumptions, and cross-machine claims.
- Surface risks, test gaps, and environment assumptions clearly.

## Escalation Rule

Start in `Routine Changes` unless the task clearly falls into `High-Risk Changes`.

Promote a task from `Routine Changes` to `High-Risk Changes` as soon as any of the following becomes true:
- the change touches release tags, assets, or publish workflows
- the change touches bundled config, secrets, or sync credentials
- the change affects desktop packaging or runtime behavior
- the change expands from one subsystem into multiple subsystems
- the change can alter user data, persisted state, or upgrade behavior

Once promoted, keep the stricter lane for the rest of that task unless the remaining work is clearly isolated and low risk.

## Verification Policy

Do not over-verify routine work.

Defaults:
- browser, desktop, and release smoke suites are not the default checks for ordinary app edits
- broad test suites should run only when the changed area is broad enough to justify them
- prefer targeted tests, narrow scripts, or no-op verification over expensive end-to-end checks for routine work

Examples:
- single-file UI tweak: inspect locally, edit, and skip or run one nearby check
- one-subsystem feature: inspect targeted files, edit, and run one relevant verification step
- release or packaging task: switch to high-risk behavior and verify the release-critical path explicitly

## Expected Repo Behavior

Future sessions in this repo should bias toward:
- less exploratory overhead for narrow edits
- fewer intermediary updates
- lighter default verification for routine work
- unchanged rigor for release-grade work

This file changes agent workflow expectations only. It does not change application runtime behavior, schemas, or public APIs.
