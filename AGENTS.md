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

## Dependency Guardrail

Do not add new libraries, packages, or framework dependencies without explicit user approval.

This applies to:
- Python dependencies in `requirements*.txt`, packaging specs, or implicit runtime imports
- Node dependencies in `package.json`, lockfiles, or build tooling
- bundled third-party assets or vendored libraries added to the repo

Default behavior:
- prefer existing standard-library, repo-local, Scrapy, and already-installed project tooling first
- if a new dependency would be the cleanest path, stop and ask the user before adding it
- do not silently add a dependency just to simplify parsing, scraping, packaging, testing, or UI work

When touching code that currently imports an unapproved dependency, prefer removing or replacing it with existing project tooling unless the user explicitly asks to keep or add that dependency.

## Packaging and Build Isolation

Treat packaging and ship-build work as isolated delivery code, not as a shortcut into the full runtime graph.

Defaults:
- Build and ship scripts under `scripts/` must not import broad runtime modules just to read constants, defaults, or registry paths.
- Prefer direct data/config file reads or narrow leaf modules over composition-root imports.
- When editing portable/ship packaging paths, verify with the narrowest release-critical command first:
  - `python scripts/build_portable_exe.py`
  - `npm run build:portable-exe`
- Treat `Path` objects as unsafe in manifests or JSON/report output unless they are explicitly serialized to strings first.
- Do not add convenience dependencies for packaging; prefer the standard library and existing project tooling.

## No Broad Composition-Root Imports

Do not import composition-root modules from narrow helpers, plugins, or build code unless the task genuinely needs the whole runtime graph.

Defaults:
- Avoid importing `src.jobs`, `src.admin_bridge`, or other top-level re-export modules from build scripts, parsers, plugins, or narrow bridge helpers.
- Prefer leaf modules such as `src/jobs/common/*`, `src/bridge/*`, `src/core/*`, or direct data-file paths.
- If a change touches an `__init__.py` that re-exports a large module tree, promote the task to high risk and verify the affected build/runtime path explicitly.

## Bridge and Route Compatibility

Bridge changes must be treated as API compatibility work, not local refactors.

Defaults:
- Route handlers should call bridge/service methods with explicit keyword arguments when signatures are evolving.
- When changing a bridge or service signature, search both route call sites and frontend payload builders before editing.
- If a change affects long-running task launch or completion flows, verify all three together:
  - task starts
  - busy state locks controls
  - log polling or attachment still works
- Do not assume a running local bridge reflects code changes; if behavior looks stale after a fix, restart the bridge before concluding the fix failed.

## Admin UI Task Controls

Any new admin task control must integrate with the shared task lifecycle, not just fire a request.

Defaults:
- New admin run buttons must participate in shared busy-state disable/restore behavior.
- New run presets or actions must document their bridge payload wiring and fallback behavior.
- Any new admin task control should land with one focused frontend unit test or bridge payload test.
- If adding a new preset such as `uncapped`, keep it distinct from existing presets rather than overloading a nearby action.

## Release and Tag Discipline

Release and tag work is always high risk.

Defaults:
- Never move, recreate, or force-push a release tag unless the user explicitly asks for tag retargeting.
- Workflow fixes after a release tag should default to a new version/tag, not silent reuse of the existing tag.
- Before suggesting a re-release, determine whether the problem is:
  - tag contents
  - workflow state on `main`
  - release artifact generation
- For release/build failures, inspect the actual workflow log or failing build path before generalizing from earlier local success.

## Parser and Scraping Guardrail

Prefer the scraping stack already in the repo over parser-by-parser reinvention or new dependencies.

Defaults:
- Prefer existing Scrapy/parsel/repo-local HTML helpers over adding or reintroducing parsing libraries.
- For static adapters, use the plugin architecture documented in `docs/architecture-ai-map.md`.
- Keep site-specific extraction logic local to the plugin or shared parser helpers, not in unrelated build/runtime code.
- If a parser fallback is added, ensure it does not silently widen packaging/runtime dependencies.

## Data-Quality and Public Output Guardrail

Changes that affect public jobs data must preserve user-facing cleanliness, not just parser success.

Defaults:
- Any change affecting public job text, locations, dropdown values, or report payloads must preserve:
  - sanitized public text
  - semantic location validity
  - filter-safe frontend values
- If touching normalization/canonicalization or adapters that emit `title`, `city`, `country`, or similar public fields, run at least one targeted contamination or location-quality check.

## Windows-Specific Safety Notes

Defaults:
- Be careful with Windows-only artifacts like phantom `nul` in `git status`; never include them in commits.
- Prefer repo-relative strings over platform-specific `Path` serialization in output contracts.
- For commands and paths in docs or scripts, avoid assumptions that hold only on POSIX shells.

## AI Tooling Palette (Efficiency Palette)

To minimize token consumption and exploration overhead, AI agents MUST favor these tools and strategies:

### 1. Zero-Waste Search & Exploration
- **Mandatory `ripgrep`**: Use `rg` for all cross-file queries. Do NOT use recursive `list_dir` or sequential `grep` on large directories.
- **Manifest-First**: Check `_out/LATEST_MANIFEST.json` and `task.md` BEFORE exploring the filesystem. This HUB provides a machine-readable summary of the last build, including hash state and artifact locations.
- **Fixed Targets**: Use `_out/latest/` to inspect recent build logs, screenshots, or packaged artifacts without searching timestamped folders.
- **Registry Over Code**: Use `frontend/shared/ui/selectors.js` as the source of truth for ALL UI element handles. Do NOT guess class names or IDs.

### 2. High-Density Communication
- **Batched Edits**: Always use `multi_replace_file_content` for multi-point changes to reduce round-trips.
- **GitHub Intelligence**: Use `gh` CLI to check workflow statuses, PR comments, and release assets instead of guessing or performing redundant local builds.

### 3. Context-First Navigation
- **Architecture Map**: Reference `docs/architecture-ai-map.md` to understand system boundaries before suggesting structural changes.
- **Data Contracts**: Refer to `docs/DATA_CONTRACT.md` for `data-ui` attribute rules, JSON shapes, and the Manifest HUD schema.
- **UI Registry**: Refer to `frontend/shared/ui/selectors.js` before adding new UI interaction logic.
- **Test layout and runs:** Python tests are 100% pytest. Use `docs/testing.md` for test layout, fixtures, and targeted run commands. Use `workspace_tmpdir` from `tests/helpers/temp_paths.py` for temp dirs; admin tests use `admin_bridge_ops_root` from `tests/admin/conftest.py`. The "Fast verification matrix" in `docs/architecture-ai-map.md` (section 5) lists the fastest command per change area.

## Command Cheat Sheet

Use these standard commands for routine development and verification. All commands automatically run version/env checks.

| Target | Command | Description |
| :--- | :--- | :--- |
| **Development** | `npm run dev:bridge` | Start the local Admin Bridge (Python logic) |
| | `npm run dev:pipeline` | Execute the core Job Pipeline (ingestion/mapping) |
| **Testing** | `npm run test` | Run all Unit (node) and Smoke (Playwright) tests |
| | `npm run test:unit` | Fast local node-based unit tests |
| | `npm run test:smoke` | Playwright integration smoke tests |
| | `npm run test:py` | Standard Python `pytest` suite |
| **Build & Prep** | `npm run build` | Build the full Baluffo Ship bundle (orchestrated) |
| | `npm run build:portable-exe` | Compile the Windows portable .exe (orchestrated) |
| **Verification** | `npm run verify` | Full verification: Rebuild + Smoke Bundle + Smoke EXE |

> [!TIP]
> **Check the HUD FIRST**: Before starting any work, read `_out/LATEST_MANIFEST.json`. It provides a machine-readable snapshot of the current build health and checksum state, saving hundreds of exploration tokens.
