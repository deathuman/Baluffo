# Refactorability Criteria

This document defines the criteria for evaluating **AI-oriented refactorability** in Baluffo.

This is **not** a generic code quality score.
It is meant to answer a more specific question:

> How safely and efficiently can an AI coding agent modify this repository without causing collateral damage?

The criteria reflect Baluffo's actual structure and working rules:
- local-first app architecture
- mixed Python + frontend + packaging code
- bridge/API compatibility requirements
- packaging/build isolation requirements
- targeted verification guidance
- canonical docs and registries such as `AGENTS.md`, `docs/testing.md`, `docs/architecture-ai-map.md`, `docs/admin-bridge-api.md`, and `frontend/shared/ui/selectors.js`

## How to interpret this assessment

A high score means:
- boundaries are clear
- contracts are explicit
- refactors stay local
- verification is easy to choose
- high-risk paths are isolated
- duplication/drift risk is low
- AI can find the right files quickly

A low score means:
- changes are hard to localize
- code paths are duplicated or unclear
- verification is hard to route
- subsystem boundaries are fuzzy
- AI is more likely to make shallow or collateral fixes

## Evidence model

Checks should prefer **evidence over assumption**.

Possible result states:
- **met**
- **unmet**
- **unknown**
- **not_applicable**

Checks may be based on:
- static file/directory presence
- command/script discoverability
- import-pattern heuristics
- file-size / hotspot heuristics
- documentation presence and cross-linking
- lightweight code scanning

The tool should avoid claiming certainty where it only has weak signals.

---

# Pillar 1: Boundaries & Imports

Measures whether subsystem boundaries are clear enough for AI to make local edits safely.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 1.1 | No composition-root imports from leaf modules | Narrow helpers, plugins, and build/package code do not import broad runtime composition roots such as top-level aggregator modules |
| 1.2 | Packaging/build paths are isolated | `scripts/` and packaging/ship code avoid unnecessary runtime graph imports |
| 1.3 | Import graph has low circularity | Circular dependencies are absent or limited to known low-risk areas |
| 1.4 | UI selectors are centralized | UI interaction code relies on the shared selector registry instead of guessed selectors |
| 1.5 | Subsystem boundaries are recognizable | Frontend, jobs, bridge, scrapers, ship, and shared core code are separated clearly |

## Strong evidence
- `AGENTS.md` explicitly discourages broad composition-root imports and guessed selectors
- canonical selector registry exists
- leaf modules import leaf modules rather than top-level re-export roots

## Common failure patterns
- build scripts importing broad runtime packages just to read constants
- plugin/helper modules importing `src.jobs` or similarly broad roots
- frontend files hardcoding selectors that should come from the shared registry
- packaging code coupled directly to runtime composition paths

---

# Pillar 2: Contracts & Schemas

Measures how much AI must guess about payloads, data shapes, config precedence, and update/release contracts.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 2.1 | Data contracts are documented | Core data shapes have a canonical reference |
| 2.2 | Bridge/API contracts are documented | Route payloads and bridge behaviors are documented |
| 2.3 | Release/update contracts are documented | Manifest/update contracts have a source of truth |
| 2.4 | Config precedence is documented | Runtime/build/local config rules are explicit |
| 2.5 | Schemas or typed models exist where useful | Important payloads and models are explicit rather than implicit |

## Strong evidence
- `docs/DATA_CONTRACT.md`
- `docs/admin-bridge-api.md`
- `docs/RELEASE.md`
- `mypy.ini`
- model/schema-heavy areas under `src/core/`

## Common failure patterns
- hand-copied payload shapes in several files
- undocumented bridge request/response variants
- config behavior defined by code drift instead of one documented precedence model
- release/update assumptions hidden in scripts only

---

# Pillar 3: Change Locality

Measures whether typical changes stay within one subsystem or spill across unrelated areas.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 3.1 | No oversized hotspot modules | Very large multi-responsibility files are limited |
| 3.2 | Feature changes stay local | Typical edits do not require touching many unrelated files |
| 3.3 | UI state and rendering are reasonably separated | Frontend files do not collapse too many roles into one module |
| 3.4 | Bridge change paths are clear | Route, service, payload, and test locations are easy to identify |
| 3.5 | Packaging changes stay in packaging code | Ship/build changes do not spill into unrelated runtime modules |

## Strong evidence
- named subsystem directories in README/project map
- targeted "where to change" guidance in docs
- smaller files with narrow import surfaces

## Common failure patterns
- giant `runtime.js` / `domain.js` / orchestrator files
- UI, state, transport, and rendering mixed in one file
- small packaging edits requiring unrelated runtime edits
- bridge changes that require repo-wide search to find affected code

---

# Pillar 4: Testability & Verification Routing

Measures whether AI can choose the correct verification step quickly and cheaply.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 4.1 | Targeted tests exist for major subsystems | Major code areas have at least one narrow verification path |
| 4.2 | Tests are mapped to source areas | Docs explain which tests cover which subsystems |
| 4.3 | Fast verification guidance exists | Cheap checks for common changes are documented |
| 4.4 | High-risk paths have explicit verification | Packaging, release, config, and bridge risks have stronger checks |
| 4.5 | Desktop/runtime smoke path exists | Packaged or runtime-critical flows have smoke coverage |

## Strong evidence
- `docs/testing.md`
- explicit test scripts in `package.json`
- command routing and verification guidance in `AGENTS.md`

## Common failure patterns
- tests exist but no one can tell which test belongs to which subsystem
- only broad smoke/full verify commands exist
- risky paths lack explicit verification guidance
- desktop or packaged flow has no narrow validation path

---

# Pillar 5: Observability & Debuggability

Measures whether an AI can diagnose failures instead of guessing.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 5.1 | Runtime health signals exist | Important services expose readiness/health concepts |
| 5.2 | Failures are diagnosable | Logs, reports, or probes exist for major failure paths |
| 5.3 | Recovery/debug docs exist | Recovery or troubleshooting paths are documented |
| 5.4 | Bridge/task lifecycle is observable | Long-running task behavior is inspectable |
| 5.5 | Release/runtime diagnostics exist | Packaged app or release regressions have explicit diagnostics |

## Strong evidence
- release/runtime health checks
- support bundle and recovery flows
- probes and packaged smoke paths
- bridge/task lifecycle guidance in docs

## Common failure patterns
- silent failures
- no health/readiness concept
- task lifecycle hidden behind UI only
- packaged/runtime failures without diagnostics or recovery path

---

# Pillar 6: Config Discipline

Measures whether configuration behavior is centralized, predictable, and safe to change.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 6.1 | Config loading is centralized | Runtime config logic is not duplicated in many scripts |
| 6.2 | Default vs local config is separated | Local overrides are distinct from committed defaults |
| 6.3 | Secrets are separated from source | Secrets and signing material are clearly kept out of tracked config |
| 6.4 | Build config does not drift from runtime config | Packaging/build paths do not silently fork config logic |
| 6.5 | Env/CLI/config precedence is explicit | Override order is documented and stable |

## Strong evidence
- documented config precedence
- one canonical config loader or narrow config modules
- local override files documented separately from tracked defaults

## Common failure patterns
- each script resolves config differently
- repeated env parsing and normalization helpers
- build code carrying a private copy of runtime config behavior
- secrets handled through tracked config or ambiguous paths

---

# Pillar 7: Duplication & Drift Risk

Measures whether AI is likely to fix one path while leaving stale copies elsewhere.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 7.1 | No duplicated config resolution logic | Similar config parsing/merging is not repeated across scripts |
| 7.2 | No duplicated payload shapes | Bridge/request/response structures are not hand-copied in many places |
| 7.3 | Shared helpers are actually shared | Similar logic is centralized instead of copied with drift |
| 7.4 | Registry-driven lookups are preferred | Shared registries replace repeated literals and guesses |
| 7.5 | Documentation and code maps agree | Docs reflect the actual command/test/module layout |

## Strong evidence
- canonical registries or selectors
- one contract doc per important payload family
- docs that match actual commands and folders

## Common failure patterns
- repeated config precedence logic
- repeated bridge payload construction in multiple files
- slightly different helper copies for similar jobs
- docs describing an old path while code moved elsewhere

---

# Pillar 8: AI Navigation Affordances

Measures whether AI can find the right files fast enough to make safe edits.

## Criteria

| # | Criterion | Description |
|---|-----------|-------------|
| 8.1 | Architecture map exists | AI can find subsystem boundaries quickly |
| 8.2 | Command map exists | Common build/test/dev commands are easy to find |
| 8.3 | Selector registry exists | UI handles are centralized and documented |
| 8.4 | Test guide exists | Test routing and fixture usage are documented |
| 8.5 | Canonical docs are cross-linked | AGENTS, README, testing, architecture, and release docs reinforce each other |
| 8.6 | Common change types have hints | The repo offers clues for where to edit common tasks |

## Strong evidence
- `AGENTS.md`
- `docs/architecture-ai-map.md`
- `docs/testing.md`
- command cheat sheet
- selector registry
- explicit "use this when…" guidance in docs

## Common failure patterns
- good docs exist but are not linked together
- agents must guess which doc is authoritative
- UI work still requires selector guessing
- no quick map from change type to files/tests/docs

---

# Hotspot Guidance

The tool should flag **hotspots** even when the repo scores decently overall.

Typical hotspot signals:
- unusually large files (>500 LOC warning, >900 LOC high-risk)
- files importing many subsystems (>20 imports)
- files mixing UI/rendering/state/transport
- duplicated config parsing patterns
- bridge or packaging code with broad dependencies
- under-documented subsystem entrypoints
- files named runtime, orchestrator, bridge, pipeline, main

Hotspots matter more than global score when deciding what to refactor next.

---

# Recommended Output Bias

The tool should prefer outputs like:
- top boundary violations
- top hotspot files
- top duplication/drift risks
- best next refactor wins

over abstract percentage-only reporting.

---

*Last updated: 2026-03-25*