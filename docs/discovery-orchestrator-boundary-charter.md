# Discovery Orchestrator Boundary Charter

## Title

Discovery orchestrator boundary cleanup

## Goal

Reduce `src/source_discovery/orchestrator.py` from a single closure-heavy execution flow into focused helper modules while preserving the public discovery API, bridge-owned lifecycle/report behavior, and the current discovery report, candidate queue, and M5 backlog contracts.

## Target Boundary

- Primary subsystem: source discovery orchestration
- Entry file(s): `src/source_discovery/orchestrator.py`, `src/source_discovery.py`
- Ownership boundary being clarified: generation and pre-probe filtering, probe plus URL-patch recovery, and final queue/report persistence move into focused helper modules behind the root orchestrator surface
- What becomes easier after this change: future discovery edits can land in phase-owned modules without re-learning the full run flow or accidentally breaking the root monkeypatch surface

## Why Now

- Current pain: `run_discovery(...)` had become the main remaining discovery monolith and mixed stage fan-out, dedupe, probe flow, recovery, persistence, and progress reporting in one function
- Why this is worth doing now: discovery already has package boundaries elsewhere, so splitting the orchestrator removes a major AI/human navigation hotspot without changing behavior
- Why this should stay narrow: this pass is structural only and does not reshard discovery tests or change discovery contracts

## In Scope

- `src/source_discovery/orchestrator.py` as the stable root run flow and test patch surface
- New helper modules for runtime/state, generation, probe/recovery, and final persistence/report assembly
- Routing/docs updates that point future work at the new helper modules

## Out of Scope

- Discovery CLI flag changes
- Discovery report/candidates/backlog schema changes
- Test-suite sharding for `tests/source_discovery/test_run_discovery_flow.py`

## Stability Impact

- Runtime behavior touched: discovery stage execution order, dedupe/probe flow, URL-patch recovery, queue balancing, auto-approval, and report persistence
- Persisted state touched: `data/source-discovery-report.json`, `data/source-discovery-candidates.json`, `data/m5-strategic-backlog.json`, and existing pending/active/rejected registry writes
- Packaging or desktop behavior touched: none directly, but bridge-triggered discovery lifecycle ownership must remain unchanged
- Compatibility concern: `src.source_discovery.orchestrator` is also a monkeypatch surface for tests; root module names such as stage generators, probe helpers, patch helpers, and `save_json_atomic` must remain patchable there
- Rollback trigger: any drift in discovery report shape, bridge `runId` ownership, URL-patch retry semantics, or test monkeypatch targets

## AI Accessibility Impact

- Source-of-truth file after refactor: `src/source_discovery/orchestrator.py` for public flow, with implementation routed to `orchestrator_runtime.py`, `orchestrator_generation.py`, `orchestrator_probe.py`, and `orchestrator_finalize.py`
- Expected search path for future edits: AI guide -> architecture map -> `src/source_discovery/orchestrator*.py` or the relevant discovery leaf module
- Docs or registry to update: `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, `docs/DATA_CONTRACT.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: helper modules resolve patch-sensitive calls through the root orchestrator module

## Implementation Shape

- Modules to shrink, split, or simplify: `orchestrator.py` shrinks; new helper modules own runtime state/progress, generation, probe/recovery, and final persistence/report work
- Interfaces or contracts to formalize: stable root `run_discovery(...)`, `parse_args(...)`, `main(...)`, bridge report priming, and existing discovery output files
- Existing abstractions to reuse: `reporting.py`, `runtime_metrics.py`, `stage_control.py`, discovery stage leaf modules, and current queue/probe helpers
- New abstraction to avoid unless proven necessary: new public service classes or alternate discovery entrypoints

## Verification

- Cheapest syntax/check step: `python -m py_compile src/source_discovery/orchestrator.py src/source_discovery/orchestrator_*.py`
- Cheapest focused test step: `python -m pytest tests/source_discovery/ -q`
- Broader verification required only if: bridge report ownership, route-level discovery start flow, or release-doc routing changes

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

- `src/source_discovery.py` remains the thin CLI entrypoint
- `src/source_discovery.__init__` exports remain unchanged in this pass
- If repo docs and Serena memory ever diverge, repo docs stay canonical
