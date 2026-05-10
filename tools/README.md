# Tools

This directory contains standalone analysis and measurement tools that are separate from:
- Build/orchestration scripts in `scripts/`
- Product code in `src/`
- Frontend code in `frontend/`

## Directory Structure

```
tools/
  mcp/               # Required Serena and Basic Memory setup plus optional local MCP tooling
  repo_health/       # Repository readiness and refactorability analysis
  measurements/      # Pipeline and source performance measurement
  reports/           # Generated outputs (not tracked in git)
```

## Categories

### `repo_health/`
Tools that assess the repository itself:
- **bin/** - Executable analyzers
- **profiles/** - Criteria definitions and maturity levels

Run with: `python tools/repo_health/bin/analyze_repo.py` or `python tools/repo_health/bin/analyze_refactorability.py`

### `measurements/`
Tools that run Baluffo or its pipeline and measure behavior:
- **pipeline/** - Pipeline-level measurements (e.g., job discovery gains)
- **sources/** - Source performance monitoring and optimization

### `mcp/`
Repo AI tooling guidance plus local MCP servers for editor-assisted workflows.
Use `tools/mcp/INDEX.md` as the MCP tooling entrypoint.
Use `tools/mcp/SERENA.md` for the required Serena setup used by Codex CLI and OpenCode.
Use `tools/mcp/BASIC_MEMORY.md` for the required Basic Memory continuity setup used by Codex CLI and OpenCode.
The Playwright server entrypoint is `tools/mcp/playwright-server.cjs` and setup notes live in `tools/mcp/PLAYWRIGHT.md`.

### `reports/`
Generated outputs (JSON reports, measurement results). This directory
is typically gitignored - do not commit generated files here.

## Quick Reference

| Task | Tool Location |
|------|---------------|
| Browse MCP tooling docs | `tools/mcp/INDEX.md` |
| Set up required Serena MCP tooling | `tools/mcp/SERENA.md` |
| Set up required Basic Memory continuity | `tools/mcp/BASIC_MEMORY.md` |
| Analyze repo maturity | `tools/repo_health/bin/analyze_repo.py` |
| Analyze refactorability | `tools/repo_health/bin/analyze_refactorability.py` |
| Run the optional Playwright MCP server | `tools/mcp/PLAYWRIGHT.md` / `node tools/mcp/playwright-server.cjs` |
| Summarize latest discovery/fetch run | `python tools/measurements/pipeline/latest_run_report.py` |
| Measure pipeline discovery gains | `tools/measurements/pipeline/job_discovery_increment_measurement.py` |
| Monitor social sources | `tools/measurements/sources/social_sources_monitoring.py` |
| Optimize social sources config | `tools/measurements/sources/social_sources_optimization.py` |
