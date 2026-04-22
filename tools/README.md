# Tools

This directory contains standalone analysis and measurement tools that are separate from:
- Build/orchestration scripts in `scripts/`
- Product code in `src/`
- Frontend code in `frontend/`

## Directory Structure

```
tools/
  mcp/               # Optional local MCP servers for developer workflows
  repo_health/       # Repository readiness and refactorability analysis
  measurements/      # Pipeline and source performance measurement
  adhoc/             # One-off scripts and experiments
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

### `adhoc/`
Temporary or exploratory scripts not yet promoted to stable workflows.
These may be one-off comparisons, experiments, or test utilities.

### `mcp/`
Optional local MCP servers for editor-assisted workflows.
The Playwright server entrypoint is `tools/mcp/playwright-server.cjs` and setup notes live in `tools/mcp/README.md`.

### `reports/`
Generated outputs (JSON reports, measurement results). This directory
is typically gitignored - do not commit generated files here.

## Quick Reference

| Task | Tool Location |
|------|---------------|
| Analyze repo maturity | `tools/repo_health/bin/analyze_repo.py` |
| Analyze refactorability | `tools/repo_health/bin/analyze_refactorability.py` |
| Run the local Playwright MCP server | `node tools/mcp/playwright-server.cjs` |
| Summarize latest discovery/fetch run | `python tools/measurements/pipeline/latest_run_report.py` |
| Measure pipeline discovery gains | `tools/measurements/pipeline/job_discovery_increment_measurement.py` |
| Monitor social sources | `tools/measurements/sources/social_sources_monitoring.py` |
| Optimize social sources config | `tools/measurements/sources/social_sources_optimization.py` |
