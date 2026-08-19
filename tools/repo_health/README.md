# Repo Health

Tools for analyzing repository readiness, maturity, and refactorability.

## Structure

```
repo_health/
  bin/
    analyze_repo.py         # Config-driven maturity analysis
    analyze_refactorability.py  # AI-oriented refactorability scoring

  profiles/baluffo/
    readiness/
      criteria.md           # Readiness evaluation pillars
      maturity-criteria.yaml # Config-driven maturity criteria
      maturity-levels.md    # Maturity status levels

    refactorability/
      criteria.md           # Refactorability evaluation criteria
      levels.md             # Refactorability levels
```

## Usage

```bash
# Analyze repository maturity
python tools/repo_health/bin/analyze_repo.py

# Analyze refactorability
python tools/repo_health/bin/analyze_refactorability.py

# Run repository policy guardrails, including fixture references
npm run lint:repo-guardrails

# Generate an optional AI-coder orientation artifact
python tools/repo_health/generate_system_map.py --output .tmp/system-map.json

# With verification (slower, runs actual commands)
python tools/repo_health/bin/analyze_repo.py --verify

# Output to JSON
python tools/repo_health/bin/analyze_repo.py -o maturity.json
```

## Background

- **Readiness/Maturity**: How ready the repo is for AI work - testing, docs, build, security
- **Refactorability**: How safely AI can modify the repo without causing issues - boundary isolation, hotspots, contracts
- **Repo guardrails**: Checked-in repository policy checks for docs, workflow, compatibility surfaces, frontend structure, repo-root layout, test shape, fixture references, test line budgets, and release artifacts. Test-shape guardrails also block generated frontend unit aggregators now that Node discovers `tests/frontend/unit/*.test.mjs` directly. Release guardrails fail fast when a locally-built `dist/` artifact (ship bundle `current.txt`, current-version portable ZIP, desktop update manifest) embeds a version other than `APP_VERSION`.
- **System map**: Optional generated JSON for broad AI orientation. It summarizes page surfaces, task flows, bridge routes from `bridge_route_inventory.py`, runtime evidence files, and high-risk areas. Treat it as advisory; canonical guidance stays in `AGENTS.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, source, and tests.
