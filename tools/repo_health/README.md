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

# With verification (slower, runs actual commands)
python tools/repo_health/bin/analyze_repo.py --verify

# Output to JSON
python tools/repo_health/bin/analyze_repo.py -o maturity.json
```

## Background

- **Readiness/Maturity**: How ready the repo is for AI work - testing, docs, build, security
- **Refactorability**: How safely AI can modify the repo without causing issues - boundary isolation, hotspots, contracts
- **Repo guardrails**: Checked-in repository policy checks for docs, workflow, compatibility surfaces, frontend structure, repo-root layout, test shape, fixture references, and test line budgets
