# Maturity Levels

This document defines the 5 maturity levels for repository maturity assessment, updated for the Baluffo model with weighted scoring and confidence intervals.

## Level 1: Initial (0-20%)

Project has minimal infrastructure:
- Has a README with basic info
- Has some code structure
- No automated tests
- No CI/CD

## Level 2: Developing (21-40%)

Project has basic infrastructure:
- README with installation instructions
- Has linting or formatting config
- Basic test setup exists
- No CI/CD yet

## Level 3: Functional (41-60%)

Project has working infrastructure:
- Complete README with usage docs
- Linting and formatting configured
- Tests exist and pass
- Basic CI/CD pipeline

## Level 4: Established (61-80%)

Project has mature infrastructure:
- AGENTS.md for AI guidance
- Good test coverage
- CI/CD runs on PRs
- Security basics in place

## Level 5: Optimized (81-100%)

Project has excellent infrastructure:
- Comprehensive documentation for AI agents
- High test coverage with coverage reports
- Full CI/CD with multiple checks
- Security scanning and policies
- Templates for issues and PRs (if applicable)

---

## Scoring Formula

### Four-State Evaluation

Each criterion can be in one of four states:
- **met**: Criterion satisfied with evidence
- **unmet**: Criterion not satisfied
- **not_applicable**: Criterion doesn't apply to this project
- **unknown**: Cannot determine without verification

### Evidence Levels

- **present**: File/config exists
- **enforced**: CI/pre-commit runs it
- **passes**: Command actually succeeds (verify mode only)

### Pillar Score

```
pillar_score = sum(weight of met applicable criteria) / sum(weight of applicable criteria) * 100
```

Where "applicable" = all criteria except those marked `not_applicable`.

### Overall Score

```
overall_score = sum(pillar_score * pillar_weight) / sum(all pillar weights)
```

### Confidence Interval

```
confidence = 1.0 - (unknown_criteria_count / total_criteria_count)
```

- High confidence (90%+): Most criteria can be evaluated statically
- Medium confidence (70-89%): Some criteria require verification
- Low confidence (<70%): Many criteria need verification to determine status

---

## Pillar Weights

All pillars are weighted based on importance to Baluffo's development model:

| Pillar | Weight | Description |
|--------|--------|-------------|
| Testing | 1.2 | High priority - core to reliability |
| Style & Validation | 1.0 | Code quality standards |
| Build System | 1.0 | Release and packaging |
| Documentation | 1.0 | Project clarity |
| Dev Environment | 1.0 | Developer experience |
| Debugging | 0.8 | Diagnostic capabilities |
| Versioning | 0.9 | Release management |
| Security | 0.9 | Safety and trust |
| Onboarding | 0.8 | First-user experience |

---

## Not Applicable Criteria

Certain criteria are automatically marked as `not_applicable` based on project type:
- Issue/PR templates for personal/projects (not community repos)
- Docker-only criteria for local-first projects
- Generic SaaS patterns for self-hosted tools

These are excluded from the denominator when calculating scores.

---

## Suggested Next Steps

The analyzer suggests next steps based on the lowest-scoring pillar:
1. Analyze each pillar's score
2. Identify pillar with lowest score
3. Recommend first item from that pillar's suggestions list

---

## Usage

```bash
# Scan mode (static analysis, fast)
python scripts/analyze_repo.py --scan

# Verify mode (runs commands, slower but more accurate)
python scripts/analyze_repo.py --verify

# Output to file
python scripts/analyze_repo.py -o analysis.json

# Custom config
python scripts/analyze_repo.py -c /path/to/config.yaml
```

---

*Last updated: 2026-03-25*
*Config: tools/repo_health/profiles/baluffo/readiness/maturity-criteria.yaml*