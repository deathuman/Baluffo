# Maturity Levels

This document defines the 5 maturity levels for AI agent readiness assessment.

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
- Templates for issues and PRs

## Scoring Formula

```
Pillar Score = (Criteria Met / Total Criteria) * 100
Overall Score = Sum of Pillar Scores / Number of Pillars
Maturity Level = Based on Overall Score
```

## Pillar Weights

All pillars are weighted equally (1/9 each) in the overall score.
