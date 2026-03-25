# Readiness Report Criteria

This document defines the 9 evaluation pillars and their criteria for AI agent readiness assessment.

## Pillar 1: Style & Validation (10 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 1.1 | Has linting config | Project has linting configuration (e.g., .eslintrc, pyproject.toml with ruff) |
| 1.2 | Linting passes | Code passes linting checks without errors |
| 1.3 | Has type hints | Python/JavaScript code uses type annotations where appropriate |
| 1.4 | Has formatting config | Project has code formatting configuration (e.g., prettier, black) |
| 1.5 | Formatting passes | Code passes formatting checks |

## Pillar 2: Build System (8 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 2.1 | Has build script | Project has build scripts (Makefile, package.json scripts, etc.) |
| 2.2 | Build is reproducible | Build can be reproduced consistently |
| 2.3 | Has dependency lock | Project uses lockfiles (package-lock.json, requirements.lock, etc.) |
| 2.4 | Build completes | Build script completes without errors |

## Pillar 3: Testing (12 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 3.1 | Has test suite | Project has automated tests |
| 3.2 | Tests are discoverable | Tests are in standard locations and discoverable |
| 3.3 | Tests pass | All tests pass |
| 3.4 | Has test coverage | Project tracks test coverage |
| 3.5 | Has CI test config | CI runs tests on pull requests |

## Pillar 4: Documentation (12 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 4.1 | Has README | Project has a README file |
| 4.2 | README is complete | README includes installation, usage, and contribution guidelines |
| 4.3 | Has AGENTS.md | Project has AGENTS.md for AI guidance |
| 4.4 | Has CONTRIBUTING.md | Project has contribution guidelines |
| 4.5 | Has API docs | Project has API documentation |
| 4.6 | Has architecture docs | Project has architecture or design documentation |

## Pillar 5: Dev Environment (8 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 5.1 | Has setup script | Project has setup/installation script |
| 5.2 | Setup is documented | Setup process is documented |
| 5.3 | Has dev server | Project can run in development mode |
| 5.4 | Dev server works | Development server starts without errors |

## Pillar 6: Debugging (8 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 6.1 | Has logging | Project has structured logging |
| 6.2 | Has error handling | Project has proper error handling |
| 6.3 | Has debug mode | Project has debug mode or verbose logging option |
| 6.4 | Has troubleshooting docs | Project has troubleshooting or FAQ documentation |

## Pillar 7: Versioning & Releases (8 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 7.1 | Uses versioning | Project uses semantic versioning |
| 7.2 | Has changelog | Project maintains a changelog |
| 7.3 | Has release process | Project has documented release process |
| 7.4 | Has CI/CD | Project has CI/CD pipeline |

## Pillar 8: Security & Reliability (8 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 8.1 | Has security policy | Project has security policy or guidelines |
| 8.2 | Dependencies are scanned | Dependencies are checked for vulnerabilities |
| 8.3 | Has .gitignore | Project has appropriate .gitignore |
| 8.4 | No secrets in code | No hardcoded secrets or API keys in source |

## Pillar 9: Onboarding (7 criteria)

| # | Criterion | Description |
|---|-----------|-------------|
| 9.1 | Has examples | Project includes example usage |
| 9.2 | Has templates | Project has templates for new contributions |
| 9.3 | Has issue templates | Project has issue templates |
| 9.4 | Has PR templates | Project has pull request templates |

---

**Total: 81 criteria across 9 pillars**
