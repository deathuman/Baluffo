# Repo Health

Tools for analyzing repository readiness, maturity, and refactorability.

## Structure

```
repo_health/
  bin/
    analyze_repo.py         # Config-driven maturity analysis
    analyze_refactorability.py  # AI-oriented refactorability scoring
    verify_split_fidelity.py    # Per-definition byte-identity check for module splits

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

# Verify a module split kept every function/class/constant body byte-identical
python tools/repo_health/bin/verify_split_fidelity.py src/bridge/foo.py \
    --leaves src/bridge/foo.py src/bridge/foo_alpha.py src/bridge/foo_beta.py

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
- **Repo guardrails**: Checked-in repository policy checks for docs, workflow, compatibility surfaces, frontend structure, repo-root layout, test shape, fixture references, test line budgets, release artifacts, and the source-registry twin-URL check. The container shipped-code version gate (`container_version_policy.py`) fails when container-affecting commits land after the last version bump without either advancing the version or declaring explicit release-tag intent (a `Release-tag: vX.Y.Z` line, or a `release(vX.Y.Z):` / `chore(release):` subject, naming a version newer than the current one) — so code can never again ship to the Umbrel container channel invisibly under a frozen `umbrel-app.yml` version string. The workflow-syntax gate (`workflow_syntax_policy.py`) runs actionlint over every `.github/workflows/*.yml` in the `workflow` group: the binary is located on PATH (honoring the `~/.local/bin` toolbelt convention) or provisioned as a pinned, checksum-verified release into the gitignored `.tmp/actionlint/` cache on first use, and the gate fails — never silently skips — if it cannot be obtained. This closes the hole opened by the container `paths-ignore` alignment: workflow-only pushes no longer trigger any build that would validate the YAML, so the guardrail guarantees workflow syntax is checked locally and in CI on every commit and push. Test-shape guardrails also block generated frontend unit aggregators now that Node discovers `tests/frontend/unit/*.test.mjs` directly. Release guardrails fail fast when a locally-built `dist/` artifact (ship bundle `current.txt`, current-version portable ZIP, desktop update manifest) embeds a version other than `APP_VERSION`. The **bundle** guardrail keeps the ship-bundle `src/` manifest complete: every top-level `src/*.py|*.json` module must be listed in `scripts/ship_bundle_manifest.py` (shipped via `APP_RUNTIME_SCRIPTS`) or explicitly declared build/container/dev tooling, and every shipped entry must still exist on disk — so a new top-level runtime module forgotten from the manifest fails precommit. The registry guardrail fails when two **active** seed rows share a canonicalized careers URL (www/apex, http/https, trailing-slash, fragment), so twins like the Scopely `join-us` pair are caught before they reach the published registry; currently reviewed collisions live in `data/defaults/source-registry-known-url-collisions.json` and shrink as they are reconciled. The same `canonicalize_careers_url` rule (shared from `src/source_registry_identity`) also gates the **runtime** conflict automations, which raise `url-twin:` cards for active rows that share a canonicalized careers URL across studio families and auto-demote the non-canonical twin to pending on registry load — so duplicates introduced by live discovery demote automatically, with the same allowlist honored on both sides. Two lockstep invariants keep the baseline honest: an **uncovered collision** (a canonical URL registered by 2+ active rows that is missing from the allowlist) fails, and a **stale entry** (a baselined canonical URL now backed by fewer than two active rows) also fails — so a reconciliation that drops a URL back to a single row must prune its baseline entry in the same change, keeping parity pruning-and-reconciliation in lockstep forever.
- **System map**: Optional generated JSON for broad AI orientation. It summarizes page surfaces, task flows, bridge routes from `bridge_route_inventory.py`, runtime evidence files, and high-risk areas. Treat it as advisory; canonical guidance stays in `AGENTS.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, source, and tests.
