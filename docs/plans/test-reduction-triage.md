# Test Reduction Triage

> - **Status:** Active — standing rules for reducing the test corpus, plus the 2026-09-27 coverage-verified baseline. The 2026-09-28 clone re-sweep found **no further safe test reductions** beyond local-fixture extraction; the methods and per-cluster outcomes are recorded in [`../measurement-methods.md`](../measurement-methods.md). The rules below remain the gate for any future sweep
> - **Use this when:** deciding whether a test can be deleted or merged, or starting a new coverage-backed reduction sweep
> - **Canonical for:** the delete/merge safety rules and the retained-test boundaries
> - **Not canonical for:** verification command ownership (see [`../testing.md`](../testing.md)), product/runtime contracts, or the history of past campaigns (git history, `docs/archive/`)
> - **Then inspect:** [`../testing.md`](../testing.md), the candidate test file, and the owning source or contract doc
> - **Last updated:** 2026-09-27 (2026-09-27 coverage sweep; May 2026 campaign history retired)

Coverage data is a **signal only**. Delete or merge a test only when the asserted behavior is
duplicated, obsolete, or already enforced by a static guardrail. Everything else defaults to
retained. Do not delete from this document's own authority: start a new sweep with fresh evidence.

## Baseline (2026-09-27, coverage-verified)

Measured with `pytest-cov` over the full Python lane (see [`../testing.md`](../testing.md) for the
plugin install):

| Measure | Value |
|---|---|
| Python collection (`-m "not slow and not packaging and not release"`) | 5,450 tests across 685 files |
| Full-lane run | 5,513 passed, 1 skipped, 134 deselected |
| Collected Python test lines | 151,524 |
| Tests area line count (`loc_budget.py`, all tracked languages) | 212,502 |
| `src/` files measured | 635 |
| `src/` files with **zero** covered lines | **3** |

The two line totals differ because `loc_budget.py` counts every tracked source file under `tests/`
(including the `.mjs` frontend units), while the 151,524 figure is the collected Python test files
only. Neither is wrong; do not compare them directly.

### Bucket distribution — the merge hypothesis is dead

Classified structurally from the collection node ids (a `--cov=src` run only reports `src/**`, so it
cannot answer "did this test file cover any src line" — do not gate a bucket on it):

| Bucket | Files | Lines | % of lines | Tests |
|---|---:|---:|---:|---:|
| `behavior` | 585 | 116,924 | 77% | 4,471 |
| `mock-bound` | 95 | 33,784 | 22% | 934 |
| `table-heavy` | **5** | **816** | **0.5%** | 45 |

The `table-heavy` bucket — the merge-candidate class the reduction plan was built around — is
essentially **empty**: 5 files, 816 lines. The largest is
`tests/jobs_static/test_static_pagination_follow.py` (600 lines, 29 tests). There is no bulk
parameterised-table reduction to make.

`mock-bound` at 22% is the only bucket of any size, and mock-heavy is not the same as
restating-implementation: those files exercise real behaviour through injected doubles, and the
retained boundaries below cover exactly that class. Do not treat "mock-heavy" as a deletion signal.

The three uncovered `src/` files are `src/ship/desktop_app/__main__.py`,
`src/ship/desktop_app/cli.py`, and `src/source_discovery.py`. All three are bootstrap shims that
exist to be importable and delegate to the real module, so their behaviour is covered by the module
they dispatch into. They are not coverage gaps and need no test.

**The corpus is not dominated by restatement.** A zero-coverage sweep over `src/` finds three
trivial shims, not a large untested surface. Reduction therefore has to be justified test-by-test
against the rules below; there is no bulk target. Line count is not the objective — 212k test lines
supporting 5,452 behavioural cases is not evidence of waste.

## Retained Python Boundaries

- Remaining soak-report tests are retained unless a future slice proves a concrete duplicate; current coverage-gap, staging, advisory, and warning-gate tests protect distinct persisted report output and route-facing contracts.
- Pipeline, discovery, source-sync, and packaged-adjacent tests often cover cross-component contracts. Do not delete them just because they are slow or share covered lines; first prove that a narrower seam test plus an existing smoke/release gate covers the same failure mode.
- Bridge conflict-adjudication, packaged-runtime, source-sync, and release-adjacent tests default to keep because their value is compatibility and integration failure detection, not unique line coverage.

## Frontend JS Triage

### Keep despite zero unique file-level coverage

Keep tests that assert persisted data compatibility, payload compatibility, bridge/runtime sequencing, or user-facing behavior even when they share all executed source lines with other files:

- `tests/frontend/unit/local-data-tracking.test.mjs`
- `tests/frontend/unit/local-data-runtime-contract.test.mjs`
- `tests/frontend/unit/saved-runtime-controllers.test.mjs`
- `tests/frontend/unit/jobs-feed-startup.test.mjs`
- `tests/frontend/unit/jobs-feed-bootstrap-confirm.test.mjs`
- `tests/frontend/unit/task-run-view-model.test.mjs`
- `tests/frontend/unit/admin-ops-controller.test.mjs`
- `tests/frontend/unit/admin-registry-controller.test.mjs`
- `tests/frontend/unit/admin-source-policy-review-controller.test.mjs`
- `tests/frontend/unit/live-task-observability.test.mjs`

### Move or keep as tooling policy

These tests exercise repo tooling or harness behavior rather than frontend product UI. Keep them if the tool remains supported; otherwise move policy checks to the owning tool guardrail:

- `tests/frontend/unit/mcp-playwright-server.test.mjs`
- `tests/frontend/unit/playwright-smoke-runtime.test.mjs`
- `tests/frontend/unit/perf-counters.test.mjs`
- `tests/frontend/unit/perf-marks.test.mjs`
- `tests/frontend/unit/startup-metrics-effects.test.mjs`

### Do not re-triage from snapshot deletion

`tests/frontend/unit/jobs-html.test.mjs` is **not** a snapshot file. Its snapshot-style checks over
retired visual structure were removed in 2026, but the file now holds 14 behavioural DOM/contract
assertions (update copy, toolbar status row, table/card alignment, desktop guard classes). Judge
it on its current contents, not on the retired snapshot framing.

Frontend smoke and packaged desktop scripts are release/runtime gates; do not judge them by unit
coverage.

## Where Consolidated Coverage Lives

The pre-2026-09-26 campaign moved duplicated coverage into these owners. They are the reason the
corpus is not larger; do not re-merge or re-split them:

| Concern | Owner |
|---|---|
| Source-text / import / retired-shape policy checks | `tools/repo_health/suite_contract_policy.py` (runs in `npm run lint:repo-guardrails`) |
| City/location noise canonicalization | `tests/jobs/adapters/parsers/test_location.py` + `tests/fixtures/city_regression_corpus.json` |
| Per-test-file line ceilings | `tools/repo_health/test_line_budget_baseline.json` |
| Area line budgets | `tools/repo_health/loc_baseline.json` |

## Safety Rules

- Do not remove compatibility tests for persisted local data, bridge payloads, release manifests, packaged desktop flows, or source-sync formats without a replacement contract test.
- Prefer merging duplicated parametrized cases over deleting edge-case data.
- If a test only scans source text for architecture policy, migrate it to repo guardrails or delete it; do not keep expanding pytest for repository policy.
- If a test protects a retired behavior, delete it unless the code still accepts that behavior as an intentional compatibility surface.
- Before any new reduction campaign, refresh collection counts and coverage-context evidence. Treat this document's baseline as a starting point, not an evergreen deletion queue.
- After any test deletion, re-run `python tools/repo_health/loc_budget.py --update` **after staging the deletion**. The ratchet compares the index against the baseline, so updating before staging records the pre-deletion count and the pre-commit gate fails on a stale baseline.
