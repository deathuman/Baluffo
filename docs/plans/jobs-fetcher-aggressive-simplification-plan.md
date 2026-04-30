# Jobs Fetcher Deletion-First Simplification Closeout

> - **Status:** Closed for broad lifecycle/C901 cleanup
> - **Use this when:** choosing approval-gated jobs fetcher deletion, source-family removal, compatibility-boundary removal, or product behavior tuning
> - **Canonical for:** deletion-first jobs fetcher goals, completed simplification baseline, protected boundaries, remaining approval gates, and validation expectations
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, frontend payload ownership, or source-discovery behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and the touched source files
> - **Last updated:** 2026-04-30

## Objective And Protected Boundaries

The jobs fetcher should find active job openings reliably and quickly enough for repeated refreshes. It does not need to preserve long-standing internal fetch/discovery compatibility layers when those layers make the implementation harder to delete, reason about, or improve.

Protected product surfaces:

- Preserve saved jobs, local user data, and user-entered job tracking information.
- Current UI/runtime invocation paths and bridge/frontend payload fields actively consumed by the app.
- Queue, pending review, tombstone, static suppression, and auto-approval behavior unless a future behavior slice explicitly changes and tests those boundaries.

Internal jobs fetcher shims are deletion candidates when current product behavior remains covered. Internal jobs fetcher imports, adapter boundaries, plugin shapes, package-shape tests, and historical compatibility shims should not be preserved only because they existed historically.

## Closeout Baseline

The broad jobs fetcher simplification track is now functionally complete for lifecycle and C901 cleanup.

Current measured baseline:

- `src/jobs`: 133 tracked Python files, 32,342 tracked Python lines.
- `src/jobs/adapters`: 73 tracked Python files, 18,700 tracked Python lines.
- `python -m ruff check --select C901 src/jobs` passes with no offenders.
- `python -m ruff check --select C901 src/jobs/adapters` passes with no offenders.
- The first source-family evidence snapshot is available at [`jobs-source-family-evidence-2026-04-30.md`](../snapshots/jobs-source-family-evidence-2026-04-30.md).

Completed simplification facts:

- Provider dispatch was unified.
- Source discovery and jobs adapters are C901-clean.
- `static_listing_flow.py`, `static_detail.py`, `static_helpers.py`, and several static/plugin/facade surfaces were deleted.
- Static listing, static detail, static Scrapy, social, community, Personio, static plugin parser, rendered-card, location-rule, adapter parser, shared classification, state lifecycle, pipeline runtime, pipeline CLI, pipeline finalization, and contamination audit hotspots were cleared or thinned.
- Evidence-backed dead-source deletion now exists with tombstones and dated snapshots.
- M2 source-health triage is an observability/product-reliability lane: completed fetch reports expose `sourceHealth` so Admin/Ops can see failed, zero-kept, slow, browser-fallback, and high-yield sources before any approval-gated source deletion or provider migration.

The conclusion is intentionally sharp: broad cleanup is closed. Future work should delete code, remove source families, tune product yield/performance, or explicitly retire compatibility boundaries. It should not be another compatibility-preserving C901/helper extraction pass.

## Remaining Approval-Gated Decisions

Ask before starting these, and bring evidence where source/runtime behavior may change:

- `scrapy_static_sources`: deletion candidate from evidence because no enabled rows were found, but do not remove the default loader, runtime surface, browser fallback lane, or compatibility export without explicit approval.
- `social_x`: zero-yield channel candidate from the 2026-04-30 evidence run; keep Mastodon unless fresh channel-specific evidence says otherwise.
- Static sources: investigate per-source deletion candidates such as Blizzard; do not pursue broad static deletion because the sampled static family is high-yield.
- Community Google Sheets: keep; future work should focus on redirect/cache/canonicalization performance, not deletion.
- `src/jobs_fetcher.py` compatibility re-exports: high-risk removal requiring explicit approval because import-style tests or external launch paths may depend on them.
- Bridge/frontend report fields, plugin `can_handle(...)` / `run(...)` signatures, saved-job/local-user storage, source rows, and default loaders remain protected until a specific behavior or deletion plan covers them.

Historical shim watchlist for compatibility-boundary review:

- `src/jobs/fetcher_compat_exports.py`
- `src/jobs/fetcher_compat_runtime.py`
- `pipeline_runtime.py`
- `src/jobs/state.py`
- `state_source_state.py`
- `src/jobs/pipeline_execution_flow.py`
- `reporting.py`
- `common/contracts.py`
- tests that enforce old package shape instead of current runtime behavior

## Future Work Gate

A future jobs fetcher slice should meet at least one of these gates:

- Delete a source row, default loader, plugin module, compatibility facade, or lifecycle branch with explicit approval where needed.
- Improve job-finding yield, refresh runtime, retry/cache behavior, or source reliability with before/after evidence.
- Produce a fresh evidence snapshot that enables an approval-gated deletion or product tuning decision.

Do not add a new helper or framework unless the same slice deletes more production code than it adds or removes a current runtime branch.

For evidence/yield work, use `_out/` only and do not mutate tracked `data/`:

```powershell
python scripts/jobs_yield_gate.py list-static-sources --limit 20
python -m src.jobs.pipeline --only-sources <valid-source-ids> --output-dir _out/<slice>/before --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <same-source-ids> --output-dir _out/<slice>/after --force-refresh-all --ignore-circuit-breaker --quiet
python scripts/jobs_yield_gate.py compare _out/<slice>/before _out/<slice>/after --allow-drops
```

## Validation Rules

Default gate for docs-only slices:

```powershell
cmd /c npm run lint:precommit
```

Default gate for code slices:

```powershell
python -m pytest -q <targeted tests for touched area>
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

Baseline checks that should remain green:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
python -m ruff check --select C901 src/jobs/adapters --output-format concise
```

Never use `--no-verify`.
