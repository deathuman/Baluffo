# Jobs Adapter Dead Source Deletion Plan

## Status

The original nine-phase jobs adapter mass-refactor is complete as of 2026-04-29. This document now tracks the deferred deletion work for repeatedly dead or unsupported jobs sources and any static plugins that become unused after source deletion.

This plan targets `src/jobs/adapters/`, source registry data under `data/`, read-only evidence tooling under `scripts/`, and docs that describe adapter/source ownership. It must not change saved-job data, local user data, bridge route contracts, fetch report payload shapes, source-discovery contracts, or public plugin `can_handle(...)` / `run(...)` signatures for surviving plugins.

## Completed Refactor Baseline

| Phase | Result |
| --- | --- |
| Provider API dispatch | Complete. Provider wrappers route through the registered provider dispatcher; BambooHR and Workday no longer use a separate direct provider path from `provider_api.py`. |
| Static plugin fixture scaffolding and yield helper | Complete. `tests/jobs/adapters/plugins/static/test_standard_plugins.py` and `scripts/jobs_yield_gate.py` exist. |
| Tiered static plugin runner | Complete. `src/jobs/adapters/plugins/static/_runner.py` owns the repeated simple static plugin lifecycle. |
| `static_listing_flow.py` complexity reduction and merge | Complete. `src/jobs/adapters/static_listing_flow.py` was deleted and the flow is consolidated into `static_listing.py`. |
| Detail traversal lifecycle merge | Complete. `src/jobs/adapters/static_detail.py` was deleted and detail traversal lifecycle lives with the static runner flow. |
| Static fetch/listing/detail orchestration thinning | Complete for the planned slice. `process_static_source(...)` is now a thin entrypoint over the consolidated static flow. |
| Detail heuristic C901 cleanup | Complete for the planned slice. The old `static_detail_heuristics.py` C901 targets are no longer current blockers. |
| Location rules C901 cleanup | Complete for the planned slice. The old `location_rules.py` phase target is below the prior baseline. |

Targeted verification from the planning sweep:

```powershell
python -m pytest -q tests/test_jobs_fetcher_providers.py tests/jobs/adapters/plugins/static tests/jobs/adapters/parsers/test_location.py
python -m ruff check --select C901 src/jobs/adapters/provider_api.py src/jobs/adapters/static_listing.py src/jobs/adapters/static_detail_heuristics.py src/jobs/adapters/location_rules.py
```

The broader `src/jobs/adapters` tree still has C901 debt in community, provider, social, parser, and custom static modules. That debt is not a reason to expand this deletion slice unless deleting confirmed dead sources removes the owning plugin code.

## Completed Deferred Work

| Order | Slice | Result |
| --- | --- | --- |
| 1 | Evidence helper | Complete. Added `dead-source-candidates`, `dead-source-registry`, and `dead-source-decisions` helpers to `scripts/jobs_yield_gate.py`. |
| 2 | Fresh evidence | Complete. Ran a two-pass evidence batch under `_out/jobs-adapter-dead-source-evidence-20260429/` with generated static source IDs. |
| 3 | Source deletion | Complete. Deleted 10 pending static rows plus one duplicate active row that shared a delete-eligible URL fingerprint. |
| 4 | Snapshot docs | Complete. Added the dated evidence snapshot and linked it from `docs/INDEX.md`. |
| 5 | Plugin deletion | Complete. Deleted unused demo/no-op static plugin registrations: `example_com`, `example_org`, and `static_pilot`. |

Remaining follow-up should be opened as new work: run a larger evidence batch, tackle broader adapter-wide C901 debt, or delete more static plugin code only after future source-row evidence proves it is unused.

## Deletion Rules

Delete a source only when both fresh evidence passes show:

- `keptCount == 0`.
- A non-recoverable classification, failure bucket, or pending reason such as `stale_or_dead_static_source`, `unsupported_static_source`, `site_changed_static_source`, `redundant_static_stronger_coverage`, `site_changed`, or `dead_listing_page`.
- No browser fallback recommendation that still needs a browser pass.
- No `empty_confirmed`, `no_openings`, `blocked_or_challenge`, `anti_bot_or_challenge`, `rate_limited`, or `timeout` result unless a later explicit browser pass proves the source unrecoverable.

Deletion means removing the row from `data/source-registry-active.json` and `data/source-registry-pending.json`, then adding a record to `data/source-registry-tombstones.json` so discovery/sync does not silently reintroduce it.

Do not delete provider, social, community, or shared static plugin code merely because one source has zero yield. Plugin deletion requires dependency proof after source deletion.

## Evidence Commands

List valid static source IDs:

```powershell
python scripts/jobs_yield_gate.py list-static-sources --limit 80
```

Rank deletion candidates after the helper slice:

```powershell
python scripts/jobs_yield_gate.py dead-source-candidates --limit 80
```

Run two evidence passes with generated source IDs only:

```powershell
python -m src.jobs.pipeline --only-sources <comma-separated-generated-source-ids> --output-dir _out/jobs-adapter-dead-source-evidence-YYYYMMDD/pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <same-source-ids> --output-dir _out/jobs-adapter-dead-source-evidence-YYYYMMDD/pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
python scripts/jobs_yield_gate.py compare _out/jobs-adapter-dead-source-evidence-YYYYMMDD/pass-1 _out/jobs-adapter-dead-source-evidence-YYYYMMDD/pass-2
```

## Validation

Docs-only slice:

```powershell
cmd /c npm run lint:precommit
```

Evidence helper slice:

```powershell
python -m pytest -q tests/test_jobs_yield_gate.py
python scripts/jobs_yield_gate.py list-static-sources --limit 20
cmd /c npm run lint:precommit
```

Source deletion slice:

```powershell
python -m pytest -q tests/admin/test_admin_bridge_ops_registry.py tests/test_source_registry_p1_operational_noise.py
cmd /c npm run lint:precommit
```

Plugin deletion slice:

```powershell
python -m pytest -q tests/jobs/adapters/plugins/static tests/jobs/adapters
python -m ruff check --select C901 <touched adapter files>
cmd /c npm run lint:precommit
```

The full `python -m ruff check --select C901 src/jobs/adapters` command still reports known adapter-wide complexity debt outside this deletion slice. Use the precommit complexity baseline plus targeted C901 checks for touched files unless the active work intentionally tackles the broader C901 backlog.

Never submit with `--no-verify`.
