# Bridge Route Inventory Guardrail Plan

> - **Status:** Archived plan with closeout
> - **Use this when:** reviewing why the bridge route inventory guardrail exists or how it was scoped
> - **Canonical for:** historical route inventory scope and closeout rationale
> - **Not canonical for:** current endpoint payloads, CORS policy, runtime behavior, task lifecycle authority, storage authority, or release behavior
> - **Then inspect:** [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../testing.md`](../testing.md), `../../src/bridge/routes/`, and `../../frontend/shared/api-client.js`
> - **Last updated:** 2026-05-13

## Closeout

Completed on 2026-05-13 by adding `tools/repo_health/bridge_route_inventory.py`,
the `routes` group in `tools/repo_health/repo_guardrails.py`, focused inventory
tests, and the missing public/support Admin Bridge API entries. Current route
behavior remains owned by `src/bridge/routes/`; this archive is historical
context only.

## Summary

The correct immediate next improvement is a checked bridge route inventory with an automated guardrail. This is still worth doing first because the latest audit found real route/documentation drift, and route strings span Python handlers, frontend callers, tests, and contract docs.

This plan is intentionally smaller than the broader AI modification safety plan. It should make later live-task typing, diagnostics correlation, and large-builder refactors safer without changing product behavior.

## Correct Assertions

- A route inventory is low-risk and useful before deeper observability or refactor work.
- Route drift is a current maintainability risk because backend handlers, frontend callers, and docs are not checked against one source.
- The inventory should include method, route pattern, owner module, known frontend callers, contract doc owner, and verification lane.
- The inventory must be testable by scanning route literals from `src/bridge/routes/*`.
- Undocumented live routes should be either documented in the inventory or explicitly marked internal.

## Immediate Work

1. Add a small dependency-free inventory module or data file under repo health tooling.
2. Populate it from the current bridge routes, including route patterns such as `/ops/task-live/<taskType>` and grouped routes such as `/tasks/run-sync-pull` / `/tasks/run-sync-push`.
3. Add a repo guardrail that extracts literal route checks from bridge route modules and fails when a route is missing from the inventory.
4. Add focused tests for the inventory/guardrail behavior.
5. Update `docs/admin-bridge-api.md` only for routes that are public or support-facing; mark internal/diagnostic routes explicitly in the inventory instead of forcing every route into the API reference.

## Out Of Scope

- Do not add custom request headers for correlation as part of this work.
- Do not inject diagnostic IDs into POST bodies.
- Do not create a new diagnostics route or support-bundle system.
- Do not change endpoint paths, payload shapes, task lifecycle authority, storage authority, CORS behavior, packaging, or release behavior.
- Do not split large evidence builders until route inventory and relevant payload shape tests exist.

## Test Plan

- Run the new focused route inventory test.
- Run `npm run lint:repo-guardrails`.
- Run `python -m pytest tests/bridge/test_routes_get.py tests/bridge/test_routes_smoke.py -q` if route docs or route handling assumptions change.
- Run `python -m pytest tests/admin/ -q` only if task-launch or route-family behavior changes.
