# Cross-Cutting Tech Debt Follow-Up Tracker

> - **Status:** Archived closeout — complete for the June 2026 P0 cleanup tranche
> - **Use this when:** checking what closed after the June 2026 P0 refactor pass
> - **Canonical for:** current cleanup priorities and explicit non-goals after the P0 work closed
> - **Not canonical for:** detailed P0 implementation history, API contracts, route ownership, or release behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../testing.md`](../testing.md), [`../DOCS_WORKFLOW.md`](../DOCS_WORKFLOW.md)
> - **Last updated:** 2026-08-28 (archived from `docs/plans/` after completion)

## Current State

The original P0 cross-cutting refactor is closed or closed by evidence. The follow-up cleanup tranche tracked here is also complete for the current scope. Detailed slice history intentionally stays in git history per [`../DOCS_WORKFLOW.md`](../DOCS_WORKFLOW.md).

Closed context worth remembering:

- `get_routes.py` route-owned behavior is decomposed; new GET behavior belongs in route leaves, with `handle_get` kept as the public delegator.
- `BridgeApi` is guardrailed as a composition object. Field inventory classifies all 90 fields and shows zero `default-only` deletion candidates, so deletion/splitting is not worth pursuing now.
- `admin_bridge.py` remains a compatibility entrypoint. P0 root/global risks are closed; remaining value is in reducing test seams, not deleting the root.
- Source broad catches are narrowed to the intentional HTTP route JSON boundary budget.
- Update and updater root/facade risks are closed by inventories and direct leaf imports; remaining facades are compatibility surfaces.
- CanonicalJob missing lifecycle/location fields, shared private shape helpers, shared JSON/storage-metrics isolation, and quick CSS theme fixes are done.
- Test sleep cleanup is done; keep `rg -n "time\\.sleep\\(" tests` empty when adding or changing tests.
- Targeted `admin_bridge` test seam cleanup is done for the shared fixture, source-policy setup, and task-launch setup; remaining direct internals are compatibility or service-holder tests.
- Port-8877 coupling cleanup is done for live/config-style test defaults; remaining literals are contract URLs, persisted payload examples, or expected assertions.
- Shared/contract-facing datetime parsing cleanup is done for local data, source-policy review timestamps, Personio source-state timestamps, and desktop update check throttling. The remaining source-health quarantine inline parser intentionally preserves exception-ratchet behavior for unexpected non-string state values.
- macOS platform work remains deferred by product priority.

## Active Work Queue

No required follow-up items remain in this tracker.

- Quick CSS fixes already landed for fetch-progress theme color and redirect-page theme initialization.
- Full CSS bundling/minification/hashing is intentionally out of scope for this cleanup goal and should only be reopened if frontend deploy/cache pain becomes active.

## Do Not Pursue Now

- **BridgeApi field deletion or interface splitting:** the inventory shows no safe `default-only` fields, and current guardrails already block route/server backslide.
- **Updater facade removal:** remaining compatibility surfaces are guarded and low-payoff to delete.
- **More `get_routes.py` decomposition:** done for route-owned behavior.
- **Full `admin_bridge.py` deletion:** keep it as the stable compatibility entrypoint.
- **macOS platform support:** explicitly deferred.
- **Large historical rewrite or archive migration:** use git history for detailed P0 provenance; do not create a new archive page for this tracker unless a future closeout needs one.

## Verification For Cleanup Slices

Use narrow tests first, then repo gates:

- Targeted pytest or node tests for the touched area.
- `rg -n "time\\.sleep\\(" tests` when changing sleep cleanup.
- `npm run lint:repo-guardrails`
- `npm run lint:precommit:changed`
- `git diff --check`

If a cleanup changes public contracts, update the owning canonical doc instead of expanding this tracker.
