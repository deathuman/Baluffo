# Cross-Cutting Tech Debt Follow-Up Tracker

> - **Status:** Active follow-up tracker
> - **Use this when:** choosing the next small cross-cutting cleanup after the June 2026 P0 refactor pass
> - **Canonical for:** current cleanup priorities and explicit non-goals after the P0 work closed
> - **Not canonical for:** detailed P0 implementation history, API contracts, route ownership, or release behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../testing.md`](../testing.md), [`../DOCS_WORKFLOW.md`](../DOCS_WORKFLOW.md)
> - **Last updated:** 2026-06-20

## Current State

The original P0 cross-cutting refactor is closed or closed by evidence. This file now tracks only follow-up cleanup that still has practical value. Detailed slice history intentionally stays in git history per [`../DOCS_WORKFLOW.md`](../DOCS_WORKFLOW.md).

Closed context worth remembering:

- `get_routes.py` route-owned behavior is decomposed; new GET behavior belongs in route leaves, with `handle_get` kept as the public delegator.
- `BridgeApi` is guardrailed as a composition object. Field inventory classifies all 90 fields and shows zero `default-only` deletion candidates, so deletion/splitting is not worth pursuing now.
- `admin_bridge.py` remains a compatibility entrypoint. P0 root/global risks are closed; remaining value is in reducing test seams, not deleting the root.
- Source broad catches are narrowed to the intentional HTTP route JSON boundary budget.
- Update and updater root/facade risks are closed by inventories and direct leaf imports; remaining facades are compatibility surfaces.
- CanonicalJob missing lifecycle/location fields, shared private shape helpers, shared JSON/storage-metrics isolation, and quick CSS theme fixes are done.
- Test sleep cleanup is done; keep `rg -n "time\\.sleep\\(" tests` empty when adding or changing tests.
- Targeted `admin_bridge` test seam cleanup is done for the shared fixture, source-policy setup, and task-launch setup; remaining direct internals are compatibility or service-holder tests.
- macOS platform work remains deferred by product priority.

## Active Work Queue

1. **Tighten port-8877 test coupling**
   - Admin `RuntimeConfig(port=8877)` test defaults now use `tests.helpers.ports.ADMIN_BRIDGE_TEST_PORT`.
   - Replace live-bind/config defaults with named fixtures or dynamic ports where tests start real servers.
   - Leave examples, expected payload URLs, and documentation-style literals alone when the literal is part of the contract being asserted.
   - Next target: desktop/packaged runtime helper defaults; avoid URL/assertion literals that intentionally prove `8877`.
   - Avoid a broad mechanical replacement.

2. **Normalize remaining datetime parsing only where behavior can drift**
   - Public bridge/source-sync `parse_iso` wrappers already delegate to `src.shared.utils.parse_iso`.
   - Only clean inline `datetime.fromisoformat(...replace("Z", "+00:00"))` variants when they affect shared behavior or contract-facing code.

3. **CSS cleanup stays optional**
   - Quick fixes already landed for fetch-progress theme color and redirect-page theme initialization.
   - Full CSS bundling/minification/hashing is useful only if frontend deploy/cache pain becomes active.

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
