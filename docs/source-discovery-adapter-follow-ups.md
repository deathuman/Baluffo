# Source Discovery Adapter Follow-Ups

> - **Status:** Active
> - **Use this when:** evaluating reusable discovery-adapter improvements after the GameDevMap audit migration
> - **Canonical for:** follow-up opportunities only; not current implementation commitments
> - **Not canonical for:** current discovery behavior, data contracts, or verification commands
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`architecture-ai-map.md`](architecture-ai-map.md), and the owning adapter modules
> - **Last updated:** 2026-04-27

GameDevMap now has a resumable audit/recovery path that proved useful for broad, validated source discovery. The items below are candidates for reusing that pattern elsewhere, not required work for the current release.

## Completed Reusable Slices

- 2026-04-27: Extracted shared internal audit-ledger helpers for artifact timing, freshness, failure aggregation, and size stamping. GameDevMap remains the only caller; no other adapter behavior changed.
- 2026-04-27: Extracted shared recovery URL planning helpers for common same-origin careers URLs, same-party jobish links, profile-host blocking, and bounded dedupe. GameDevMap and Gameprog use the helper without changing fetch/probe/queue behavior.

## Reusable Opportunities

- Generalize the resumable audit engine for Gamesmap, Gameprog, sheet-directory, and web-search sources where a full scan has meaningful intermediate progress, timing, cache, and resume needs.
- Extend shared recovery URL planning only when another adapter adopts the same behavior; provider inference, browser-candidate classification, and adapter diagnostics remain adapter-owned for now.
- Extend the shared audit-ledger helpers only when a second adapter adopts them; report-summary logic remains adapter-owned for now.
- Define a shared prevalidated queue-cap policy for candidates that already passed `jobsFound > 0`, while preserving dedupe, tombstones, pending/rejected state, and admin auto-approval gates.
- Add explicit browser-recovery lanes for other adapters that can first produce an HTTP-only `browserRecoveryCandidates` list and then run opt-in rendered recovery without slowing normal scans.

## Guardrails

- Keep adapter-specific behavior in the owning adapter until at least two adapters need the same abstraction.
- Do not bypass the normal discovery queue, pending registry, tombstone, static suppression, or auto-approval flow.
- Treat audit artifacts as operational ledgers and report metadata, not as active-source registries.
- Prefer targeted tests around the adapter that adopts a shared helper before adding broad utility tests.
