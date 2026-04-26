# Source Discovery Adapter Follow-Ups

> - **Status:** Active
> - **Use this when:** evaluating reusable discovery-adapter improvements after the GameDevMap audit migration
> - **Canonical for:** follow-up opportunities only; not current implementation commitments
> - **Not canonical for:** current discovery behavior, data contracts, or verification commands
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`architecture-ai-map.md`](architecture-ai-map.md), and the owning adapter modules
> - **Last updated:** 2026-04-26

GameDevMap now has a resumable audit/recovery path that proved useful for broad, validated source discovery. The items below are candidates for reusing that pattern elsewhere, not required work for the current release.

## Reusable Opportunities

- Generalize the resumable audit engine for Gamesmap, Gameprog, sheet-directory, and web-search sources where a full scan has meaningful intermediate progress, timing, cache, and resume needs.
- Extract shared recovery URL planning for directory-derived company homepages, including same-origin common careers paths, same-party jobish links, provider URL extraction, profile-host classification, and browser-candidate detection.
- Share artifact, timing, cache-signature, failure aggregation, and report-summary helpers so future adapters do not duplicate GameDevMap-specific ledger logic.
- Define a shared prevalidated queue-cap policy for candidates that already passed `jobsFound > 0`, while preserving dedupe, tombstones, pending/rejected state, and admin auto-approval gates.
- Add explicit browser-recovery lanes for other adapters that can first produce an HTTP-only `browserRecoveryCandidates` list and then run opt-in rendered recovery without slowing normal scans.

## Guardrails

- Keep adapter-specific behavior in the owning adapter until at least two adapters need the same abstraction.
- Do not bypass the normal discovery queue, pending registry, tombstone, static suppression, or auto-approval flow.
- Treat audit artifacts as operational ledgers and report metadata, not as active-source registries.
- Prefer targeted tests around the adapter that adopts a shared helper before adding broad utility tests.
