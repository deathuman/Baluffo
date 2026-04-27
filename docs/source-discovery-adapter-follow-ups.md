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
- 2026-04-27: Extracted shared directory cache helpers for Gamesmap/Gameprog TTL, signature, safe JSON load/write, and candidate dedupe mechanics without changing cache shape or adapter behavior.
- 2026-04-27: Extracted shared prevalidated queue-cap policy helpers for internal adapter/domain cap overrides. GameDevMap remains the only producer; queue limits, candidate ordering, and registry behavior did not change.
- 2026-04-27: Extracted shared directory fetch-job builders for Gamesmap/Gameprog website fetch jobs. This standardizes an audit-readiness seam without changing fetch, cache, probe, or queue behavior.
- 2026-04-27: Extended shared directory fetch-job builders to seed-careers and web-search page fetches. Web-search still owns URL selection and analysis; only the `fetch_directory_pages` job shape is shared.

## Reusable Opportunities

- Generalize the resumable audit engine for Gamesmap, Gameprog, sheet-directory, and web-search sources where a full scan has meaningful intermediate progress, timing, cache, and resume needs.
- Extend shared recovery URL planning only when another adapter adopts the same behavior; provider inference, browser-candidate classification, and adapter diagnostics remain adapter-owned for now.
- Extend the shared audit-ledger helpers only when a second adapter adopts them; report-summary logic remains adapter-owned for now.
- Extend shared directory-cache helpers only when another adapter has the same cache shape and bypass semantics.
- Extend the shared prevalidated queue-cap policy only when another adapter produces candidates that already passed `jobsFound > 0`, while preserving dedupe, tombstones, pending/rejected state, and admin auto-approval gates.
- Extend shared directory fetch-job builders only for additional adapters that already use the same `fetch_directory_pages` job shape.
- Add explicit browser-recovery lanes for other adapters that can first produce an HTTP-only `browserRecoveryCandidates` list and then run opt-in rendered recovery without slowing normal scans.

## Audit Readiness Notes

- Gamesmap and Gameprog now share cache and fetch-job helper seams. A future audit-engine plan can treat index/teams fetch, parsed-entry caps, website fetch jobs, candidate analysis, and cache writes as explicit resumable boundaries.
- Sheet-directory, seed-careers, and web-search still own different input shapes. They should only join a generalized audit engine after a separate plan defines artifact shape, progress keys, and report metadata for those flows.
- Browser recovery and active promotion stay out of shared audit-engine readiness. Any adapter adopting rendered recovery should first emit HTTP-only browser candidates and still route validated rows through the normal discovery queue.

## Guardrails

- Keep adapter-specific behavior in the owning adapter until at least two adapters need the same abstraction.
- Do not bypass the normal discovery queue, pending registry, tombstone, static suppression, or auto-approval flow.
- Treat audit artifacts as operational ledgers and report metadata, not as active-source registries.
- Prefer targeted tests around the adapter that adopts a shared helper before adding broad utility tests.
