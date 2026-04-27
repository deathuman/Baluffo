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
- 2026-04-27: Extracted shared audit report-summary helpers for active split, top failure buckets, and artifact size fallback. GameDevMap remains the only caller; report fields did not change.
- 2026-04-27: Characterized the future directory audit contract for Gamesmap/Gameprog. This documented resumable boundaries and locked cache/fetch/candidate provenance behavior in tests without adding artifacts.
- 2026-04-27: Added an opt-in Gameprog directory audit pilot using the shared audit-ledger helper. The default Gameprog path, cache shape, registry flow, queue behavior, and report fields remain unchanged.
- 2026-04-27: Extracted a shared directory audit engine and adopted it for opt-in Gameprog/Gamesmap audit artifacts. Adapter parsing/candidate logic remains local; default cache, queue, registry, and report behavior did not change.
- 2026-04-27: Added additive discovery-report visibility for opt-in Gameprog/Gamesmap directory audits. Reports now expose cache hit, completion, runtime, artifact size, timing totals, top failure buckets, and adapter boundary counts only when an audit ran or reused a fresh artifact in the current process.
- 2026-04-27: Default-enabled directory audits for Gameprog and Gamesmap when each adapter is enabled. Gamesmap remains disabled by default, and `activeAuditEnabled=false` remains the rollback to legacy cache scanning.
- 2026-04-27: Added an opt-in sheet-directory audit pilot using the shared directory audit engine. Default sheet discovery remains the legacy CSV scan; `sheetDirectory.activeAuditEnabled=true` writes/reuses `data/sheet-directory-discovery-audit.json` and reports metadata through `directoryAuditSummaries`.

## Reusable Opportunities

- Generalize the resumable audit engine for web-search sources where a full scan has meaningful intermediate progress, timing, cache, and resume needs.
- Extend shared recovery URL planning only when another adapter adopts the same behavior; provider inference, browser-candidate classification, and adapter diagnostics remain adapter-owned for now.
- Use directory audit report evidence to decide whether sheet-directory should become default and whether web-search should get a separate audit migration.
- Extend shared directory-cache helpers only when another adapter has the same cache shape and bypass semantics.
- Extend the shared prevalidated queue-cap policy only when another adapter produces candidates that already passed `jobsFound > 0`, while preserving dedupe, tombstones, pending/rejected state, and admin auto-approval gates.
- Extend shared directory fetch-job builders only for additional adapters that already use the same `fetch_directory_pages` job shape.
- Add explicit browser-recovery lanes for other adapters that can first produce an HTTP-only `browserRecoveryCandidates` list and then run opt-in rendered recovery without slowing normal scans.

## Audit Readiness Notes

- Gamesmap and Gameprog now share cache, fetch-job, audit-engine, and report-summary seams. Their audit paths are default when each adapter is enabled; future work should focus on runtime evidence and broader adapter migration.
- Sheet-directory now has an opt-in audit artifact, but still defaults to its legacy CSV scan. Seed-careers and web-search still own different input shapes and need a separate plan before adopting an audit artifact.
- Browser recovery and active promotion stay out of shared audit-engine readiness. Any adapter adopting rendered recovery should first emit HTTP-only browser candidates and still route validated rows through the normal discovery queue.

## Future Directory Audit Contract

- Resumable boundaries for Gamesmap/Gameprog should be: directory index or teams fetch, parsed-entry selection after configured caps, website fetch job creation, fetched-page candidate analysis, and final cache-compatible candidate/failure write.
- A future directory audit artifact should own only operational state: progress cursor, completed URL identities, timing totals, aggregated failures with bounded samples, provider/static candidates, and completion/cache freshness state.
- Candidate outputs must preserve the current provenance fields from the directory adapters so a later audit engine can resume without inventing new registry fields.
- Non-goals for the directory audit migration are browser recovery, direct active/pending/rejected registry writes, queue-cap bypass, public report fields, and cache shape changes unless a separate plan explicitly adds them.

## Guardrails

- Keep adapter-specific behavior in the owning adapter until at least two adapters need the same abstraction.
- Do not bypass the normal discovery queue, pending registry, tombstone, static suppression, or auto-approval flow.
- Treat audit artifacts as operational ledgers and report metadata, not as active-source registries.
- Prefer targeted tests around the adapter that adopts a shared helper before adding broad utility tests.
