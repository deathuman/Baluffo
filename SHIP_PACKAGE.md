# Ship Package: Release Notes + PR Description

## Release Notes

### What's New
- Added a unified local aggregation pipeline that publishes:
  - `data/jobs-unified.json`
  - `data/jobs-unified.csv`
  - `data/jobs-fetch-report.json`
- Expanded job intake through multiple adapters (core feed + ATS/static adapters), with non-blocking partial-success behavior.
- Added admin-managed source discovery and approval lifecycle (pending, active, rejected) with local persistence.
- Improved Jobs/Saved auth presentation with a compact profile pill and clearer sign in/out actions.
- Refined filters, country picker, and pagination visuals for better readability and theme consistency.

### Reliability & Data Coverage
- Fetch pipeline now merges multi-source inputs into one unified feed while preserving fallback outputs and diagnostics.
- Source failures are isolated per adapter/source and no longer block full feed publication.
- Dynamic discovery supports probe-based candidate validation before queuing for approval.
- Dedup/merge behavior keeps a single visible role row while preserving source provenance metadata.

### Admin Workflows
- Admin page can run:
  - Jobs fetcher
  - Source discovery
  - Discovery report reload
- Full discovery review loop now available in Admin:
  - Approve selected pending sources
  - Reject selected pending sources
  - Restore selected rejected sources
- Added operational status/log surfaces (bridge state, fetch/discovery logs, latest report loading).

### UI/UX Improvements
- Jobs page header/navigation was streamlined:
  - top isolated `Back` button
  - `Saved Jobs` moved into profile action area
- Filter area readability improvements:
  - tighter country option spacing
  - cleaner option rows and scroll treatment
  - improved select/dropdown contrast in dark mode
- Pagination styling now aligns with the updated visual language in both themes.
- Light theme button contrast/saturation improved; destructive clear actions remain visually distinct.

### Breaking/Non-breaking Notes
- Non-breaking for frontend listing behavior and core filter/sort workflows.
- Unified feed adds optional metadata fields (additive):
  - `source`, `sourceJobId`, `fetchedAt`, `postedAt`, `dedupKey`, `qualityScore`, `focusScore`
  - `sourceBundleCount`, `sourceBundle`
- No internet-facing API changes were introduced.
- Admin bridge interfaces are localhost-only operational endpoints.

### Known Limitations
- Some discovered endpoints may return `404`/`429` or anti-bot responses.
- Discovery is best-effort and coverage-first; candidates still require admin approval before activation.
- Certain sources may be valid but currently return zero open jobs.

### Validation
- Verified fetch pipeline continues producing report artifacts under partial source failure conditions.
- Verified admin discovery flow updates pending/active/rejected states through approve/reject/restore actions.
- Verified approved sources are applied on subsequent fetch runs and reflected in fetch reporting.
- Verified Jobs/Saved auth controls still represent guest/signed-in states and sign in/out behavior.
- Verified filter/pagination rendering and behavior remain stable after visual updates in dark and light themes.

### Operator Notes (Local Usage)
- Run the local admin bridge before using discovery controls in Admin.
- Discovery actions depend on bridge availability.
- Approvals affect the next fetch run (run fetcher after approving sources).
- This setup is optimized for local personal operation, not internet-exposed deployment.

---

## PR Description

### What's New
This PR ships a broader local jobs platform upgrade spanning ingestion, discovery, admin operations, and UI polish:
- Unified multi-source fetch pipeline with durable output artifacts and diagnostics.
- Admin-managed source lifecycle with discovery, approval/rejection, and rejected-source restore.
- Enhanced job source coverage strategy with ATS/static adapters and probe hardening.
- Jobs/Saved auth module refresh and listing/filter UX improvements.

### Reliability & Data Coverage
- Implemented resilient source execution semantics:
  - per-source outcomes tracked in reports
  - partial-success publication preserved
  - fallback outputs retained for inspection and resilience
- Discovery engine now includes:
  - hybrid candidate finding (seed/pattern/search)
  - adapter-aware probing
  - retry/backoff/pacing for transient failures
  - fallback probe paths for selected adapters
- Coverage remains constrained by third-party anti-bot/rate-limit behavior; failures are explicit in reports.

### Admin Workflows
- Added admin as central control surface for:
  - fetch execution
  - discovery execution
  - pending review and approval/rejection
  - rejected-source restore
- Registry model is file-backed:
  - active sources
  - pending sources
  - rejected sources
- Admin logs and bridge status improve observability of local operations.

### UI/UX Improvements
- Jobs/Saved auth controls consolidated into a profile pill pattern with action grouping.
- Jobs page navigation layout clarified (`Back` isolated, `Saved Jobs` profile-adjacent).
- Filters improved for readability and density, especially country selection.
- Pagination and button styles aligned to updated theme behavior in dark/light modes.

### Breaking/Non-breaking Notes
- Feed contract remains backward-compatible for existing consumer fields.
- Added optional metadata fields only; no required field removals for frontend consumption.
- Admin/discovery bridge remains local-only; no external API surface added.

### Known Limitations
- External source reliability varies by provider (notably rate limits and anti-bot controls).
- Discovery is intentionally conservative for activation: candidates are queued, not auto-enabled.
- Zero-job candidates may still appear in discovery outputs depending on provider behavior.

### Validation
- Unit/integration coverage includes discovery probing, dedup behavior, and pipeline invariants.
- Manual sanity checks confirm:
  - outputs generated (`jobs-unified.*`, fetch/discovery reports)
  - admin actions mutate source registry state correctly
  - listing, auth, filtering, and pagination remain functional after UI updates

### Notes for Reviewers
- Scope is additive and local-ops focused.
- Main risks are external dependency variability (ATS endpoint changes, anti-bot policy changes), mitigated by explicit reporting and non-blocking execution paths.
