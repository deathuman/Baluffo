# Ship Package: Release Notes + PR Description

## Release Notes

### What's New
- Added two new Google Sheets sources to the jobs ingestion pipeline.
- Improved Google Sheets compatibility:
  - supports alternate public export paths (`gviz`/`pub`) when direct CSV export is blocked
  - supports additional header variants (`Studio`, `Job`, `Job Title`, `Job Type`, `Link`, etc.)
- Refined Jobs page pipeline button UX:
  - idle button now shows the real action label instead of `Ready`
  - button includes a tooltip warning that runs may exceed 5 minutes
  - button height is aligned with `Refresh Jobs`
- Cleaned up Jobs page Data Sources panel:
  - source URLs are sanitized before rendering
  - static-source rows are compacted to reduce panel bloat

### Reliability & Data Coverage
- Increased effective jobs coverage via additional public Sheets ingestion paths and parser tolerance.
- Confirmed full pipeline output updates and packaged-runtime visibility of refreshed totals.
- Source-level failures remain isolated and reported without blocking successful source publication.

### Admin Workflows
- No breaking admin workflow changes in this release.
- Existing fetch/discovery/sync controls remain unchanged.

### UI/UX Improvements
- Pipeline run button is now consistent with adjacent toolbar controls.
- Jobs results summary now better reflects loaded dataset context after refresh/filtering.
- Data Sources panel readability is improved by condensing static-source noise.

### Breaking/Non-breaking Notes
- Non-breaking release for existing jobs browsing and admin flows.
- No required schema changes for consumers of unified feed outputs.
- No internet-facing API changes introduced.

### Known Limitations
- Some third-party static/careers sources can still return `404`, timeout, or anti-bot responses.
- Dynamic source availability remains dependent on provider uptime and access policies.

### Validation
- Verified frontend unit suite (`node --test tests/frontend/unit/all.test.mjs`) is green.
- Verified targeted pipeline/source metadata unit coverage for new behavior is green.
- Verified full fetch pipeline run updates `data/jobs-unified.*` and `data/jobs-fetch-report.json`.
- Verified new packaged build launches and reports refreshed jobs totals in startup metrics.

### Operator Notes (Local Usage)
- Tag-driven GitHub release flow remains unchanged:
  - create/push `v0.0.4` tag on `main`
  - workflow publishes portable and ship zip artifacts with `0.0.4` versioned names
  - release body is sourced from this `## Release Notes` section

---

## PR Description

### What's New
This PR ships a broader local jobs platform upgrade spanning ingestion, discovery, admin operations, and UI polish:
- Unified multi-source fetch pipeline with durable output artifacts and diagnostics.
- Admin-managed source lifecycle with discovery, approval/rejection, and rejected-source restore.
- Enhanced job source coverage strategy with ATS/static adapters and probe hardening.
- Jobs/Saved auth module refresh and listing/filter UX improvements.
- Frontend maintainability refactor:
  - local-data internals split into domain modules behind a stable facade
  - controller/service/state-sync boundaries tightened
  - dispatch transition coverage expanded for core UI mutations
  - non-essential utility/config globals removed

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
- Frontend test gates now include both:
  - Node unit tests for dispatch/state-transition and local-data service contracts
  - Playwright smoke regression suite for jobs/saved/admin core flows
- Manual sanity checks confirm:
  - outputs generated (`jobs-unified.*`, fetch/discovery reports)
  - admin actions mutate source registry state correctly
  - listing, auth, filtering, and pagination remain functional after UI updates

### Notes for Reviewers
- Scope is additive and local-ops focused.
- Main risks are external dependency variability (ATS endpoint changes, anti-bot policy changes), mitigated by explicit reporting and non-blocking execution paths.
