## [0.2.136] - 2026-08-25

> Shared Desktop + Umbrel Admin review-panels UX release: registry-conflict
> paging and search, dedup evidence readability, source-policy bulk actions,
> discovery lane honesty, and Ops tab/filter URL persistence.

### Added

- `/registry/conflicts` GET supports optional additive paging params
  (`limit`, `offset`, `queue`). When any param is present, conflict cards are
  sorted by `reviewPriority`/`reviewQueue`/`familyKey`, the response gains
  `returnedCount`, and `summary.conflictCount` stays the untouched total;
  without params the payload is unchanged. The Admin Registry Conflicts panel
  now loads 50 cards per page with a "Show 50 more" footer, a family/source
  text search, and P0/P1-only auto-expanded groups.
- Source Policy Review supports bulk acknowledge/snooze: checkbox selection
  persisted across poll re-renders, "Acknowledge selected"/"Snooze selected"
  actions reusing the existing per-pair review-action route, one summary
  toast, and in-flight double-submit protection.
- Discovery Review candidate lanes show honest "showing X of N" counts with
  per-lane "Show 10 more" expansion (Ops panel only; the read-only registry
  page preview stays static).
- Ops tab selection and Registry Conflicts triage/queue/search filters persist
  in the URL hash and restore on page load.
- The Registry Conflicts action strip highlights the first conflict-source
  check as the recommended step when conflicts are queued but no check has
  ever run.

### Changed

- Dedup Lists suppress zero-count buckets across all count summaries, gate
  metric chips, and the merge-reason line ("none" fallback), and raise
  evidence-table/example caps from 5 to 10 rows. The dedup review-queue table
  replaces its single semicolon-joined evidence string with labeled
  per-row evidence disclosures.
- Source Policy Review rows keep the first five metadata fields inline and
  collapse the rest behind "More details" disclosures (pair rows, migration
  candidates, blocked candidates, linked identities, suppression eligibility).
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Removed

- Unused plain-text `formatDedupAuditGate` dedup gate formatter and its
  re-export.
