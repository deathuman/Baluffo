## [0.2.138] - 2026-08-25

> Follow-up patch restoring status-chip severity coloring and fixing the Ops
> Dedup badge against the live fetch-report shape.

### Fixed

- Status chip severity coloring restored: the shared `--tint-ok/--tint-warning/
  --tint-critical` variables referenced themselves (cyclic definitions), which
  invalidated every `color-mix` usage and left "Warning"/"Succeeded"/
  "Auto-Approvable" chips uncolored. Definitions now use literal colors; a
  cycle guard stops this from shipping again.
- Ops Dedup badge loads from the fetch report shape the bridge actually
  writes (top-level `dedupEvidence`) instead of assuming a `latestRun` wrapper,
  so the badge shows the real review count on first load.
- Admin CSS cache-bust bumped to v15 so browsers fetch the fixed stylesheet.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.137] - 2026-08-25

> Shared Desktop + Umbrel release fixing Admin review-panel interactions and
> styling consistency, repairing Action Center copy over plain HTTP, removing
> the redundant Ops Health refresh button, and eliminating Jobs feed
> first-load jumps.

### Fixed

- Registry Conflicts: the document-level Inspector click delegate no longer
  hijacks native `<details>` toggles, so "Decision details" and per-row
  evidence sections open again; card bodies keep opening the Inspector.
- Dedup tab badge shows a real review count from the jobs fetch report as
  soon as Ops loads (previously stuck on "..." until the tab was opened),
  with fetch-report changes invalidating the counts cache.
- Source Policy Review badge turns warning-toned only when actionable items
  exist; artifact warnings moved into the tooltip.
- Action Center "Copy all diagnostics" works over non-secure HTTP (Umbrel
  LAN) via a clipboard fallback; failure toasts only when every path fails.

### Changed

- Admin panels unify on the elevated card language (12px gradient shells):
  Action Center internals adopt alert-banner severity tints and standard
  buttons; Stored Profiles Overview matches; severity colors consolidated
  into shared `--tint-ok/--tint-warning/--tint-critical` variables, fixing
  the light-theme bulk-busy message contrast.
- Sticky Admin section nav centers its links so they clear the floating
  bridge badge at narrow widths.
- Removed the redundant "Refresh Ops Health" button; Operations Health stays
  auto-refreshed by its existing pollers.
- Conflict cards are more compact: winner-vs-loser summary line plus folded
  decision-signal and adjudication/diff disclosures; hover affordance added.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Performance

- Jobs page first-load jumps eliminated: identical list content no longer
  rewrites the DOM when boot/auth/auto-refresh paths re-render; unified feed
  mirrors race instead of chaining sequential timeouts; the guest notice
  renders visible by default so auth resolution no longer shifts layout by
  ~62px (measured CLS 0.094 -> 0.0025 locally).

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
