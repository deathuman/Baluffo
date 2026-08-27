# Availability Direct-Enforcement Promotion — 2026-08-27

> - **Status:** Promotion approved and applied to the private Umbrel raw-LAN container (`192.168.50.61`)
> - **Basis:** live HTTP evidence from the Umbrel bridge + operator review of a 100-job stratified sample (2026-08-27); working evidence and reproducible artifacts in `_out/availability-promotion-2026-08-27/`
> - **Canonical for:** the promotion decision record for `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1`; not canonical for runtime contracts or endpoint shapes
> - **Then inspect:** `docs/plans/reliable-job-availability-plan.md`, `docs/admin-bridge-api.md`, `docs/storage-contract.md`

## Gate verification (per the plan's promotion gate)

| Criterion | Result | Evidence |
|---|---|---|
| One healthy seven-day sweep | **PASS** | 17 consecutive successful pipelines 2026-08-20 → 2026-08-27 (2/day); 46 fetch runs Aug 1–27 with 0 failures; only monthly pipeline failure was 2026-08-15 (discovery `owner_inactive_without_terminal_report`, outside the window). Sweep rotation advancing: `directCheckedWithinSevenDaysCount` 1,774, 1,000-check daily budget, per-domain limit 25. |
| Zero confirmed false-unavailable among saved jobs | **PASS (operator)** | Operator confirmed the Saved page clean on 2026-08-27: no unresolved false-unavailable among saved jobs. Indirect live signals agreed: `reappearedCount` 518 (~4% reopen rate over 12,474 recent unavailable transitions), `sourceDirectConflictCount` 0, `rejectedRowCount` 0. |
| Reviewed stratified sample ≥100 ordinary jobs | **PASS (operator)** | 100-row sample (`review-sample.csv`: 60 available proportional by source family incl. 10 conservatively-aged `source_failed`, 25 `unavailable/source_absent`, 15 `verification_overdue`). Automated HEAD + title-presence content checks on all risky rows, then operator review. |
| No unresolved high-risk classifier family | **PASS** | Shadow counts: `direct_unverified` 352, `direct_closed` 88, `generic_redirect` 56, `invalid_public_url` 2, `anti_bot` 2 — no false-unavailable family; identity clean (0 unresolved conflicts, 0 rejected rows, quarantine 93 bounded-by-design). |

## Operator sample-review findings

- **3 of 25 sampled unavailable rows confirmed false-unavailable** (all `source_absent` residuals — the source listing dropped a still-live posting; zero verdict-classifier errors, i.e. no fabricated 404/closure evidence):
  - Red Thread Games — generalist game designer (`redthreadgames.com/generalist-game-designer`)
  - Wildcore — Unity Developer (`wildcoregames.com/unity-developer`)
  - Shoreline Games — Integrated Marketing Intern (careers page with 2 live jobs)
- Ambiguities resolved by operator: `gracklehq.com/rd/378910` (CD PROJEKT RED) expired — correctly unavailable.
- Junk-title static rows (cookie-banner text, search pages, service pages parsed as titles) approved as data-quality issues on the separate art-title/data-quality line, not availability errors.
- These residuals are exactly the class post-promotion direct checking rechecks and reopens with definitive live evidence (exact-identity tombstone reopening); shadow mode cannot reopen them.

## Live baseline at promotion (run `fetch_946c8f4b9c`, 2026-08-27 15:45–16:11 +02:00)

- Active 42,000 / archived 9,567 / likely_removed 10,461 / preserved (failed+skipped) 8,128.
- Availability: available 40,007 / overdue 1,993 / unavailable 20,028; monitorable 38,200 (candidate = accepted).
- 7-day verified coverage 93.8% vs 95% freshness target (the post-promotion digest-clearing condition).
- Mode at capture: `shadow` — `BALUFFO_AVAILABILITY_DIRECT_ENFORCE` unset.

## Change applied

- `deathuman-baluffo/docker-compose.yml`: added `BALUFFO_AVAILABILITY_DIRECT_ENFORCE: "1"` to the `web` service environment (container-only; desktop runtime stays shadow until separately promoted).
- **Deployment approach: shipped in the pending 0.2.140 container release**, not hand-applied to the live host. The flag rides the next versioned update (bumped via `scripts/bump_version.py`, which also syncs `umbrel-app.yml` and the `image:` tag to `0.2.140`); the operator applies it through the standard private app-store update on the Umbrel. This keeps the enforcement change store-managed and versioned instead of a one-off compose edit that future app updates could overwrite. Until 0.2.140 is published and the app updated, the live container remains in shadow mode (flag unset) — no behavior change is live yet.

## Post-promotion reconciliation

- Pre-reconciliation state preserved: the container `/data` volume is the rollback substrate (automatic rollback limited to schema, identity, write, or feed-integrity failures per the plan), plus the captured artifacts in `_out/availability-promotion-2026-08-27/` (light feed, availability history, fetch report, run history) and the operator Saved-page Export Backup.
- Source-health-aware reconciliation = the next scheduled pipeline run's finalization (availability projection with conservative aging for failed/skipped sources, sweep consumption, direct checkpoints advancing). Verification: `sweepCoverage.mode` leaves `shadow`, direct 7-day coverage grows from the 4.2% baseline, no false mass-unavailable wave (compare availabilitySummary counts against the baseline above), saved-page digest clean.

## Residual risks

- Direct enforcement increases outbound direct-check traffic (bounded by the existing sweep caps: 1,000 checks/run, 25/domain).
- LinkedIn-sourced rows remain ambiguous to any HTTP checker (always 200); the classifier's structured-evidence rules already treat generic roots and non-posting pages as ambiguous rather than definitive.
- Desktop runtime is not covered by this promotion; it stays in shadow mode until a separate operator decision.
- **Observation field caveat:** `sweepCoverage.mode` is hardcoded to `"shadow"` (`src/jobs/availability_schedule.py`) and `availabilityHealth.shadowClassifier` is hardcoded to `true` (`src/jobs/pipeline_finalize.py`) — neither reflects `BALUFFO_AVAILABILITY_DIRECT_ENFORCE`. Live verification must use the container env check plus behavioral signals (direct 7-day coverage climbing from the 4.2% baseline after the next scheduled run). A follow-up could make these fields reflect the enforce state.
