# Availability Direct-Enforcement Promotion — 2026-08-27

> - **Status:** Promotion applied — **live and enforced on 0.2.140** as of 2026-08-28 (container `192.168.50.61` updated via the private app store; `appVersion 0.2.140`, `health healthy`, `startupReady true`); direct-enforcement behavioral confirmation pending the next auto pipeline run (scheduled `2026-08-28T03:17:28+02:00`)
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
- **Deployment approach: shipped in the 0.2.140 container release**, not hand-applied to the live host. The flag rode the versioned update — `scripts/bump_version.py 0.2.140` (commit `f46baa4c`, 2026-08-27) synced `src/app_version.py`, `umbrel-app.yml`, and the compose `image:` tag to `0.2.140`; the GitHub `Build Container` workflow published `ghcr.io/deathuman/baluffo:0.2.140` + `latest` (multi-arch linux/amd64 + linux/arm64; digest `sha256:926817f5c85ede3e23c6a97ae2ce01358376f768f499008e61e5371974405462`, run #201). This keeps the enforcement change store-managed and versioned instead of a one-off compose edit that future app updates could overwrite. Remaining operator step: standard private app-store update on the Umbrel applies the new compose carrying the flag. Until the update is installed the live container stays in shadow mode (flag unset) — no behavior change is live yet.

## Post-promotion reconciliation

- Pre-reconciliation state preserved: the container `/data` volume is the rollback substrate (automatic rollback limited to schema, identity, write, or feed-integrity failures per the plan), plus the captured artifacts in `_out/availability-promotion-2026-08-27/` (light feed, availability history, fetch report, run history) and the operator Saved-page Export Backup.
- Source-health-aware reconciliation = the next scheduled pipeline run's finalization (availability projection with conservative aging for failed/skipped sources, sweep consumption, direct checkpoints advancing). Verification: `sweepCoverage.mode` leaves `shadow`, direct 7-day coverage grows from the 4.2% baseline, no false mass-unavailable wave (compare availabilitySummary counts against the baseline above), saved-page digest clean.

## Residual risks

- Direct enforcement increases outbound direct-check traffic (bounded by the existing sweep caps: 1,000 checks/run, 25/domain).
- LinkedIn-sourced rows remain ambiguous to any HTTP checker (always 200); the classifier's structured-evidence rules already treat generic roots and non-posting pages as ambiguous rather than definitive.
- Desktop runtime is not covered by this promotion; it stays in shadow mode until a separate operator decision.
- **Observation field caveat:** `sweepCoverage.mode` is hardcoded to `"shadow"` (`src/jobs/availability_schedule.py`) and `availabilityHealth.shadowClassifier` is hardcoded to `true` (`src/jobs/pipeline_finalize.py`) — neither reflects `BALUFFO_AVAILABILITY_DIRECT_ENFORCE`. Live verification must use the container env check plus behavioral signals (direct 7-day coverage climbing from the 4.2% baseline after the next scheduled run). A follow-up could make these fields reflect the enforce state.

## Post-update live verification (2026-08-28, after app-store update to 0.2.140)

- `/ops/health`: `appVersion 0.2.140`, `status healthy`.
- `/ops/dashboard-health`: `startupReady true`, `status healthy`, `bridgeAlive`;
  pipeline scheduler configured (enabled, `intervalHours 11`, next run `2026-08-28T03:17:28+02:00`);
  registry sync healthy (`lastSyncStatus ok`, active 2305 / pending 866, conflict 0).
- `/tasks/run-jobs-pipeline-status`: idle, `appVersion 0.2.140`, `gatewayReady true`.
- Capture artifacts: `ops-health-post-02140.json`, `dashboard-post-02140.json`,
  `pipeline-status-post-02140.json`, `ops-history-post-02140.json`, `fetch-report-post-02140.json`
  (the fetch report still shows pre-update run `fetch_946c8f4b9c`, mode `shadow`, direct7d 1,774 —
  **expected**: the report only updates on the next pipeline generation after the update).
- Flag verification note: the availability-enforcement env (`BALUFFO_AVAILABILITY_DIRECT_ENFORCE`)
  is not observable over the public HTTP surface (all `jobs-availability-*` artifacts are private/404
  by design). The compose applied by the 0.2.140 app-store update carries `"1"`, and the service reads
  it at construction. Decisive behavioral confirmation = next run's `directCheckedWithinSevenDaysCount`
  climbing from 1,774 and availability counts adjusting without a false mass-unavailable wave.
