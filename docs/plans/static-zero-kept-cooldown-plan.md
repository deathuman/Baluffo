# Static zero-kept cooldown plan

> - **Class:** enhance-stable
> - **Trigger:** the 2026-11-03 full-population run (2,539 sources) measured 2,013 zero-kept static boards consuming 63.41M ms, or 66.5% of all source work; 1,563 boards with three or more consecutive zero-kept runs consumed 51.13M ms, or 53.6%.
> - **Verified against:** `5a34b5f4`
> - **Status:** Planned; not started — the cooldown itself is unimplemented and the constants in §3 are still unapproved. Container `0.3.032` ships the Admin bridge availability fixes recorded in §11 instead.

## 1. Objective

Stop re-downloading static boards that have repeatedly produced zero jobs. This is a
**selection cooldown**, not a fetch-budget change and not a registry deletion: the board
stays registered and visible, but is skipped for 30 days after three or more consecutive
zero-kept runs. A recheck after the cooldown either clears the condition (the board kept
jobs) or starts another cooldown.

This slice deliberately does **not** include the other levers from the run review: the
zero-yield budget fix (P1), task-state write throttling (P2), ETA correction (P3), browser
fallback/Scrapy changes (P4), or worker-count changes. It is the P0-only implementation
plan.

## 2. Baseline and evidence

Completed run: started `2026-11-02T23:22:50Z`, finished `2026-11-03T01:56:39Z`
(2h33m49s end-to-end). Fetch phase 8,968.5 s (2h29m28s); setup 22.4 s; merge 3.9 s;
finalize 374.4 s (dedup 2m13s, identity 1m13s, lifecycle 1m, outputs 1m32s, audits 16s).

| Measure | Baseline |
|---|---:|
| Sources selected / completed | 2,539 / 2,539 |
| Source status | 2,048 ok, 491 error, 5 excluded |
| Output | 84,069 merged input → 44,035 → **39,562 jobs** |
| Total source work | **95.36M ms** (26.5 source-hours) |
| Fetch wall with 12 workers | 8,968.5 s; 10.63 concurrent sources; 89% of 12 lanes |
| Fetcher CPU average | 0.745 core |
| Static rows / work | 2,513 / **91.14M ms (95.6%)** |
| Static kept | 4,892 jobs (11% of output) |
| Static zero-kept | 2,013 / **63.41M ms (66.5% of all work)** |
| Zero-kept with ≥3 consecutive runs | 1,563 / **51.13M ms (53.6%)** |
| Zero-kept with ≥10 consecutive runs | 1,356 / 41.43M ms (43.4%) |
| Zero-kept with ≥20 consecutive runs | 1,201 / 32.70M ms (34.3%) |
| Zero-kept, `health=broken` | 1,634 / 60.17M ms (94.9% of zero-kept work) |
| Zero-kept, `zeroKeptClassification=needs_review` | 1,665 / 51.80M ms (81.7%) |
| Zero-kept, `broken_extraction` | 340 / 11.52M ms (18.2%) |
| Zero-kept, `legit_empty` | 3 / 84.8k ms (0.1%) |
| `lastSuccessAt` non-empty | 1,938 / 2,013; median age ~148 days |
| Existing empty-source window | 12 h (`DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES = 720`) |
| Existing dead-source window | 24 h (`DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES = 1440`) |
| Incremental/cadence skips this run | **0 / 0** |

The existing windows cannot reach this population: at selection time the boards were
centuries of hours past their last success, so `get_incremental_cache_decision` returned
`run_now`/`static_refresh_due`. The run selected 2,539 sources and skipped none through
TTL or cadence. `respectSourceCadence` was false, and enabling it would not help because
it keys off a recent `lastSuccessAt`.

Supporting evidence: [`docs/snapshots/fetch-pipeline-stall-attribution-2026-10-30.md`](../snapshots/fetch-pipeline-stall-attribution-2026-10-30.md)
(81.7% of static source wall is HTTP; browser prepare 5.6%) and
[`docs/snapshots/fetch-pipeline-zero-kept-revalidation-2026-10-24.md`](../snapshots/fetch-pipeline-zero-kept-revalidation-2026-10-24.md)
(the existing revalidation lane covers only the boards that already have a recent success
or a validator; it is not this cooldown).

## 3. Policy

### 3.1 Predicate

A selected source is skipped only when **all** of the following hold:

1. It is static: the name starts with `static_source::` or its report metadata adapter is
   `static`.
2. `lastKeptCount <= 0` (canonical counter read via
   `src/shared/source_counter_aliases.read_counter`).
3. `consecutiveZeroKept >= 3`.
4. `zeroKeptClassification` is not `legit_empty`.
5. `failureBucket` is not `no_openings`.
6. A last-attempt timestamp exists — prefer `lastSeenInFetchAt`, then `lastRunAt`, then
   `lastCheckedAt` — and it is **less than 30 days old**.

The cooldown is therefore a rolling 30-day window from the last real attempt. If the
timestamp is missing, do not skip: fetch once so the state can record it.

**Do not use `health == "broken"` as the gate.** A policy exclusion is written through
the normal excluded-source path, where `derive_source_health_fields` maps
`lastStatus=excluded` to `health=unknown`. The durable evidence is the persisted
`consecutiveZeroKept` and `zeroKeptClassification`, not the current health label. The
baseline's `health=broken` cohort is the reason this policy is worth doing, not the state
key the next run can rely on.

`legit_empty` and `no_openings` are excluded because they are honest empty boards, not
broken ones. Their measured cost is negligible (3 boards, 84.8k ms).

### 3.2 Constants and overrides

- `ZERO_KEPT_COOLDOWN_MIN_STREAK = 3`
- `ZERO_KEPT_COOLDOWN_DAYS = 30`
- `BALUFFO_ZERO_KEPT_COOLDOWN=0` is a global kill switch (rollback without a code change).
- `BALUFFO_ZERO_KEPT_COOLDOWN_FORCE_SOURCES=<comma-separated names>` forces named boards
  back into the run for a manual review.
- `force_refresh_all=True` bypasses the policy entirely (existing CLI/bridge behavior).
- Any future kept job clears the condition naturally; no separate reset is required.

The 30-day duration is chosen because these boards had a median last-success age of
~148 days and are not newly empty. A shorter 7-day window is fresher but may expire before
the next manual run, producing no saving on that run. The open decision is recorded in
§8; the implementation should make the duration a module constant, not a scattered
literal.

### 3.3 Exclusion report

Use the canonical builder
`src/jobs/state_source_records.py::build_excluded_source_report` (line ~607) with:

- `exclusionReason = "zero_kept_cooldown"`
- `status = "excluded"`, `fetchedCount = 0`, `keptCount = 0`, `durationMs = 0`
- additive evidence fields: `consecutiveZeroKept`, `zeroKeptClassification`,
  `lastSeenInFetchAt`, `zeroKeptCooldownUntil`.

No registry/tombstone mutation. The excluded row is the audit trail and the manual
review surface.

## 4. Implementation design

### 4.1 New leaf policy

Create `src/jobs/static_zero_kept_policy.py`:

- `should_skip_static_zero_kept_source(entry, *, now, cooldown_days=..., min_streak=...) -> tuple[bool, str]`
- `build_zero_kept_exclusion_report(...)` (or have the caller pass
  `build_excluded_source_report` and only add evidence fields)
- a small summary builder `build_zero_kept_cooldown_summary(...)`.

Use `read_counter` for counter aliases, `parse_datetime` for the clock, and
`clean_text`/`norm_text` for fields. Keep the leaf free of composition-root imports, per
[`AGENTS.md`](../../AGENTS.md) code boundaries.

### 4.2 Selection wire-up

Add `apply_zero_kept_static_exclusions(...)` alongside the existing selectors in
`src/jobs/pipeline_loader_selection.py` (the file already owns
`apply_dynamic_redundant_static_exclusions` at ~line 146,
`apply_source_cadence_exclusions` at ~261, and `apply_incremental_cache_exclusions` at
~300). It receives the selected loaders, `source_state_rows`, `source_report_meta`,
`force_refresh_all`, and the excluded-report builder.

Call it from `src/jobs/pipeline_run_setup.py` after the circuit-breaker exclusions
(~line 635) and before the task runtime is initialized (~line 660), so the new excluded
rows are part of the run's source reports and selected-source count. The existing
incremental/cadence exclusions are at ~596 and ~613; this policy is independent of both
and runs after them.

Update:

- `source_reports.extend(zero_kept_skipped)`
- `runtime_payload["selectedSourceCount"] = len(selected_loaders)`
- `runtime_payload["zeroKeptCooldown"] = summary` (enabled, minStreak, days,
  skippedCount)
- the `applying_exclusions` progress counts with `zeroKeptSkippedSources`.

### 4.3 State semantics (the part that makes the cooldown actually expire)

A policy skip must not teach the state that the board was fetched:

- Add `zero_kept_cooldown` to `NON_FETCH_SKIP_REASONS` in
  `src/jobs/state_incremental.py` (~line 44), so
  `refresh_next_eligible_check_at` does not advance the board's freshness deadline.
- Verify `lastSeenInFetchAt` remains the last real attempt. `_copy_report_fields` in
  `src/jobs/state_source_state.py` (~line 150) sets `lastRunAt`/`lastCheckedAt` for an
  excluded row, but `derive_source_health_fields` in
  `src/jobs/state_source_records.py` (~line 303) prefers an existing
  `lastSeenInFetchAt`; pin this with a test. If the field is absent, the first skip may
  start the clock; that is acceptable and still expires.
- `apply_excluded_source_state` (~line 926 in `state_source_records.py`) only refreshes
  clocks for a 304; do not extend it for this reason. The board was not fetched.
- The excluded row may read `health=unknown` on the next run. That is a display
  consequence of `lastStatus=excluded`; the cooldown predicate uses the persisted
  classification/streak. Add a test that a second selection still skips after the first
  exclusion wrote the row.

Do **not** add a new persisted state key for this slice. The existing fields are enough
and the state normalizer stays unchanged.

### 4.4 Reporting and failure accounting

- The excluded row is already carried by `build_excluded_source_report` and the report
  normalizer's `exclusionReason` field; no new schema is needed.
- Audit `src/bridge/task_failure_attempts.py::EXPECTED_FETCH_EXCLUSION_REASONS` (~line
  42). If the new reason would be counted as a hard failure anywhere, add it to the
  expected-exclusion set; otherwise leave it alone. Do not weaken hard-failure counting.
- The UI should display the reason like other excluded reasons. Confirm the existing
  Jobs/Admin excluded-source surface renders it; no new component is planned.

## 5. Ordered work

| # | Work | Files | Gate |
|---|---|---|---|
| 1 | Add the pure policy leaf and constants | `src/jobs/static_zero_kept_policy.py` | Unit tests for every predicate |
| 2 | Add the selection helper and wire it into setup | `src/jobs/pipeline_loader_selection.py`, `src/jobs/pipeline_run_setup.py` | Selection tests; selected count and excluded rows assert |
| 3 | Preserve the cooldown clock and add the skip reason to non-fetch reasons | `src/jobs/state_incremental.py`, `src/jobs/state_source_records.py` tests | State test: skip does not advance `lastSeenInFetchAt`; second run still skips |
| 4 | Add report/runtime summary and audit the failure classifier | `pipeline_run_setup.py`, `bridge/task_failure_attempts.py` | Report row has reason; no hard-failure increase |
| 5 | Box acceptance run | external | Output and wall measurements below |

## 6. Tests

Add `tests/test_jobs_zero_kept_static_cooldown.py` and extend the existing pipeline
incremental tests. Cover at least:

- streak 2 → run; streak 3 → skip.
- `lastKeptCount > 0` → run (never skip a board that has jobs).
- `zeroKeptClassification=legit_empty` → run.
- `failureBucket=no_openings` → run.
- `lastSeenInFetchAt` 29 days old → skip; 31 days old → run.
- missing `lastSeenInFetchAt`, `lastRunAt`, and `lastCheckedAt` → run.
- `force_refresh_all` → run.
- `BALUFFO_ZERO_KEPT_COOLDOWN=0` → run.
- force-source env → named board runs.
- a non-static/provider source with the same counters → not considered.
- after a policy exclusion, a second selection at day 10 still skips and the excluded
  row carries `zero_kept_cooldown`; at day 31 it runs.
- a kept-job success clears the condition.

Focused command (global Python; the repo `.venv` is not usable on Windows):

```bash
python -m pytest \
  tests/test_jobs_zero_kept_static_cooldown.py \
  tests/test_jobs_fetcher_pipeline_incremental.py \
  tests/test_state_incremental_empty_streak.py \
  tests/test_jobs_dynamic_static_suppression.py -q
npm run lint:repo-guardrails
```

Also run the existing source-health/report tests touched by the excluded-row fields, and
keep `git status data/` clean.

## 7. Acceptance

**Expected selection:** 2,539 → roughly 976 selected on the first run after the cooldown
starts (the 1,563-board cohort is skipped; other exclusions are already separate). Exact
counts depend on the cooldown clock and on boards that changed since the baseline.

**Expected work:** 51.13M ms removed at the ≥3 threshold. At the observed 10.63
concurrency that is about **80 minutes** of fetch wall; a conservative target is a fetch
of **1h10–1h30** and a total pipeline of **1h15–1h35**, versus 2h29/2h34 in the
baseline. The 1,634-board `health=broken` cohort is the upper bound (60.17M ms, ~94
minutes); the 1,563-board cohort is the implementation target.

**Output:** the baseline cohort kept 0 jobs, so the first run should be within 1% of
39,562 output. Any material drop must be reviewed: identify the skipped board, inspect
its current live listing, and force it back with
`BALUFFO_ZERO_KEPT_COOLDOWN_FORCE_SOURCES` or `--force-refresh-all`. Do not silently
accept a loss.

**Cooldown proof:** a policy skip does not advance `lastSeenInFetchAt`; a second run
inside 30 days skips again; a run after 30 days re-fetches. A forced board runs
immediately.

**Full-refresh control:** `--force-refresh-all` still selects all 2,539 and produces the
full source report; no stale cooldown overrides it.

## 8. Risks and rollback

- **A board reopens during the cooldown.** This is the coverage tradeoff. A 30-day
  window, a visible excluded row, and the force-source env limit the blast radius; a
  shorter window can be chosen in §8. The policy does not delete the board or its state.
- **The clock is advanced by a skip.** The state tests above pin this; without them the
  cooldown would never expire and the board would be invisible forever.
- **A policy skip erases health.** The next run may show `health=unknown`; the report
  keeps `zeroKeptClassification`/`consecutiveZeroKept`, and the cooldown predicate does
  not depend on health. Do not “fix” this by making the policy skip look like a fetch.
- **Unexpected output loss.** Stop, inspect the excluded rows, and use the force list.
- **Rollback:** set `BALUFFO_ZERO_KEPT_COOLDOWN=0`; no registry or schema rollback is
  needed. The excluded rows remain as historical reports only.

## 9. Out of scope

P1 zero-yield budget accounting, P2 task-state write throttling, P3 ETA correction, P4
browser fallback/Scrapy, worker-count A/B, finalize/setup optimization, and any registry
deletion or tombstoning. Those have separate evidence and should not be mixed into this
change.

## 10. Open decision

The only product decision is the cooldown duration and cohort:

- **Recommended:** 30 days for static boards with ≥3 consecutive zero-kept runs, and
  non-`legit_empty` classification. This is the measured 1,563-board/51.13M ms cohort.
- **Fresher alternative:** 14 days; higher coverage, roughly half the savings if the next
  run lands inside the window.
- **Narrower alternative:** only `health=broken` boards (1,634/60.17M ms), but the
  predicate still has to use the persisted streak/classification because the exclusion
  itself changes health to `unknown`.

Approve one of these before implementation. The rest of the plan is independent of the
choice.

## 11. Release vehicle: container 0.3.032

The cooldown in §§3–8 is **not** in this release. `0.3.032` carries the Admin-page
bridge availability fixes instead: the page flipping between "Bridge Reconnecting" and
connected every few seconds, while every other route answered in milliseconds, was
diagnosed in the same session, and that fix is what needs to reach the Umbrel install
now. The cooldown stays parked here with its policy constants still unapproved.

### 11.1 What 0.3.032 ships

| Fix | Files | Effect |
|---|---|---|
| Storage health is bounded and cached | `src/storage/baluffo_store.py`, `src/bridge/storage_health.py` | `/ops/storage-health` no longer waits on the writer lock or on another thread's store open; it answers from a 5-second single-flight cache and reports `healthSource`, `healthTiming`, `healthLockBusy` |
| A slow read keeps its last good body | `src/container_gateway.py` | a 504 or proxy timeout on that route serves the last good payload with `stale: true`, `gatewayFallback: true`, bounded to 5 minutes |
| A failed diagnostic is no longer "bridge offline" | `frontend/admin/app/runtime/actions.js`, `runtime.js`, `action-center.js` | the Action Center's storage poll opts out of reachability attribution; real bridge failures on `/ops/fetch-kpis` and `/sync/status` still count |
| The badge returns to its real state | `frontend/admin/app/ops/bridge-reconnect-banner.js` | the pre-wait state class and label are snapshotted and restored, so "Bridge Reconnecting" cannot outlive the wait |

### 11.2 What it does not ship

- The zero-kept cooldown (§§3–8): no selection, state, or reporting code changes.
- Any registry, state-database, or schema change. The release is additive payload and UI
  fields, so a rollback needs no data migration.

### 11.3 How the container is produced

1. `python scripts/bump_version.py 0.3.032` moved `src/app_version.py`,
   `deathuman-baluffo/umbrel-app.yml`, `deathuman-baluffo/docker-compose.yml`,
   `docs/CHANGELOG.md`, and `release-notes.md` together; the store manifest's
   `releaseNotes` was rewritten in the same step.
2. The bump must be committed **with** the shipped fixes: `container_version_policy`
   fails when `src/**` lands after the last bump without either. Do not land the bump in
   an earlier commit and do not amend it into the shipped one; put
   `Release-tag: v0.3.032` on the shipped commit if the bump and the code cannot share
   one.
3. Pushing `main` runs `Build Container`, which publishes
   `ghcr.io/deathuman/baluffo:0.3.032` and `:latest` for `linux/amd64` and
   `linux/arm64` from the same `APP_VERSION`.
4. Confirm both manifests exist before touching the box, and record the multi-arch
   digest ([`docs/RELEASE.md`](../RELEASE.md), container checklist step 6).

### 11.4 Updating the Umbrel app

1. Umbrel → App Store → Baluffo → **Update**. `deathuman-baluffo/docker-compose.yml`
   pins `ghcr.io/deathuman/baluffo:0.3.032`, so the update pulls the new image; when the
   store does not offer it, uninstall/reinstall preserves `${APP_DATA_DIR}/data` (the
   `/data` mount).
2. The restart is itself part of the fix: the reproduced `8.0–11.3 s` 504s on
   `/ops/storage-health` came from the process that was running before the fix existed,
   and its wedged threads do not clear on their own.

### 11.5 Acceptance on the box

```bash
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{http_code} %{time_total}\n' \
    http://192.168.50.61:8877/ops/storage-health
done
```

- All ten probes `200`, each well under a second; no `504` and no `bridge_degraded`.
- The payload reports `healthSource: live` on the first call and `cached` on the next,
  with `healthTiming.lockWaitMs` bounded and `healthLockBusy` false in the steady state.
- `/ops/performance-profile` lists the route with a real duration instead of omitting it.
- Admin page left open for three minutes: the badge text always matches its state class,
  the reconnect banner stays hidden, and the browser network log shows no failed
  `/ops/storage-health`.
- If a probe is still slow, `healthLockBusy` and `healthTiming` in the payload say whether
  it is the writer lock or the store open — the next thing to fix, not a reason to
  re-diagnose from the browser.

### 11.6 Rollback

Reinstall `0.3.031` in Umbrel; that image stays published. The state database and the
source registry are untouched by this release, so no data step is involved.
