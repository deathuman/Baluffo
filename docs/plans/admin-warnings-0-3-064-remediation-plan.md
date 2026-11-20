# Admin warnings remediation plan — live container 0.3.064

> - **Class:** shipped-defect + live operational evidence
> - **Trigger:** `http://192.168.50.61:8877/admin.html` polled 2026-11-18T09:00Z on container
>   `0.3.064` (`fc92fbfe`, built 2026-11-17T08:54:57Z): two Action Center warnings
>   (`storage_unhealthy`, `browser_fallback_health`) and a storage-reclaim panel reading
>   "Auto-reclaim is not armed yet" beside "not bloated enough to shrink right now"
> - **Verified against:** `7f04f83c` (every claim below is read from the live bridge payloads and
>   the tree; nothing here has been re-tested against the container after a change)
> - **Status:** Active — W1–W3 and B1–B2 implemented in the tree (2026-11-18); B3 host evidence is
>   operator-run; R1 needs the next container release; C1 waits on a working browser lane.

## What the live poll shows

| Surface | Value |
|---|---|
| `/ops/fetch-kpis?view=summary` | `status: "warning"`, `alertBasis: "history+storage+browser"`, `suppressedAlertsCount: 0`, two alerts |
| `storage_unhealthy` (warning) | "WAL checkpoint could not finish — 883.4 MB WAL on 519.8 MB database; attempt 2026-11-17T20:43:02.726852Z", `value: "busy"` |
| `browser_fallback_health` (warning) | "Browser fallback environment blocked at navigation for 239 sources (TargetClosedError)…", `value: 239`, `triggeredAt: 2026-11-18T07:44:43Z` |
| `/ops/storage-health` | `databaseBytes: 272695296` (260.1 MiB), `walBytes: 0`, `freelistBytes: 0`, `healthy: true`, `unhealthyReason: {}`, `quickCheck: "deferred-large-database"` (cap 67 108 864) |
| `/ops/storage-health` `walMaintenance` | `status: "busy"`, `mode: "PASSIVE"`, `attemptedAt: 2026-11-17T20:43:02.726852Z`, `busy: 1`, `logFrames: -1`, `complete: false`, `detail: "passive-incomplete"`, attempt sizes `walBytes: 926311992` / `databaseBytes: 545017856`; `checkpointThresholdBytes: 67108864`, `truncateThresholdBytes: 268435456`, `passiveCheckpointBusyRetries: 27` |
| `/ops/storage-reclaim` | `verdict: "ok"`, `ratio: 0.0`, `reclaimable: 0` (historical 0 + freelist 0), `databaseBytes: 272695296`, `currentGenerationEstimatedBytes: 97965639`, `freeBytes: 1534029024256` (1.39 TiB), `requiredBytes: 335544320`, `blockers: ["auto-disabled"]`, `autoStart: {enabled: false, switch: "unset", armed: false, reason: "not-armed", evidence: null}` |
| `kpis.sourceOutcomeSummary` | `totalSources: 2465`, `failedSources: 435` (17.65 %), `excludedSources: 426`, `zeroKeptSources: 1547`, `zeroKeptNeedsReviewNonFailedSources: 1105`, `browserFallbackRecommendedSources: 61`, `browserFallbackAttemptedSources/Failed/EnvironmentBlocked: 239/239/239`, `providerChildFailureCount: 129` |
| `kpis.browserFallbackDemand` | **all zeros** — `attempts 0`, `refused 0`, `pages 0`, `servedWithHtml 0`, `servedEmpty 0` |
| `browserFallbackHealth.forensics` | cgroup v2 `memoryMax: "max"`, `memoryCurrent: 1.33 GB`, `memoryPeak: 4.42 GB`, `oom`/`oom_kill` 0 before/after/delta; `/dev/shm` 64 MiB, 0 used; `browserStderr: {available: true, source: "chromium_log_file", bytes: 0}`; `disconnect: {observed: true, at: 07:44:43.562Z, duringProbe: true, intentional: false}` |
| `/ops/dashboard-health` | `appVersion: 0.3.064`, registry `active 2941 / pending 865 / rejected 104`, `duplicatePendingCount: 1`, `conflictCount: 0` |
| `/sync/status` | `enabled: true`, `state: "ready"`, `lastPush 08:46Z ok`, `lastResult: "ok"` — no sync warning |

No signal is critical, and the store is healthy. Both warnings are wrong in the same way: each
reports a condition that has since ended, or that never started where its label says.

## Issue 1 — the storage warning quotes a checkpoint that already succeeded

### What happened

1. At 2026-11-17T20:43Z the WAL was 926 311 992 B (883.4 MiB) beside a 545 017 856 B (519.8 MiB)
   database, so `schedule_wal_maintenance_if_needed` chose **PASSIVE** (below the 256 MiB truncate
   threshold, above the 64 MiB checkpoint threshold) and `_run_wal_maintenance` recorded
   `status: "busy"` — `_checkpoint_pass` returned `busy: 1`, `logFrames: -1`, after 5 150 ms and
   27 busy retries.
2. Twelve hours later the live WAL is **0 bytes**, the database is 260.1 MiB, `healthy` is true and
   `unhealthyReason` is empty, so the condition the record describes is gone.
3. The warning still fires because `storage_health_alert` (`src/bridge/ops_health.py`) warns whenever
   `walMaintenance.status` is `busy`/`failed` and the store is healthy, with no age and no live-size
   test. The frontend carries the same predicate: `evaluateStorageHealth`
   (`frontend/admin/app/action-center.js:218-241`) treats `walStatus === "busy" || "failed"` as
   `walNeedsAttention`.
4. The message is worse than stale: `_storage_size_copy` (`src/bridge/ops_health.py`) prefers the
   record's `wal.walBytes`/`wal.databaseBytes` over the live `storage.walBytes`/`databaseBytes`, and
   `walCheckpointCopy` (`frontend/admin/app/action-center.js:197-209`) does the same. So the panel
   claims a 883 MB WAL on a 520 MB database while both live files are 0 B and 260 MiB.
5. The record cannot age out. `_last_checkpoint` is written only by `_run_wal_maintenance` and
   `_record_deferred_maintenance`; `schedule_wal_maintenance_if_needed` returns early once
   `walBytes < wal_checkpoint_threshold_bytes`, so a WAL emptied by another path produces no new
   attempt and no new record. The other checkpoint paths never write it: the bulk-delete final
   checkpoint (`src/storage/baluffo_store.py:666`, `checkpoint_required("TRUNCATE")`),
   `install_rebuilt_database`, `backup_to`, and `vacuum`.

### Cost

The operator cannot separate "a checkpoint failed 12 h ago and was resolved" from "the WAL is
failing to checkpoint now", and this warning outranks real work in the panel on every poll. The
0.3.062 remediation deliberately kept a busy checkpoint visible (S1.2/S1.3) because a healthy-but-
busy store was the original 1 GB-WAL symptom; it never gave the record a lifetime.

### Remediation

**W1 — gate the alert on the live condition.** Warn only while `storage.walBytes` is still at or
above `walMaintenance.checkpointThresholdBytes` with an incomplete last attempt, or while the store
is `healthy: false`. A WAL that has since fallen below the threshold raises nothing. Same predicate
on both surfaces (`src/bridge/ops_health.py`, `frontend/admin/app/action-center.js`).

**W2 — make the record current.** Record a completed checkpoint in `_last_checkpoint` from every
path that completes one — a small `_record_checkpoint_result` called by `checkpoint_required`, with
`_run_wal_maintenance` still adding its duration and before-sizes. A TRUNCATE from bulk delete,
`install_rebuilt_database`, `backup_to`, or the post-serve pass then replaces `busy` with `ok` and a
fresh `attemptedAt`, so the record can never outlive the WAL it describes.

**W3 — say what is live and what is history.** Prefer the live sizes in both copy helpers, append
the attempt's sizes only when they differ, and add `attemptAgeS` to the record so the frontend does
not parse timestamps: "WAL checkpoint could not finish 12 h ago (busy) — now 0 B WAL on 260 MB
database; the attempt saw 883.4 MB WAL on 519.8 MB".

**Acceptance.** The captured payload renders no storage signal or alert on either surface; a live WAL
at or above the threshold with a busy/failed attempt still renders the warning with live sizes; a
later successful checkpoint flips the record to `ok`; no existing storage or Action Center test is
weakened.

**Pinned by.** `tests/admin/test_admin_bridge_ops_health.py` (add the captured payload as a
negative case and a 900 MB positive case), `tests/storage/test_baluffo_store_checkpoint.py` (record
currency), `tests/frontend/unit/admin-action-center-signals.test.mjs`.

## Issue 2 — the browser lane was refused wholesale, and the evidence to name the cause is missing

### What the numbers prove

`browserFallbackDemand` is all zeros while `browserFallbackAttemptedSources ==
browserFallbackFailedSources == browserFallbackEnvironmentBlockedSources == 239`. That is exactly
the preflight-blocked shape the 0.3.062 S2.5 fix introduced: two failed health probes mark the run
`browserFallbackEnvironmentBlocked`, the lane is replaced by short-circuit wrappers, and 239 source
rows record the "environment blocked" error without a per-source attempt. So this is **one
environment failure, not 239 crashes** — the ambiguity the 0.3.062 plan set out to remove is gone,
and it says the reader produced no HTML for 239 of the 61+ sources that wanted it.

### What the forensics show, and what they do not

The probe died at `context.new_page()` after 1 573 ms (`TargetClosedError`), and Playwright reported
the browser's `disconnected` event 2.7 ms earlier with `intentional: false` — the browser went away,
the probe did not close it. Everything else in the death block is negative: no cgroup memory limit
(`memoryMax: "max"`), zero `oom`/`oom_kill` events across the before/after/delta snapshots, `/dev/shm`
64 MiB with 0 bytes used, and `browserStderr: {source: "chromium_log_file", bytes: 0}`.

That last field is the gap. `0.3.064` ships the pipe-backed raw-stderr sink
(`BrowserStderrSink` / `capture_browser_stderr_for_driver`), and `docs/DATA_CONTRACT.md:1715` says
`chromium_stderr` is the normal source with `chromium_log_file` only as the fallback. The container
reported the fallback with zero bytes, so the one artifact that would name the crash (crashpad /
Chromium stderr) is missing. A cgroup OOM is ruled out for the cgroup; with no memory limit the host
OOM killer is not covered by those counters.

Secondary: `stage: "navigation"` is the wrong attribution for a `new_page` failure, so the alert says
"blocked at navigation" for a browser that died before any page existed.

### Remediation

**B1 — make the sink engage, and say when it does not.** `_start_browser`
(`src/jobs/browser_fallback_pool.py:378-400`) fell back silently when
`capture_browser_stderr_for_driver` reported the hook unavailable. Record a
`browserStderr.driver` block (`reason: driver-hook-unavailable`), the resolved module
name and the installed Playwright distribution version, and resolve the module by
trying both `playwright._impl.transport` and `playwright._impl._transport` before
giving up. The local Windows probe produced `chromium_stderr` (STATE 2026-11-16), so the difference is the
image's Playwright build, not the hook design.

**B2 — attribute the stage.** A `launch`/`new_context`/`new_page` failure records
`stage: "launch"` or `"context"` with `lastLaunchError` filled, never `navigation`.

**B3 — host evidence, operator-run (no repo change).** The probe is a `data:` URL
(`BROWSER_HEALTH_PROBE_URL`), so a failing probe is never DNS, TLS or a board: it is the browser
process, its host environment, or memory. Capture in this order on the failing probe, before
restarting or recreating the container. Run these at the host console: this checkout's keys do not
open the umbrel host (SSH is refused, the Docker API port is closed). With no fresh failure, stage
one with **Reproduce the probe on demand** below.

1. **Target and window.** `docker ps --format '{{.ID}}\t{{.Names}}\t{{.Image}}' | grep baluffo` (umbrel
   names it `<app>_web_1`, e.g. `baluffo_web_1`). Record the payload's UTC stamps —
   `kpis.browserFallbackHealth.forensics.capturedAt`, `disconnect.at`, `lastFailureAt` — because the
   kernel log prints host-local time. `docker inspect <c> --format '{{json .State}}'` and
   `docker inspect <c> --format '{{.RestartCount}}'`: a restart wipes the app's in-memory death record
   and resets its cgroup counters, so read them first.
2. **Limits.** `docker inspect <c> --format '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}'`.
   This compose sets neither, so `0 0` (unlimited) is expected, and an OOM kill then has to be
   host-global; a nonzero `Memory` is what would let the container's own cgroup be the constraint.
   Nothing else in `HostConfig` changes that reading: `ShmSize` is bypassed by
   `--disable-dev-shm-usage`, and `OomScoreAdj` only biases who is chosen.
3. **cgroup.** The payload already carries this read (`forensics.cgroup`): v2, `memoryMax: "max"`,
   and `events`/`eventsBefore` with `eventsDelta`/`oomKilledDuringProbe` across the probe. Only if
   `available` is `false`, read the same files by hand:
   `docker exec <c> cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.events`. There is no host
   cgroup path to construct and no driver to detect. Under `memoryMax: "max"` a host-global kill
   leaves `oom_kill` at `0`, so `0` never rules OOM out.
4. **Kernel — the only proof of a host OOM kill.** `journalctl -k` implies `-b`, and `--since` does
   not lift that, so a host reboot hides the window: run `sudo journalctl --list-boots`, then
   `sudo journalctl -k -b 0 --since '<t-2m>' --until '<t+2m>'` (or `-b -1` for the boot that was
   running then), and grep for `oom|killed process|traps|segfault|general protection`.
   `kernel.dmesg_restrict=1` (the default on Ubuntu and Debian) blocks plain `dmesg`, and the ring
   buffer dies with a reboot — the journal is the source that can outlive it.
5. **The app's death record.** `docker exec <c> cat /data/jobs-fetch-report-summary.json` (host:
   `${APP_DATA_DIR}/data`, umbrel `~/umbrel/app-data/baluffo/data`), or the live
   `/ops/fetch-kpis?view=summary` → `kpis.browserFallbackHealth`: `stage`, `errorType`,
   `lastLaunchError`, `forensics.disconnect`, and `forensics.browserStderr` — the browser's last
   words: `source: chromium_stderr` with a tail, or `source: chromium_log_file` with
   `driver.active: false` and its reason, where the tail is empty by construction and only the
   kernel side can decide.
6. **Docker events.** `docker events --since <t> --until <t> --filter container=<c>` for `oom`,
   `die` and `restart`. An `oom` event corroborates a cgroup OOM; its absence proves nothing while
   `memoryMax` is `"max"`.

No crashpad step and no profile copy: Playwright launches with `--disable-breakpad`, so no dump
exists, and a dump could not tell an OOM kill from a crash. `/dev/shm` is bypassed by
`--disable-dev-shm-usage`, so its free space is not evidence either. The child is dead or already
relaunched by the time anyone runs these, so `docker top` and its cmdline cannot change the verdict.

**Reproduce the probe on demand.** The probe is a pool method, not a route, so it can be triggered
in the container instead of waiting for the next fetch run. Run it while no fetch is active (the
probe launches a browser, and a second Chromium is pressure the verdict should not include); it
writes nothing, so the result exists only in the command's output. Healthy:

```bash
docker exec -w /app <c> python -c 'import json; from src.jobs.browser_fallback_pool import BrowserFallbackPool as P; p = P(); print(json.dumps(p.health_probe(), indent=2)); p.close()'
```

A launch failure is that call with the browser directory broken — expect `stage: "launch"` and
`Executable doesn't exist`:

```bash
docker exec -e PLAYWRIGHT_BROWSERS_PATH=/nonexistent -w /app <c> python -c 'import json; from src.jobs.browser_fallback_pool import BrowserFallbackPool as P; p = P(); print(json.dumps(p.health_probe(), indent=2)); p.close()'
```

The live symptom is a browser that dies after the page exists and before it navigates. Stage that
death while keeping the production probe path (`_open_page`, then `goto`); expect
`stage: "navigation"`, `errorType: "TargetClosedError"`, `disconnect.duringProbe: true` and
`intentional: false`:

```bash
docker exec -i -w /app <c> python - <<'PY'
import json
import src.jobs.browser_fallback_pool as pool

p = pool.BrowserFallbackPool()

async def dying_probe(timeout_s):
    _context, page = await pool._open_page(p._browser)
    await p._browser.close()  # the death: page exists, no navigation yet
    await page.goto(
        pool.BROWSER_HEALTH_PROBE_URL, wait_until="domcontentloaded", timeout=timeout_s * 1000
    )

p._probe_page = dying_probe
print(json.dumps(p.health_probe(5), indent=2, default=str))
p.close()
PY
```

`_probe_page` and `_open_page` are private; re-read them if the pool changes. The run's preflight
calls the same `health_probe()` (`_run_browser_preflight`), so the printed payload matches
`kpis.browserFallbackHealth`.

**Which reading decides it.**

| Reading | Verdict |
|---|---|
| `journalctl -k`: `Out of memory: Killed process <pid> (headless_shell…)` with `oom-kill:constraint=CONSTRAINT_NONE`, `oom_memcg=/` | **host OOM kill**: global exhaustion, which is what an unlimited container gets |
| the same line with `constraint=CONSTRAINT_MEMCG`, `task_memcg=/system.slice/docker-<id>.scope` | **container cgroup OOM kill** (needs a `Memory` limit; not this compose) |
| `forensics.cgroup.memory.events.oom_kill` delta > 0, i.e. `cgroup.oomKilledDuringProbe: true` | cgroup OOM — but under `memoryMax: "max"` a host-global kill leaves it `0`, so `0` never rules OOM out |
| `docker inspect .State.OOMKilled: true` | the container's own init process died of OOM (a restart). The bridge runs under `init: true`, so a Chromium death never sets it; `false` proves nothing |
| `journalctl -k`: `traps: headless_shell[pid] segfault at …`, `general protection fault` | **Chromium crash** (signal 11/7), not memory pressure |
| `browserStderr.tail` has `Received signal`, `Fatal error`, `Check failed`, `GPU process isn't usable` | **Chromium crash** with its own cause (requires `driver.active: true`) |
| empty stderr tail, no `traps`, `docker events` `oom` | **kernel kill**; the OOM line should still be in the journal for the boot that was running |
| `page.goto` failed, `disconnect.observed: false`, browser still connected | **renderer/page death**, not the lane dying |

Two traps this host sets. With no memory limit, a host-global OOM kill leaves both
`.State.OOMKilled` and the container cgroup's `oom_kill` untouched, so only the kernel log proves it.
And Playwright's defaults suppress crashpad dumps and `/dev/shm`, so an absent dump or an intact
`/dev/shm` is not evidence of anything. **Do not add Chromium launch flags or a Playwright upgrade
before that lands** — the 0.3.062 non-goal still applies.

**B4 — do not read the blocked rows as boards.** 1 105 `zeroKeptNeedsReviewNonFailedSources` is not
a board finding while 239 sources were short-circuited and 61 wanted the lane.

**Acceptance.** A container health probe reports `source: "chromium_stderr"`, or reports why not; a
launch death reads `stage: "launch"`, not `navigation`; a run whose probes fail reports one
environment-blocked reason with an empty demand split, and no source row claims a per-source crash.

**Pinned by.** `tests/test_browser_fallback_health.py`, `tests/test_browser_fallback_pool.py`,
`tests/test_browser_fallback_death_breaker.py`.

## Issue 3 — the reclaim panel is right about the database and silent about the WAL

`/ops/storage-reclaim` says `verdict: "ok"`, `reclaimable: 0`, `ratio: 0.0` on a 260.1 MiB database
against a 512 MiB absolute floor, with 1.39 TiB free. The panel's "not bloated enough to shrink right
now" and the disabled Run button are therefore **correct**: a rebuild would reclaim nothing, and the
883 MB WAL was never a disk incident on this host. `autoStart.reason: "not-armed"` is 0.3.064's
state; main (commit `48eb34fb`) now rehearses an unarmed host at startup from the configured data
directory and arms auto only from an adequate completed measurement, so the panel's instruction is
satisfied by the next release rather than by a click.

The residual is presentation, not policy: `reclaim_verdict` (`src/storage/reclaim_policy.py`) reads
`databaseBytes`, `freelistBytes` and generation samples only, so nothing on that panel can explain
that "883 MB WAL" and "not bloated" describe different files. **Do not add WAL bytes to the bloat
verdict** — a rebuild does not fix a WAL; W2 and W1 are what fix a WAL.

**R1 — redeploy, then verify arming.** Ship the next release (main is `0.3.066`), then confirm
`/ops/storage-reclaim` reports `autoStart.armed: true`, `source: "rehearsal"`,
`reason: "rehearsal-adequate"`, and `measuredStagingBytes > 0` after first startup — or the recorded
`rehearsal-inadequate`/`rehearsal-failed` reason with its evidence.

**R2 — one copy line.** The storage/reclaim panel names what it measures (database,
generations, freelist) and that WAL pressure is checkpointed separately. Pin beside the existing
panel copy tests.

## Issue 4 — the coverage numbers cannot be acted on until the lane works

1 547 zero-kept of 2 465 sources, 1 105 of them needs-review and non-failed, 435 failed (17.65 %),
426 excluded, 129 provider child failures. With the browser lane blocked, every client-rendered board
necessarily kept zero, so the count is contaminated by known reader failures. Order of work: B1/B2 →
release → R1 → one real fetch → the offline half of `tools/coverage_drain.py --verify-collected`
under an isolated `--data-dir`, bucketed by `failureBucket`, before any retirement or repoint. The
0.3.062 S3 replay (1 466 zero-kept; reader-side 360, explicit board findings 2) is the calibration,
not this payload.

## Secondary observations

| Observation | Evidence | State |
|---|---|---|
| `quickCheck` never runs on this database | `deferred-large-database` against a 67 108 864 cap on 272 695 296 bytes | **Fixed:** a background verification runs on its own connection once a day, `integrityCheck` reports its result and age, and the panel says when the file was last verified |
| `tableSizes` leaves 106.9 MB (39 %) unattributed | 12 tables listed, `truncated: true`, `otherBytes: 106876001` | The breakdown cannot show what grew. `job_sources` (186 526 rows, 93 985 788 B, 34.5 %) is the largest measured table and is in the current generation, so reclaim cannot see it |
| `avgFetchDurationMs7d: 3031504` | `collect_fetch_history_metrics` divided a sum by run count | **Fixed:** the KPI now reports a median, its p90 and the sample count over measured runs, and the Admin card shows the median. The mean stays in the payload for compatibility only |
| `pendingApprovalsCount: 865`, `duplicatePendingCount: 1` | `/ops/dashboard-health` | Normal registry queue; drain through the approval path, never a bulk state edit |
| `probeLifecycle` dated 2026-11-02 | `/ops/dashboard-health` | **Fixed:** the row prints the wave's finish time, and a stale or undated receipt renders as a warning instead of “clean” |
| `sync.runtime.lastPull`/`lastPush` are empty objects | `lastPullAt`/`lastResult` are set | Known cosmetic projection gap; not an Action Center signal |

## Slices

| # | Change | Files | Pinned by |
|---|---|---|---|
| W1 | Live-condition gate on the WAL warning | `src/bridge/ops_health.py`, `frontend/admin/app/action-center.js` | `tests/admin/test_admin_bridge_ops_health.py`, `tests/frontend/unit/admin-action-center-signals.test.mjs` |
| W2 | Checkpoint record currency on every completing path | `src/storage/baluffo_store.py` | `tests/storage/test_baluffo_store_checkpoint.py` |
| W3 | Live sizes plus attempt age in both copy helpers | `src/bridge/ops_health.py`, `frontend/admin/app/action-center.js` | `tests/admin/test_admin_bridge_ops_health.py`, `tests/frontend/unit/admin-action-center-signals.test.mjs` |
| B1 | Driver-hook resolution by both module names; record the fallback reason, module and Playwright version | `src/jobs/browser_death_forensics.py`, `src/jobs/browser_fallback_pool.py` | `tests/test_browser_fallback_health.py` |
| B2 | Launch/context stage attribution | `src/jobs/browser_fallback_pool.py`, `src/bridge/ops_api_health.py` | `tests/test_browser_fallback_health.py` |
| R1 | Redeploy and verify startup arming | no code | `/ops/storage-reclaim` payload |
| R2 | Panel names what it measures | `frontend/admin/app/storage-reclaim-panel.js`, `frontend/admin/render/storage-reclaim-dialog.js` | existing panel copy tests |
| C1 | Isolated re-measure after the lane works | no code | `tools/coverage_drain.py --verify-collected` output |

Each slice is independently reversible. W2 is the only one that changes what the container stores;
W1/W3/B1/B2 change what it reports and how it attributes.

## Verification

- `python -m pytest tests/admin/test_admin_bridge_ops_health.py tests/storage/test_baluffo_store_checkpoint.py tests/test_browser_fallback_pool.py tests/test_browser_fallback_health.py tests/test_browser_fallback_death_breaker.py -q`
- `node --test tests/frontend/unit/admin-action-center-signals.test.mjs tests/frontend/unit/admin-action-center-storage-signals.test.mjs`
- `npm run lint:repo-guardrails`
- Container acceptance after the release: `/ops/fetch-kpis?view=summary` raises no
  `storage_unhealthy` on this payload shape; `/ops/storage-reclaim` shows `autoStart.armed`; a fetch
  run's `browserFallbackHealth.forensics.browserStderr.source` is `chromium_stderr`.

## Non-goals

- No board retirement, registry bulk edit, or coverage mutation from this payload.
- No Chromium flags, memory limits, or Playwright upgrade before B3's host evidence.
- No WAL term in the reclaim bloat verdict.
- No change to public job text, locations, or persisted contracts.
- No new route and no new polling interval.

## Open questions

1. When a busy record is older than the cooldown and the live WAL is below the threshold, should the
   signal disappear silently or leave a one-line "last attempt" note in the storage panel?
   (Recommendation: drop the signal, keep the record visible.)
2. Is the `--log-file` fallback still worth keeping now that it produced 0 bytes and cannot see
   crashpad output? (Recommendation: keep it as a last resort, but always record why it is in use, so
   the next report does not present an empty tail as evidence.)
