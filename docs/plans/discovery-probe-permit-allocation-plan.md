# Discovery probe permit-allocation plan

> - **Class:** shipped-defect
> - **Trigger:** the probe wave's lane budget was being spent on bucket queues (6.8-8.7 of 40 pool
>   permits held by blocked candidates, provider starved to 21.0 of its 40), and the acquire order was
>   priced but not shipped because the event loop was the wall at the time.
> - **Verified against:** `9b8901f8` plus the 2026-10-12 working-tree discovery changes. Items 1-4 have
>   **landed**; the transport diff and the acquire-order swap are both in the tree and covered by
>   deterministic tests.
> - **Status:** Active — items 1, 3, and 4 have landed. The observed close-under-fetch failure in
>   item 2 is fixed, and the late-entry race is proven and closed: retirement defers while a pool
>   thread may still enter, pinned by `test_probe_batch_late_entry_race.py`
>   ([the batch bounds](../scraping-pipeline.md)). Item 5's audit
>   leg has run: queue/verdict passes and wall is inconclusive. The normal discovery smoke has since
>   run as well — flow identity holds and the wall is inside the drift again; a matching `appVersion`
>   deployment remains open.

## Objective

Reduce probe-phase wall without changing source verdicts, queue decisions, politeness limits, or
freshness. Treat HTTP execution placement and semaphore order as separate variables: validate the
transport candidate first, then remeasure the permit-order change against the selected transport. Do
not stack both changes in one A/B.

## Runtime architecture and blast radius

- The stable CLI entrypoint, [src/source_discovery.py](../../src/source_discovery.py), delegates to the
  discovery package. The normal probe path is
  [orchestrator_probe.py](../../src/source_discovery/orchestrator_probe.py), which passes
  DiscoveryRunDeps.fetcher to the shared
  [probe_runtime.py](../../src/source_discovery/probe_runtime.py) batcher. The same batcher is reached by
  the [GameDevMap active audit](../../src/source_discovery/gamedevmap_active_dry_run.py),
  [GameDevMap recovery queue](../../src/source_discovery/gamedevmap_recovery_queue.py), and
  [browser recovery](../../src/source_discovery/browser_recovery.py). This is a shared discovery
  execution primitive, not a GameDevMap-only optimization.
- [probe.py](../../src/source_discovery/probe.py) owns candidate validation, retry policy, URL
  selection, and job-count parsing. probe_runtime owns candidate-level concurrency, fetch deadlines,
  and result collection. [core_scoring.py](../../src/source_discovery/core_scoring.py) maps adapter
  families to limits: static → static, Teamtailor → Teamtailor, everything else → provider. Defaults
  are total=40, static=16, provider=40, Teamtailor=15. **None of these changed.**
- The provider bucket is 80% of the measured population but spans four ATS API hosts; the attribution
  snapshot records 238 URLs receiving 429. Keep current caps fixed. The per-bucket semaphores are family
  politeness limits, not independent host pools.
- Results return in completion order. [orchestrator_probe.py](../../src/source_discovery/orchestrator_probe.py)
  appends successful rows in that order; [core_queue.py](../../src/source_discovery/core_queue.py)
  later applies caps and ranking. Both landed changes preserve candidate-level outcomes.

## Evidence

- **Transport (landed).** The default path sends its request through a shared `httpx.Client` on the
  batch's own `probe-fetch` pool instead of doing connection, protocol and body work on the event-loop
  thread. Measured on the audit's 3,038-candidate population: loop CPU **22.28 → 4.97 s**, loop share
  of the wall **42.6% → 10.9%**, process CPU **23.14 → 16.53 s**, identical verdicts. The wall improved
  in the same-session pairs and drifts ±8% between runs, so the CPU figures are the claim. Evidence:
  [discovery-probe-httpx-off-loop-2026-10-12.md](../snapshots/discovery-probe-httpx-off-loop-2026-10-12.md).
- **Permit order (landed).** Re-measured on the shipped transport with the order as a bench variable,
  three balanced pairs at `total=40 / static=16`: **54.2 → 36.4**, **42.9 → 37.7**, **45.6 → 36.3 s**
  (means **47.6 → 36.8 s**, 63.9 → 82.6 candidates/s). Pool-permit time waiting on a bucket seam
  **371.9 s (21% of pool-permit time, all static's) → 0**; provider probe lanes **20.1 → 24.0** of 40.
  Evidence:
  [discovery-probe-permit-order-2026-10-12.md](../snapshots/discovery-probe-permit-order-2026-10-12.md).
- **Transport lifecycle (closed).** A fetch abandoned by its per-call bound keeps running; the batch's
  shared client is now closed by the last such fetch rather than under it. Evidence:
  [discovery-probe-client-lifecycle-2026-10-12.md](../snapshots/discovery-probe-client-lifecycle-2026-10-12.md).
- **Still open on this checkout:** the end-to-end audit against a deployed build (item 5).

## Scope and non-goals

- No change to total or family concurrency, no negative-result caching, no parallel web-search queries,
  no homepage/recovery deadline change in this phase. Those change politeness or freshness policy and
  have separate evidence.
- Keep the measurement scripts and arm-specific controls under `_out/perf-runs/discovery`. Do not add a
  production toggle, root-injection seam, or new benchmark framework.
- This plan covers backend discovery probe time only, not Admin/Jobs page rendering. The reachable
  Umbrel reports 0.3.014 versus this checkout's 0.3.018, has no active task, and returned 504 from
  `/ops/health`; it is not acceptance evidence until the matching build is deployed and healthy.

## Ordered work

1. ~~**Reconcile the current transport candidate.**~~ **Done** — the diff and its tests are settled;
   the sibling snapshot records the arms, the identity trap in the old bench label, and the loop/process
   CPU receipts.
2. **Transport lifecycle — measured close-under-fetch failure fixed; late-entry proof open.** The
   loopback reproducer and two tests cover fetches already inside the shared client at normal/deadline
   close. Static review finds an uncovered interleaving: `enter()` always increments, while
   `retire_when_idle()` observes zero under lock and closes after unlocking. A late worker can enter in
   that gap or after retirement. This is a static race hypothesis, not a reproduced failure; add a
   deterministic admission test and close the item only if it proves a retired client cannot be used.
   Closed by the deferred retirement in [the batch bounds](../scraping-pipeline.md):
   `retire_when_idle(..., defer_if_pool_active=True)` plus `test_probe_batch_late_entry_race.py`.
   Evidence: [discovery-probe-client-lifecycle-2026-10-12.md](../snapshots/discovery-probe-client-lifecycle-2026-10-12.md).
3. ~~**Reprice permit allocation on the chosen transport.**~~ **Done** — three balanced pairs, both
   arms on the shipped path, with permit-wait time captured by bucket.
4. ~~**Implement the surviving scheduler fix.**~~ **Done** — `_probe_one` acquires the bucket permit
   before the pool permit, with two deterministic contract tests (a blocked bucket cannot hold a pool
   permit; a saturated bucket leaves the pool's lanes to the other families), both verified to fail
   against a pool-first twin of the shipped function. Caps unchanged.
5. **Verify the production-shaped flow — audit and smoke legs done, deployed build open.** The
   three-batch GameDevMap audit ran end to end on this checkout's own engine (`APP_VERSION = "0.3.018"` at `9b8901f8`), into an
   isolated output directory, and was compared against the retained arms. The runtime delta against
   those arms is exactly two files (a modification-time partition of the tree), so the comparison is
   well formed. **Queue/defer identity passes:** on the 1,045 candidates both arms queued, ten
   queue/defer fields are identical on every one, where the two identical-code retained arms differ on
   1 of 1,130; 2,556 of 2,571 common candidates carry an unchanged verdict, all 15 flips are traced to
   DNS, 429, 403 and slow-host events named in the fetch log, and 16 of the 19 lost actives are too
   (the other three differ by candidate identity rather than by decision). Request,
   retry, 404 and 5xx counts are flat or between the retained arms. **The wall is inconclusive:** the
   probe phase runs 8.1% under the faster retained arm and 16.7% under the slower, but the two
   same-tree arms differ by 8.0% between themselves with the same flat-latency signature, so this arm
   cannot separate the change from the drift — no speedup is claimed, per this plan's stop rule.
   Evidence: [discovery-audit-e2e-verification-2026-10-12.md](../snapshots/discovery-audit-e2e-verification-2026-10-12.md).
   **The normal discovery smoke has run** — CI's own discovery leg, twice on this tree: both arms exit
   0, build the same **733**-candidate population, probe the same **725** over the same **728** deduped
   rows, queue the same **5** sources through the same three adapters (workday 3, teamtailor 1,
   smartrecruiters 1) with **0** static suppressions, and account for **every** probe failure as a
   remote condition (129/125 of 725: 91/86 DNS, 32/33 HTTP 429, and the same six named rows — no
   timeout, deadline or pool class anywhere). The run total moves **29,231 → 22,718 ms** between the
   two arms of identical code, so the smoke prices nothing either. The same measurement carries the
   leg the audit could not: a capped GameDevMap run through the same benchmark entrypoint writes
   `phaseRuntime` with process CPU and lane receipts — probe **17 of 17** configured lanes in flight at
   peak, `clientsUnclosed` 0, `deadlineExits` 0, `fetchesBoundedOut` 0, and `inFlightAtFinish` 0 on
   every phase, in both arms. Evidence:
   [discovery-smoke-verification-2026-10-12.md](../snapshots/discovery-smoke-verification-2026-10-12.md).
   Still open: a deployed build at this checkout's version for the packaged path.

## Acceptance and stop rule

- Each change was measured as its own variable: the transport on loop/process CPU and verdict
  identity, the order on three paired runs. The 80-lane order pair is **inconclusive** and is not
  claimed as a wall improvement.
- Deterministic tests preserve every candidate's result, configured concurrency bounds, per-request and
  batch deadlines, and pool and client lifecycle.
- Do not accept a result with unexplained 429/5xx, request-attempt, candidate, or queue differences. If
  live network drift prevents a verdict, report inconclusive and do not claim a speedup.
- No bucket or total cap is raised. No changes to search, negative-cache freshness, homepage fetch,
  recovery policy, Admin UI, or release configuration.

Item 5 is the last gate. When it passes, fold whatever remains into the subsystem docs and delete this
plan; if it fails, the negative result stays recorded and the change is reverted rather than defended.
