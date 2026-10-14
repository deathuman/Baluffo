# The GameDevMap audit measures its own CPU and lanes in place — 2026-10-12

> - **Class:** shipped instrumentation in the discovery lane; the audit artifact gains a measurement
>   section, no phase's behaviour changes
> - **Trigger:** the audit's end-to-end verification
>   ([`discovery-audit-e2e-verification-2026-10-12.md`](discovery-audit-e2e-verification-2026-10-12.md))
>   could not claim a probe-phase speedup even though one arm was **8.1%** faster, because the same
>   tree measured against itself differs by **8.0%** on the same phase. Every "did this help" question
>   was being answered with the one number that drifts.
> - **Status:** shipped. Each phase of every batch now records wall, process CPU and lane occupancy in
>   `timings.batches[].phaseRuntime`, and the run rolls them up in `summary.phaseRuntime` -- live, on
>   every run, with no bench and no sampler thread. On the repo's 2-row fixture the rollup reads
>   **9 phases, 172 ms CPU over 199 ms wall (86.4%)**, and the homepage phase reads **2 items on 4
>   configured lanes, peak 2 in flight, 303 ms of lane-busy time**.
> - **Contract:** `tests/source_discovery/test_phase_runtime_receipt.py` (10 tests) pins the receipt's
>   arithmetic on an injected clock and the audit's in-situ wiring -- including a fetcher gate that
>   holds two homepage fetches in flight at once, so a reported peak of 2 is measured rather than
>   scheduled. The recovery-cache parity test now classifies the section as a measurement and compares
>   the **phase names and lane budgets** across cache scopes instead of the numbers.
> - **Not a behaviour change for the run.** Same fetches, same caps, same verdicts; the receipt costs
>   two counter increments per fetch and two clock reads per phase. No wall-clock total moved:
>   `timings.totalsMs` keeps exactly the keys it had, because a CPU number must never be summed into a
>   wall-clock contract other readers consume.
> - **Verified against:** `9b8901f8` plus this session's uncommitted cuts. Changed here:
>   `src/source_discovery/phase_runtime_receipt.py` (new),
>   `src/shared/http_batch.py`, `src/source_discovery/probe_runtime.py`,
>   `src/source_discovery/directory_fetch.py`,
>   `src/source_discovery/gamedevmap_recovery_queue.py`,
>   `src/source_discovery/gamedevmap_active_dry_run.py`,
>   `src/source_discovery/active_audit_contracts.py`,
>   `src/source_discovery/active_audit_batch_strategy.py`,
>   `src/source_discovery/active_audit_batch_runtime.py`,
>   `src/source_discovery/gamedevmap_active_batch_plumbing.py`,
>   `src/source_discovery/gamedevmap_artifact_store.py`,
>   `tests/source_discovery/test_phase_runtime_receipt.py` (new),
>   `tests/source_discovery/test_gamedevmap_recovery_cache_scope.py`.
> - **Then inspect:** [`discovery-probe-client-lifecycle-2026-10-12.md`](discovery-probe-client-lifecycle-2026-10-12.md)
>   for the sibling receipt (abandoned fetches, pool/client retirement), and
>   [`measurement-methods.md`](../measurement-methods.md) for the repo's rules on keeping a measured
>   claim honest.

## The instrument

One object, `PhaseRuntimeReceipt`, per phase. It reads `time.process_time()` at construction and at
`finish()`, so `cpuMs` is the CPU the *process* spent while the phase ran, and it integrates lane
occupancy from the fetches themselves: `fetch_pages_batched` calls `note_lane_enter()` after a row has
both its permits, `note_lane_leave()` in a `finally`, and the probe wave does the same around each
candidate's probe. From those two events it derives peak in flight, mean in flight (the integral over
the phase's wall), occupancy against the configured budget, and total lane-busy time -- the Σ-latency
analogue the earlier probes were measured with, without needing a per-request sample.

The audit's batch runtime records each batch's receipts under `phaseRuntime`, and the artifact's
summary aggregates them per phase name. Both keys are deliberately not `*Ms`: `append_batch_timing`
sums every `keys ending in "Ms"` into `timings.totalsMs`, and those totals are wall-clock numbers
consumed by the directory audit report. Mixing CPU into them would have been a silent contract break;
`test_the_receipts_stay_out_of_the_wall_clock_totals` is the guard.

## What a run reports

Per batch, in phase order:

| Key | Meaning |
|---|---|
| `phase` | `direct_inference`, `homepage_fetch`, `homepage_analysis`, the two recovery fetch/analysis pairs, `merge`, `probe` |
| `wallMs`, `cpuMs` | the phase's wall and process CPU |
| `cpuOverWallPct` | 100 = one core busy for the phase's whole span |
| `items`, `wallPerItemMs`, `cpuPerItemMs` | fetches run and their unit costs (0 items for parsing phases) |
| `configuredLanes`, `peakInFlight`, `meanInFlight`, `laneOccupancyPct`, `laneBusyMs` | the lane budget, what used it, and for how long |
| `inFlightAtFinish` | fetches still out when the phase closed (the straggler shape) |

Per run, `summary.phaseRuntime` aggregates by phase -- sums for wall/CPU/items, maxima for the budget
and peak, wall-weighted mean for occupancy -- and ranks the phases by CPU, so the first row answers
"where did this run's work go". On the fixture: `probe` 172 ms CPU of the run's 199 ms wall; the
homepage phase 2 items on 4 lanes at peak 2 and 49% occupancy.

## What the tests hold

The arithmetic is driven by a hand-cranked clock, because the lane integral is the part that can be
silently wrong and a sleeping test cannot see it: two lanes entering together, leaving one second
apart, must yield 3,000 ms of lane-busy time over a 4 s phase (`meanInFlight` 0.75 of a 4-lane
budget). Two negative controls matter as much: a phase whose lanes never entered reports zero
occupancy rather than a phantom, and a CPU counter that goes backwards contributes zero rather than a
negative.

In situ, the wiring is proven where it is read. A fetcher gate refuses to answer until two homepage
fetches are inside it, so `peakInFlight == 2` is the batch really running two lanes at once; a second
test runs two batches and asserts each `phaseRuntime` carries exactly the nine phases **it** ran (the
list is cleared at `prepare_rows`), and that the rollup counts each batch's phases once.

## Limits

- **CPU is process-wide, so each phase's number is an upper bound.** `time.process_time()` sums every
  thread, including a straggler from an earlier phase and any unrelated work in the same process. The
  run-level total is fair; a single phase's figure can include someone else's work, and a phase with a
  ~1 ms wall can read `cpuOverWallPct` above 100% for exactly that reason -- read it beside `wallMs`
  and `items`, not alone.
- **A straggler stops accruing at `finish()`.** A fetch that outlives its phase is reported by
  `inFlightAtFinish` and by the probe lifecycle receipt beside this one; its remaining time is not
  charged to any phase.
- **Occupancy is against the clipped budget.** `configuredLanes` is `min(configured, items)`, so a
  2-item batch reports 2 lanes, not 32 -- occupancy percentages are "of the budget this phase could
  have used", which is the lane question the earlier audits asked.
- **This is an instrument, not a reading.** The fixture numbers above come from the repo's 2-row
  fixture, not from the audit's 3,000-studio population; what the audit's own CPU and occupancy look
  like is the next live run's business.
- **The fixture run's `recovery_*` phases read 0 items on a 1 ms wall.** That is the honest shape of a
  batch with no recovery jobs, and the reason the receipt records every phase rather than only the
  busy ones.
