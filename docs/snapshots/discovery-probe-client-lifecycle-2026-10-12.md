# The probe batch's shared HTTP client now outlives only the fetches still inside it — 2026-10-12

> - **Class:** shipped execution-path fix in the discovery lane, measured before and after
> - **Trigger:** the transport change
>   ([`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md))
>   left an open item: a sync fetch cannot be cancelled, so one whose per-call `asyncio.wait_for` bound
>   has fired keeps running in a pool thread after the batch returns — and the batch retired its shared
>   `httpx.Client` at that moment. The client-lifecycle question was whether a timed-out request can end
>   up using a closed client.
> - **Status:** shipped, and the answer to that question was **yes**. On a loopback dripper with a 1 s
>   per-call bound, the abandoned request was **still inside the client** when it was closed
>   (`in_flight=1`) and died with `ReadError: [WinError 10038] An operation was attempted on something
>   that is not a socket` at the same instant the batch returned. The mechanism is httpx 0.28's
>   `Client.__exit__`: it sets the client's state to closed and tears down the transport **without
>   calling `close()`**, which is why the first instrumented run recorded no close at all while the
>   socket still died underneath the fetch. The batch now counts the fetches using the client and closes
>   it only once the last one has left: the same run returns with the fetch still in flight and closes
>   at **`in_flight=0`**, on the last fetch's own thread.
> - **Contract:** two new deterministic tests assert the order of events — the batch returns promptly
>   with the fetch still inside the client, the client is still open at that moment and when the fetch
>   exits, and it closes afterwards from a `probe-fetch` thread. One covers the normal exit
>   (`test_the_shared_client_closes_only_after_its_last_fetch_has_left`), the other the batch-deadline
>   exit, where `_cancel_pending` abandons the most work at once
>   (`test_the_batch_deadline_also_waits_for_outstanding_fetches`). Both are verified to **fail 2 of 2**
>   against the old eager close and pass on the shipped code (see below). A second contract pins what a
>   run can *read* about the same events: `tests/source_discovery/test_probe_batch_lifecycle_report.py`
>   (see *What a run now reports* below).
> - **Not a behavior change for the run.** The abandoned candidate's verdict was already the per-call
>   timeout the batch recorded; what changes is what that thread sees while it finishes, and that the
>   client is never closed under a live request. No cap, deadline, verdict, result order or pool size
>   changed.
> - **Verified against:** `9b8901f8` plus this session's uncommitted cuts. Changed here:
>   `src/source_discovery/probe_runtime.py`, `src/source_discovery/active_audit_contracts.py`,
>   `src/source_discovery/active_audit_batch_strategy.py`,
>   `src/source_discovery/active_audit_batch_runtime.py`,
>   `src/source_discovery/gamedevmap_recovery_queue.py`,
>   `src/source_discovery/gamedevmap_active_dry_run.py`,
>   `src/source_discovery/gamedevmap_active_batch_plumbing.py`,
>   `src/source_discovery/gamedevmap_artifact_store.py`,
>   `tests/source_discovery/test_probe_batch_client_lifecycle.py`,
>   `tests/source_discovery/test_probe_batch_lifecycle_report.py`. Instrument (untracked):
>   `_out/perf-runs/discovery/probe_client_lifecycle_probe.py`.
> - **Then inspect:** [`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md)
>   for the transport this belongs to, and
>   [`discovery-probe-permit-order-2026-10-12.md`](discovery-probe-permit-order-2026-10-12.md) for the
>   lane change beside it.

## The measurement

`probe_client_lifecycle_probe.py` runs the real batch (default path, so the batch owns the client)
against a loopback server that drips one chunk every 200 ms for 8 s, with `timeout_s=1`. It stamps the
client's `close()`, the batch's return, and the fetch's entry/exit, and reports what the abandoned
request saw. `--arm eager` restores the pre-fix close semantics so the two arms are one script.

| Arm | Fetch enters | Batch returns | Client closed | Abandoned request's own outcome |
|---|---|---|---|---|
| eager (pre-fix) | 153.0 ms | 1157.5 ms | **1157.3 ms, `in_flight=1`** | `ReadError [WinError 10038]` at 1157.4 ms |
| shipped (deferred) | 153.1 ms | 1166.1 ms | **8173.7 ms, `in_flight=0`** | `RemoteProtocolError` (server stopped mid-chunk) at 8173.7 ms |

Three things this settles:

- **The close lands under a live request in the old shape, and not by accident of timing.** The eager
  arm closes at 1157.3 ms with a fetch entered and not exited, and that fetch dies 0.1 ms later with a
  socket error. The batch's per-call bound fired at ~1 s, so the request had been reading for a second
  and was killed by the teardown, not by its own timeout.
- **The batch's clock is untouched.** Both arms return at ~1.16 s against a 1 s per-call bound; the
  deferred close is what keeps that true, because waiting for stragglers would park the batch on them.
- **The straggler now ends on its own terms.** With the close deferred, the request runs until the
  server stops dripping and fails with its own protocol error at 8173.7 ms, and the client closes in the
  same millisecond (`in_flight=0`). Nothing was torn down underneath it.

## What shipped

- `_InFlightFetches` in `src/source_discovery/probe_runtime.py`: a lock, a count, and the client handed
  over for retirement when the count is non-zero.
- `sync_fetch` wraps the default path's `fetch_text_httpx(client, ...)` in `enter()`/`leave()`; the
  wrapping-fetcher path uses no client and is not tracked.
- The batch creates the client and then calls `_close_client_when_fetches_stop(client, in_flight)` in
  its `finally`, replacing the `with httpx.Client(...)` block on **both** exits — the normal one and the
  batch-deadline path, where `_cancel_pending` leaves every in-flight fetch mid-request. Both exits are
  covered by a test.
- When nothing was abandoned (the common case) the batch closes the client itself, as before. When
  something was, the **last fetch out closes it**, so no waiter thread is parked on the client and
  nothing else can be inside it at that point. `httpx.Client.close` is idempotent, so the two paths
  cannot double-close anything.

## What a run now reports

The deferred close was correct but invisible: an artifact could not tell a batch that abandoned three
fetches and left its client open from one that abandoned nothing. The batch now carries a receipt, and
the audit writes it where a run already looks:

- **Per batch** — `timings.batches[].probeLifecycle`: the pool's size, fetches started, how many went
  through the shared client, how many outlived their per-call bound, how many were still running (and
  still inside the client) at the batch's exit, the pool threads alive at exit and at close, whether the
  client closed and from which thread, how long after the exit it closed, and whether the batch left
  through its deadline.
- **Per run** — `summary.probeLifecycle`, rolled up from the *live* receipts at artifact-write time:
  the totals, `clientsUnclosed` (the leak flag — a wave whose close never happened), which retirement
  path each batch used, the worst deferral, and the most fetches any one batch left outstanding.

Two counters cover the fetches because the paths differ. `fetchesStarted` and
`fetchesOutstandingAtExit` count the batch's own pool, which is what "abandoned" means whatever fetcher
the caller passed; `fetchesThroughClient` counts only the default `httpx` path — a wrapped fetcher (the
audit bench passes one) never enters the client, so its client counts are zero by design and
`fetchesThroughClient` is what a reader uses to tell. `PROBE_POOL_THREAD_PREFIX` is now the single name
for the pool's `probe-fetch` prefix, shared by the receipt and by the lane guard that reads each
batch's pool prefix.

The receipt is an object the run holds rather than a snapshot taken at the batch's exit: the deferred
close lands on the straggler's own thread later, so a run that reads it while writing its artifact sees
the settled retirement. `tests/source_discovery/test_probe_batch_lifecycle_report.py` pins it — a clean
batch (closed by its own thread, nothing outstanding), an abandoned fetch (client open at exit, later
closed by a `probe-fetch` thread with the deferral it measures), the deadline exit, the rollup's own
negative control (`clientsUnclosed`), and the artifact round trip through the audit's entrypoint. The
real client is covered where the real transport already was: the loopback-drip test in
`test_probe_runtime_bounded_fetch.py` now also asserts that the receipt sees all three of its fetches
as client-carried with the client still open, so `fetchesThroughClient` cannot silently read zero on
every live run.

**Limit:** these are contract tests, not a reading. The receipt is the instrument — the next audit is
what says how often a wave abandons a fetch (the audited population is mostly fast 404s, so this is its
tail), and `clientsUnclosed` / `poolThreadsAliveNow` stay non-zero for exactly as long as a leak is
open.

## The contract, and the proof the test can see it

Both tests fake the client (recording whether, when and from which thread it was closed) and the fetch
(parked past its per-call bound, released by the test only after the batch has returned), then assert
the order: the batch returns promptly, the client is open at that moment, it is still open when the
fetch exits, it closes after that, and it closes from a `probe-fetch` thread rather than a waiter. They
differ in which exit they drive — the normal one, and the deadline one where the probe task itself is
cancelled while its fetch thread is still inside the client.

Run against the shipped code and against the pre-fix eager close
(`_out/perf-runs/discovery/verify_client_lifecycle_test_discriminates.py`):

```
[shipped (deferred close)] 0 of 2 failed
[patched eager close]      2 of 2 failed
    test_the_shared_client_closes_only_after_its_last_fetch_has_left: the batch returned with its
    client already closed while the fetch was still in it
    test_the_batch_deadline_also_waits_for_outstanding_fetches: the deadline exit closed the client
    while a fetch was still inside it
discriminating: True
```

## Reproducer

```bash
# the timeline on both arms (loopback dripper, ~10 s each)
PYTHONPATH=. python _out/perf-runs/discovery/probe_client_lifecycle_probe.py --arm eager
PYTHONPATH=. python _out/perf-runs/discovery/probe_client_lifecycle_probe.py --arm shipped

# the contract tests, and the proof they discriminate
python -m pytest tests/source_discovery/test_probe_batch_client_lifecycle.py -q
PYTHONPATH=. python _out/perf-runs/discovery/verify_client_lifecycle_test_discriminates.py
```

## Limits

- **The measurement is one candidate on a loopback dripper**, chosen because it makes a fetch outlive its
  own bound deterministically. It prices the *shape* (close before or after the last user), not a rate of
  occurrence; the audit's population is mostly fast 404s, so abandoned fetches are its tail, not its
  norm.
- **The deferred client now lives as long as the straggler.** That is the trade this fix makes on
  purpose: a straggler ends at its own httpx timeout (per-operation, so a server that drips can hold it
  well past the request's nominal budget), and until then the client holds one idle connection. The
  abandoned *thread* was already unbounded in the old shape — this changes only who outlives whom, and
  it removes the ability of the batch's teardown to end a request early.
- **A fetch that has not started when the last in-flight one leaves still meets a closed client.** The
  per-call bound already recorded that candidate's failure, so the `RuntimeError` httpx raises inside
  that abandoned thread is dropped — but this is a residual race, not a proved-safe path. The deadline
  test covers the *started* fetches on that exit, not this one.
- **The deadline test's probe deliberately outlives its own batch deadline**, which is what puts the
  client's close on that exit with a fetch still parked. It is a shape, not a rate: how often a live
  batch's deadline fires with a fetch in flight is a property of the population, not of this fix.
- **The close is not the only teardown in the process.** `httpx.Client.__exit__`'s transport-only close is
  what this snapshot measures; other modules own their own clients, and their lifecycle is unchanged.

## Earlier

This follows
[`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md), whose
Limits section posed exactly this question, and closes item 2 of
[`discovery-probe-permit-allocation-plan.md`](../plans/discovery-probe-permit-allocation-plan.md).
