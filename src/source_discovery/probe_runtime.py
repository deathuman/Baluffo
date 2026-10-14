"""Source discovery probe runtime helpers.

Shared probe mechanics for source-discovery audit and recovery paths.

AI boundary owns: probe runtime batching, result envelopes, and probe execution support.
AI boundary implement in: this file for probe runtime mechanics; candidate validation/parsing stays in probe.py.
AI boundary search before contracts: orchestrator probe, runtime metrics, and probe runtime tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused probe runtime tests.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from src.shared.utils import now_iso
from src.source_registry import source_identity

from . import audit_ledger
from .core import (
    compute_candidate_score,
    normalize_candidate,
    probe_bucket_for,
    probe_concurrency_defaults,
)
from .io_runtime import endpoint_url
from .phase_runtime_receipt import PhaseRuntimeReceipt
from .web_search_fetch import fetch_text, fetch_text_httpx

ProbeResult = tuple[dict[str, Any], bool, int, str, int]
AsyncProbe = Callable[..., Awaitable[tuple[bool, int, str]]]
ProbeFailedRejection = Callable[[dict[str, Any], str], dict[str, Any]]
ZeroJobsRejection = Callable[[dict[str, Any], int], dict[str, Any]]
ProbeNormalizer = Callable[[dict[str, Any], int], dict[str, Any]]

# ponytail: mirrors probe.py's _EXPECTED_PROBE_FETCH_EXCEPTIONS. A probe failure
# must degrade that one candidate, never the batch, so the batch loop catches the
# same family. TimeoutError matters most: it is what a hung fetcher raises once
# _call_fetch is bounded, and letting it escape used to abort every other
# candidate's result.
_EXPECTED_PROBE_BATCH_EXCEPTIONS: tuple[type[BaseException], ...] = (
    OSError,
    TimeoutError,
    RuntimeError,
    KeyError,
    ValueError,
    json.JSONDecodeError,
    ET.ParseError,
    httpx.HTTPError,
)


@dataclass
class ProbeClassification:
    positive_candidates: list[dict[str, Any]]
    zero_job_candidates: list[dict[str, Any]]
    rejected_rows: list[dict[str, Any]]


def candidate_id(candidate: dict[str, Any]) -> str:
    return str(source_identity(candidate) or "").strip()


def candidate_with_probe_evidence(
    candidate: dict[str, Any],
    jobs_found: int,
    *,
    prevalidated_discovery: bool = False,
) -> dict[str, Any]:
    score, reasons = compute_candidate_score(candidate, jobs_found)
    normalized = normalize_candidate(candidate, score, reasons, jobs_found, probed_at=now_iso())
    normalized["deferred"] = False
    normalized.pop("deferReason", None)
    normalized["probeStatus"] = "ok"
    normalized["candidateState"] = "validated"
    if prevalidated_discovery:
        normalized["prevalidatedDiscovery"] = True
    identity = candidate_id(normalized)
    if identity:
        normalized["id"] = identity
    return normalized


def classify_probe_results(
    probe_results: list[ProbeResult],
    *,
    probe_failed_rejection: ProbeFailedRejection,
    zero_jobs_rejection: ZeroJobsRejection,
    normalize_candidate: ProbeNormalizer = candidate_with_probe_evidence,
) -> ProbeClassification:
    positive_candidates: list[dict[str, Any]] = []
    zero_job_candidates: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for candidate, ok, jobs_found, error, duration_ms in probe_results:
        if not ok:
            rejected_rows.append(probe_failed_rejection(candidate, error))
            continue
        normalized = normalize_candidate(candidate, jobs_found)
        normalized["probeDurationMs"] = int(duration_ms)
        if jobs_found > 0:
            positive_candidates.append(normalized)
        else:
            zero_job_candidates.append(normalized)
            rejected_rows.append(zero_jobs_rejection(normalized, jobs_found))
    return ProbeClassification(
        positive_candidates=positive_candidates,
        zero_job_candidates=zero_job_candidates,
        rejected_rows=rejected_rows,
    )


def rendered_static_probe_result(
    candidate: dict[str, Any],
    *,
    rendered_url: str,
    rendered_html: str,
) -> ProbeResult | None:
    from .probe import static_probe_evidence

    if str(candidate.get("adapter") or "").strip().lower() != "static":
        return None
    candidate_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
    if candidate_url.rstrip("/") != str(rendered_url or "").strip().rstrip("/"):
        return None
    try:
        evidence = static_probe_evidence(rendered_html, rendered_url)
    except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
        return None
    jobs_found = int(evidence.count)
    if jobs_found <= 0:
        return None
    candidate["lastProbeCountReason"] = evidence.reason
    candidate["lastProbeCountConfidence"] = evidence.confidence
    candidate["lastReliableJobsFound"] = jobs_found if evidence.confidence == "high" else 0
    if evidence.sample_urls:
        candidate["lastProbeSampleUrls"] = list(evidence.sample_urls)
    if evidence.confidence != "high":
        candidate["lastProbeWeakSignal"] = True
        candidate["weakSignal"] = True
    return candidate, True, int(jobs_found), "", 0


def probe_candidates_after_rendered_results(
    all_candidates: list[dict[str, Any]],
    rendered_probe_results: list[ProbeResult],
) -> list[dict[str, Any]]:
    rendered_ids = {candidate_id(result[0]) for result in rendered_probe_results}
    return [
        row for row in all_candidates if candidate_id(row) and candidate_id(row) not in rendered_ids
    ]


PROBE_POOL_THREAD_PREFIX = "probe-fetch"


def _pool_threads_alive(prefix: str = PROBE_POOL_THREAD_PREFIX) -> int:
    """Threads still running in a batch's own pool.

    The pool is retired with `shutdown(wait=False)`, so its workers leave on their own; this is the
    live count, read at the two moments that decide whether a batch retired cleanly -- its exit and
    its client's close. A count that stays above zero long after a batch has returned is the shape
    of abandoned work, so it is reported rather than waited on (waiting is exactly the straggler
    parking the batch's own deadline walks away from).
    """
    if not prefix:
        return 0
    return sum(1 for thread in threading.enumerate() if thread.name.startswith(prefix))


class ProbeBatchLifecycle:
    """What a probe batch's fetches, pool and shared client did, for the run to report.

    The batch's own contract is one result per candidate; this is the other half of what a run needs
    to know about it. A sync fetch cannot be cancelled, so a per-call bound can abandon one -- and an
    abandoned fetch is what holds a pool thread and the shared client open after the batch has
    returned. Nothing about that was observable from a live run's artifact: the batch either closed
    its client under a fetch or it did not, and both looked the same from the outside.

    A caller that wants the record passes an instance as `lifecycle=`. The object is *live*: the
    deferred close lands later, on the abandoned fetch's own thread, and writes itself into it, so a
    run that reads `snapshot()` at the end of its phase sees the settled retirement rather than only
    the state at the batch's exit.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pool_workers = 0
        self._fetches_started = 0
        self._fetches_through_client = 0
        self._fetches_bounded_out = 0
        self._outstanding_at_exit = 0
        self._client_outstanding_at_exit = 0
        self._pool_threads_alive_at_exit = 0
        self._exit_at: float | None = None
        self._retired_by = "unclosed"
        self._closed_from = ""
        self._closed_at: float | None = None
        self._pool_threads_alive_at_close = 0
        self._deadline_exit = False

    def note_fetch_started(self, *, through_client: bool = False) -> None:
        """A pool fetch began. `through_client` also counts it as one the shared client carries."""
        with self._lock:
            self._fetches_started += 1
            if through_client:
                self._fetches_through_client += 1

    def note_fetch_bounded_out(self) -> None:
        """The per-call bound fired: this fetch's await is over, its thread is not."""
        with self._lock:
            self._fetches_bounded_out += 1

    def note_deadline_exit(self) -> None:
        """The batch left through its own deadline rather than by finishing its candidates."""
        with self._lock:
            self._deadline_exit = True

    def note_pool_created(self, *, workers: int) -> None:
        with self._lock:
            self._pool_workers = max(0, int(workers))

    def note_batch_exit(
        self, *, outstanding: int, through_client: int, pool_threads_alive: int
    ) -> None:
        """Record the state the batch returned in: what is still running, and what holds the client."""
        with self._lock:
            self._exit_at = time.perf_counter()
            self._outstanding_at_exit = max(0, int(outstanding))
            self._client_outstanding_at_exit = max(0, int(through_client))
            self._pool_threads_alive_at_exit = max(0, int(pool_threads_alive))

    def note_client_closed(self, *, deferred: bool) -> None:
        """Record the close: the batch's own thread when idle, else the last fetch out."""
        with self._lock:
            self._closed_at = time.perf_counter()
            self._closed_from = threading.current_thread().name
            self._retired_by = "last-fetch" if deferred else "batch"
            self._pool_threads_alive_at_close = _pool_threads_alive()

    def snapshot(self) -> dict[str, Any]:
        """The receipt, as JSON-safe keys -- readable at any time, including while a fetch runs."""
        with self._lock:
            closed_at = self._closed_at
            closed = closed_at is not None
            deferred_ms = 0
            if closed_at is not None and self._exit_at is not None:
                deferred_ms = max(0, int((closed_at - self._exit_at) * 1000))
            return {
                "poolWorkers": self._pool_workers,
                "fetchesStarted": self._fetches_started,
                "fetchesThroughClient": self._fetches_through_client,
                "fetchesBoundedOut": self._fetches_bounded_out,
                "fetchesOutstandingAtExit": self._outstanding_at_exit,
                "clientFetchesOutstandingAtExit": self._client_outstanding_at_exit,
                "poolThreadsAliveAtExit": self._pool_threads_alive_at_exit,
                "clientClosed": closed,
                "clientRetiredBy": self._retired_by if closed else "unclosed",
                "clientClosedFrom": self._closed_from,
                "clientCloseDeferredMs": deferred_ms,
                "poolThreadsAliveAtClose": self._pool_threads_alive_at_close,
                "poolThreadsAliveNow": _pool_threads_alive(),
                "deadlineExit": self._deadline_exit,
            }


class _InFlightFetches:
    """Counts the fetches inside a batch's HTTP client, so it is closed after the last one leaves.

    A sync fetch runs in a pool thread and cannot be cancelled: once the per-call `asyncio.wait_for`
    bound fires, the thread keeps running and may outlive the whole batch. Closing the client then
    tears a live socket out from under it -- measured on the shipped code as a `ReadError`
    ("[WinError 10038] An operation was attempted on something that is not a socket") raised inside
    the abandoned request at the instant the batch returned, because `httpx.Client.__exit__` closes
    the transport without calling `close()`. So the close waits for this count instead.
    """

    def __init__(self, lifecycle: ProbeBatchLifecycle | None = None) -> None:
        self._lock = threading.Lock()
        self._count = 0
        self._retiring: httpx.Client | None = None
        self._lifecycle = lifecycle

    def enter(self) -> None:
        with self._lock:
            self._count += 1

    def outstanding(self) -> int:
        with self._lock:
            return self._count

    def leave(self) -> None:
        retiring: httpx.Client | None = None
        with self._lock:
            self._count -= 1
            if self._count == 0 and self._retiring is not None:
                retiring = self._retiring
                self._retiring = None
        if retiring is not None:
            _close_and_notify(retiring, self._lifecycle, deferred=True)

    def retire_when_idle(self, client: httpx.Client, *, defer_if_pool_active: bool = False) -> bool:
        """Close `client` now when nothing is inside it, else from the last fetch that leaves.

        Returns True when the client was actually closed in this call, False when the close was
        deferred (to a leave() or to the caller). Idempotent: ``httpx.Client.close`` is, so a
        second call on an already-retired client is fine.

        When *defer_if_pool_active* is True and the count is zero, the close is deferred instead
        of happening immediately: a pool thread may be about to enter in_flight but has not yet
        reached enter(), so closing the client under it would tear a live socket away.
        """
        with self._lock:
            idle = self._count == 0
            if not idle:
                self._retiring = client
            elif defer_if_pool_active:
                self._retiring = (
                    client  # Defer: let a late-entering thread's leave() or the caller handle it.
                )
            elif self._retiring is None:
                self._retiring = client  # Marker so a second call does not double-close.
                _close_and_notify(client, self._lifecycle, deferred=False)
                return True
            return False


async def run_bounded_probe_batch_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
    async_probe: AsyncProbe,
    default_fetcher=fetch_text,
    probe_kwargs: dict[str, Any] | None = None,
    lifecycle: ProbeBatchLifecycle | None = None,
    receipt: PhaseRuntimeReceipt | None = None,
    requests_per_row: int = 1,
) -> list[ProbeResult]:
    probe_options = dict(probe_kwargs or {})
    limits = probe_concurrency_defaults()
    total_sem = asyncio.Semaphore(int(limits["total"]))
    bucket_sems = {
        "static": asyncio.Semaphore(int(limits["static"])),
        "provider": asyncio.Semaphore(int(limits["provider"])),
        "teamtailor": asyncio.Semaphore(int(limits["teamtailor"])),
    }
    # The wave's realized concurrency is less than `total` when a single family fills the wave: a
    # candidate only ever holds its own bucket permit, so a provider-only wave runs at `provider`
    # lanes, not `total`. Deriving the deadline from `total` alone overstates throughput by up to
    # `total / family_cap` and can falsely truncate a valid family-saturated wave. The realized
    # bound is `total` clipped by how many lanes the buckets actually admit in aggregate.
    bucket_row_counts: dict[str, int] = {}
    for row in candidates:
        bucket = probe_bucket_for(row)
        bucket_row_counts[bucket] = bucket_row_counts.get(bucket, 0) + 1
    aggregate_bucket_capacity = sum(
        min(int(limits[bucket]), count)
        for bucket, count in bucket_row_counts.items()
        if bucket in limits
    )
    effective_concurrency = max(
        1,
        min(
            int(limits["total"]),
            max(1, aggregate_bucket_capacity),
            max(1, len(candidates)),
        ),
    )
    # The probe wave's lanes are `limits["total"]`, clipped by the rows this batch actually has.
    # ponytail: this pool is not only for a wrapper any more. `httpx`'s ASYNC client does the TLS
    # handshake, the HTTP/1.1 parse and the body accumulation on the event loop thread, and that
    # thread is the wave's only one -- so every response's cost there is a throughput ceiling rather
    # than a lane. Measured on the audit's 3,038-candidate population at the configured lanes: the
    # default (async httpx) path spent **22.28 s of loop CPU** (42.6% of its 52.28 s wall, 7.33 ms
    # per candidate, ~136 candidates/s) against **4.97 s** (10.9%, 1.64 ms) when the same requests ran
    # on this pool, and the phase itself was 13% faster (45.46 s, 66.8 candidates/s) with identical
    # verdicts. The decode is not the cost -- handing `Response.text` to a thread moved 0.26 s of the
    # 22.28 s -- the connection and body work is, so the whole request moves rather than a slice.
    #
    # History that still shapes this: the sync branch used to submit to `asyncio.to_thread`, i.e. the
    # loop's SHARED default executor, whose size is `min(32, cpu_count + 4)` -- 20 on the 16-core box
    # the audit runs use. A wrapping fetcher's probe fetches therefore ran at 20 in-flight against a
    # configured 40, silently: measured as exactly 20 pinned for 8.7%/10.7% of the phase's span in
    # both `lane-ceiling` audit runs. The lane count is the caller's budget, so every path this batch
    # runs gets a pool it sizes and retires.
    if lifecycle is not None:
        lifecycle.note_pool_created(workers=effective_concurrency)
    if receipt is not None:
        # The wave's lane budget, and the candidates it actually probed -- the receipt's occupancy is
        # read against this budget, so a run can say "24 of 40 lanes" without a bench.
        receipt.note_configured_lanes(effective_concurrency)
        receipt.note_items(len(candidates))
    pool = ThreadPoolExecutor(
        max_workers=effective_concurrency, thread_name_prefix=PROBE_POOL_THREAD_PREFIX
    )
    # Two counters, because they answer different questions. `pool_in_flight` is every fetch the
    # batch's pool is running -- which is what "abandoned" means, whatever fetcher the caller passed.
    # `in_flight` is the subset inside the shared client, which only the default path enters.
    pool_in_flight = _InFlightFetches()
    in_flight = _InFlightFetches(lifecycle)

    async def _call_fetch(url: str, call_timeout_s: int) -> str:
        # ponytail: `httpx.Timeout` is a PER-OPERATION budget, not a total one -- the read timeout
        # re-arms on every chunk received -- so a slow-drip server holds this call open past its
        # own timeout and returns "successfully" minutes later (measured 40.3 s against a 2 s
        # timeout on the loopback drip server, and 40.7 s against Timeout(2.0) in the shared
        # batcher, where the same loophole parked a GameDevMap batch for 900 s). Every path is
        # wrapped this way, the default HTTP client included.
        budget = max(1.0, float(call_timeout_s))

        def sync_fetch() -> str:
            through_client = fetcher is default_fetcher
            pool_in_flight.enter()
            if lifecycle is not None:
                lifecycle.note_fetch_started(through_client=through_client)
            try:
                if not through_client:
                    return str(fetcher(url, call_timeout_s))
                # Tracked because this thread can outlive the batch: the client must not be closed
                # while a request is still inside it (see `_InFlightFetches`).
                in_flight.enter()
                try:
                    return fetch_text_httpx(client, url, call_timeout_s)
                finally:
                    in_flight.leave()
            finally:
                pool_in_flight.leave()

        try:
            # ponytail: this runs in a worker thread, which cannot be cancelled. The module has no
            # wait_for anywhere else, so a fetcher whose socket read never returns parks `_probe_one`
            # forever: its semaphores stay held, and the whole probe batch stops with no error. Give
            # the await its own deadline so a hung fetch becomes an ordinary per-candidate
            # TimeoutError (already handled by the probe retry/except path) instead of an unbounded
            # stall. The thread itself still runs to completion -- unavoidable for sync callables --
            # but the batch is no longer blocked by it.
            return await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(pool, sync_fetch),
                timeout=budget,
            )
        except TimeoutError as exc:
            # The await is over; the thread behind it is not -- this is the count the batch's own
            # deadline walks away from, and the reason a client close has to wait.
            if lifecycle is not None:
                lifecycle.note_fetch_bounded_out()
            # say why: asyncio's own TimeoutError stringifies to "", and an empty error is a
            # failure row an operator cannot classify. The wording keeps the "timed out" token
            # the retry predicate and the failure-stage classifier already recognise.
            raise TimeoutError(f"fetch timed out after {budget:.0f}s") from exc

    async def _probe_one(index: int, row: dict[str, Any]) -> tuple[int, ProbeResult]:
        bucket = probe_bucket_for(row)
        bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
        # ponytail: the BUCKET permit is taken before the POOL permit, and the order is load-bearing.
        # Taken the other way round, a candidate whose family is saturated holds a pool permit while it
        # waits on its own bucket seam -- and the pool is 40 while the buckets sum to 71. On the audit's
        # 3,038 candidates at total=40 / static=16 the first (bench-only, pre-transport) measurement put
        # static's queue at **6.8 pool permits** held at any moment and the provider bucket -- 80% of the
        # population -- at **21.0 of the 40 lanes** its own cap allows. Bucket-first is that
        # measurement's fix, re-measured on the shipped transport as three balanced pairs: **54.2 ->
        # 36.4, 42.9 -> 37.7, 45.6 -> 36.3 s**, blocked pool-permit time **0**, provider probe lanes
        # **20.1 -> 24.0** of 40.
        # No cap changes -- same politeness limits, same pool size, and every candidate still passes
        # through both seams; only the permit that carries the wait moves. It is also the deadlock-free
        # order: the pool is only ever acquired last, so no task holds the pool and waits on a bucket.
        # See `docs/snapshots/discovery-probe-permit-order-2026-10-12.md`.
        async with bucket_sem:
            async with total_sem:
                started = time.perf_counter()
                # A lane is held from the moment this candidate is actually probing, which is what the
                # receipt's occupancy measures against the budget above.
                if receipt is not None:
                    receipt.note_lane_enter()
                try:
                    # ponytail: a probe must never escape as an exception. Without
                    # this guard a failure raised out of async_probe (including the
                    # TimeoutError produced when the fetcher thread hangs) propagates
                    # through as_completed, aborts the whole batch, and returns no
                    # results at all. Converting it to an ordinary per-candidate
                    # failure keeps the one-result-per-candidate contract and lets
                    # the caller's normal rejection handling record it.
                    try:
                        ok, jobs_found, error = await async_probe(
                            row,
                            timeout_s,
                            fetcher=_call_fetch,
                            **probe_options,
                        )
                    except _EXPECTED_PROBE_BATCH_EXCEPTIONS as exc:
                        ok, jobs_found, error = False, 0, str(exc)
                    return index, (row, ok, jobs_found, error, audit_ledger.duration_ms(started))
                finally:
                    if receipt is not None:
                        receipt.note_lane_leave()

    tasks: list[asyncio.Task[tuple[int, ProbeResult]]] = []
    by_index: dict[int, ProbeResult] = {}
    finished: list[int] = []

    async def _run(client: Any) -> list[ProbeResult]:
        tasks.extend(
            asyncio.create_task(_probe_one(index, row)) for index, row in enumerate(candidates)
        )
        for fut in asyncio.as_completed(tasks):
            index, result = await fut
            by_index[index] = result
            finished.append(index)
        return [by_index[index] for index in finished]

    # ponytail: the same defect class as src/shared/http_batch.py, and the same shape of bound. A
    # probe batch funnels every candidate in the wave, so one candidate whose requests never
    # return parks the whole stage with no error and no timeout -- the pathological case is one
    # board that trickles bytes, not one that is slow. Bounding it at `timeout_s` would be wrong:
    # that is a per-request budget and a probe may make several requests (fallbacks, structured
    # APIs) plus retries. Let every candidate spend its own timeout, divided by the concurrency
    # actually available, plus a grace for scheduling -- measured at 4 s for 3 candidates at a 2 s
    # timeout, against 40.3 s unbounded on the loopback drip server.
    per_row_s = max(1.0, float(timeout_s))
    # P4: the per-row budget is `requests_per_row` requests, not one. A probe row makes several
    # requests (fallbacks, per-URL timeout retries, the Workday structured POST with retries +
    # backoff), each with its own `timeout_s`-sized budget. A deadline sized for a single request
    # falsely truncates valid multi-request work (see probe_row_timeout_units). `requests_per_row`
    # defaults to 1 so non-probe-aware callers (orchestrator) keep the previous bound, while the
    # probe-aware entry (`probe_candidates_async`) supplies the wave's ceiling.
    row_work_s = per_row_s * max(1, int(requests_per_row))
    total_deadline_s = row_work_s * (len(candidates) / effective_concurrency) + row_work_s
    deadline_error = f"total probe deadline exceeded ({total_deadline_s:.0f}s)"
    # A sync client on the wave's pool. `httpx.Client` is thread-safe (one shared keep-alive pool
    # behind a lock) and resolves trust the same way `httpx.AsyncClient` does -- certifi by
    # default -- so hosts the async path reads stay readable and no caller sees a different
    # certificate story. It is retired by `_close_client_when_fetches_stop` below rather than by a
    # `with`: `httpx.Client.__exit__` tears the transport down without calling `close()`, which closes
    # the socket under a fetch whose per-call bound already fired.
    client = httpx.Client(timeout=httpx.Timeout(timeout_s))
    try:
        batch_results = await asyncio.wait_for(_run(client), timeout=total_deadline_s)
    except TimeoutError:
        if lifecycle is not None:
            lifecycle.note_deadline_exit()
        _cancel_pending(tasks)
        return _deadline_results(
            candidates,
            by_index,
            error=deadline_error,
            duration_ms=int(total_deadline_s * 1000),
        )
    finally:
        # `wait=False`: sync work cannot be cancelled, and the per-call bound above can abandon a
        # fetch whose thread is still running, so waiting here would park the batch on exactly the
        # straggler its own deadline walks away from. The pool retires as those threads finish rather
        # than outliving the batch for the run -- and the client with them, for the same reason.
        pool.shutdown(wait=False)
        if lifecycle is not None:
            lifecycle.note_batch_exit(
                outstanding=pool_in_flight.outstanding(),
                through_client=in_flight.outstanding(),
                pool_threads_alive=_pool_threads_alive(),
            )
        _close_client_when_fetches_stop(
            client, in_flight, lifecycle, pool_still_active=(pool_in_flight.outstanding() > 0)
        )
    return batch_results


def _close_and_notify(
    client: httpx.Client, lifecycle: ProbeBatchLifecycle | None, *, deferred: bool
) -> None:
    """Close *client* and notify the lifecycle."""
    try:
        client.close()
    finally:
        if lifecycle is not None:
            lifecycle.note_client_closed(deferred=deferred)


def _close_client_when_fetches_stop(
    client: httpx.Client,
    in_flight: _InFlightFetches,
    lifecycle: ProbeBatchLifecycle | None = None,
    *,
    pool_still_active: bool = False,
) -> None:
    """Retire the batch's shared client once no fetch is still inside it (see `_InFlightFetches`).

    The common case is a batch with nothing abandoned: the count is zero and the close happens here,
    on the batch's own thread. When a per-call bound has abandoned a fetch, the last fetch out closes
    it instead, so the batch's clock never regresses to the straggler's and no waiter is parked on it.

    When *pool_still_active* is True and the in_flight count is zero, the close is deferred: a pool
    thread may be about to enter in_flight but hasn't reached the enter() call yet, and closing the
    client under it would tear its connection. The close is then retried briefly after giving that
    thread time to reach enter() -- or, if the thread is a custom-fetcher thread that never enters
    in_flight, the force-close after the brief wait is safe.
    """
    client_closed = in_flight.retire_when_idle(client, defer_if_pool_active=pool_still_active)
    if not client_closed and pool_still_active:
        # A pool thread may be in the gap between pool_in_flight.enter() and
        # in_flight.enter(). Give it the CPU to reach enter() before closing.
        for _ in range(10):
            if in_flight.outstanding() > 0:
                break  # A thread entered; its leave() will close via retire_when_idle.
            time.sleep(0.01)
        # Now close if still idling: remaining pool threads don't use the client.
        if in_flight.outstanding() == 0:
            _close_and_notify(client, lifecycle, deferred=False)


def _cancel_pending(tasks: list[asyncio.Task[Any]]) -> None:
    """Cancel every wave task still running, so a deadline leaves no orphan work behind."""
    for task in tasks:
        if not task.done():
            task.cancel()


def _deadline_results(
    candidates: list[dict[str, Any]],
    completed: dict[int, ProbeResult],
    *,
    error: str,
    duration_ms: int,
) -> list[ProbeResult]:
    """One result per candidate for a wave that ran out of time: what finished, plus failures."""
    return [
        completed[index] if index in completed else (row, False, 0, error, duration_ms)
        for index, row in enumerate(candidates)
    ]


async def probe_candidates_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
    lifecycle: ProbeBatchLifecycle | None = None,
    receipt: PhaseRuntimeReceipt | None = None,
) -> list[ProbeResult]:
    from .probe import async_probe_candidate, probe_row_timeout_units

    # P4: the deadline must cover the wave's worst legitimate multi-request row (fallbacks, per-URL
    # timeout retries, Workday structured POST + retries + backoff). Derive the ceiling from the
    # actual candidates, not an assumed single request per row.
    requests_per_row = max(
        (probe_row_timeout_units(row, timeout_s) for row in candidates),
        default=1,
    )

    return await run_bounded_probe_batch_async(
        candidates,
        timeout_s=timeout_s,
        fetcher=fetcher,
        async_probe=async_probe_candidate,
        lifecycle=lifecycle,
        receipt=receipt,
        requests_per_row=requests_per_row,
    )
