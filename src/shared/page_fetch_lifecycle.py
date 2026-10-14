"""Dependency-neutral accounting for physical page-fetch worker lifetimes.

`PageFetchLifecycle` is deliberately separate from probe lifecycle and phase-runtime
receipts. Phase lane occupancy may end when an awaiting coroutine is cancelled; this
receipt remains live until the actual sync worker or async fetch task settles.
"""

from __future__ import annotations

import math
import threading
import uuid
from collections.abc import Iterable
from typing import Literal

Transport = Literal["sync", "async"]
BatchExit = Literal["completed", "deadline_exceeded", "cancelled", "failed"]
OwnedClientState = Literal[
    "not_applicable",
    "not_created",
    "open",
    "close_requested",
    "closed",
    "close_failed",
    "unknown",
]

_SCHEMA_VERSION = 1


class PageFetchWorkTicket:
    """Idempotent, thread-safe handle for one submitted page-fetch job."""

    def __init__(self, owner: PageFetchLifecycle) -> None:
        self._owner = owner
        self._state: Literal["queued", "started", "finished", "cancelled_before_start"] = "queued"
        self._worker_submitted = False
        self._async_cancel_requested = False
        self._async_cleanup_finished = False

    def mark_worker_submitted(self) -> bool:
        return self._owner._mark_worker_submitted(self)

    def worker_was_submitted(self) -> bool:
        return self._owner._worker_was_submitted(self)

    def start(self) -> bool:
        return self._owner._start(self)

    def finish(self, *, failed: bool = False) -> bool:
        return self._owner._finish(self, failed=failed)

    def cancel_before_start(self) -> bool:
        return self._owner._cancel_before_start(self)

    def request_async_cancel(self) -> bool:
        return self._owner._request_async_cancel(self)

    def finish_async_cleanup(self) -> bool:
        return self._owner._finish_async_cleanup(self)


class PageFetchLifecycle:
    """A live per-batch receipt whose snapshots never mutate after publication.

    Counts are updated at physical worker/task boundaries. A canceled waiter does
    not finish a still-running synchronous worker, and queued work is retired once.
    """

    def __init__(
        self,
        *,
        batch_id: str | None = None,
        source: str | None = None,
        run_id: str | None = None,
        phase: str | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._batch_id = batch_id or uuid.uuid4().hex
        self._source = source
        self._run_id = run_id
        self._phase = phase
        self._transport: Transport | None = None
        self._deadline_seconds: float | None = None
        self._deadline_exit: BatchExit | Literal["not_started", "running"] = "not_started"
        self._batch_started = False
        self._owned_client_state: OwnedClientState = "not_applicable"
        self._submitted = 0
        self._started = 0
        self._finished = 0
        self._failed = 0
        self._cancelled_before_start = 0
        self._async_cancellation_requested = 0
        self._async_cleanup_finished = 0
        self._exit_snapshot: dict[str, object] | None = None

    def start_batch(
        self,
        *,
        transport: Transport,
        deadline_seconds: float,
        owned_client_applicable: bool = False,
    ) -> None:
        """Bind the actual transport and total-batch deadline exactly once."""
        if not math.isfinite(deadline_seconds) or deadline_seconds < 0:
            raise ValueError("deadline_seconds must be finite and non-negative")
        with self._lock:
            if self._batch_started:
                raise RuntimeError("a page-fetch lifecycle cannot be reused for another batch")
            self._batch_started = True
            self._transport = transport
            self._deadline_seconds = deadline_seconds
            self._deadline_exit = "running"
            self._owned_client_state = (
                "not_created" if owned_client_applicable else "not_applicable"
            )

    def submit(self) -> PageFetchWorkTicket:
        """Record one logical physical-fetch submission and return its transition handle."""
        with self._lock:
            if self._deadline_exit not in {"not_started", "running"}:
                raise RuntimeError("cannot submit page work after the batch has exited")
            self._submitted += 1
            return PageFetchWorkTicket(self)

    def mark_batch_exit(self, exit_reason: BatchExit) -> None:
        """Record the first batch exit; worker settlement may continue afterwards."""
        with self._lock:
            if self._deadline_exit in {"not_started", "running"}:
                self._deadline_exit = exit_reason
                self._exit_snapshot = self.snapshot()

    def exit_snapshot(self) -> dict[str, object] | None:
        """Return a copy of the frozen batch-exit snapshot, if the batch has exited."""
        with self._lock:
            return dict(self._exit_snapshot) if self._exit_snapshot is not None else None

    def set_owned_client_state(self, state: OwnedClientState) -> None:
        """Record transport-owned client cleanup without inferring global thread state."""
        with self._lock:
            if self._owned_client_state == "not_applicable" and state != "not_applicable":
                raise RuntimeError("this transport does not own a client")
            self._owned_client_state = state

    def snapshot(self) -> dict[str, object]:
        """Return an isolated point-in-time snapshot; later settlement cannot rewrite it."""
        with self._lock:
            queued_now = self._submitted - self._started - self._cancelled_before_start
            outstanding_now = self._started - self._finished
            if queued_now < 0 or outstanding_now < 0 or self._failed > self._finished:
                raise RuntimeError("page-fetch lifecycle accounting invariant violated")
            return {
                "schemaVersion": _SCHEMA_VERSION,
                "receiptType": "pageFetchLifecycle",
                "batchId": self._batch_id,
                "source": self._source,
                "runId": self._run_id,
                "phase": self._phase,
                "transport": self._transport or "not_started",
                "deadlineKind": "total_batch",
                "deadlineSeconds": self._deadline_seconds,
                "deadlineExit": self._deadline_exit,
                "ownedClientState": self._owned_client_state,
                "submitted": self._submitted,
                "started": self._started,
                "finished": self._finished,
                "failed": self._failed,
                "cancelledBeforeStart": self._cancelled_before_start,
                "queuedNow": queued_now,
                "outstandingNow": outstanding_now,
                "asyncCancellationRequested": self._async_cancellation_requested,
                "asyncCleanupFinished": self._async_cleanup_finished,
            }

    def _mark_worker_submitted(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            if ticket._state == "cancelled_before_start":
                return False
            ticket._worker_submitted = True
            return True

    def _worker_was_submitted(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            return ticket._worker_submitted

    def _start(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            if ticket._state != "queued":
                return False
            ticket._state = "started"
            self._started += 1
            return True

    def _finish(self, ticket: PageFetchWorkTicket, *, failed: bool) -> bool:
        with self._lock:
            if ticket._state != "started":
                return False
            ticket._state = "finished"
            self._finished += 1
            if failed:
                self._failed += 1
            return True

    def _cancel_before_start(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            if ticket._state != "queued":
                return False
            ticket._state = "cancelled_before_start"
            self._cancelled_before_start += 1
            return True

    def _request_async_cancel(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            if ticket._state in {"finished", "cancelled_before_start"}:
                return False
            if ticket._async_cancel_requested:
                return False
            ticket._async_cancel_requested = True
            self._async_cancellation_requested += 1
            return True

    def _finish_async_cleanup(self, ticket: PageFetchWorkTicket) -> bool:
        with self._lock:
            if not ticket._async_cancel_requested or ticket._async_cleanup_finished:
                return False
            ticket._async_cleanup_finished = True
            self._async_cleanup_finished += 1
            return True


class PageFetchLifecycleContext:
    """Run identity and thread-safe, per-phase batch allocation; no receipt collector."""

    def __init__(self, *, run_id: str, source: str) -> None:
        self._run_id = run_id
        self._source = source
        self._lock = threading.Lock()
        self._phase_batches: dict[str, int] = {}

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def source(self) -> str:
        return self._source

    def new_receipt(self, phase: str) -> PageFetchLifecycle:
        with self._lock:
            batch = self._phase_batches.get(phase, 0) + 1
            self._phase_batches[phase] = batch
        return PageFetchLifecycle(
            batch_id=str(batch), run_id=self._run_id, source=self._source, phase=phase
        )


def rollup_page_fetch_lifecycles(
    lifecycles: Iterable[PageFetchLifecycle],
) -> dict[str, object]:
    """Aggregate unique batch identities without hiding pending worker settlement."""
    snapshots_by_identity: dict[
        tuple[str, str | None, str | None, str | None], dict[str, object]
    ] = {}
    for lifecycle in lifecycles:
        snapshot = lifecycle.snapshot()
        identity = (
            str(snapshot["batchId"]),
            snapshot["source"] if isinstance(snapshot["source"], str) else None,
            snapshot["runId"] if isinstance(snapshot["runId"], str) else None,
            snapshot["phase"] if isinstance(snapshot["phase"], str) else None,
        )
        snapshots_by_identity.setdefault(identity, snapshot)

    count_fields = (
        "submitted",
        "started",
        "finished",
        "failed",
        "cancelledBeforeStart",
        "queuedNow",
        "outstandingNow",
        "asyncCancellationRequested",
        "asyncCleanupFinished",
    )
    totals = {field: 0 for field in count_fields}
    for snapshot in snapshots_by_identity.values():
        for field in count_fields:
            value = snapshot[field]
            if isinstance(value, int):
                totals[field] += value

    pending_async_cleanup = max(
        0,
        totals["asyncCancellationRequested"] - totals["asyncCleanupFinished"],
    )
    batch_running = any(
        snapshot["deadlineExit"] == "running" for snapshot in snapshots_by_identity.values()
    )
    pending = (
        batch_running
        or totals["queuedNow"] > 0
        or totals["outstandingNow"] > 0
        or pending_async_cleanup > 0
    )
    return {
        "schemaVersion": _SCHEMA_VERSION,
        "receiptType": "pageFetchLifecycleRollup",
        "batchCount": len(snapshots_by_identity),
        **totals,
        "pendingAsyncCleanup": pending_async_cleanup,
        "state": "pending" if pending else "clean",
    }
