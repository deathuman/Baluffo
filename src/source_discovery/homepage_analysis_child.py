"""One batch's homepage analysis run in a worker process, overlapped with the next fetch.

AI boundary owns: the analysis worker transport (spawn, hand-off, result pickup), its fallback to
in-process analysis, and the transport's own receipt.
AI boundary implement in: this file for the process boundary; the batch head/tail that uses it stays
in active_audit_batch_runtime and the audit wiring stays in gamedevmap_active_dry_run.
AI boundary search before contracts: active-audit batch contracts, phase runtime receipts, and the
homepage analysis cost snapshot in docs/snapshots.
AI boundary verify: `python -m pytest tests/source_discovery/test_homepage_analysis_child.py -q`.

Why this exists (measured, `docs/snapshots/discovery-homepage-analysis-cost-2026-10-27.md`): the
phase is **17%** of a full audit arm's wall and saturates one core, while the audit's fetch waves
leave the machine idle. Threads do not help (the phase is GIL-bound: 1.02x at four workers) and
running it beside the live fetch in-process is a wash (the fetch's lanes wait on the GIL), but the
same analysis in a child process keeps the fetch's solo wall. This module is that child.

The contract: `start(results)` hands one batch over and returns immediately, `collect(handle)`
returns the same `ActiveHomepageBatchResult` the in-process analysis would have produced. The
child is handed exactly the parent's own inputs and runs the parent's own analysis function, so the
result is identical by construction; the pilot pinned that on real corpus data
(`_out/perf-runs/discovery/homepage-analysis-overlap-20261027/pilot.json`, one digest from the
in-process call, the child call and the threaded child call).

Deliberate limits, all of them visible in `receipt()` rather than hidden:

* **Any failure falls back to in-process analysis for that batch** (spawn refused, worker died,
  reply timed out, malformed reply) and the transport is retired for the rest of the run. The audit
  therefore cannot lose a batch to this change; it can only lose the overlap.
* **A frozen runtime never uses the transport.** `multiprocessing`'s spawn re-launches
  `sys.executable`, and the packaged launcher has no multiprocessing bootstrap, so `analysis_child_
  supported()` reports unsupported and every batch is analyzed in-process.
* **The child's cost is the child's**: the phase receipt around a deferred analysis measures the
  collect -- the parent's time inside the phase, not the wait that precedes it -- so this receipt
  carries the worker's own wall and CPU per batch, the hand-off and the parent's wait, and the
  caller publishes it beside the artifact.
"""

from __future__ import annotations

import contextlib
import multiprocessing
import os
import pickle
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .active_audit_contracts import ActiveHomepageBatchResult

# `0`/`false`/`no`/`off` retires the transport; anything else (including unset) keeps it enabled.
HOMEPAGE_ANALYSIS_CHILD_ENV = "BALUFFO_DISCOVERY_HOMEPAGE_ANALYSIS_CHILD"
DISABLED_ENV_VALUES = frozenset({"0", "false", "no", "off"})
# A batch's analysis is seconds, not minutes; the bound exists so a wedged worker cannot park the
# audit (the fetch batches have their own total deadline for the same reason).
DEFAULT_COLLECT_TIMEOUT_S = 600.0
_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


class HomepageAnalysisChildFailure(RuntimeError):
    """The child could not produce this batch's result; the caller analyzes it in-process."""


@dataclass
class HomepageAnalysisHandle:
    """One handed-over batch: an in-flight worker job, or the batch to analyze in the caller."""

    index: int
    results: list[dict[str, Any]]
    offloaded: bool
    started_at: float
    writer: threading.Thread | None = None
    payload_bytes: int = 0
    handoff_ms: int = 0
    send_error: str = ""


def analysis_child_env_enabled() -> bool:
    """Whether the operator left the worker transport enabled (default: enabled)."""
    raw = str(os.environ.get(HOMEPAGE_ANALYSIS_CHILD_ENV, "")).strip().lower()
    return raw not in DISABLED_ENV_VALUES


def analysis_child_supported() -> tuple[bool, str]:
    """Whether a spawn worker can run here, and the reason when it cannot."""
    if getattr(sys, "frozen", False):
        return False, "frozen-runtime"
    try:
        multiprocessing.get_context("spawn")
    except (AttributeError, ValueError) as exc:  # pragma: no cover - no interpreter lacks spawn
        return False, f"spawn-unavailable: {exc}"
    return True, ""


def analysis_child_requested(enabled: bool | None = None) -> tuple[bool, str]:
    """The run-level decision: an explicit caller choice, else the environment, else capability.

    An explicit `True` is a caller's own opt-in and outranks the environment knob (tests rely on
    that); `None` is what every audit passes, so an operator's `..._CHILD=0` retires the transport
    for a whole run without a code change.
    """
    if enabled is not None:
        if not bool(enabled):
            return False, "disabled"
        return analysis_child_supported()
    if not analysis_child_env_enabled():
        return False, "disabled-by-env"
    return analysis_child_supported()


def _analysis_worker(connection: Any, analyze: Any, analysis_kwargs: dict[str, Any]) -> None:
    """The worker loop: receive one batch, analyze it, send the result and its own cost back.

    Module-level, and receives the analysis function itself, so the spawn context can import it by
    name and the child runs the parent's own code path on the parent's own arguments. Every batch is
    independent, so the worker holds no state between them and a restart costs at most one batch.
    """
    while True:
        try:
            message = pickle.loads(connection.recv_bytes())
        except (EOFError, OSError):
            return
        command = str(message[0])
        if command == "exit":
            try:
                connection.send_bytes(pickle.dumps(("bye", None, None), protocol=_PICKLE_PROTOCOL))
            except OSError:
                pass
            return
        _, _index, results = message
        started = time.perf_counter()
        cpu_started = time.process_time()
        # Deliberately blind: the analysis runs on arbitrary page content, and any failure has to
        # come back as this batch's error reply so the parent can analyze it in-process instead.
        payload: tuple[str, Any, Any]
        try:
            result = analyze(results, **analysis_kwargs)
        except Exception as exc:
            payload = ("error", f"{type(exc).__name__}: {exc}", None)
        else:
            payload = (
                "ok",
                result,
                {
                    "wallS": time.perf_counter() - started,
                    "cpuS": time.process_time() - cpu_started,
                },
            )
        try:
            connection.send_bytes(pickle.dumps(payload, protocol=_PICKLE_PROTOCOL))
        except (BrokenPipeError, OSError):
            return


class HomepageAnalysisChild:
    """A persistent worker that analyzes one batch while the caller fetches the next.

    `start` and `collect` are the whole surface: `start(results)` is the caller's hand-off (a thread
    picks and ships the payload, so the caller's critical path carries neither the pickling nor the
    child's start-up), and `collect(handle)` joins, waits for the reply and returns the result.
    Calls that never went to a worker -- the transport is off, or it failed and retired -- are
    analyzed in-process by `collect`, so the caller does not branch on the transport at all.
    """

    def __init__(
        self,
        analyze: Callable[..., ActiveHomepageBatchResult],
        *,
        analysis_kwargs: dict[str, Any] | None = None,
        enabled: bool | None = None,
        log: Callable[[str], None] | None = None,
        collect_timeout_s: float = DEFAULT_COLLECT_TIMEOUT_S,
    ) -> None:
        self._analyze = analyze
        self._analysis_kwargs = dict(analysis_kwargs or {})
        self._log = log
        self._requested, self._disabled_reason = analysis_child_requested(enabled)
        self._collect_timeout_s = max(1.0, float(collect_timeout_s))
        self._context: Any | None = None
        self._process: Any | None = None
        self._connection: Any | None = None
        self._batches: list[dict[str, Any]] = []
        self._index = 0
        if self._requested and not _picklable((analyze, self._analysis_kwargs)):
            self._requested, self._disabled_reason = False, "analysis-not-picklable"

    # --- caller surface ---------------------------------------------------------------

    def start(self, results: list[dict[str, Any]]) -> HomepageAnalysisHandle:
        """Hand one batch over and return at once; the worker analyzes it from here on."""
        self._index += 1
        started_at = time.perf_counter()
        handle = HomepageAnalysisHandle(
            index=self._index,
            results=results,
            offloaded=False,
            started_at=started_at,
        )
        if self._worker_ready():
            handle.offloaded = True
            handle.writer = threading.Thread(
                target=self._ship_batch,
                args=(handle,),
                name=f"homepage-analysis-send-{handle.index}",
                daemon=True,
            )
            handle.writer.start()
        return handle

    def collect(self, handle: HomepageAnalysisHandle) -> ActiveHomepageBatchResult:
        """Block until this batch's result exists, in the worker or here, and return it."""
        if handle.offloaded:
            try:
                result, receipt, wait_ms, decode_ms = self._receive(handle)
            except HomepageAnalysisChildFailure as failure:
                self._retire(str(failure))
                result, collect_ms = self._analyze_inline(handle)
                self._record(handle, transport="fallback", collect_ms=collect_ms, fell_back=True)
                return result
            self._record(
                handle,
                transport="child",
                wait_ms=wait_ms,
                decode_ms=decode_ms,
                child_receipt=receipt,
            )
            return result
        result, collect_ms = self._analyze_inline(handle)
        self._record(handle, transport="inline", collect_ms=collect_ms)
        return result

    def close(self) -> None:
        """Retire the worker. Safe to call twice, and safe when no worker ever started."""
        connection = self._connection
        process = self._process
        self._connection = None
        self._process = None
        if connection is not None:
            try:
                connection.send_bytes(pickle.dumps(("exit", 0, []), protocol=_PICKLE_PROTOCOL))
            except (BrokenPipeError, EOFError, OSError, ValueError):
                pass
            _close_quietly(connection)
        if process is not None:
            process.join(timeout=5.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5.0)

    @property
    def active(self) -> bool:
        """Whether later batches will still be handed to the worker."""
        return self._requested and not self._disabled_reason

    def receipt(self) -> dict[str, Any]:
        """The transport's own record: per batch, and the run's totals.

        The totals are the honest split of the phase: `childWallS`/`childCpuS` are what the worker
        spent analyzing, `parentWaitS` is what the caller spent waiting for results it needed, and
        `handoffBytes`/`parentHandoffS` are the price of the boundary itself.
        """
        child_wall_s = sum(float(row["childWallMs"]) for row in self._batches) / 1000.0
        child_cpu_s = sum(float(row["childCpuMs"]) for row in self._batches) / 1000.0
        handoff_bytes = sum(int(row["handoffBytes"]) for row in self._batches)
        parent_wait_s = sum(float(row["waitMs"]) for row in self._batches) / 1000.0
        parent_handoff_s = sum(float(row["handoffMs"]) for row in self._batches) / 1000.0
        inline_s = (
            sum(float(row["collectMs"]) for row in self._batches if row["transport"] != "child")
            / 1000.0
        )
        return {
            "schemaVersion": 1,
            "mode": "child" if self.active else "inline",
            "enabled": bool(self._requested),
            "disabledReason": self._disabled_reason,
            "batches": [dict(row) for row in self._batches],
            "summary": {
                "calls": len(self._batches),
                "offloaded": sum(1 for row in self._batches if row["transport"] == "child"),
                "inlined": sum(
                    1 for row in self._batches if row["transport"] in {"inline", "fallback"}
                ),
                "fallbacks": sum(1 for row in self._batches if row["fellBack"]),
                "childWallS": round(child_wall_s, 2),
                "childCpuS": round(child_cpu_s, 2),
                "parentWaitS": round(parent_wait_s, 2),
                "parentHandoffS": round(parent_handoff_s, 2),
                "parentInlineS": round(inline_s, 2),
                "handoffBytes": handoff_bytes,
            },
        }

    # --- worker lifecycle -------------------------------------------------------------

    def _worker_ready(self) -> bool:
        if not self.active:
            return False
        process = self._process
        if process is not None and process.is_alive():
            return True
        # Deliberately blind: a spawn failure of any kind (OSError, RuntimeError, a missing
        # bootstrap, an attribute error on a partial pool) means in-process analysis for this run.
        try:
            self._spawn_worker()
        except Exception as exc:
            self._retire(f"spawn-failed: {type(exc).__name__}: {exc}")
            return False
        return self._process is not None and self._process.is_alive()

    def _spawn_worker(self) -> None:
        self._context = multiprocessing.get_context("spawn")
        parent_connection, child_connection = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_analysis_worker,
            args=(child_connection, self._analyze, self._analysis_kwargs),
            name="homepage-analysis",
            # A worker must never outlive the audit that owns it.
            daemon=True,
        )
        try:
            process.start()
        except BaseException:
            _close_quietly(parent_connection)
            _close_quietly(child_connection)
            raise
        child_connection.close()
        self._process = process
        self._connection = parent_connection

    def _retire(self, reason: str) -> None:
        """Retire the transport for the rest of the run, logging the cause exactly once."""
        if not reason:
            return
        if not self._disabled_reason:
            self._disabled_reason = reason
            if self._log is not None:
                self._log(
                    "homepage analysis worker retired; analyzing this and later batches "
                    f"in-process: {reason}"
                )
        process = self._process
        connection = self._connection
        self._process = None
        self._connection = None
        if connection is not None:
            _close_quietly(connection)
        if process is not None:
            with contextlib.suppress(RuntimeError, OSError, ValueError):
                if process.is_alive():
                    process.terminate()
            process.join(timeout=5.0)

    # --- one batch -------------------------------------------------------------------

    def _ship_batch(self, handle: HomepageAnalysisHandle) -> None:
        """The hand-off thread: serialize the batch and send it, off the caller's critical path."""
        started = time.perf_counter()
        connection = self._connection
        # Deliberately blind: whatever went wrong -- serializing the batch or writing it to the
        # pipe -- is recorded here and raised to `collect`, which analyzes the batch in-process.
        try:
            if connection is None:
                raise HomepageAnalysisChildFailure("no worker connection")
            payload = pickle.dumps(
                ("analyze", handle.index, handle.results), protocol=_PICKLE_PROTOCOL
            )
            handle.payload_bytes = len(payload)
            connection.send_bytes(payload)
        except Exception as exc:
            handle.send_error = f"{type(exc).__name__}: {exc}"
        finally:
            handle.handoff_ms = int((time.perf_counter() - started) * 1000)

    def _receive(
        self, handle: HomepageAnalysisHandle
    ) -> tuple[ActiveHomepageBatchResult, dict[str, Any], int, int]:
        if handle.writer is not None:
            handle.writer.join()
        if handle.send_error:
            raise HomepageAnalysisChildFailure(handle.send_error)
        connection = self._connection
        if connection is None:
            raise HomepageAnalysisChildFailure("no worker connection")
        wait_started = time.perf_counter()
        if not connection.poll(self._collect_timeout_s):
            raise HomepageAnalysisChildFailure(f"no result within {self._collect_timeout_s:.0f}s")
        try:
            raw = connection.recv_bytes()
        except (EOFError, OSError) as exc:
            raise HomepageAnalysisChildFailure(f"{type(exc).__name__}: {exc}") from exc
        wait_ms = int((time.perf_counter() - wait_started) * 1000)
        decode_started = time.perf_counter()
        try:
            status, payload, receipt = pickle.loads(raw)
        except (pickle.UnpicklingError, EOFError, ValueError) as exc:
            raise HomepageAnalysisChildFailure(f"undecodable result: {exc}") from exc
        decode_ms = int((time.perf_counter() - decode_started) * 1000)
        if str(status) != "ok" or not isinstance(receipt, dict):
            raise HomepageAnalysisChildFailure(f"worker error: {payload}")
        return payload, receipt, wait_ms, decode_ms

    def _analyze_inline(
        self, handle: HomepageAnalysisHandle
    ) -> tuple[ActiveHomepageBatchResult, int]:
        """Analyze here, returning the result and what the caller spent inside the phase."""
        started = time.perf_counter()
        result = self._analyze(handle.results, **self._analysis_kwargs)
        return result, int((time.perf_counter() - started) * 1000)

    def _record(
        self,
        handle: HomepageAnalysisHandle,
        *,
        transport: str,
        collect_ms: int = 0,
        wait_ms: int = 0,
        decode_ms: int = 0,
        child_receipt: dict[str, Any] | None = None,
        fell_back: bool = False,
    ) -> None:
        receipt = child_receipt or {}
        self._batches.append(
            {
                "index": int(handle.index),
                "transport": transport,
                "fellBack": bool(fell_back),
                "beginMs": int((time.perf_counter() - handle.started_at) * 1000),
                "handoffMs": int(handle.handoff_ms),
                "handoffBytes": int(handle.payload_bytes),
                "childWallMs": int(round(float(receipt.get("wallS") or 0.0) * 1000)),
                "childCpuMs": int(round(float(receipt.get("cpuS") or 0.0) * 1000)),
                "waitMs": int(wait_ms),
                "decodeMs": int(decode_ms),
                "collectMs": int(collect_ms),
            }
        )


def _picklable(value: Any) -> bool:
    """Whether the worker could be handed this (spawn pickles the target and its arguments)."""
    # Deliberately blind: an answer of "no" is all this asks, so any failure to serialize is that
    # answer rather than an error -- the constructor's decision is to analyze in-process.
    try:
        pickle.dumps(value, protocol=_PICKLE_PROTOCOL)
    except Exception:
        return False
    return True


def _close_quietly(connection: Any) -> None:
    try:
        connection.close()
    except (AttributeError, OSError, ValueError):
        pass
