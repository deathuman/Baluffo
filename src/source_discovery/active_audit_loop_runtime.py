"""Active audit resumable loop execution and batch selection.

AI boundary owns: selecting the next unprocessed batch and driving the resumable active-audit loop
to completion or pause, sequentially or with batch heads and tails pipelined.
AI boundary implement in: this file for loop control; single-batch execution stays in active_audit_batch_runtime.
AI boundary search before contracts: loop strategy callables, artifact progress keys, and active audit batch tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime_batch.py -q`.

Two loop shapes share the batch selection, the artifact writes and the progress stream:

* **Sequential** -- the default, and what every strategy without `start_batch`/`finish_batch` gets:
  each batch runs whole, then its artifact is written.
* **Pipelined** -- what a strategy that declares both gets. Batch N's tail (its phases after the
  homepage analysis) runs *after* batch N+1's head (prepare plus homepage fetch) has started, so a
  deferred analysis overlaps the next fetch. Batch N's rows are claimed for selection the moment its
  head starts, and its artifact write still happens as soon as its own tail finishes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.shared.json_shapes import (
    as_json_object as _as_dict,
)

from .active_audit_batch_strategy import _emit_progress as _emit_progress
from .active_audit_contracts import ActiveAuditBatchHead as ActiveAuditBatchHead
from .active_audit_contracts import ActiveAuditLoopResult as ActiveAuditLoopResult
from .active_audit_contracts import ActiveAuditLoopStrategy as ActiveAuditLoopStrategy


def _next_unprocessed_batch(
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    row_identity: Callable[[dict[str, Any]], str],
) -> tuple[list[dict[str, Any]], int]:
    batch: list[dict[str, Any]] = []
    cursor = len(source_rows)
    for index, row in enumerate(source_rows):
        row_id = row_identity(row)
        if row_id and row_id in completed_identities:
            continue
        if not batch:
            cursor = index
        batch.append(row)
        if len(batch) >= batch_size:
            break
    return batch, cursor


def _emit_loop_exhausted(
    strategy: ActiveAuditLoopStrategy,
    *,
    phase: str,
    label: str,
    completed: int,
    total: int,
    batch_size: int,
) -> None:
    _emit_progress(
        strategy.progress_callback,
        {
            "phase": phase,
            "phaseLabel": label,
            "completed": completed,
            "total": total,
            "batchSize": batch_size,
        },
        force=True,
    )


def run_active_audit_loop(
    *,
    artifact: dict[str, Any],
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    max_batches: int,
    strategy: ActiveAuditLoopStrategy,
) -> ActiveAuditLoopResult:
    effective_batch_size = max(1, int(batch_size or 1))
    effective_max_batches = max(0, int(max_batches or 0))
    _emit_progress(
        strategy.progress_callback,
        {
            "phase": "audit_setup",
            "phaseLabel": "Preparing active audit queue",
            "completed": len(completed_identities),
            "total": len(source_rows),
            "batchSize": effective_batch_size,
        },
        force=True,
    )
    if strategy.start_batch is not None and strategy.finish_batch is not None:
        return _run_pipelined_active_audit_loop(
            artifact=artifact,
            source_rows=source_rows,
            completed_identities=completed_identities,
            batch_size=effective_batch_size,
            max_batches=effective_max_batches,
            strategy=strategy,
        )
    return _run_sequential_active_audit_loop(
        artifact=artifact,
        source_rows=source_rows,
        completed_identities=completed_identities,
        batch_size=effective_batch_size,
        max_batches=effective_max_batches,
        strategy=strategy,
    )


def _run_sequential_active_audit_loop(
    *,
    artifact: dict[str, Any],
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    max_batches: int,
    strategy: ActiveAuditLoopStrategy,
) -> ActiveAuditLoopResult:
    batches_run = 0
    while True:
        batch_rows, cursor = _next_unprocessed_batch(
            source_rows,
            completed_identities,
            batch_size,
            strategy.row_identity,
        )
        progress = _as_dict(artifact.get("progress"))
        progress["cursorPosition"] = int(cursor)
        artifact["progress"] = progress

        if not batch_rows:
            strategy.before_write()
            strategy.write_artifact(True)
            _emit_loop_exhausted(
                strategy,
                phase="audit_complete",
                label="Completed active audit",
                completed=len(completed_identities),
                total=len(source_rows),
                batch_size=batch_size,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=True,
            )

        if max_batches and batches_run >= max_batches:
            strategy.before_write()
            strategy.write_artifact(False)
            _emit_loop_exhausted(
                strategy,
                phase="audit_paused",
                label="Paused active audit",
                completed=len(completed_identities),
                total=len(source_rows),
                batch_size=batch_size,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=False,
            )

        batch_number = batches_run + 1
        strategy.emit_batch_log(batch_number, len(batch_rows), cursor)
        _emit_progress(
            strategy.progress_callback,
            {
                "phase": "batch_queued",
                "phaseLabel": "Queued active audit batch",
                "batch": batch_number,
                "batchRows": len(batch_rows),
                "cursor": int(cursor),
                "completed": len(completed_identities),
                "total": len(source_rows),
                "batchSize": batch_size,
            },
            force=True,
        )
        strategy.run_batch(batch_rows, cursor, batch_number)
        batches_run += 1

        strategy.before_write()
        complete = len(completed_identities) >= len(source_rows)
        strategy.write_artifact(complete)
        _emit_progress(
            strategy.progress_callback,
            {
                "phase": "batch_written",
                "phaseLabel": "Wrote active audit batch",
                "batch": batch_number,
                "batchRows": len(batch_rows),
                "cursor": int(cursor),
                "completed": len(completed_identities),
                "total": len(source_rows),
                "batchSize": batch_size,
            },
            force=True,
        )
        if max_batches and batches_run >= max_batches:
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=complete,
            )


def _run_pipelined_active_audit_loop(
    *,
    artifact: dict[str, Any],
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    max_batches: int,
    strategy: ActiveAuditLoopStrategy,
) -> ActiveAuditLoopResult:
    """Start batch N+1's head, then finish batch N's tail: the deferred analysis hides in between.

    A batch's rows are claimed for selection when its head starts -- its identities join
    `inflight_identities` until its tail completes them -- because the next selection happens
    before that tail runs and would otherwise pick the same rows again.
    """
    start_batch = strategy.start_batch
    finish_batch = strategy.finish_batch
    if start_batch is None or finish_batch is None:  # pragma: no cover - guarded by the dispatcher
        raise RuntimeError("the pipelined loop needs both start_batch and finish_batch")
    batches_run = 0
    batches_started = 0
    pending: ActiveAuditBatchHead | None = None
    inflight_identities: set[str] = set()

    while True:
        batch_rows, cursor = _next_unprocessed_batch(
            source_rows,
            completed_identities | inflight_identities,
            batch_size,
            strategy.row_identity,
        )
        progress = _as_dict(artifact.get("progress"))
        progress["cursorPosition"] = int(cursor)
        artifact["progress"] = progress

        head: ActiveAuditBatchHead | None = None
        can_start = bool(batch_rows) and (not max_batches or batches_started < max_batches)
        if can_start:
            batches_started += 1
            batch_number = batches_started
            strategy.emit_batch_log(batch_number, len(batch_rows), cursor)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "batch_queued",
                    "phaseLabel": "Queued active audit batch",
                    "batch": batch_number,
                    "batchRows": len(batch_rows),
                    "cursor": int(cursor),
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": batch_size,
                },
                force=True,
            )
            head = start_batch(batch_rows, cursor, batch_number)
            inflight_identities = {
                identity
                for identity in (strategy.row_identity(row) for row in batch_rows)
                if identity
            }

        complete = False
        finished_batch = False
        if pending is not None:
            batch_number = pending.batch_number
            cursor = pending.cursor
            finished_rows = len(pending.batch_rows)
            finish_batch(pending)
            pending = None
            batches_run += 1
            finished_batch = True
            if head is None:
                # Only the head started this iteration still holds a claim; without one the loop
                # has no work in flight and the next selection may take every unfinished row.
                inflight_identities = set()

            strategy.before_write()
            complete = len(completed_identities) >= len(source_rows)
            strategy.write_artifact(complete)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "batch_written",
                    "phaseLabel": "Wrote active audit batch",
                    "batch": batch_number,
                    "batchRows": finished_rows,
                    "cursor": int(cursor),
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": batch_size,
                },
                force=True,
            )

        if head is not None:
            pending = head
            continue

        if finished_batch and max_batches and batches_started >= max_batches:
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=complete,
            )

        if not batch_rows:
            strategy.before_write()
            strategy.write_artifact(True)
            _emit_loop_exhausted(
                strategy,
                phase="audit_complete",
                label="Completed active audit",
                completed=len(completed_identities),
                total=len(source_rows),
                batch_size=batch_size,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=True,
            )

        strategy.before_write()
        strategy.write_artifact(False)
        _emit_loop_exhausted(
            strategy,
            phase="audit_paused",
            label="Paused active audit",
            completed=len(completed_identities),
            total=len(source_rows),
            batch_size=batch_size,
        )
        return ActiveAuditLoopResult(
            batches_run=batches_run,
            completed_identities=set(completed_identities),
            complete=False,
        )
