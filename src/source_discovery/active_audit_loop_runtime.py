"""Active audit resumable loop execution and batch selection.

AI boundary owns: selecting the next unprocessed batch and driving the resumable active-audit loop to completion or pause.
AI boundary implement in: this file for loop control; single-batch execution stays in active_audit_batch_runtime.
AI boundary search before contracts: loop strategy callables, artifact progress keys, and active audit batch tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime_batch.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.shared.json_shapes import (
    as_json_object as _as_dict,
)

from .active_audit_batch_strategy import _emit_progress as _emit_progress
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


def run_active_audit_loop(
    *,
    artifact: dict[str, Any],
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    max_batches: int,
    strategy: ActiveAuditLoopStrategy,
) -> ActiveAuditLoopResult:
    batches_run = 0
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

    while True:
        batch_rows, cursor = _next_unprocessed_batch(
            source_rows,
            completed_identities,
            effective_batch_size,
            strategy.row_identity,
        )
        progress = _as_dict(artifact.get("progress"))
        progress["cursorPosition"] = int(cursor)
        artifact["progress"] = progress

        if not batch_rows:
            strategy.before_write()
            strategy.write_artifact(True)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "audit_complete",
                    "phaseLabel": "Completed active audit",
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": effective_batch_size,
                },
                force=True,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=True,
            )

        if effective_max_batches and batches_run >= effective_max_batches:
            strategy.before_write()
            strategy.write_artifact(False)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "audit_paused",
                    "phaseLabel": "Paused active audit",
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": effective_batch_size,
                },
                force=True,
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
                "batchSize": effective_batch_size,
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
                "batchSize": effective_batch_size,
            },
            force=True,
        )
        if effective_max_batches and batches_run >= effective_max_batches:
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=complete,
            )
