"""Bounded run identity for the fetch report and the summary sidecar beside it.

AI boundary owns: reading ``(runId, finishedAt)`` from a bounded report prefix.
AI boundary implement in: this file for the reader; callers own what the identity decides.
AI boundary search before contracts: the desktop success gate and the pipeline launch baseline count.
AI boundary verify: focused fetch-report gate and admin task-runtime tests.
"""

from __future__ import annotations

from pathlib import Path

from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)

FETCH_REPORT_IDENTITY_PREFIX_BYTES = 8 * 1024


def fetch_report_identity(report_path: Path | str) -> tuple[str, str] | None:
    """Read ``(runId, finishedAt)`` from a bounded prefix of the fetch report.

    Every report writer puts these keys ahead of the bulky ``taskProgress`` and
    ``sources`` values, so a few kilobytes settle which run a report — or the
    summary sidecar written beside it — describes, without parsing the rest of a
    multi-megabyte file. A prefix that cannot decode both keys, or a report whose
    run has not finished, returns ``None`` so the caller falls back to the full
    report rather than guessing.

    Identity is used instead of mtime ordering because filesystem timestamps
    advance in coarse clock ticks: two back-to-back writes of a small report and
    its sidecar can share a timestamp, and a rewritten identical report can keep
    an older one, neither of which says the sidecar is stale.
    """
    text = read_json_prefix(Path(report_path), max_bytes=FETCH_REPORT_IDENTITY_PREFIX_BYTES)
    if not text:
        return None
    spans = top_level_json_field_spans(text)
    if "runId" not in spans or "finishedAt" not in spans:
        return None
    finished_at = str(decode_json_span(text, spans, "finishedAt", "") or "").strip()
    if not finished_at:
        return None
    run_id = str(decode_json_span(text, spans, "runId", "") or "").strip()
    return (run_id, finished_at)
