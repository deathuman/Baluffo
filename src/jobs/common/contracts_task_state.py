"""Task-state normalization helpers for jobs fetch progress payloads.

AI boundary owns: task-state progress contract normalization for jobs runtime payloads.
AI boundary implement in: this file for task-state contract fields; live route summaries stay in bridge/shared live-task helpers.
AI boundary search before contracts: fetcher runtime contracts, pipeline progress, and task-state tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task-state contract tests.
"""

from __future__ import annotations

from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object
from src.shared.live_task import (
    build_live_task_contract_fields,
    normalize_live_task_payload,
)


def normalize_task_state_payload(
    payload: dict[str, Any],
    *,
    run_id: str = "",
    started_at: str,
    finished_at: str = "",
    report_path: str = "",
) -> dict[str, Any]:
    src = as_json_object(payload)
    normalized = normalize_live_task_payload(
        src,
        task_type="fetch",
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
    )
    live_task_fields = build_live_task_contract_fields(normalized)
    summary = as_json_object(src.get("summary"))
    outputs = as_json_object(normalized.get("outputs"))
    return {
        "schemaVersion": SCHEMA_VERSION,
        "taskType": clean_text(normalized.get("taskType")) or "fetch",
        "status": norm_text(normalized.get("status")),
        "active": bool(normalized.get("active")),
        "runId": clean_text(normalized.get("runId")) or clean_text(run_id),
        "startedAt": clean_text(normalized.get("startedAt")) or clean_text(started_at),
        "finishedAt": clean_text(normalized.get("finishedAt")) or clean_text(finished_at),
        **live_task_fields,
        "summary": {
            "queued": _clamped_int(summary.get("queued"), 0, 0),
            "running": _clamped_int(summary.get("running"), 0, 0),
            "ok": _clamped_int(summary.get("ok"), 0, 0),
            "error": _clamped_int(summary.get("error"), 0, 0),
            "excluded": _clamped_int(summary.get("excluded"), 0, 0),
        },
        "outputs": {"report": clean_text(outputs.get("report")) or clean_text(report_path)},
    }
