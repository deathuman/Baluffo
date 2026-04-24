"""Task-state normalization helpers for jobs fetch progress payloads."""

from __future__ import annotations

from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.live_task import (
    build_live_task_contract_fields,
    normalize_live_task_payload,
)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_task_state_payload(
    payload: dict[str, Any],
    *,
    run_id: str = "",
    started_at: str,
    finished_at: str = "",
    report_path: str = "",
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    normalized = normalize_live_task_payload(
        src,
        task_type="fetch",
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
    )
    live_task_fields = build_live_task_contract_fields(normalized)
    summary = _as_dict(src.get("summary"))
    outputs = _as_dict(normalized.get("outputs"))
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
