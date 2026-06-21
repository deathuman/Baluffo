"""Compact ops live-task summary helpers.

AI boundary owns: compact current-task payload summaries for low-cost admin polling.
AI boundary implement in: this file for compact summary trimming; full payload assembly stays in ops live helpers.
AI boundary search before contracts: ops status summary routes, active task snapshot, and frontend polling tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused live-task summary tests.
"""

from __future__ import annotations

from typing import Any

from src.shared.live_task import normalize_live_task_payload


def compact_live_task_payload(payload: dict[str, Any], *, task_type: str) -> dict[str, Any]:
    normalized = normalize_live_task_payload(payload, task_type=task_type)
    work_items = (
        normalized.get("workItems") if isinstance(normalized.get("workItems"), list) else []
    )
    recent_events = (
        normalized.get("recentEvents") if isinstance(normalized.get("recentEvents"), list) else []
    )
    return {
        **normalized,
        "summaryView": True,
        "detailLevel": "summary",
        "workItemCount": len(work_items),
        "workItemsTruncated": len(work_items) > 0,
        "workItems": [],
        "recentEventCount": len(recent_events),
        "recentEvents": list(recent_events[-5:]),
        "recentEventsTruncated": len(recent_events) > 5,
    }
