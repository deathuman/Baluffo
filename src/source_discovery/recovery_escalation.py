"""Recovery escalation helpers for directory discovery.

When a directory row (e.g. GameDevMap) fails same-party careers recovery,
escalate before rejecting: emit bounded provider-pattern candidates from the
studio name (existing `provider_patterns` builders), so studios whose careers
live on a supported ATS under a different domain (Workable, Teamtailor,
Greenhouse, ...) are still discovered.

AI boundary owns: bounded recovery-escalation candidate generation.
AI boundary implement in: this file for escalation logic; pattern builders and
web-search generation stay in their own leaves.
AI boundary search before contracts: provider patterns, gamedevmap recovery, and
escalation tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused escalation tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import DEFAULT_DISCOVERY_CONFIG
from .provider_patterns import build_pattern_candidates

_ESCALATION_CONFIG = dict(DEFAULT_DISCOVERY_CONFIG.get("gamedevmap") or {})
_RECHECK_QUEUE_PATH: Path | None = None


def set_escalation_config(cfg: dict[str, Any] | None) -> None:
    """Point the escalation knobs at the active run's gamedevmap config section."""
    global _ESCALATION_CONFIG
    _ESCALATION_CONFIG = dict(cfg or DEFAULT_DISCOVERY_CONFIG.get("gamedevmap") or {})


def set_recheck_queue_path(path: Path | str | None) -> None:
    """Allow tests and the orchestrator to point at the runtime recheck queue."""
    global _RECHECK_QUEUE_PATH
    _RECHECK_QUEUE_PATH = Path(path) if path else None


def enqueue_recheck_rows(
    rows: list[dict[str, Any]],
    *,
    max_rows: int | None = None,
) -> int:
    """Best-effort bounded append of rejected directory rows for web-search re-staging.

    Rows land in the same recheck queue consumed by `studio_seeds_with_feed_recheck`
    in config.py, so the next discovery run's web-search stage queries the studio.
    Never raises; returns the number of rows actually appended.
    """
    queue_path = _RECHECK_QUEUE_PATH
    if queue_path is None:
        return 0
    max_rows = max(0, int(max_rows) if max_rows is not None else escalation_max_rows())
    if not rows or max_rows <= 0:
        return 0
    try:
        existing: list[dict[str, Any]] = []
        if queue_path.exists():
            payload = json.loads(queue_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                existing = [row for row in payload if isinstance(row, dict)]
        seen = {str(row.get("studio") or "").strip().lower() for row in existing}
        appended = 0
        for row in rows:
            studio = str(row.get("studio") or "").strip()
            if not studio or studio.lower() in seen or appended >= max_rows:
                continue
            seen.add(studio.lower())
            existing.append(
                {
                    "studio": studio,
                    "name": str(row.get("name") or studio),
                    "url": str(row.get("url") or ""),
                    "reason": str(row.get("reason") or ""),
                    "detectedAt": str(row.get("detectedAt") or ""),
                }
            )
            appended += 1
        if appended:
            queue_path.parent.mkdir(parents=True, exist_ok=True)
            queue_path.write_text(
                json.dumps(existing[-1000:], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return appended
    except (OSError, ValueError, TypeError):
        return 0


def _config_value(key: str, default: Any) -> Any:
    return _ESCALATION_CONFIG.get(key, default)


def escalation_enabled() -> bool:
    return bool(_config_value("activeAuditRecoveryEscalationEnabled", True))


def escalation_max_rows() -> int:
    return max(0, int(_config_value("activeAuditRecoveryEscalationMaxRows", 200)))


def escalation_pattern_limit() -> int:
    return max(0, int(_config_value("activeAuditRecoveryEscalationPatternLimit", 4)))


def _row_studio(row: dict[str, Any]) -> str:
    return str(row.get("studio") or "").strip()


def provider_pattern_escalation_candidates(
    row: dict[str, Any],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Emit provider-pattern candidates for a rejected directory row.

    Builds a minimal seed from the row's studio name and runs the existing
    provider-pattern builders (workable/greenhouse/teamtailor/...), bounded by
    `limit`. Returns [] when the row carries no usable studio name.
    """
    studio = _row_studio(row)
    if not studio or limit <= 0:
        return []
    seed = {
        "name": studio,
        "studio": studio,
        "nlPriority": False,
    }
    candidates = build_pattern_candidates([seed])
    return candidates[:limit]


def escalate_rejected_rows(
    rejected_rows: list[dict[str, Any]],
    *,
    max_rows: int | None = None,
    pattern_limit: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split `no_careers_evidence` rejections into escalated candidates + kept rejections.

    Returns (escalated_candidates, remaining_rejections). Escalation is bounded
    by `max_rows` (default from config) and per-row pattern candidates by
    `pattern_limit` (default from config). Non-no-careers rejections pass
    through unchanged.
    """
    if not escalation_enabled():
        return [], list(rejected_rows)
    max_rows = max(0, int(max_rows) if max_rows is not None else escalation_max_rows())
    pattern_limit = max(
        0, int(pattern_limit) if pattern_limit is not None else escalation_pattern_limit()
    )
    escalated: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    escalated_count = 0
    for row in rejected_rows:
        if (
            escalated_count < max_rows
            and str(row.get("reason") or "").strip() == "no_careers_evidence"
        ):
            candidates = provider_pattern_escalation_candidates(row, limit=pattern_limit)
            if candidates:
                for candidate in candidates:
                    candidate["evidenceSource"] = "recovery_escalation"
                escalated.extend(candidates)
                escalated_count += 1
                continue
        remaining.append(row)
    return escalated, remaining


def enqueue_rejected_for_web_search(
    rejected_rows: list[dict[str, Any]],
    *,
    max_rows: int | None = None,
) -> int:
    """Append `no_careers_evidence` rejections to the web-search recheck queue."""
    eligible = [
        row
        for row in rejected_rows
        if str(row.get("reason") or "").strip() == "no_careers_evidence"
    ]
    return enqueue_recheck_rows(eligible, max_rows=max_rows)
