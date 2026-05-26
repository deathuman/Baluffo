"""Summary and cache payload assembly for registry conflicts.

Extracted from registry_conflicts.py as part of the conflict split.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts_row import (
    _as_dict,
    _as_list,
    _clean_text,
    _int_value,
)
from src.shared.json_io import read_json_object as read_pipeline_json_object


def _summary_conflict_card(card: dict[str, Any]) -> dict[str, Any]:
    safe_automation = _as_dict(card.get("safeAutomation"))
    target_ids = [
        _clean_text(target_id)
        for target_id in _as_list(safe_automation.get("targetIds"))[:10]
        if _clean_text(target_id)
    ]
    return {
        "familyKey": _clean_text(card.get("familyKey")),
        "rowCount": int(card.get("rowCount") or 0),
        "triageBucket": _clean_text(card.get("triageBucket")),
        "triageLabel": _clean_text(card.get("triageLabel")),
        "reviewPriority": int(card.get("reviewPriority") or 0),
        "reviewQueue": _clean_text(card.get("reviewQueue")),
        "reviewLabel": _clean_text(card.get("reviewLabel")),
        "suggestedDisposition": _clean_text(card.get("suggestedDisposition")),
        "safeAutomation": {
            key: value
            for key, value in safe_automation.items()
            if key in {"eligible", "action", "label", "route"}
        },
        "safeAutomationTargetIds": target_ids,
        "safeAutomationTargetIdsTruncated": len(_as_list(safe_automation.get("targetIds"))) > 10,
    }


def _summary_action(action: dict[str, Any]) -> dict[str, Any]:
    target_ids = [
        _clean_text(target_id)
        for target_id in _as_list(action.get("targetIds"))[:20]
        if _clean_text(target_id)
    ]
    return {
        "action": _clean_text(action.get("action")),
        "label": _clean_text(action.get("label")),
        "route": _clean_text(action.get("route")),
        "count": int(action.get("count") or len(_as_list(action.get("targetIds"))) or 0),
        "targetIds": target_ids,
        "targetIdsTruncated": len(_as_list(action.get("targetIds"))) > len(target_ids),
    }


def _summary_audit(value: Any) -> Any:
    if isinstance(value, list):
        return {"count": len(value)}
    if isinstance(value, dict):
        return {str(key): _summary_audit(row) for key, row in value.items()}
    return value


def _summary_automation(value: Any) -> dict[str, Any]:
    automation = _as_dict(value)
    return {
        "summary": _as_dict(automation.get("summary")),
        "actions": [
            _summary_action(action)
            for action in _as_list(automation.get("actions"))
            if isinstance(action, dict)
        ],
        "audit": _summary_audit(automation.get("audit")),
    }


def _summary_adjudication(value: Any) -> dict[str, Any]:
    adjudication = _as_dict(value)
    payload = {
        key: adjudication.get(key)
        for key in (
            "ok",
            "status",
            "runId",
            "startedAt",
            "finishedAt",
            "heartbeatAt",
            "applyAutopilot",
            "checkedFamilyCount",
            "checkedSourceCount",
            "demoted",
            "appliedIds",
            "taskProgress",
            "progress",
            "summary",
        )
        if key in adjudication
    }
    applied_ids = [_clean_text(row) for row in _as_list(payload.get("appliedIds"))[:20]]
    if "appliedIds" in payload:
        payload["appliedIds"] = applied_ids
        payload["appliedIdsTruncated"] = len(_as_list(adjudication.get("appliedIds"))) > len(
            applied_ids
        )
    return payload


def _registry_conflicts_summary_cache_path(source_state_path: Path) -> Path:
    return Path(source_state_path).with_name("registry-conflicts-summary.json")


def _file_signature(path: Path) -> dict[str, Any]:
    try:
        stat = Path(path).stat()
    except OSError:
        return {"path": Path(path).name, "exists": False, "size": 0, "mtimeNs": 0}
    return {
        "path": Path(path).name,
        "exists": True,
        "size": int(stat.st_size),
        "mtimeNs": int(stat.st_mtime_ns),
    }


def build_registry_conflicts_summary_cache_key(
    *,
    registry_summary: Any,
    source_state_path: Path,
    adjudication_payload: Any = None,
) -> str:
    source_path = Path(source_state_path)
    key_payload = {
        "registry": {
            "stateHash": _clean_text(_as_dict(registry_summary).get("stateHash")),
            "stateFingerprint": _clean_text(_as_dict(registry_summary).get("stateFingerprint")),
            "sqliteStateHash": _clean_text(_as_dict(registry_summary).get("sqliteStateHash")),
            "tombstoneHash": _clean_text(_as_dict(registry_summary).get("tombstoneHash")),
            "sqliteTombstoneHash": _clean_text(
                _as_dict(registry_summary).get("sqliteTombstoneHash")
            ),
            "activeCount": _int_value(_as_dict(registry_summary).get("activeCount")),
            "pendingCount": _int_value(_as_dict(registry_summary).get("pendingCount")),
            "rejectedCount": _int_value(_as_dict(registry_summary).get("rejectedCount")),
            "tombstoneCount": _int_value(_as_dict(registry_summary).get("tombstoneCount")),
        },
        "sourceState": _file_signature(source_path),
        "fetchReport": _file_signature(source_path.with_name("jobs-fetch-report.json")),
        "jobsUnified": _file_signature(source_path.with_name("jobs-unified.json")),
        "adjudication": _summary_adjudication(adjudication_payload),
    }
    return sha256(
        json.dumps(key_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def load_cached_registry_conflicts_summary(
    *,
    source_state_path: Path,
    cache_key: str,
) -> dict[str, Any] | None:
    payload = read_pipeline_json_object(
        _registry_conflicts_summary_cache_path(source_state_path), {}
    )
    if not isinstance(payload, dict) or payload.get("cacheKey") != cache_key:
        return None
    summary = _as_dict(payload.get("payload"))
    return summary or None


def write_registry_conflicts_summary_cache(
    *,
    source_state_path: Path,
    cache_key: str,
    payload: dict[str, Any],
) -> None:
    cache_path = _registry_conflicts_summary_cache_path(source_state_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp")
    tmp_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "cacheKey": cache_key,
                "payload": payload,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    tmp_path.replace(cache_path)


def load_registry_conflicts_summary_payload(
    *,
    registry_summary: Any,
    source_state_path: Path,
    adjudication_payload: Any = None,
    registry_auto_heal: Any = None,
) -> dict[str, Any]:
    normalized_registry_summary = _as_dict(registry_summary)
    cache_key = build_registry_conflicts_summary_cache_key(
        registry_summary=normalized_registry_summary,
        source_state_path=source_state_path,
        adjudication_payload=adjudication_payload,
    )
    cached = load_cached_registry_conflicts_summary(
        source_state_path=source_state_path,
        cache_key=cache_key,
    )
    if cached:
        cached["registrySummary"] = normalized_registry_summary
        cached["registryAutoHeal"] = _as_dict(registry_auto_heal)
        cached["summaryStatus"] = "ready"
        cached["summaryExact"] = True
        return cached
    return {
        "ok": True,
        "summary": {"conflictCount": 0, "familyCount": 0, "rowCount": 0},
        "summaryStatus": "pending",
        "summaryExact": False,
        "triage": {},
        "review": {},
        "automation": _summary_automation({}),
        "adjudication": _summary_adjudication(adjudication_payload),
        "registrySummary": normalized_registry_summary,
        "registryAutoHeal": _as_dict(registry_auto_heal),
        "warnings": [],
        "conflicts": [],
        "conflictSampleCount": 0,
        "conflictsTruncated": False,
        "detailRoute": "/registry/conflicts",
        "summaryView": True,
    }


def summarize_registry_conflicts_payload(
    payload: dict[str, Any],
    *,
    sample_limit: int = 5,
) -> dict[str, Any]:
    conflicts = [row for row in _as_list(payload.get("conflicts")) if isinstance(row, dict)]
    limit = max(0, int(sample_limit or 0))
    sampled_conflicts = [_summary_conflict_card(row) for row in conflicts[:limit]]
    return {
        "ok": bool(payload.get("ok", True)),
        "summary": _as_dict(payload.get("summary")),
        "summaryStatus": "ready",
        "summaryExact": True,
        "triage": _as_dict(payload.get("triage")),
        "review": _as_dict(payload.get("review")),
        "automation": _summary_automation(payload.get("automation")),
        "adjudication": _summary_adjudication(payload.get("adjudication")),
        "registrySummary": _as_dict(payload.get("registrySummary")),
        "registryAutoHeal": _as_dict(payload.get("registryAutoHeal")),
        "warnings": _as_list(payload.get("warnings")),
        "conflicts": sampled_conflicts,
        "conflictSampleCount": len(sampled_conflicts),
        "conflictsTruncated": len(conflicts) > len(sampled_conflicts),
        "detailRoute": "/registry/conflicts",
        "summaryView": True,
    }
