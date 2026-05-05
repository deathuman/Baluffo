from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.source_registry import source_identity
from src.source_registry_policy import duplicate_family_conflict_cards

SOURCE_HEALTH_FIELD_NAMES = (
    "healthScore",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_DIFF_FIELDS = (
    "name",
    "sourceId",
    "id",
    "registryState",
    "candidateState",
    "transitionReason",
    "pendingReason",
    "quarantineReason",
    "stateChangedAt",
    "stateChangedBy",
    "lastPromotedAt",
    "lastDemotedAt",
    "duplicateFamilyKey",
    "duplicateOfSourceId",
    "duplicateOfSourceName",
    "adapter",
    "jobsFound",
    "rankScore",
    "score",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_ACTIONS_BY_STATE = {
    "active": ({"action": "demote-active", "label": "Demote", "route": "/registry/demote-active"},),
    "pending": (
        {"action": "approve", "label": "Promote", "route": "/registry/approve"},
        {"action": "reject", "label": "Reject", "route": "/registry/reject"},
    ),
    "rejected": (
        {"action": "restore-rejected", "label": "Restore", "route": "/registry/restore-rejected"},
    ),
}

PROVIDER_ADAPTERS = {
    "ashby",
    "bamboohr",
    "breezy",
    "greenhouse",
    "jazzhr",
    "lever",
    "personio",
    "pinpoint",
    "recruitee",
    "smartrecruiters",
    "teamtailor",
    "workable",
}

TRIAGE_BUCKETS = (
    {
        "bucket": "exact_duplicate_auto_healable",
        "label": "Exact duplicate",
        "risk": "low",
        "description": "Rows share the same canonical source identity and are eligible for existing exact-duplicate repair.",
    },
    {
        "bucket": "active_active_likely_duplicate",
        "label": "Active-active likely duplicate",
        "risk": "high",
        "description": "More than one active row exists for the same source family.",
    },
    {
        "bucket": "pending_duplicate_of_active",
        "label": "Pending duplicate of active",
        "risk": "medium",
        "description": "A pending candidate belongs to a family that already has one active source.",
    },
    {
        "bucket": "rejected_historical_noise",
        "label": "Rejected historical noise",
        "risk": "low",
        "description": "Rejected rows are present without a higher-priority active/pending duplicate pattern.",
    },
    {
        "bucket": "ambiguous_manual_review",
        "label": "Manual review",
        "risk": "medium",
        "description": "The conflict shape is not safe to categorize more narrowly.",
    },
)

_TRIAGE_BY_BUCKET = {str(row["bucket"]): row for row in TRIAGE_BUCKETS}

REVIEW_QUEUES = (
    {
        "queue": "p0_multi_active_provider",
        "priority": 0,
        "label": "Multiple active providers",
        "description": "Multiple active API/provider rows exist for one source family.",
    },
    {
        "queue": "p1_active_provider_static",
        "priority": 1,
        "label": "Active provider + static",
        "description": "Active provider rows coexist with active static rows.",
    },
    {
        "queue": "p1_pending_provider_against_active",
        "priority": 1,
        "label": "Pending provider vs active",
        "description": "A pending API/provider candidate is competing with one active source.",
    },
    {
        "queue": "p2_same_adapter_active_variant",
        "priority": 2,
        "label": "Same-adapter active variant",
        "description": "Multiple active rows use the same non-static source type.",
    },
    {
        "queue": "p2_static_url_variant_active",
        "priority": 2,
        "label": "Active static URL variants",
        "description": "Multiple active static rows look like URL variants.",
    },
    {
        "queue": "p2_pending_static_variant",
        "priority": 2,
        "label": "Pending static variant",
        "description": "Pending static rows compete with one active source.",
    },
    {
        "queue": "p3_pending_only_intake",
        "priority": 3,
        "label": "Pending-only intake",
        "description": "Duplicate candidates are pending only, so they are not active fetch duplication.",
    },
    {
        "queue": "p3_low_signal_manual",
        "priority": 3,
        "label": "Low-signal manual review",
        "description": "The conflict does not match a higher-confidence review queue.",
    },
)

_REVIEW_BY_QUEUE = {str(row["queue"]): row for row in REVIEW_QUEUES}

SAFE_AUTO_DEMOTE_ACTION = "auto_demote_same_adapter_provider_alias"
SAFE_AUTO_DEMOTE_LABEL = "Auto-demote safe duplicate"
SAFE_AUTO_DEMOTE_ROUTE = "/registry/conflicts/auto-demote-safe"
SAFE_AUTO_DEMOTE_REASON = "registry_conflict_safe_auto_demote"

_FIELD_LABELS = {
    "name": "Name",
    "sourceId": "Source ID",
    "id": "ID",
    "registryState": "Registry state",
    "candidateState": "Candidate state",
    "transitionReason": "Transition reason",
    "pendingReason": "Pending reason",
    "quarantineReason": "Quarantine reason",
    "stateChangedAt": "State changed at",
    "stateChangedBy": "State changed by",
    "lastPromotedAt": "Last promoted at",
    "lastDemotedAt": "Last demoted at",
    "duplicateFamilyKey": "Duplicate family",
    "duplicateOfSourceId": "Duplicate of source ID",
    "duplicateOfSourceName": "Duplicate of source name",
    "adapter": "Adapter",
    "jobsFound": "Jobs found",
    "rankScore": "Rank score",
    "score": "Score",
    "lastStatus": "Last status",
    "lastRunAt": "Last run at",
    "lastCheckedAt": "Last checked at",
    "lastSuccessAt": "Last success at",
    "lastSuccessfulFetchAt": "Last successful fetch at",
    "lastSeenInFetchAt": "Last seen in fetch at",
    "lastKeptCount": "Last kept count",
    "lastJobsKept": "Last jobs kept",
    "consecutiveFailures": "Consecutive failures",
    "failureCount": "Failure count",
    "consecutiveZeroKept": "Consecutive zero-kept",
    "zeroJobStreak": "Zero-job streak",
    "health": "Health",
    "healthReason": "Health reason",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _row_identity(row: dict[str, Any]) -> str:
    return _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))


def _row_state(row: dict[str, Any]) -> str:
    return _clean_text(row.get("registryState") or row.get("candidateState")).lower()


def _row_adapter(row: dict[str, Any]) -> str:
    adapter = _clean_text(row.get("adapter") or row.get("sourceType")).lower()
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row)).lower()
    if not adapter and ":" in row_id:
        adapter = row_id.split(":", 1)[0]
    return adapter or "unknown"


def _is_static_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) == "static"


def _is_provider_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) in PROVIDER_ADAPTERS


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _positive_evidence_score(row: dict[str, Any]) -> int:
    return sum(
        max(0, _int_value(row.get(key)))
        for key in ("jobsFound", "rankScore", "score", "lastJobsKept", "lastKeptCount")
    )


def _has_fresh_or_healthy_signal(row: dict[str, Any]) -> bool:
    health = _clean_text(row.get("health") or row.get("lastStatus")).lower()
    return health in {"healthy", "ok", "success"}


def _row_urls(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "id",
            "sourceId",
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s]+", _clean_text(value)):
            urls.append(match.rstrip("),.;'\""))
    return urls


def _provider_endpoint_shape(row: dict[str, Any]) -> str:
    for url in _row_urls(row):
        parsed = urlparse(url)
        path = parsed.path.strip().lower().rstrip("/")
        if path:
            return path
    return ""


def _json_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value or "").strip()


def _source_state_rows_by_name(source_state_payload: Any) -> dict[str, dict[str, Any]]:
    rows = _as_dict(_as_dict(source_state_payload).get("sources"))
    return {str(key).strip().lower(): row for key, row in rows.items() if isinstance(row, dict)}


def _source_state_row_for_registry_row(
    row: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any], str]:
    for key in (
        _clean_text(row.get("name")),
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("id")),
        source_identity(row),
    ):
        lookup = key.strip().lower()
        if lookup and lookup in source_state_rows:
            return source_state_rows[lookup], lookup
    return {}, ""


def _row_actions(row: dict[str, Any]) -> list[dict[str, Any]]:
    state = _row_state(row)
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))
    actions = [dict(action) for action in CONFLICT_ACTIONS_BY_STATE.get(state, ())]
    if row_id:
        for action in actions:
            action["ids"] = [row_id]
    return actions


def _source_identity_counts(rows: list[dict[str, Any]]) -> Counter[str]:
    identities: Counter[str] = Counter()
    for row in rows:
        row_id = source_identity(row)
        if row_id:
            identities[row_id] += 1
    return identities


def _classify_conflict_triage(rows: list[dict[str, Any]]) -> dict[str, str]:
    identity_counts = _source_identity_counts(rows)
    duplicate_ids = sorted(row_id for row_id, count in identity_counts.items() if count > 1)
    state_counts = Counter(_row_state(row) for row in rows)
    active_count = int(state_counts.get("active") or 0)
    pending_count = int(state_counts.get("pending") or 0)
    rejected_count = int(state_counts.get("rejected") or 0)
    if duplicate_ids:
        bucket = "exact_duplicate_auto_healable"
        reason = f"Duplicate canonical source identity: {', '.join(duplicate_ids)}."
    elif active_count >= 2:
        bucket = "active_active_likely_duplicate"
        reason = f"{active_count} active rows share this source family."
    elif active_count == 1 and pending_count >= 1:
        bucket = "pending_duplicate_of_active"
        reason = f"{pending_count} pending row(s) match a family with one active source."
    elif rejected_count >= 1:
        bucket = "rejected_historical_noise"
        reason = f"{rejected_count} rejected row(s) are retained as historical registry noise."
    else:
        bucket = "ambiguous_manual_review"
        reason = "No low-risk active/pending/rejected pattern matched this conflict."
    meta = _TRIAGE_BY_BUCKET[bucket]
    return {
        "bucket": bucket,
        "label": str(meta["label"]),
        "reason": reason,
        "risk": str(meta["risk"]),
    }


def _build_triage_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(
        str(card.get("triageBucket") or "ambiguous_manual_review") for card in conflicts
    )
    buckets = []
    for meta in TRIAGE_BUCKETS:
        bucket = str(meta["bucket"])
        buckets.append(
            {
                "bucket": bucket,
                "label": str(meta["label"]),
                "risk": str(meta["risk"]),
                "description": str(meta["description"]),
                "count": int(counts.get(bucket) or 0),
            }
        )
    return {
        "summary": {
            "totalConflictCount": len(conflicts),
            "bucketCounts": {row["bucket"]: int(row["count"]) for row in buckets},
        },
        "buckets": buckets,
    }


def _classify_conflict_review(rows: list[dict[str, Any]], triage_bucket: str) -> dict[str, Any]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    pending_rows = [row for row in rows if _row_state(row) == "pending"]
    active_provider_rows = [row for row in active_rows if _is_provider_row(row)]
    active_static_rows = [row for row in active_rows if _is_static_row(row)]
    pending_provider_rows = [row for row in pending_rows if _is_provider_row(row)]
    pending_static_rows = [row for row in pending_rows if _is_static_row(row)]
    active_adapters = sorted({_row_adapter(row) for row in active_rows})
    evidence_flags = [
        f"triage:{triage_bucket}",
        f"active_rows:{len(active_rows)}",
        f"pending_rows:{len(pending_rows)}",
    ]
    if active_provider_rows:
        evidence_flags.append(f"active_provider_rows:{len(active_provider_rows)}")
    if active_static_rows:
        evidence_flags.append(f"active_static_rows:{len(active_static_rows)}")
    if pending_provider_rows:
        evidence_flags.append(f"pending_provider_rows:{len(pending_provider_rows)}")
    if pending_static_rows:
        evidence_flags.append(f"pending_static_rows:{len(pending_static_rows)}")
    if len(active_adapters) == 1 and len(active_rows) >= 2:
        evidence_flags.append(f"same_active_adapter:{active_adapters[0]}")

    if len(active_provider_rows) >= 2:
        queue = "p0_multi_active_provider"
        reason = f"{len(active_provider_rows)} active provider rows can duplicate fetches."
        disposition = "Review duplicate active provider sources"
        confidence = "high"
    elif active_provider_rows and active_static_rows:
        queue = "p1_active_provider_static"
        reason = "Active provider rows coexist with active static rows."
        disposition = "Review provider/static replacement"
        confidence = "medium"
    elif len(active_rows) == 1 and pending_provider_rows:
        queue = "p1_pending_provider_against_active"
        reason = (
            f"{len(pending_provider_rows)} pending provider row(s) compete with one active source."
        )
        disposition = "Check provider quality before promotion"
        confidence = "medium"
    elif len(active_rows) >= 2 and len(active_adapters) == 1 and active_adapters[0] != "static":
        queue = "p2_same_adapter_active_variant"
        reason = f"{len(active_rows)} active rows share adapter {active_adapters[0]}."
        disposition = "Review same-adapter active variants"
        confidence = "medium"
    elif len(active_static_rows) >= 2 and not active_provider_rows:
        queue = "p2_static_url_variant_active"
        reason = f"{len(active_static_rows)} active static rows look like URL variants."
        disposition = "Review active static URL variants"
        confidence = "medium"
    elif len(active_rows) == 1 and pending_static_rows:
        queue = "p2_pending_static_variant"
        reason = f"{len(pending_static_rows)} pending static row(s) compete with one active source."
        disposition = "Review pending static duplicate"
        confidence = "medium"
    elif pending_rows and not active_rows:
        queue = "p3_pending_only_intake"
        reason = f"{len(pending_rows)} pending row(s) are not active fetch duplication."
        disposition = "Pending-only intake"
        confidence = "low"
    else:
        queue = "p3_low_signal_manual"
        reason = "No higher-confidence review queue matched this conflict."
        disposition = "Manual review"
        confidence = "low"

    meta = _REVIEW_BY_QUEUE[queue]
    return {
        "priority": int(meta["priority"]),
        "queue": queue,
        "label": str(meta["label"]),
        "reason": reason,
        "suggestedDisposition": disposition,
        "suggestedConfidence": confidence,
        "evidenceFlags": evidence_flags,
    }


def _build_review_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(card.get("reviewQueue") or "p3_low_signal_manual") for card in conflicts)
    priority_counts = Counter(str(card.get("reviewPriority", 3)) for card in conflicts)
    queues = []
    for meta in REVIEW_QUEUES:
        queue = str(meta["queue"])
        queues.append(
            {
                "queue": queue,
                "priority": int(meta["priority"]),
                "label": str(meta["label"]),
                "description": str(meta["description"]),
                "count": int(counts.get(queue) or 0),
            }
        )
    return {
        "summary": {
            "totalConflictCount": len(conflicts),
            "priorityCounts": {
                str(priority): int(priority_counts.get(str(priority)) or 0) for priority in range(4)
            },
            "queueCounts": {row["queue"]: int(row["count"]) for row in queues},
        },
        "queues": queues,
    }


def _blocked_automation(reason: str, blocked_reasons: list[str]) -> dict[str, Any]:
    return {
        "eligible": False,
        "action": "",
        "label": "",
        "reason": reason,
        "route": "",
        "targetIds": [],
        "blockedReasons": blocked_reasons,
    }


def _eligible_automation(target_id: str, reason: str) -> dict[str, Any]:
    return {
        "eligible": True,
        "action": SAFE_AUTO_DEMOTE_ACTION,
        "label": SAFE_AUTO_DEMOTE_LABEL,
        "reason": reason,
        "route": SAFE_AUTO_DEMOTE_ROUTE,
        "targetIds": [target_id],
        "blockedReasons": [],
    }


def _analyze_safe_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked: list[str] = []
    if len(rows) != 2:
        blocked.append("requires_exactly_two_rows")
    if any(_row_state(row) != "active" for row in rows):
        blocked.append("requires_active_rows_only")
    if len(losers) != 1:
        blocked.append("requires_one_loser")

    adapters = {_row_adapter(row) for row in rows}
    adapter = next(iter(adapters), "")
    if len(adapters) != 1:
        blocked.append("requires_same_adapter")
    elif adapter not in PROVIDER_ADAPTERS:
        blocked.append("requires_known_provider_adapter")

    endpoint_shapes = {_provider_endpoint_shape(row) for row in rows}
    if "" in endpoint_shapes or len(endpoint_shapes) != 1:
        blocked.append("requires_same_provider_endpoint_shape")

    loser = losers[0] if len(losers) == 1 else {}
    winner_score = _positive_evidence_score(winner)
    loser_score = _positive_evidence_score(loser)
    if winner_score <= 0:
        blocked.append("winner_has_no_positive_evidence")
    if loser_score > 0:
        blocked.append("loser_has_positive_evidence")
    if loser_score >= winner_score:
        blocked.append("loser_has_equal_or_stronger_evidence")
    if _has_fresh_or_healthy_signal(loser):
        blocked.append("loser_has_fresh_or_healthy_signal")

    target_id = _row_identity(loser)
    if not target_id:
        blocked.append("missing_loser_identity")

    if blocked:
        return _blocked_automation(
            "Not eligible for safe auto-demotion.",
            sorted(set(blocked)),
        )
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active {adapter} rows with the same endpoint shape; "
            "the winner has positive evidence and the loser has none."
        ),
    )


def _build_automation_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    eligible_cards = [
        card for card in conflicts if bool(_as_dict(card.get("safeAutomation")).get("eligible"))
    ]
    target_ids = [
        target_id
        for card in eligible_cards
        for target_id in _as_list(_as_dict(card.get("safeAutomation")).get("targetIds"))
        if _clean_text(target_id)
    ]
    return {
        "summary": {
            "eligibleCount": len(eligible_cards),
            "demotableCount": len(target_ids),
        },
        "actions": [
            {
                "action": SAFE_AUTO_DEMOTE_ACTION,
                "label": SAFE_AUTO_DEMOTE_LABEL,
                "route": SAFE_AUTO_DEMOTE_ROUTE,
                "count": len(target_ids),
                "targetIds": target_ids,
            }
        ]
        if target_ids
        else [],
    }


def _join_source_health_aliases(
    row: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    merged = dict(row)
    source_state_row, source_state_name = _source_state_row_for_registry_row(row, source_state_rows)
    if source_state_name:
        merged["sourceStateName"] = source_state_name
    for key in SOURCE_HEALTH_FIELD_NAMES:
        value = source_state_row.get(key)
        if value not in {"", None}:
            merged[key] = value
    transition_reason = _clean_text(
        merged.get("pendingReason")
        or merged.get("quarantineReason")
        or merged.get("reason")
        or merged.get("registryReason")
    )
    merged["transitionReason"] = transition_reason
    merged["actions"] = _row_actions(merged)
    return merged


def _compare_registry_rows(winner: dict[str, Any], loser: dict[str, Any]) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    for key in CONFLICT_DIFF_FIELDS:
        winner_value = winner.get(key)
        loser_value = loser.get(key)
        if _json_value(winner_value) == _json_value(loser_value):
            continue
        diffs.append(
            {
                "key": key,
                "label": _FIELD_LABELS.get(key, key.replace("_", " ").title()),
                "winnerValue": winner_value,
                "loserValue": loser_value,
            }
        )
    return diffs


def derive_registry_conflict_queue(
    registry_state: Any, source_state_payload: Any = None
) -> dict[str, Any]:
    registry = _as_dict(registry_state)
    registry_rows = [
        dict(row)
        for bucket in ("active", "pending", "rejected")
        for row in _as_list(registry.get(bucket))
        if isinstance(row, dict)
    ]
    source_state_rows = _source_state_rows_by_name(source_state_payload)
    family_cards = duplicate_family_conflict_cards(
        registry_rows,
        source_state=source_state_payload,
    )
    conflicts: list[dict[str, Any]] = []
    for card in family_cards:
        winner = _join_source_health_aliases(_as_dict(card.get("winner")), source_state_rows)
        losers = [
            _join_source_health_aliases(_as_dict(row), source_state_rows)
            for row in _as_list(card.get("losers"))
            if isinstance(row, dict)
        ]
        rows = [winner, *losers]
        triage = _classify_conflict_triage(rows)
        review = _classify_conflict_review(rows, triage["bucket"])
        family_key = _clean_text(card.get("familyKey"))
        safe_automation = _analyze_safe_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        conflicts.append(
            {
                "familyKey": family_key,
                "rowCount": max(0, int(card.get("rowCount") or len(rows))),
                "triageBucket": triage["bucket"],
                "triageLabel": triage["label"],
                "triageReason": triage["reason"],
                "triageRisk": triage["risk"],
                "reviewPriority": review["priority"],
                "reviewQueue": review["queue"],
                "reviewLabel": review["label"],
                "reviewReason": review["reason"],
                "suggestedDisposition": review["suggestedDisposition"],
                "suggestedConfidence": review["suggestedConfidence"],
                "evidenceFlags": review["evidenceFlags"],
                "safeAutomation": safe_automation,
                "winner": winner,
                "winnerScore": _as_dict(card.get("winnerScore")),
                "winnerRationale": _as_list(card.get("winnerRationale")),
                "losers": losers,
                "rows": rows,
                "diffs": [
                    {
                        "loserId": _clean_text(
                            row.get("id") or row.get("sourceId") or source_identity(row)
                        ),
                        "loserName": _clean_text(row.get("name")),
                        "fields": _compare_registry_rows(winner, row),
                    }
                    for row in losers
                ],
            }
        )
    conflicts.sort(
        key=lambda card: (
            int(card.get("reviewPriority", 3)),
            _clean_text(card.get("reviewQueue")),
            _clean_text(card.get("familyKey")),
        )
    )
    return {
        "summary": {
            "conflictCount": len(conflicts),
            "familyCount": len(conflicts),
            "rowCount": sum(int(card.get("rowCount") or 0) for card in conflicts),
            "winnerCount": len(conflicts),
            "loserCount": sum(len(card.get("losers") or []) for card in conflicts),
        },
        "triage": _build_triage_summary(conflicts),
        "review": _build_review_summary(conflicts),
        "automation": _build_automation_summary(conflicts),
        "conflicts": conflicts,
    }


def load_registry_conflicts_payload(
    *,
    load_state: Callable[[], Any],
    load_json_object: Callable[..., Any],
    source_state_path: Path,
) -> dict[str, Any]:
    registry_state = load_state()
    source_state_payload = load_json_object(source_state_path, {})
    payload = derive_registry_conflict_queue(registry_state, source_state_payload)
    warnings: list[str] = []
    if not Path(source_state_path).exists():
        warnings.append("missing_jobs_source_state_artifact")
    if warnings:
        payload["warnings"] = warnings
    return payload
