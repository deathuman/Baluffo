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
from src.source_registry_state import transition_registry_to_pending

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
SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION = "auto_demote_static_normalized_url_alias"
SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL = "Auto-demote static URL alias"
SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION = "auto_demote_static_same_host_listing_variant"
SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL = "Auto-demote static listing variant"
SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION = "auto_demote_static_generated_listing_variants"
SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL = "Auto-demote generated static listing variants"
SAFE_AUTO_DEMOTE_ACTIONS = {
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
}
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


def _row_jobs_evidence(row: dict[str, Any]) -> int:
    for key in (
        "jobsFound",
        "jobs_found",
        "lastKeptCount",
        "lastJobsKept",
        "keptCount",
        "kept_count",
    ):
        value = _int_value(row.get(key))
        if value > 0:
            return value
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


def _normalized_static_url_aliases(row: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    for url in _row_urls(row):
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host:
            continue
        path = parsed.path.strip().lower().rstrip("/") or "/"
        query = parsed.query.strip().lower()
        aliases.add(f"https://{host}{path}{'?' + query if query else ''}")
    return aliases


def _static_url_host_paths(row: dict[str, Any]) -> set[tuple[str, str]]:
    host_paths: set[tuple[str, str]] = set()
    for url in _row_urls(row):
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host:
            continue
        path = parsed.path.strip().lower().rstrip("/") or "/"
        host_paths.add((host, path))
    return host_paths


def _family_tokens(family_key: str) -> set[str]:
    stop_words = {
        "digital",
        "entertainment",
        "game",
        "games",
        "group",
        "interactive",
        "online",
        "software",
        "studio",
        "studios",
        "world",
    }
    return {
        token
        for token in re.split(r"[^a-z0-9]+", family_key.lower())
        if len(token) > 2 and token not in stop_words
    }


def _host_matches_family(host: str, family_key: str) -> bool:
    compact_host = host.replace("-", "").replace(".", "")
    return any(token in compact_host for token in _family_tokens(family_key))


def _is_parent_child_path(left: str, right: str) -> bool:
    if left == right:
        return True
    return left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _is_careerish_path(path: str) -> bool:
    return bool(
        set(re.split(r"[^a-z0-9]+", path.lower()))
        & {
            "career",
            "careers",
            "hiring",
            "job",
            "jobs",
            "join",
            "opening",
            "openings",
            "position",
            "positions",
            "vacancies",
            "work",
        }
    )


def _json_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value or "").strip()


def _source_state_rows_by_name(source_state_payload: Any) -> dict[str, dict[str, Any]]:
    rows = _as_dict(_as_dict(source_state_payload).get("sources"))
    by_key: dict[str, dict[str, Any]] = {}
    for raw_key, row in rows.items():
        if not isinstance(row, dict):
            continue
        for key in (
            str(raw_key).strip().lower(),
            _clean_text(row.get("sourceId")).lower(),
            _clean_text(row.get("sourceIdentity")).lower(),
        ):
            if key:
                by_key[key] = row
    return by_key


def _source_state_lookup_keys(row: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for key in (
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("id")),
        source_identity(row),
    ):
        if key:
            keys.append(key)
            keys.append(f"static_source::{key}")
    aliases = row.get("sourceStateAliases")
    if isinstance(aliases, list):
        keys.extend(_clean_text(alias) for alias in aliases)
    keys.append(_clean_text(row.get("name")))
    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        lookup = key.strip().lower()
        if lookup and lookup not in seen:
            seen.add(lookup)
            out.append(lookup)
    return out


def _source_state_row_for_registry_row(
    row: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any], str]:
    for lookup in _source_state_lookup_keys(row):
        if lookup in source_state_rows:
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


def _is_safe_auto_demoted_pending(row: dict[str, Any]) -> bool:
    if _row_state(row) != "pending":
        return False
    return SAFE_AUTO_DEMOTE_REASON in {
        _clean_text(row.get("pendingReason")),
        _clean_text(row.get("stateChangedBy")),
        _clean_text(row.get("transitionReason")),
    }


def _safe_auto_demoted_pending_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _row_identity(row),
        "name": _clean_text(row.get("name")),
        "registryState": _row_state(row),
        "pendingReason": _clean_text(row.get("pendingReason")),
        "stateChangedAt": _clean_text(row.get("stateChangedAt")),
        "stateChangedBy": _clean_text(row.get("stateChangedBy")),
    }


def _is_safe_pending_static_weaker_alias(
    winner: dict[str, Any], loser: dict[str, Any], family_key: str
) -> bool:
    if _row_state(winner) != "active" or _row_state(loser) != "pending":
        return False
    if not _is_static_row(winner) or not _is_static_row(loser):
        return False
    winner_host_paths = _static_url_host_paths(winner)
    loser_host_paths = _static_url_host_paths(loser)
    shared_hosts = {
        winner_host
        for winner_host, _winner_path in winner_host_paths
        for loser_host, _loser_path in loser_host_paths
        if winner_host == loser_host and _host_matches_family(winner_host, family_key)
    }
    if not shared_hosts:
        return False
    if not any(_is_careerish_path(path) for _host, path in winner_host_paths):
        return False
    if not any(_is_careerish_path(path) for _host, path in loser_host_paths):
        return False
    return (
        _row_jobs_evidence(winner) >= _row_jobs_evidence(loser)
        and _positive_evidence_score(winner) >= _positive_evidence_score(loser) + 20
    )


def _build_pending_audit_section(cards: list[dict[str, Any]]) -> dict[str, Any]:
    row_count = sum(len(_as_list(card.get("rows"))) for card in cards)
    return {
        "summary": {
            "familyCount": len(cards),
            "rowCount": row_count,
        },
        "families": cards,
    }


def _build_pending_conflict_audit(
    *,
    safe_auto_demoted_cards: list[dict[str, Any]],
    safe_static_alias_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "safeAutoDemotedPending": _build_pending_audit_section(safe_auto_demoted_cards),
        "safePendingStaticAlias": _build_pending_audit_section(safe_static_alias_cards),
    }


def _unique_registry_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        row_id = source_identity(row)
        if row_id and row_id in seen:
            continue
        if row_id:
            seen.add(row_id)
        unique.append(row)
    return unique


def _empty_safe_demotion_result(state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {
        "ok": True,
        "demoted": 0,
        "skipped": 0,
        "applied": [],
        "skippedRows": [],
        "state": state,
    }


def _safe_demotion_state(registry_state: Any) -> dict[str, list[dict[str, Any]]]:
    registry = _as_dict(registry_state)
    return {
        bucket: [dict(row) for row in _as_list(registry.get(bucket)) if isinstance(row, dict)]
        for bucket in ("active", "pending", "rejected")
    }


def _eligible_safe_demotion_cards(
    conflict_payload: dict[str, Any], action_filter: str
) -> dict[str, dict[str, Any]]:
    eligible_by_id: dict[str, dict[str, Any]] = {}
    for card in _as_list(conflict_payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        safe_automation = _as_dict(card.get("safeAutomation"))
        safe_action = _clean_text(safe_automation.get("action"))
        if not safe_automation.get("eligible"):
            continue
        if action_filter and safe_action != action_filter:
            continue
        if not action_filter and safe_action not in SAFE_AUTO_DEMOTE_ACTIONS:
            continue
        for target_id in _as_list(safe_automation.get("targetIds")):
            target = _clean_text(target_id)
            if target:
                eligible_by_id[target] = card
    return eligible_by_id


def _safe_demotion_applied_entry(row_id: str, card: dict[str, Any]) -> dict[str, str]:
    return {
        "id": row_id,
        "familyKey": _clean_text(card.get("familyKey")),
        "action": _clean_text(_as_dict(card.get("safeAutomation")).get("action")),
    }


def _apply_safe_demotion_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, Any]]]:
    moved: list[dict[str, Any]] = []
    active_remaining: list[dict[str, Any]] = []
    applied: list[dict[str, str]] = []
    for row in state["active"]:
        row_id = source_identity(row)
        if row_id not in target_ids:
            active_remaining.append(row)
            continue
        moved.append(
            transition_registry_to_pending(
                row,
                reason=SAFE_AUTO_DEMOTE_REASON,
                actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
                at=now or None,
            )
        )
        applied.append(_safe_demotion_applied_entry(row_id, eligible_by_id.get(row_id) or {}))
    return active_remaining, applied, moved


def apply_registry_conflict_safe_demotions(
    registry_state: Any,
    source_state_payload: Any = None,
    *,
    action: str = "",
    ids: list[str] | None = None,
    now: str = "",
    actor: str = SAFE_AUTO_DEMOTE_REASON,
) -> dict[str, Any]:
    state = _safe_demotion_state(registry_state)
    action_filter = _clean_text(action)
    if action_filter and action_filter not in SAFE_AUTO_DEMOTE_ACTIONS:
        return {
            **_empty_safe_demotion_result(state),
            "ok": False,
            "error": "Unsupported safe automation action.",
        }

    requested_ids = {_clean_text(item) for item in (ids or []) if _clean_text(item)}
    conflict_payload = derive_registry_conflict_queue(state, source_state_payload)
    eligible_by_id = _eligible_safe_demotion_cards(conflict_payload, action_filter)
    selected_ids = requested_ids or set(eligible_by_id)
    target_ids = selected_ids & set(eligible_by_id)
    skipped_rows = [
        {
            "id": row_id,
            "reason": "not_currently_safe_auto_demote_eligible",
        }
        for row_id in sorted(selected_ids - target_ids)
    ]
    active_remaining, applied, moved = _apply_safe_demotion_targets(
        state,
        target_ids=target_ids,
        eligible_by_id=eligible_by_id,
        now=now,
        actor=actor,
    )
    moved_ids = {source_identity(row) for row in moved}
    for row_id in sorted(target_ids - moved_ids):
        skipped_rows.append({"id": row_id, "reason": "eligible_target_not_active"})

    state["active"] = active_remaining
    state["pending"] = _unique_registry_rows([*state["pending"], *moved])
    return {
        "ok": True,
        "demoted": len(moved),
        "skipped": len(skipped_rows),
        "applied": applied,
        "skippedRows": skipped_rows,
        "state": state,
    }


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


def _eligible_automation(
    target_id: str,
    reason: str,
    *,
    action: str = SAFE_AUTO_DEMOTE_ACTION,
    label: str = SAFE_AUTO_DEMOTE_LABEL,
) -> dict[str, Any]:
    return {
        "eligible": True,
        "action": action,
        "label": label,
        "reason": reason,
        "route": SAFE_AUTO_DEMOTE_ROUTE,
        "targetIds": [target_id],
        "blockedReasons": [],
    }


def _eligible_multi_automation(
    target_ids: list[str],
    reason: str,
    *,
    action: str,
    label: str,
) -> dict[str, Any]:
    return {
        "eligible": True,
        "action": action,
        "label": label,
        "reason": reason,
        "route": SAFE_AUTO_DEMOTE_ROUTE,
        "targetIds": target_ids,
        "blockedReasons": [],
    }


def _safe_pair_blockers(
    rows: list[dict[str, Any]], losers: list[dict[str, Any]], *, static_only: bool = False
) -> list[str]:
    checks = [
        (len(rows) != 2, "requires_exactly_two_rows"),
        (any(_row_state(row) != "active" for row in rows), "requires_active_rows_only"),
        (len(losers) != 1, "requires_one_loser"),
        (static_only and any(not _is_static_row(row) for row in rows), "requires_static_rows_only"),
    ]
    return [reason for blocked, reason in checks if blocked]


def _provider_alias_blockers(rows: list[dict[str, Any]]) -> tuple[list[str], str]:
    blocked: list[str] = []
    adapters = {_row_adapter(row) for row in rows}
    adapter = next(iter(adapters), "")
    if len(adapters) != 1:
        blocked.append("requires_same_adapter")
    elif adapter not in PROVIDER_ADAPTERS:
        blocked.append("requires_known_provider_adapter")
    endpoint_shapes = {_provider_endpoint_shape(row) for row in rows}
    if "" in endpoint_shapes or len(endpoint_shapes) != 1:
        blocked.append("requires_same_provider_endpoint_shape")
    return blocked, adapter


def _evidence_blockers(
    winner: dict[str, Any], loser: dict[str, Any], *, loser_must_have_none: bool
) -> list[str]:
    blocked: list[str] = []
    winner_score = _positive_evidence_score(winner)
    loser_score = _positive_evidence_score(loser)
    if winner_score <= 0:
        blocked.append("winner_has_no_positive_evidence")
    if loser_must_have_none and loser_score > 0:
        blocked.append("loser_has_positive_evidence")
    if loser_score >= winner_score:
        blocked.append("loser_has_equal_or_stronger_evidence")
    return blocked


def _target_identity_blocker(target_id: str) -> list[str]:
    return [] if target_id else ["missing_loser_identity"]


def _static_url_alias_blockers(winner_aliases: set[str], loser_aliases: set[str]) -> list[str]:
    if not winner_aliases or not loser_aliases:
        return ["requires_normalized_static_urls"]
    if not (winner_aliases & loser_aliases):
        return ["requires_same_normalized_static_url"]
    if loser_aliases - winner_aliases:
        return ["loser_has_unique_normalized_url"]
    return []


def _shared_static_hosts(
    winner_host_paths: set[tuple[str, str]], loser_host_paths: set[tuple[str, str]]
) -> set[str]:
    return {
        winner_host
        for winner_host, _winner_path in winner_host_paths
        for loser_host, _loser_path in loser_host_paths
        if winner_host == loser_host
    }


def _has_parent_child_listing_path(
    winner_host_paths: set[tuple[str, str]], loser_host_paths: set[tuple[str, str]]
) -> bool:
    return any(
        _is_parent_child_path(winner_path, loser_path)
        for winner_host, winner_path in winner_host_paths
        for loser_host, loser_path in loser_host_paths
        if winner_host == loser_host
    )


def _static_listing_variant_blockers(
    *,
    family_key: str,
    winner_host_paths: set[tuple[str, str]],
    loser_host_paths: set[tuple[str, str]],
    shared_hosts: set[str],
) -> list[str]:
    if not winner_host_paths or not loser_host_paths:
        return ["requires_static_urls"]
    if not shared_hosts:
        return ["requires_same_static_host"]
    if not any(_host_matches_family(host, family_key) for host in shared_hosts):
        return ["requires_studio_specific_host"]
    if not _has_parent_child_listing_path(winner_host_paths, loser_host_paths):
        return ["requires_parent_child_listing_path"]
    return []


def _static_listing_evidence_blockers(winner: dict[str, Any], loser: dict[str, Any]) -> list[str]:
    blocked: list[str] = []
    if _row_jobs_evidence(winner) <= _row_jobs_evidence(loser):
        blocked.append("winner_jobs_not_stronger")
    if _positive_evidence_score(winner) < _positive_evidence_score(loser) + 30:
        blocked.append("winner_evidence_delta_too_small")
    return blocked


def _single_static_host_path(row: dict[str, Any]) -> tuple[str, str]:
    host_paths = _static_url_host_paths(row)
    if len(host_paths) != 1:
        return "", ""
    return next(iter(host_paths))


def _analyze_static_generated_listing_variants_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked: list[str] = []
    if len(rows) < 3:
        blocked.append("requires_three_or_more_rows")
    if any(_row_state(row) != "active" for row in rows):
        blocked.append("requires_active_rows_only")
    if any(not _is_static_row(row) for row in rows):
        blocked.append("requires_static_rows_only")
    if not losers:
        blocked.append("requires_losers")

    host_paths_by_id = {source_identity(row): _single_static_host_path(row) for row in rows}
    if any(not host or not path for host, path in host_paths_by_id.values()):
        blocked.append("requires_single_static_url_per_row")
    hosts = {host for host, _path in host_paths_by_id.values() if host}
    if len(hosts) != 1:
        blocked.append("requires_same_static_host")
    shared_host = next(iter(hosts), "")
    if shared_host and not _host_matches_family(shared_host, family_key):
        blocked.append("requires_studio_specific_host")
    paths = [path for _host, path in host_paths_by_id.values() if path]
    if not paths or any(not _is_careerish_path(path) for path in paths):
        blocked.append("requires_careerish_listing_paths")

    winner_jobs = _row_jobs_evidence(winner)
    winner_score = _positive_evidence_score(winner)
    if any(_row_jobs_evidence(loser) > winner_jobs for loser in losers):
        blocked.append("loser_jobs_stronger")
    if any(_positive_evidence_score(loser) > winner_score for loser in losers):
        blocked.append("loser_has_stronger_evidence")
    target_ids = [_row_identity(loser) for loser in losers]
    if any(not target_id for target_id in target_ids):
        blocked.append("missing_loser_identity")

    if blocked:
        return _blocked_automation(
            "Not eligible for generated static listing-variant auto-demotion.",
            sorted(set(blocked)),
        )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} has {len(rows)} active static rows on {shared_host} "
            "with generated career-ish listing paths; none of the losers has "
            "stronger job evidence than the advisory winner."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL,
    )


def _analyze_provider_alias_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers)
    provider_blockers, adapter = _provider_alias_blockers(rows)
    blocked.extend(provider_blockers)
    loser = losers[0] if len(losers) == 1 else {}
    blocked.extend(_evidence_blockers(winner, loser, loser_must_have_none=True))
    if _has_fresh_or_healthy_signal(loser):
        blocked.append("loser_has_fresh_or_healthy_signal")
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

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


def _analyze_static_url_alias_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers, static_only=True)
    loser = losers[0] if len(losers) == 1 else {}
    winner_aliases = _normalized_static_url_aliases(winner)
    loser_aliases = _normalized_static_url_aliases(loser)
    blocked.extend(_static_url_alias_blockers(winner_aliases, loser_aliases))
    blocked.extend(_evidence_blockers(winner, loser, loser_must_have_none=False))
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe static URL alias auto-demotion.",
            sorted(set(blocked)),
        )
    shared_alias = sorted(winner_aliases & loser_aliases)[0]
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active static rows for the same normalized URL "
            f"({shared_alias}); the advisory winner has stronger evidence."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
    )


def _analyze_static_listing_variant_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers, static_only=True)
    loser = losers[0] if len(losers) == 1 else {}
    winner_host_paths = _static_url_host_paths(winner)
    loser_host_paths = _static_url_host_paths(loser)
    shared_hosts = _shared_static_hosts(winner_host_paths, loser_host_paths)
    blocked.extend(
        _static_listing_variant_blockers(
            family_key=family_key,
            winner_host_paths=winner_host_paths,
            loser_host_paths=loser_host_paths,
            shared_hosts=shared_hosts,
        )
    )
    blocked.extend(_static_listing_evidence_blockers(winner, loser))
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe static listing-variant auto-demotion.",
            sorted(set(blocked)),
        )
    shared_host = sorted(shared_hosts)[0]
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active static rows on {shared_host} with parent/child "
            "listing paths; the advisory winner has materially stronger job evidence."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL,
    )


def _analyze_safe_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    provider_result = _analyze_provider_alias_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    if provider_result.get("eligible"):
        return provider_result
    static_result = _analyze_static_url_alias_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    if static_result.get("eligible") or all(_is_static_row(row) for row in rows):
        if static_result.get("eligible"):
            return static_result
        listing_variant_result = _analyze_static_listing_variant_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        if listing_variant_result.get("eligible"):
            return listing_variant_result
        generated_variant_result = _analyze_static_generated_listing_variants_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        if generated_variant_result.get("eligible"):
            return generated_variant_result
        return static_result
    return provider_result


def _build_automation_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    eligible_cards = [
        card for card in conflicts if bool(_as_dict(card.get("safeAutomation")).get("eligible"))
    ]
    target_ids_by_action: dict[str, list[str]] = {}
    labels_by_action = {
        SAFE_AUTO_DEMOTE_ACTION: SAFE_AUTO_DEMOTE_LABEL,
        SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION: SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
        SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION: (
            SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL
        ),
        SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION: (
            SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL
        ),
    }
    for card in eligible_cards:
        safe_automation = _as_dict(card.get("safeAutomation"))
        action = _clean_text(safe_automation.get("action"))
        if action not in SAFE_AUTO_DEMOTE_ACTIONS:
            continue
        target_ids_by_action.setdefault(action, [])
        for target_id in _as_list(safe_automation.get("targetIds")):
            clean_target_id = _clean_text(target_id)
            if clean_target_id:
                target_ids_by_action[action].append(clean_target_id)
    target_ids = [
        target_id
        for action_target_ids in target_ids_by_action.values()
        for target_id in action_target_ids
    ]
    return {
        "summary": {
            "eligibleCount": len(eligible_cards),
            "demotableCount": len(target_ids),
        },
        "actions": [
            {
                "action": action,
                "label": labels_by_action.get(action, "Apply safe demotions"),
                "route": SAFE_AUTO_DEMOTE_ROUTE,
                "count": len(action_target_ids),
                "targetIds": action_target_ids,
            }
            for action, action_target_ids in target_ids_by_action.items()
            if action_target_ids
        ]
        if target_ids_by_action
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
    if not merged.get("lastSuccessfulFetchAt") and merged.get("lastSuccessAt"):
        merged["lastSuccessfulFetchAt"] = merged.get("lastSuccessAt")
    if not merged.get("lastSeenInFetchAt"):
        merged["lastSeenInFetchAt"] = merged.get("lastCheckedAt") or merged.get("lastRunAt") or ""
    if merged.get("lastJobsKept") in {"", None} and merged.get("lastKeptCount") not in {"", None}:
        merged["lastJobsKept"] = merged.get("lastKeptCount")
    if merged.get("failureCount") in {"", None} and merged.get("consecutiveFailures") not in {
        "",
        None,
    }:
        merged["failureCount"] = merged.get("consecutiveFailures")
    if merged.get("zeroJobStreak") in {"", None} and merged.get("consecutiveZeroKept") not in {
        "",
        None,
    }:
        merged["zeroJobStreak"] = merged.get("consecutiveZeroKept")
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
    safe_auto_demoted_pending_audit: list[dict[str, Any]] = []
    safe_pending_static_alias_audit: list[dict[str, Any]] = []
    for card in family_cards:
        family_key = _clean_text(card.get("familyKey"))
        winner = _join_source_health_aliases(_as_dict(card.get("winner")), source_state_rows)
        losers = [
            _join_source_health_aliases(_as_dict(row), source_state_rows)
            for row in _as_list(card.get("losers"))
            if isinstance(row, dict)
        ]
        suppressed_losers = [row for row in losers if _is_safe_auto_demoted_pending(row)]
        if suppressed_losers:
            safe_auto_demoted_pending_audit.append(
                {
                    "familyKey": _clean_text(card.get("familyKey")),
                    "rowCount": len(suppressed_losers),
                    "rows": [
                        _safe_auto_demoted_pending_audit_row(row) for row in suppressed_losers
                    ],
                }
            )
            losers = [row for row in losers if not _is_safe_auto_demoted_pending(row)]
        suppressed_static_alias_losers = [
            row for row in losers if _is_safe_pending_static_weaker_alias(winner, row, family_key)
        ]
        if suppressed_static_alias_losers:
            safe_pending_static_alias_audit.append(
                {
                    "familyKey": family_key,
                    "rowCount": len(suppressed_static_alias_losers),
                    "rows": [
                        _safe_auto_demoted_pending_audit_row(row)
                        for row in suppressed_static_alias_losers
                    ],
                }
            )
            losers = [
                row
                for row in losers
                if not _is_safe_pending_static_weaker_alias(winner, row, family_key)
            ]
        rows = [winner, *losers]
        if len(rows) < 2:
            continue
        triage = _classify_conflict_triage(rows)
        review = _classify_conflict_review(rows, triage["bucket"])
        safe_automation = _analyze_safe_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        conflicts.append(
            {
                "familyKey": family_key,
                "rowCount": len(rows),
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
    automation = _build_automation_summary(conflicts)
    automation["audit"] = _build_pending_conflict_audit(
        safe_auto_demoted_cards=safe_auto_demoted_pending_audit,
        safe_static_alias_cards=safe_pending_static_alias_audit,
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
        "automation": automation,
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
