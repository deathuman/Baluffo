#!/usr/bin/env python3
"""Source registry utilities for discovery/approval workflows."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunsplit

from src.baluffo_config import get_storage_defaults
from src.shared.utils import now_iso

_STORAGE_DEFAULTS = get_storage_defaults()
_DEFAULT_DATA_DIR = _STORAGE_DEFAULTS["data_dir"]
DATA_DIR = Path(os.getenv("BALUFFO_DATA_DIR") or _DEFAULT_DATA_DIR).expanduser().resolve()
ACTIVE_PATH = DATA_DIR / "source-registry-active.json"
PENDING_PATH = DATA_DIR / "source-registry-pending.json"
REJECTED_PATH = DATA_DIR / "source-registry-rejected.json"
DISCOVERY_REPORT_PATH = DATA_DIR / "source-discovery-report.json"
DISCOVERY_CANDIDATES_PATH = DATA_DIR / "source-discovery-candidates.json"
M5_STRATEGIC_BACKLOG_PATH = DATA_DIR / "m5-strategic-backlog.json"
URL_PATCH_MANIFEST_PATH = DATA_DIR / "url-patch-manifest.json"
APPROVAL_STATE_PATH = DATA_DIR / "source-approval-state.json"
TOMBSTONES_PATH = DATA_DIR / "source-registry-tombstones.json"
AUTO_APPROVAL_STRONG_ADAPTERS = frozenset({"greenhouse", "lever", "ashby"})
AUTO_APPROVAL_SECONDARY_ADAPTERS = frozenset({"bamboohr", "workday"})
AUTO_APPROVAL_CAP_DEFER_REASONS = frozenset({"adapter_cap", "domain_cap", "top_n_cap"})
AUTO_APPROVAL_EXISTING_MATCH_REASONS = frozenset(
    {"existing_registry_match", "existing_family_match"}
)
REGISTRY_STATE_ACTIVE = "active"
REGISTRY_STATE_PENDING = "pending"
REGISTRY_STATE_REJECTED = "rejected"
REGISTRY_STATES = frozenset(
    {REGISTRY_STATE_ACTIVE, REGISTRY_STATE_PENDING, REGISTRY_STATE_REJECTED}
)
REGISTRY_REASON_MANUAL_SOURCE = "manual_source"
REGISTRY_REASON_MANUAL_SOURCE_VARIANT = "manual_source_variant_added"
REGISTRY_REASON_DISCOVERY_AUTO_APPROVE = "discovery_auto_approve"
REGISTRY_REASON_ROLLBACK = "registry_rollback"
REGISTRY_REASON_RESTORE_REJECTED = "registry_restore_rejected"
REGISTRY_REASON_REJECT = "registry_reject"
REGISTRY_REASON_APPROVE = "registry_approve"
REGISTRY_REASON_DELETE = "registry_delete"
REGISTRY_REASON_RESTORE_DELETED = "registry_restore_deleted"
REGISTRY_REASON_FETCH_EMPTY_DEMOTE = "fetch_empty_demote"
REGISTRY_REASON_FETCH_FAILURE_DEMOTE = "fetch_failure_demote"
REGISTRY_MIGRATION_V2 = "registry_migration_v2"


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _coerce_state(value: Any, default: str = REGISTRY_STATE_PENDING) -> str:
    token = str(value or "").strip().lower()
    if token in REGISTRY_STATES:
        return token
    return default


def _infer_registry_state(row: dict[str, Any], *, bucket: str = "") -> str:
    bucket_token = str(bucket or "").strip().lower()
    if bucket_token in REGISTRY_STATES:
        return bucket_token
    registry_state = _coerce_state(row.get("registryState"), "")
    if registry_state:
        return registry_state
    candidate_state = str(row.get("candidateState") or "").strip().lower()
    if candidate_state == "live" or bool(row.get("enabledByDefault")):
        return REGISTRY_STATE_ACTIVE
    if candidate_state == "quarantined":
        return REGISTRY_STATE_REJECTED
    if candidate_state == "validated":
        return REGISTRY_STATE_PENDING
    pending_reason = str(row.get("pendingReason") or "").strip().lower()
    if pending_reason:
        return REGISTRY_STATE_PENDING
    quarantine_reason = str(row.get("quarantineReason") or "").strip().lower()
    if quarantine_reason:
        return REGISTRY_STATE_REJECTED
    return REGISTRY_STATE_PENDING


def _infer_pending_reason(row: dict[str, Any], *, registry_state: str, bucket: str = "") -> str:
    current = str(row.get("pendingReason") or "").strip()
    if current:
        return current
    if registry_state == REGISTRY_STATE_ACTIVE:
        return ""
    if registry_state == REGISTRY_STATE_REJECTED:
        return _first_text(
            row.get("quarantineReason"),
            row.get("pendingReason"),
            row.get("reason"),
            REGISTRY_REASON_REJECT,
        )
    bucket_token = str(bucket or "").strip().lower()
    if bucket_token == REGISTRY_STATE_PENDING:
        return _first_text(
            row.get("pendingReason"),
            row.get("discoveryMethod"),
            row.get("manualFallback"),
            REGISTRY_REASON_PENDING_DEFAULT,
        )
    return ""


def _infer_state_changed_at(row: dict[str, Any], *, registry_state: str) -> str:
    return _first_text(
        row.get("stateChangedAt"),
        row.get("approvedAt") if registry_state == REGISTRY_STATE_ACTIVE else "",
        row.get("quarantinedAt") if registry_state == REGISTRY_STATE_REJECTED else "",
        row.get("lastPromotedAt") if registry_state == REGISTRY_STATE_ACTIVE else "",
        row.get("lastDemotedAt") if registry_state != REGISTRY_STATE_ACTIVE else "",
        row.get("manualAddedAt"),
        row.get("discoveredAt"),
        row.get("firstDeferredAt"),
        row.get("lastProbedAt"),
        row.get("updatedAt"),
        row.get("createdAt"),
    )


def _infer_state_changed_by(row: dict[str, Any]) -> str:
    return _first_text(
        row.get("stateChangedBy"),
        row.get("approvedBy"),
        row.get("quarantinedBy"),
        row.get("manualAddedBy"),
        row.get("discoveredBy"),
    )


def _apply_registry_legacy_fields(
    updated: dict[str, Any],
    *,
    registry_state: str,
    state_changed_at: str,
    state_changed_by: str,
    reason: str,
) -> dict[str, Any]:
    updated["registryState"] = registry_state
    updated["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    updated["stateChangedAt"] = state_changed_at
    updated["stateChangedBy"] = state_changed_by
    updated["lastPromotedAt"] = str(updated.get("lastPromotedAt") or "")
    updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or "")
    if registry_state == REGISTRY_STATE_ACTIVE:
        updated["candidateState"] = "live"
        updated["enabledByDefault"] = True
        updated["approvedAt"] = str(updated.get("approvedAt") or state_changed_at)
        updated["approvedBy"] = str(updated.get("approvedBy") or state_changed_by or "")
        updated["liveAt"] = str(updated.get("liveAt") or state_changed_at)
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
        updated["lastPromotedAt"] = str(updated.get("lastPromotedAt") or state_changed_at)
    elif registry_state == REGISTRY_STATE_PENDING:
        updated["candidateState"] = "validated"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or state_changed_at)
    else:
        updated["candidateState"] = "quarantined"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = str(updated.get("quarantinedAt") or state_changed_at)
        updated["quarantineReason"] = str(
            updated.get("quarantineReason") or reason or REGISTRY_REASON_REJECT
        )
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or state_changed_at)
    return updated


def canonicalize_registry_row(row: dict[str, Any], *, bucket: str = "") -> dict[str, Any]:
    normalized = dict(row)
    normalized = ensure_source_id(normalized)
    registry_state = _infer_registry_state(normalized, bucket=bucket)
    state_changed_at = _infer_state_changed_at(normalized, registry_state=registry_state)
    state_changed_by = _infer_state_changed_by(normalized)
    if state_changed_at and not state_changed_by:
        state_changed_by = REGISTRY_MIGRATION_V2
    reason = _infer_pending_reason(normalized, registry_state=registry_state, bucket=bucket)
    normalized = _apply_registry_legacy_fields(
        normalized,
        registry_state=registry_state,
        state_changed_at=state_changed_at,
        state_changed_by=state_changed_by,
        reason=reason,
    )
    normalized["registryState"] = registry_state
    normalized["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    normalized["stateChangedAt"] = state_changed_at
    normalized["stateChangedBy"] = state_changed_by
    if (
        registry_state == REGISTRY_STATE_ACTIVE
        and not str(normalized.get("lastPromotedAt") or "").strip()
    ):
        normalized["lastPromotedAt"] = state_changed_at
    if (
        registry_state != REGISTRY_STATE_ACTIVE
        and not str(normalized.get("lastDemotedAt") or "").strip()
    ):
        normalized["lastDemotedAt"] = state_changed_at
    return normalized


def sort_sources_by_identity(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (ensure_source_id(dict(row)) for row in rows if isinstance(row, dict)),
        key=lambda row: (
            source_identity(row),
            str(row.get("stateChangedAt") or ""),
            str(row.get("lastPromotedAt") or ""),
            str(row.get("lastDemotedAt") or ""),
        ),
    )


def _transition_state_metadata(
    row: dict[str, Any],
    *,
    registry_state: str,
    reason: str,
    actor: str,
    at: str,
) -> dict[str, Any]:
    updated = canonicalize_registry_row(row, bucket=registry_state)
    updated["registryState"] = registry_state
    updated["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    updated["stateChangedAt"] = at
    updated["stateChangedBy"] = str(actor or "").strip()
    if registry_state == REGISTRY_STATE_ACTIVE:
        updated["lastPromotedAt"] = at
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or "")
        updated["candidateState"] = "live"
        updated["enabledByDefault"] = True
        updated["approvedAt"] = str(updated.get("approvedAt") or at)
        updated["approvedBy"] = str(updated.get("approvedBy") or actor or "")
        updated["liveAt"] = str(updated.get("liveAt") or at)
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
    elif registry_state == REGISTRY_STATE_PENDING:
        updated["lastDemotedAt"] = at
        updated["candidateState"] = "validated"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
    else:
        updated["lastDemotedAt"] = at
        updated["candidateState"] = "quarantined"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = at
        updated["quarantineReason"] = reason or REGISTRY_REASON_REJECT
    return ensure_source_id(updated)


def transition_registry_to_active(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_ACTIVE,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


def transition_registry_to_pending(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_PENDING,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


def transition_registry_to_rejected(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_REJECTED,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


REGISTRY_REASON_PENDING_DEFAULT = REGISTRY_REASON_MANUAL_SOURCE


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    try:
        if not path.exists():
            return [dict(row) for row in fallback]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        return [row for row in payload if isinstance(row, dict)]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        if not path.exists():
            return fallback
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    # Use a unique temp file per write to avoid collisions across threads/processes.
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        last_error: Exception | None = None
        for attempt in range(18):
            try:
                os.replace(tmp, path)
                last_error = None
                break
            except PermissionError as exc:
                last_error = exc
                # Windows can transiently lock the destination while another thread replaces it.
                time.sleep(0.012 * (attempt + 1))
        if last_error is not None:
            raise last_error
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def source_identity(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    explicit_id = str(row.get("id") or "").strip()
    if explicit_id:
        return explicit_id.lower()
    for key in (
        "id",
        "slug",
        "account",
        "company_id",
        "api_url",
        "feed_url",
        "board_url",
        "listing_url",
        "name",
    ):
        value = str(row.get(key) or "").strip().lower()
        if value:
            return f"{adapter}:{key}:{value}"
    digest = hashlib.sha1(
        json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"{adapter}:unknown:{digest}"


def ensure_source_id(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["id"] = source_identity(normalized)
    return normalized


def normalize_source_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return ""
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, host, path, "", ""))


def source_endpoint_url(row: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = str(value or "").strip()
            if text:
                return text
    return ""


def source_url_fingerprint(row: dict[str, Any]) -> str:
    return normalize_source_url(source_endpoint_url(row))


def unique_sources(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(ensure_source_id(row))
    return out


def _normalize_discovery_health_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in {"healthy", "success"}:
        return "ok"
    if token in {"failed", "failure"}:
        return "error"
    return token


def _discovery_jobs_count(row: dict[str, Any], report: dict[str, Any] | None = None) -> int:
    report_row = report if isinstance(report, dict) else {}
    for value in (
        row.get("jobsFound"),
        row.get("sampleCount"),
        report_row.get("jobsFound"),
        report_row.get("sampleCount"),
    ):
        try:
            numeric = int(value or 0)
        except (TypeError, ValueError):
            numeric = 0
        if numeric > 0:
            return numeric
    return 0


def _discovery_row_has_blocking_error(
    row: dict[str, Any], report: dict[str, Any] | None = None
) -> bool:
    report_row = report if isinstance(report, dict) else {}
    last_probe_error = str(
        report_row.get("lastProbeError") or row.get("lastProbeError") or ""
    ).strip()
    if last_probe_error:
        return True
    status = _normalize_discovery_health_status(
        report_row.get("_lastStatus")
        or report_row.get("status")
        or row.get("_lastStatus")
        or row.get("status")
    )
    return status == "error"


def _discovery_row_has_blocking_state(
    row: dict[str, Any], report: dict[str, Any] | None = None
) -> bool:
    report_row = report if isinstance(report, dict) else {}
    candidate_state = str(row.get("candidateState") or "").strip().lower()
    report_candidate_state = str(report_row.get("candidateState") or "").strip().lower()
    return candidate_state in {"quarantined", "rejected"} or report_candidate_state in {
        "quarantined",
        "rejected",
    }


def _rank_reason_tokens(row: dict[str, Any]) -> set[str]:
    return {
        str(item or "").strip()
        for item in (row.get("rankReasons") or row.get("reasons") or [])
        if str(item or "").strip()
    }


def _pending_row_is_auto_approvable(
    row: dict[str, Any], *, report_row: dict[str, Any] | None = None
) -> bool:
    """Return True when a pending discovery row has concrete approval evidence.

    Advisory signals like weakSignal and promotionReason are intentionally ignored here.
    Report-side queue throttles such as domain_cap do not override a clean pending row.
    """
    if not isinstance(row, dict):
        return False
    report = report_row if isinstance(report_row, dict) else {}
    if bool(row.get("deferred")):
        return False
    if bool(row.get("weakSignal")) or bool(report.get("weakSignal")):
        return False
    if _discovery_row_has_blocking_state(row, report):
        return False
    if _discovery_jobs_count(row, report) <= 0:
        return False
    if _discovery_row_has_blocking_error(row, report):
        return False
    return True


def _cap_deferred_candidate_is_auto_approvable(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    if not bool(row.get("deferred")):
        return False
    defer_reason = str(row.get("deferReason") or row.get("dropReason") or "").strip()
    if defer_reason not in AUTO_APPROVAL_CAP_DEFER_REASONS:
        return False
    if bool(row.get("weakSignal")):
        return False
    if _discovery_row_has_blocking_state(row):
        return False
    if _discovery_jobs_count(row) <= 0:
        return False
    if _discovery_row_has_blocking_error(row):
        return False
    if _rank_reason_tokens(row) & AUTO_APPROVAL_EXISTING_MATCH_REASONS:
        return False
    return True


def _stamp_live_transition(
    row: dict[str, Any], *, approved_by: str, approved_at: str, promotion_reason: str = ""
) -> dict[str, Any]:
    updated = transition_registry_to_active(
        row,
        reason=promotion_reason or REGISTRY_REASON_DISCOVERY_AUTO_APPROVE,
        actor=approved_by,
        at=approved_at,
    )
    if promotion_reason:
        updated["promotionReason"] = str(promotion_reason)
    return updated


def _promotion_reason_for_candidate(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    confidence = str(row.get("confidence") or "").strip().lower()
    promotion_lane = str(row.get("promotionLane") or "").strip().lower()
    evidence_score = max(0, int(row.get("evidenceScore") or 0))
    jobs_found = max(0, int(row.get("jobsFound") or row.get("sampleCount") or 0))
    rank_reasons = {
        str(item or "").strip()
        for item in (row.get("rankReasons") or row.get("reasons") or [])
        if str(item or "").strip()
    }

    if bool(row.get("deferred")):
        defer_reason = str(row.get("deferReason") or row.get("dropReason") or "").strip()
        if defer_reason in AUTO_APPROVAL_CAP_DEFER_REASONS:
            if rank_reasons & AUTO_APPROVAL_EXISTING_MATCH_REASONS:
                return "skipped_existing_family_match"
            if jobs_found > 0:
                return "cap_deferred_jobs_found"
        return "deferred_candidate"
    if bool(row.get("weakSignal")):
        return "weak_candidate"

    if adapter in AUTO_APPROVAL_STRONG_ADAPTERS:
        if (
            promotion_lane == "structured_batch"
            and confidence in {"high", "medium"}
            and jobs_found > 0
            and "structured_batch_family" in rank_reasons
        ):
            return "structured_batch_family"
        return "structured_batch_gate"

    if adapter in AUTO_APPROVAL_SECONDARY_ADAPTERS:
        if (
            jobs_found > 0
            and "structured_family" in rank_reasons
            and (confidence == "high" or evidence_score >= 26)
        ):
            return "structured_family_high_confidence"
        return "structured_family_gate"

    return "manual_review_only"


def apply_discovery_auto_approval(
    state: dict[str, list[dict[str, Any]]],
    report: dict[str, Any],
    *,
    auto_approve_enabled: bool,
    approval_state_path: Path = APPROVAL_STATE_PATH,
    now_iso_fn: Callable[[], str] | None = now_iso,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    normalized_state = {
        bucket: unique_sources(
            dict(row) for row in list(state.get(bucket) or []) if isinstance(row, dict)
        )
        for bucket in ("active", "pending", "rejected")
    }
    summary = _as_dict(report.get("summary"))
    runtime = _as_dict(report.get("runtime"))
    runtime_auto = _as_dict(runtime.get("autoApproval"))
    report_candidates = _as_list(report.get("candidates"))
    report_candidates_by_id = {
        source_identity(row): row
        for row in report_candidates
        if isinstance(row, dict) and source_identity(row)
    }
    approved_at = str(now_iso_fn() if callable(now_iso_fn) else now_iso())
    moved: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    moved_ids: set[str] = set()
    active_ids = {
        source_identity(row) for row in normalized_state["active"] if source_identity(row)
    }

    if auto_approve_enabled:
        for row in normalized_state["pending"]:
            row_id = source_identity(row)
            report_row = report_candidates_by_id.get(row_id)
            merged_row = dict(report_row or row)
            promotion_reason = _promotion_reason_for_candidate(merged_row)
            if _pending_row_is_auto_approvable(row, report_row=report_row):
                moved_ids.add(row_id)
                moved.append(
                    _stamp_live_transition(
                        row,
                        approved_by="discovery_auto_approve",
                        approved_at=approved_at,
                        promotion_reason=promotion_reason,
                    )
                )
            else:
                remaining.append(dict(row))
        for row in report_candidates:
            if not isinstance(row, dict):
                continue
            row_id = source_identity(row)
            if not row_id or row_id in active_ids or row_id in moved_ids:
                continue
            if not _cap_deferred_candidate_is_auto_approvable(row):
                continue
            promotion_reason = _promotion_reason_for_candidate(row)
            moved_ids.add(row_id)
            moved.append(
                _stamp_live_transition(
                    row,
                    approved_by="discovery_auto_approve",
                    approved_at=approved_at,
                    promotion_reason=promotion_reason,
                )
            )
        remaining = [row for row in remaining if source_identity(row) not in moved_ids]
        next_state = {
            "active": unique_sources([*normalized_state["active"], *moved]),
            "pending": unique_sources(remaining),
            "rejected": unique_sources(normalized_state["rejected"]),
        }
    else:
        next_state = normalized_state

    approved_count = max(int(summary.get("approvedCandidateCount") or 0), len(moved))
    summary["approvedCandidateCount"] = approved_count
    summary["liveCandidateCount"] = max(int(summary.get("liveCandidateCount") or 0), approved_count)
    report["summary"] = summary

    runtime_auto = dict(runtime_auto)
    runtime_auto["enabled"] = bool(auto_approve_enabled)
    runtime_auto["approvedCount"] = max(int(runtime_auto.get("approvedCount") or 0), approved_count)
    runtime = dict(runtime)
    runtime["autoApproval"] = runtime_auto
    report["runtime"] = runtime

    if report_candidates:
        next_candidates: list[Any] = []
        for row in report_candidates:
            if not isinstance(row, dict):
                next_candidates.append(row)
                continue
            row_id = source_identity(row)
            promotion_reason = _promotion_reason_for_candidate(row)
            updated_row = dict(row)
            if promotion_reason:
                updated_row["promotionReason"] = promotion_reason
            if row_id in moved_ids or _pending_row_is_auto_approvable(updated_row):
                updated_row = _stamp_live_transition(
                    updated_row,
                    approved_by="discovery_auto_approve",
                    approved_at=approved_at,
                    promotion_reason=promotion_reason,
                )
            next_candidates.append(updated_row)
        report["candidates"] = next_candidates

    if auto_approve_enabled and moved:
        approval_state = load_json_object(approval_state_path, {"approvedSinceLastRun": 0})
        approval_state["approvedSinceLastRun"] = int(
            approval_state.get("approvedSinceLastRun") or 0
        ) + len(moved)
        save_json_atomic(approval_state_path, approval_state)

    return next_state, approved_count
