"""Rejection-row helpers extracted from ``gamedevmap_active_dry_run.py``.

All functions are pure or thin delegates — no coordinator import.
"""

from __future__ import annotations

from typing import Any

from . import active_audit_runtime
from .probe_runtime import candidate_id as probe_candidate_id

TECHNICAL_REJECTION_REASONS = {
    "bad_provider_inference",
    "homepage_fetch_failed",
    "probe_failed",
}


# pure helper
def _normalize_failure_bucket(reason: str, detail: str = "") -> str:
    reason_key = str(reason or "").strip()
    detail_key = str(detail or "").strip()
    if reason_key in TECHNICAL_REJECTION_REASONS or detail_key == "recovery_fetch_failed":
        return "technical_failure"
    if reason_key in {"no_careers_evidence", "zero_jobs"}:
        return "coverage_miss"
    return "other"


# pure helper
def _error_text(result: dict[str, Any]) -> str:
    error = str(result.get("error") or "").strip()
    if error:
        return error
    failure = result.get("failure")
    if isinstance(failure, dict):
        error = str(failure.get("error") or "").strip()
        if error:
            return error
    return ""


# pure — URL/key helper
def _row_url(row: dict[str, Any]) -> str:
    return str(row.get("url") or "").strip()


# pure helper — builds a rejection row dict
def _rejection(
    *,
    reason: str,
    row: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
    error: str = "",
    jobs_found: int = 0,
    reason_detail: str = "",
    failure_bucket: str = "",
) -> dict[str, Any]:
    detail = str(reason_detail or "").strip()
    payload: dict[str, Any] = {
        "reason": str(reason),
        "reasonDetail": detail,
        "failureBucket": str(failure_bucket or _normalize_failure_bucket(reason, detail)),
        "error": str(error or ""),
        "jobsFound": max(0, int(jobs_found or 0)),
    }
    if isinstance(row, dict):
        payload["studio"] = str(row.get("studio") or "")
        payload["url"] = _row_url(row)
        payload["sourceDirectoryEntryUrl"] = str(row.get("sourceDirectoryEntryUrl") or "")
    if isinstance(candidate, dict):
        payload["candidate"] = dict(candidate)
        payload["sourceId"] = probe_candidate_id(candidate)
        payload["adapter"] = str(candidate.get("adapter") or "")
        payload["name"] = str(candidate.get("name") or "")
    return payload


# pure helper — builds a rejection row dict
def _gamedevmap_probe_failed_rejection(candidate: dict[str, Any], error: str) -> dict[str, Any]:
    return _rejection(
        reason="probe_failed",
        candidate=candidate,
        error=error,
        reason_detail="probe_failed",
    )


# pure helper — builds a rejection row dict
def _gamedevmap_zero_jobs_rejection(candidate: dict[str, Any], jobs_found: int) -> dict[str, Any]:
    return _rejection(
        reason="zero_jobs",
        candidate=candidate,
        jobs_found=jobs_found,
        reason_detail="zero_jobs",
    )


# pure helper
def _rejection_row_key(rejection: dict[str, Any]) -> str:
    return active_audit_runtime.rejection_rerun_key(rejection)


__all__ = [
    "TECHNICAL_REJECTION_REASONS",
    "_normalize_failure_bucket",
    "_error_text",
    "_row_url",
    "_rejection",
    "_gamedevmap_probe_failed_rejection",
    "_gamedevmap_zero_jobs_rejection",
    "_rejection_row_key",
]
