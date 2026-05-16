#!/usr/bin/env python3
"""Saved-job application tracking normalization helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

PIPELINE_PHASES = [
    "bookmark",
    "applied",
    "screening",
    "assignment",
    "interview_1",
    "interview_2",
    "final",
    "offer",
]

OUTCOME_STATUSES = ["active", "rejected", "withdrawn", "ghosted", "closed", "accepted"]
TERMINAL_OUTCOME_STATUSES = [status for status in OUTCOME_STATUSES if status != "active"]
APPLICATION_STATUSES = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"]


def _token(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_pipeline_phase(value: Any) -> str:
    raw = _token(value)
    aliases = {
        "bookmarked": "bookmark",
        "saved": "bookmark",
        "recruiter": "screening",
        "recruiter_call": "screening",
        "phone_screen": "screening",
        "take_home": "assignment",
        "technical_test": "assignment",
        "art_test": "assignment",
        "final_round": "final",
        "final_interview": "final",
    }
    raw = aliases.get(raw, raw)
    return raw if raw in PIPELINE_PHASES else "bookmark"


def normalize_outcome_status(value: Any) -> str:
    raw = _token(value)
    if raw in {"no_response", "no-response"}:
        return "ghosted"
    return raw if raw in OUTCOME_STATUSES else "active"


def is_terminal_outcome(value: Any) -> bool:
    return normalize_outcome_status(value) != "active"


def _normalize_timestamp_map(value: Any, allowed_keys: list[str]) -> dict[str, str]:
    source = value if isinstance(value, dict) else {}
    normalized: dict[str, str] = {}
    for key, timestamp in source.items():
        safe_key = str(key)
        if safe_key not in allowed_keys:
            safe_key = normalize_pipeline_phase(safe_key)
        if safe_key not in allowed_keys:
            continue
        text = str(timestamp or "").strip()
        if text:
            normalized[safe_key] = text
    return normalized


def normalize_phase_timestamps(value: Any, *, saved_at: str = "") -> dict[str, str]:
    timestamps = _normalize_timestamp_map(value, PIPELINE_PHASES)
    if saved_at and not timestamps.get("bookmark"):
        timestamps["bookmark"] = str(saved_at)
    return timestamps


def normalize_outcome_timestamps(value: Any) -> dict[str, str]:
    return _normalize_timestamp_map(value, TERMINAL_OUTCOME_STATUSES)


def best_pipeline_phase_from_timestamps(
    phase_timestamps: dict[str, Any] | None, fallback: str = "applied"
) -> str:
    timestamps = phase_timestamps or {}
    for phase in reversed(PIPELINE_PHASES):
        if phase != "bookmark" and str(timestamps.get(phase) or "").strip():
            return phase
    normalized_fallback = normalize_pipeline_phase(fallback)
    return "applied" if normalized_fallback == "bookmark" else normalized_fallback


def split_application_status(
    status: Any,
    *,
    phase_timestamps: dict[str, Any] | None = None,
    fallback_phase: str = "",
) -> dict[str, str]:
    raw = _token(status)
    if raw in TERMINAL_OUTCOME_STATUSES:
        return {
            "pipelinePhase": best_pipeline_phase_from_timestamps(
                phase_timestamps, fallback_phase or "applied"
            ),
            "outcomeStatus": raw,
        }
    return {
        "pipelinePhase": normalize_pipeline_phase(raw or fallback_phase),
        "outcomeStatus": "active",
    }


def to_application_status_mirror(pipeline_phase: Any, outcome_status: Any) -> str:
    outcome = normalize_outcome_status(outcome_status)
    if outcome != "active":
        return outcome
    return normalize_pipeline_phase(pipeline_phase)


def normalize_application_status(status: Any) -> str:
    raw = _token(status)
    if raw == "bookmarked":
        return "bookmark"
    if raw in TERMINAL_OUTCOME_STATUSES:
        return raw
    return normalize_pipeline_phase(raw)


def can_transition_pipeline_phase(current: Any, nxt: Any, outcome_status: Any = "active") -> bool:
    if is_terminal_outcome(outcome_status):
        return False
    left = normalize_pipeline_phase(current)
    right = normalize_pipeline_phase(nxt)
    if left == right:
        return True
    return PIPELINE_PHASES.index(right) == PIPELINE_PHASES.index(left) + 1


def can_set_outcome_status(current: Any, nxt: Any, *, override: bool = False) -> bool:
    left = normalize_outcome_status(current)
    right = normalize_outcome_status(nxt)
    if left == right:
        return True
    if left != "active" and not override:
        return False
    return True


def can_transition_phase(current: Any, nxt: Any) -> bool:
    left = split_application_status(current)
    right = split_application_status(nxt)
    left_mirror = to_application_status_mirror(left["pipelinePhase"], left["outcomeStatus"])
    right_mirror = to_application_status_mirror(right["pipelinePhase"], right["outcomeStatus"])
    if left_mirror == right_mirror:
        return True
    if right["outcomeStatus"] != "active":
        return can_set_outcome_status(left["outcomeStatus"], right["outcomeStatus"])
    return can_transition_pipeline_phase(
        left["pipelinePhase"], right["pipelinePhase"], left["outcomeStatus"]
    )


def normalize_tracking_fields(
    source: dict[str, Any] | None,
    base: dict[str, Any] | None = None,
    *,
    saved_at: str = "",
    now_iso: Callable[[], str],
    normalize_iso: Callable[[Any, str], str],
) -> dict[str, Any]:
    source = dict(source or {})
    base = dict(base or {})
    current_iso = now_iso()
    phase_timestamps = normalize_phase_timestamps(
        {
            **dict(base.get("phaseTimestamps") or {}),
            **dict(source.get("phaseTimestamps") or {}),
        },
        saved_at=saved_at,
    )
    source_phase = (
        str(source.get("pipelinePhase") or "").strip() if "pipelinePhase" in source else ""
    )
    source_outcome = (
        str(source.get("outcomeStatus") or "").strip() if "outcomeStatus" in source else ""
    )
    source_application_status = (
        str(source.get("applicationStatus") or "").strip() if "applicationStatus" in source else ""
    )
    base_phase = str(base.get("pipelinePhase") or "").strip()
    base_outcome = str(base.get("outcomeStatus") or "").strip()
    legacy_split = split_application_status(
        source_application_status or base.get("applicationStatus"),
        phase_timestamps=phase_timestamps,
        fallback_phase=source_phase or base_phase,
    )
    pipeline_phase = legacy_split["pipelinePhase"]
    if source_phase:
        pipeline_phase = normalize_pipeline_phase(source_phase)
    elif not source_application_status and base_phase:
        pipeline_phase = normalize_pipeline_phase(base_phase)
    outcome_status = legacy_split["outcomeStatus"]
    if source_outcome:
        outcome_status = normalize_outcome_status(source_outcome)
    elif not source_application_status and base_outcome:
        outcome_status = normalize_outcome_status(base_outcome)
    if outcome_status != "active" and pipeline_phase == "bookmark":
        pipeline_phase = best_pipeline_phase_from_timestamps(phase_timestamps, "applied")

    outcome_timestamps = {
        **normalize_outcome_timestamps(base.get("outcomeTimestamps")),
        **normalize_outcome_timestamps(source.get("outcomeTimestamps")),
    }
    if outcome_status != "active" and not outcome_timestamps.get(outcome_status):
        outcome_timestamps[outcome_status] = normalize_iso(
            source.get("outcomeUpdatedAt")
            or base.get("outcomeUpdatedAt")
            or source.get("updatedAt")
            or base.get("updatedAt"),
            current_iso,
        )

    fallback_updated_at = source.get("updatedAt") or base.get("updatedAt") or saved_at
    return {
        "pipelinePhase": pipeline_phase,
        "outcomeStatus": outcome_status,
        "applicationStatus": to_application_status_mirror(pipeline_phase, outcome_status),
        "phaseTimestamps": phase_timestamps,
        "outcomeTimestamps": outcome_timestamps,
        "contentUpdatedAt": normalize_iso(
            source.get("contentUpdatedAt") or base.get("contentUpdatedAt") or fallback_updated_at,
            saved_at or current_iso,
        ),
        "trackingUpdatedAt": normalize_iso(
            source.get("trackingUpdatedAt") or base.get("trackingUpdatedAt") or fallback_updated_at,
            saved_at or current_iso,
        ),
        "notesUpdatedAt": normalize_iso(
            source.get("notesUpdatedAt") or base.get("notesUpdatedAt"), ""
        ),
        "lastActivityAt": normalize_iso(
            source.get("lastActivityAt") or base.get("lastActivityAt") or fallback_updated_at,
            saved_at or current_iso,
        ),
    }
