"""Bounded normalization for availability fetch-report diagnostics."""

from __future__ import annotations

from typing import Any

from src.shared.utils import coerce_non_negative_int as _count


def _object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _count_map(value: Any) -> dict[str, int]:
    return {
        str(key).strip(): _count(item) for key, item in _object(value).items() if str(key).strip()
    }


def _int_map(value: Any) -> dict[str, int]:
    """Bounded map of signed ints (per-source deltas may be negative)."""

    result: dict[str, int] = {}
    if isinstance(value, dict):
        for key, item in value.items():
            try:
                result[str(key).strip()] = int(item)
            except (TypeError, ValueError):
                continue
    return {key: item for key, item in result.items() if key}


def _signed_int(value: Any) -> int | None:
    """Signed int or None (run-level overdue deltas may be negative or unknown)."""

    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ratio(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0)))
    except (TypeError, ValueError):
        return 0.0


def normalize_availability_summary(value: Any) -> dict[str, Any]:
    source = _object(value)
    counts: dict[str, Any] = {
        str(key): _count(item)
        for key, item in source.items()
        if str(key).endswith("Count") and str(key) != "shadowClassifierCounts"
    }
    counts["shadowClassifierCounts"] = _count_map(source.get("shadowClassifierCounts"))
    counts["rejectionReasonCounts"] = _count_map(source.get("rejectionReasonCounts"))
    return counts


def normalize_availability_health(value: Any) -> dict[str, Any] | None:
    """Normalize a present availabilityHealth payload; fabricate nothing.

    Absent input (``None``, empty dict, or non-dict) returns ``None`` — callers
    emit no health field rather than a default-shaped payload. This is the
    structural fix for the baseline-poisoning class: the old ``overdueCount: 0``
    default for absent input survived the compact summary writer and was read by
    the next terminal finalize as a real "0 overdue" baseline, faking
    ``overdueDelta == overdueCount`` on every run. Present payloads keep
    field-level coercion (empty status / zero counts are honest readings of a
    present payload; the terminal-baseline shape gate rejects non-terminal
    ones).
    """

    if not isinstance(value, dict) or not value:
        return None
    source = dict(value)
    identity = normalize_availability_summary(source.get("identity"))
    return {
        "status": str(source.get("status") or "").strip(),
        "overdueCount": _count(source.get("overdueCount")),
        "overdueBySource": _count_map(source.get("overdueBySource")),
        "overdueSourceCount": _count(source.get("overdueSourceCount")),
        "overdueBySourceDelta": _int_map(source.get("overdueBySourceDelta")),
        "overdueDelta": _signed_int(source.get("overdueDelta")),
        "overdueRising": bool(source.get("overdueRising")),
        "coverageTargetMissed": bool(source.get("coverageTargetMissed")),
        "healthReasons": [
            str(reason).strip()
            for reason in (source.get("healthReasons") or [])
            if str(reason).strip()
        ],
        "verifiedWithinDaysTarget": _count(source.get("verifiedWithinDaysTarget")),
        "verifiedCoverageTarget": _ratio(source.get("verifiedCoverageTarget")),
        "verifiedWithinSevenDaysCoverage": _ratio(source.get("verifiedWithinSevenDaysCoverage")),
        "sweepSelectedCount": _count(source.get("sweepSelectedCount")),
        "sweepDeferredCount": _count(source.get("sweepDeferredCount")),
        "degradedCoverage": bool(source.get("degradedCoverage")),
        "shadowClassifier": bool(source.get("shadowClassifier")),
        "identity": identity,
    }


def normalize_source_direct_conflicts(value: Any) -> list[dict[str, Any]]:
    rows = value if isinstance(value, list) else []
    return [
        {
            "availabilityId": str(row.get("availabilityId") or "").strip(),
            "sourceStatus": str(row.get("sourceStatus") or "").strip(),
            "directKind": str(row.get("directKind") or "").strip(),
            "checkedAt": str(row.get("checkedAt") or "").strip(),
        }
        for row in rows[-100:]
        if isinstance(row, dict)
    ]


def normalize_sweep_coverage(value: Any) -> dict[str, Any]:
    source = _object(value)
    return {
        str(key): item
        for key, item in source.items()
        if str(key) != "rows" and isinstance(item, (str, int, float, bool))
    }


__all__ = [
    "normalize_availability_health",
    "normalize_availability_summary",
    "normalize_source_direct_conflicts",
    "normalize_sweep_coverage",
]
