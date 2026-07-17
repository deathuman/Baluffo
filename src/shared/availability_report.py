"""Bounded normalization for availability fetch-report diagnostics."""

from __future__ import annotations

from typing import Any


def _object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _count_map(value: Any) -> dict[str, int]:
    return {
        str(key).strip(): _count(item) for key, item in _object(value).items() if str(key).strip()
    }


def _ratio(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0)))
    except (TypeError, ValueError):
        return 0.0


def normalize_availability_summary(value: Any) -> dict[str, Any]:
    source = _object(value)
    counts = {
        str(key): _count(item)
        for key, item in source.items()
        if str(key).endswith("Count") and str(key) != "shadowClassifierCounts"
    }
    counts["shadowClassifierCounts"] = _count_map(source.get("shadowClassifierCounts"))
    counts["rejectionReasonCounts"] = _count_map(source.get("rejectionReasonCounts"))
    return counts


def normalize_availability_health(value: Any) -> dict[str, Any]:
    source = _object(value)
    identity = normalize_availability_summary(source.get("identity"))
    return {
        "status": str(source.get("status") or "").strip(),
        "overdueCount": _count(source.get("overdueCount")),
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
