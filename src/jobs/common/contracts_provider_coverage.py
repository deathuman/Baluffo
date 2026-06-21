"""Provider migration coverage summary helpers.

AI boundary owns: provider coverage contract rows and summary normalization for migration evidence.
AI boundary implement in: this file for provider coverage contract shape; migration link actions stay in bridge/source-policy leaves.
AI boundary search before contracts: source-policy recommendations, provider coverage tests, and bridge backfill helpers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider coverage tests.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object

PROVIDER_COVERAGE_STATUSES = frozenset(
    {
        "untested",
        "probing",
        "validated_provider",
        "unstable_provider",
        "failed_provider",
        "needs_review",
    }
)
PROVIDER_REPLACEMENT_READINESS = frozenset({"none", "candidate", "ready_later"})


def _status(value: Any) -> str:
    token = norm_text(value)
    return token if token in PROVIDER_COVERAGE_STATUSES else "untested"


def _readiness(value: Any, *, status: str, consecutive_successes: int) -> str:
    token = norm_text(value)
    if token in PROVIDER_REPLACEMENT_READINESS:
        return token
    if status == "validated_provider":
        return "ready_later" if consecutive_successes >= 2 else "candidate"
    return "none"


def _compact(name: str, row: dict[str, Any]) -> dict[str, Any]:
    status = _status(row.get("providerCoverageStatus"))
    consecutive_successes = _clamped_int(row.get("providerCoverageConsecutiveSuccesses"), 0, 0)
    return {
        "name": clean_text(name),
        "adapter": clean_text(row.get("lastAdapter")) or clean_text(row.get("adapter")),
        "providerCoverageStatus": status,
        "providerReplacementReadiness": _readiness(
            row.get("providerReplacementReadiness"),
            status=status,
            consecutive_successes=consecutive_successes,
        ),
        "migrationSourceIdentity": clean_text(row.get("migrationSourceIdentity")),
        "detectedProviderFamily": clean_text(row.get("detectedProviderFamily")),
        "providerCoverageLastSuccessAt": clean_text(row.get("providerCoverageLastSuccessAt")),
        "providerCoverageConsecutiveSuccesses": consecutive_successes,
        "providerCoverageConsecutiveFailures": _clamped_int(
            row.get("providerCoverageConsecutiveFailures"), 0, 0
        ),
        "providerCoverageLatestKeptCount": _clamped_int(
            row.get("providerCoverageLatestKeptCount"), 0, 0
        ),
        "providerCoverageLatestError": clean_text(row.get("providerCoverageLatestError")),
    }


def _top(rows: list[dict[str, Any]], *statuses: str, limit: int = 8) -> list[dict[str, Any]]:
    allowed = set(statuses)
    selected = [row for row in rows if row.get("providerCoverageStatus") in allowed]
    selected.sort(
        key=lambda row: (
            _clamped_int(row.get("providerCoverageConsecutiveSuccesses"), 0, 0),
            _clamped_int(row.get("providerCoverageLatestKeptCount"), 0, 0),
        ),
        reverse=True,
    )
    return selected[:limit]


def build_provider_coverage_summary(
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    compact_rows = [
        _compact(name, as_json_object(row))
        for name, row in (source_state_rows or {}).items()
        if clean_text(name)
        and clean_text(as_json_object(row).get("migrationSourceIdentity"))
        and clean_text(as_json_object(row).get("providerCoverageStatus"))
    ]
    counts = Counter(str(row.get("providerCoverageStatus") or "") for row in compact_rows)
    ready_later = [
        row for row in compact_rows if row.get("providerReplacementReadiness") == "ready_later"
    ]
    return {
        "totalProviderCandidates": len(compact_rows),
        "statusCounts": dict(sorted((key, value) for key, value in counts.items() if key)),
        "probingProviders": _top(compact_rows, "untested", "probing"),
        "validatedProviders": _top(compact_rows, "validated_provider"),
        "unstableOrFailedProviders": _top(compact_rows, "unstable_provider", "failed_provider"),
        "needsReviewProviders": _top(compact_rows, "needs_review"),
        "readyLaterProviders": ready_later[:8],
    }


def normalize_provider_coverage_payload(
    payload: Any,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    derived = build_provider_coverage_summary(source_state_rows)
    src = as_json_object(payload)
    if not src:
        return derived
    normalized = dict(derived)
    normalized["totalProviderCandidates"] = _clamped_int(
        src.get("totalProviderCandidates"),
        int(derived.get("totalProviderCandidates") or 0),
        0,
    )
    status_counts = as_json_object(src.get("statusCounts"))
    if status_counts:
        normalized["statusCounts"] = {
            clean_text(key): _clamped_int(value, 0, 0)
            for key, value in status_counts.items()
            if clean_text(key)
        }
    for key in (
        "probingProviders",
        "validatedProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "readyLaterProviders",
    ):
        rows = src.get(key)
        if isinstance(rows, list):
            normalized[key] = [
                _compact(clean_text(as_json_object(row).get("name")), as_json_object(row))
                for row in rows
                if isinstance(row, dict)
            ][:8]
    return normalized
