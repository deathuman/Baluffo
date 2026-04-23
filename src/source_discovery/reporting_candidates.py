from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from src.shared.utils import now_iso

from .config import EVIDENCE_TYPES_SET
from .io_runtime import endpoint_url
from .reporting_progress import emit_log
from .scoring import unique_string_list


def _validate_evidence_types(values: list[str], *, context: str) -> list[str]:
    cleaned = unique_string_list(
        [str(item or "").strip() for item in (values or []) if str(item or "").strip()]
    )
    unknown = [item for item in cleaned if item not in EVIDENCE_TYPES_SET]
    if unknown:
        emit_log(f"Warning: dropping unknown evidenceTypes in {context}: {unknown}")
    return [item for item in cleaned if item in EVIDENCE_TYPES_SET]


def stage_curated_seed_candidates() -> list[dict[str, Any]]:
    from src.source_registry import unique_sources

    from .config import STATIC_DISCOVERY_CANDIDATES

    rows: list[dict[str, Any]] = []
    for raw in STATIC_DISCOVERY_CANDIDATES:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
        row["discoveryStage"] = "curated_seed"
        row["evidenceScore"] = int(row.get("evidenceScore") or 52)
        row["evidenceTypes"] = _validate_evidence_types(
            list(row.get("evidenceTypes") or ["seed_curated"]),
            context="stage_curated_seed_candidates",
        )
        row["evidenceSource"] = str(row.get("evidenceSource") or "seed")
        row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
        rows.append(row)
    return unique_sources(rows)


def merge_candidate_streams(
    streams: Iterable[tuple[str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryStage"] = str(row.get("discoveryStage") or stage)
            row["discoveryMethod"] = str(
                row.get("discoveryMethod") or ("seed" if stage == "curated_seed" else "pattern")
            )
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            row["evidenceTypes"] = _validate_evidence_types(
                list(row.get("evidenceTypes") or []),
                context="merge_candidate_streams",
            )
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows
