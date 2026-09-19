"""Fetch-report social channels: channel rows and social summary.

AI boundary owns: fetch-report social channel normalization and the social summary rollup.
AI boundary implement in: this leaf for social normalization; row primitives stay in the fetch_report_normalization coordinator.
AI boundary search before contracts: bridge report normalizer, jobs report contracts, and social summary tests.
AI boundary verify: `python -m pytest tests/test_fetch_report_normalization_parity.py -q`.
"""

from __future__ import annotations

from typing import Any

from src.shared.coerce import as_float as _float_or_zero
from src.shared.coerce import as_text as _clean_text
from src.shared.fetch_report_normalization_primitives import _clamped_int
from src.shared.json_shapes import as_json_object


def normalize_fetch_report_social_channel(payload: Any) -> dict[str, Any]:
    src_channel = as_json_object(payload)
    return {
        "keptCount": _clamped_int(src_channel.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src_channel.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src_channel.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src_channel.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src_channel.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src_channel.get("lowConfidenceDropped")),
    }


def normalize_fetch_report_social_summary(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    if not src:
        return {}
    channels = as_json_object(src.get("channels"))
    return {
        "pilotWindowStartAt": _clean_text(src.get("pilotWindowStartAt")),
        "pilotWindowEndAt": _clean_text(src.get("pilotWindowEndAt")),
        "scheduledRunCount": _clamped_int(src.get("scheduledRunCount")),
        "keptCount": _clamped_int(src.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src.get("lowConfidenceDropped")),
        "sampleSize": _clamped_int(src.get("sampleSize")),
        "reviewedCount": _clamped_int(src.get("reviewedCount")),
        "falsePositiveCount": _clamped_int(src.get("falsePositiveCount")),
        "falsePositiveRate": max(0.0, min(1.0, _float_or_zero(src.get("falsePositiveRate")))),
        "reviewArtifactPath": _clean_text(src.get("reviewArtifactPath")),
        "channels": {
            _clean_text(key): normalize_fetch_report_social_channel(value)
            for key, value in channels.items()
            if _clean_text(key)
        },
    }
