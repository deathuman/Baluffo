"""Fetch-report row primitives: shared coercion, text, and base source-row normalization.

AI boundary owns: the shared fetch-report row primitives every other normalization leaf builds on, including the base source-row normalizers and the literal-parsing detail-row coercer.
AI boundary implement in: this leaf for row primitives; enrichment, loss, detail, timing, and social rules live in their own fetch_report_normalization_* leaves.
AI boundary search before contracts: bridge report normalizer, jobs report contracts, and DATA_CONTRACT.md.
AI boundary verify: `python -m pytest tests/test_fetch_report_normalization_parity.py tests/bridge/test_report_normalizer_exception_ratchet.py -q`.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.shared.coerce import as_float as _float_or_zero
from src.shared.coerce import as_text as _clean_text
from src.shared.json_shapes import as_json_list, as_json_object

FINALIZATION_TIMING_KEYS = (
    "deduplicatingMs",
    "reconciling_identitiesMs",
    "applying_lifecycleMs",
    "running_quality_auditsMs",
    "writing_outputsMs",
)


def _clamped_int(
    value: Any,
    default: int = 0,
    minimum: int = 0,
    maximum: int = 1_000_000,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _normalize_text(value: Any) -> str:
    return " ".join(_clean_text(value).split()).lower()


def _clean_label(value: Any, *, clean_text_func: Any = _clean_text) -> str:
    text = clean_text_func(value)
    return "" if text.lower() in {"n/a", "na", "none"} else text


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "")


def _clean_pages(pages: Any, *, clean_text_func: Any = _clean_text) -> list[str]:
    return [clean_text_func(page) for page in as_json_list(pages) if clean_text_func(page)]


@dataclass(frozen=True)
class _SourceRowBaseOptions:
    clean_text_func: Callable[[Any], str]
    normalize_text_func: Callable[[Any], str] | None
    lowercase_status: bool
    lowercase_adapter: bool
    status_default: str
    adapter_default: str
    fetch_strategy_default: str
    last_status_fallback_status: bool
    last_status_lowercase: bool
    last_checked_fallback_last_seen: bool
    last_success_fallback_last_successful: bool
    last_successful_fallback_last_success: bool
    last_seen_fallback_last_checked: bool
    last_seen_fallback_last_run: bool
    health_score_default: int
    health_score_max: int | None
    count_max: int | None
    duration_max: int | None
    include_duplicate_rate: bool


def _source_row_clean(value: Any, options: _SourceRowBaseOptions) -> str:
    return str(options.clean_text_func(value) or "").strip()


def _source_row_norm(value: Any, options: _SourceRowBaseOptions) -> str:
    if options.normalize_text_func is not None:
        return str(options.normalize_text_func(value) or "").strip()
    return _normalize_text(value)


def _source_row_number(
    value: Any,
    options: _SourceRowBaseOptions,
    *,
    default: int = 0,
    maximum: int | None = None,
) -> int:
    ceiling = options.count_max if maximum is None else maximum
    parsed = _clamped_int(value, default=default, maximum=ceiling or 1_000_000_000)
    return parsed if ceiling is None else min(ceiling, parsed)


def _first_text(
    src: dict[str, Any],
    options: _SourceRowBaseOptions,
    *keys: str,
) -> str:
    for key in keys:
        value = _source_row_clean(src.get(key), options)
        if value:
            return value
    return ""


def _source_row_text_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, Any]:
    clean = lambda value: _source_row_clean(value, options)
    norm = lambda value: _source_row_norm(value, options)
    status = norm(src.get("status")) if options.lowercase_status else clean(src.get("status"))
    adapter = clean(src.get("adapter"))
    if options.lowercase_adapter:
        adapter = adapter.lower()
    last_status = clean(src.get("lastStatus"))
    if not last_status and options.last_status_fallback_status:
        last_status = clean(src.get("status"))
    if options.last_status_lowercase:
        last_status = last_status.lower()
    return {
        "name": clean(src.get("name")),
        "status": status or options.status_default,
        "adapter": adapter or options.adapter_default,
        "fetchStrategy": clean(src.get("fetchStrategy")) or options.fetch_strategy_default,
        "studio": clean(src.get("studio")),
        "error": clean(src.get("error")),
        "lastStatus": last_status,
        "lastRunAt": clean(src.get("lastRunAt")),
        "health": norm(src.get("health")) if options.lowercase_status else clean(src.get("health")),
        "healthReason": clean(src.get("healthReason")),
    }


def _source_row_date_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, str]:
    checked_keys = (
        ("lastCheckedAt", "lastSeenInFetchAt")
        if options.last_checked_fallback_last_seen
        else ("lastCheckedAt",)
    )
    success_keys = (
        ("lastSuccessAt", "lastSuccessfulFetchAt")
        if options.last_success_fallback_last_successful
        else ("lastSuccessAt",)
    )
    successful_keys = (
        ("lastSuccessfulFetchAt", "lastSuccessAt")
        if options.last_successful_fallback_last_success
        else ("lastSuccessfulFetchAt",)
    )
    seen_keys = ["lastSeenInFetchAt"]
    if options.last_seen_fallback_last_checked:
        seen_keys.append("lastCheckedAt")
    if options.last_seen_fallback_last_run:
        seen_keys.append("lastRunAt")
    return {
        "lastCheckedAt": _first_text(src, options, *checked_keys),
        "lastSuccessAt": _first_text(src, options, *success_keys),
        "lastSuccessfulFetchAt": _first_text(src, options, *successful_keys),
        "lastSeenInFetchAt": _first_text(src, options, *seen_keys),
    }


def _source_row_count_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, int]:
    number = lambda value, default=0, maximum=None: _source_row_number(
        value, options, default=default, maximum=maximum
    )
    # Canonical counters only (alias collapse Phase 5): the wire contract no
    # longer carries the legacy alias spellings, so legacy alias-only inputs
    # are normalized away entirely (docs/plans/source-health-counter-collapse-plan.md).
    return {
        "fetchedCount": number(src.get("fetchedCount")),
        "keptCount": number(src.get("keptCount")),
        "lowConfidenceDropped": number(src.get("lowConfidenceDropped")),
        "durationMs": number(src.get("durationMs"), maximum=options.duration_max),
        "lastKeptCount": number(src.get("lastKeptCount")),
        "consecutiveFailures": number(src.get("consecutiveFailures")),
        "consecutiveZeroKept": number(src.get("consecutiveZeroKept")),
        "healthScore": number(
            src.get("healthScore"),
            default=options.health_score_default,
            maximum=options.health_score_max,
        ),
    }


def normalize_fetch_report_source_row_base(
    row: Any,
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = None,
    lowercase_status: bool = False,
    lowercase_adapter: bool = False,
    status_default: str = "",
    adapter_default: str = "",
    fetch_strategy_default: str = "",
    last_status_fallback_status: bool = False,
    last_status_lowercase: bool = False,
    last_checked_fallback_last_seen: bool = False,
    last_success_fallback_last_successful: bool = False,
    last_successful_fallback_last_success: bool = True,
    last_seen_fallback_last_checked: bool = True,
    last_seen_fallback_last_run: bool = False,
    # Alias collapse Phase 5: the wire contract is canonical-only — the legacy
    # alias input spellings are no longer normalized into the output at all
    # (docs/plans/source-health-counter-collapse-plan.md).
    health_score_default: int = 0,
    health_score_max: int | None = 100,
    count_max: int | None = None,
    duration_max: int | None = None,
    include_duplicate_rate: bool = False,
) -> dict[str, Any]:
    src = as_json_object(row)
    options = _SourceRowBaseOptions(
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
        lowercase_status=lowercase_status,
        lowercase_adapter=lowercase_adapter,
        status_default=status_default,
        adapter_default=adapter_default,
        fetch_strategy_default=fetch_strategy_default,
        last_status_fallback_status=last_status_fallback_status,
        last_status_lowercase=last_status_lowercase,
        last_checked_fallback_last_seen=last_checked_fallback_last_seen,
        last_success_fallback_last_successful=last_success_fallback_last_successful,
        last_successful_fallback_last_success=last_successful_fallback_last_success,
        last_seen_fallback_last_checked=last_seen_fallback_last_checked,
        last_seen_fallback_last_run=last_seen_fallback_last_run,
        health_score_default=health_score_default,
        health_score_max=health_score_max,
        count_max=count_max,
        duration_max=duration_max,
        include_duplicate_rate=include_duplicate_rate,
    )
    payload = _source_row_text_fields(src, options)
    payload.update(_source_row_date_fields(src, options))
    payload.update(_source_row_count_fields(src, options))
    if options.include_duplicate_rate:
        payload["duplicateRate"] = _float_or_zero(src.get("duplicateRate"))
    return payload


def normalize_jobs_fetch_report_source_row_base(
    row: Any,
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> dict[str, Any]:
    # Alias collapse Phases 4–5
    # (docs/plans/source-health-counter-collapse-plan.md): repo-side producers
    # normalize the canonical counters only — no alias-shaped dual-write on
    # any wire surface since Phase 5 dropped the bridge alias contract.
    return normalize_fetch_report_source_row_base(
        row,
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
        lowercase_status=True,
        status_default="error",
        adapter_default="custom",
        fetch_strategy_default="auto",
        last_seen_fallback_last_run=True,
        health_score_default=100,
        health_score_max=None,
        include_duplicate_rate=True,
    )


def coerce_fetch_report_detail_row(detail: Any) -> dict[str, Any] | None:
    candidate: dict[str, Any] | None = None
    if isinstance(detail, dict):
        candidate = detail
    elif isinstance(detail, str):
        raw = str(detail).strip()
        if raw.startswith("{") and raw.endswith("}"):
            parsed: Any = None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except (SyntaxError, ValueError):
                    parsed = None
            if isinstance(parsed, dict):
                candidate = parsed
    if not isinstance(candidate, dict):
        return None
    return {
        "name": _clean_text(candidate.get("name")),
        "status": _clean_text(candidate.get("status")).lower(),
        "adapter": _clean_text(candidate.get("adapter")).lower(),
        "studio": _clean_text(candidate.get("studio")),
        "fetchedCount": _clamped_int(candidate.get("fetchedCount")),
        "keptCount": _clamped_int(candidate.get("keptCount")),
        "lowConfidenceDropped": _clamped_int(candidate.get("lowConfidenceDropped")),
        "error": _clean_text(candidate.get("error")),
    }
