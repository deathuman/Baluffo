"""Location/city filtering guardrails for pipeline output finalization.

AI boundary owns: final-location entry cleaning, city-filter rejection classification,
and location quality guardrail application on final output rows.
AI boundary implement in: this file for location guardrails; report writing, lifecycle,
and availability finalization live in sibling finalize_* leaves coordinated by
``pipeline_finalize.py``.
AI boundary search before contracts: pipeline finalization tests and fetch-report location fields.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.jobs.text_utils import (
    classify_city_filter_rejection,
    clean_text,
    get_city_filter_option_values,
    norm_text,
    sanitize_location_text,
)

_MISSING_COUNTRY_PLACEHOLDERS = {"", "unknown", "n/a", "na", "none", "null"}


def _is_missing_country_placeholder(value: Any) -> bool:
    return norm_text(value) in _MISSING_COUNTRY_PLACEHOLDERS


def _clean_final_location_entry(
    item: dict[str, Any],
) -> tuple[list[dict[str, str]], dict[str, str]]:
    raw_city = clean_text(item.get("city"))
    raw_country = clean_text(item.get("country"))
    city, city_reason = sanitize_location_text(raw_city, field_name="city")
    country, country_reason = ("", "")
    if raw_country and not _is_missing_country_placeholder(raw_country):
        country, country_reason = sanitize_location_text(raw_country, field_name="country")
    city_options = get_city_filter_option_values(city, country) if city else []
    city_filter_reason = classify_city_filter_rejection(city) if city and not city_options else ""
    cleaned_items = [{"city": option, "country": country} for option in city_options]
    if not cleaned_items and country:
        cleaned_items = [{"city": "", "country": country}]
    reasons: dict[str, str] = {}
    if raw_city and (not city_options or city_options != [city]):
        reasons["city"] = city_reason or city_filter_reason or "split_compound_city"
    if raw_country and raw_country != country and not _is_missing_country_placeholder(raw_country):
        reasons["country"] = country_reason or "cleaned_country"
    return cleaned_items, reasons


def _location_summary_from_clean_entries(entries: list[dict[str, str]]) -> str:
    return " | ".join(
        ", ".join(
            part for part in [clean_text(item.get("city")), clean_text(item.get("country"))] if part
        )
        for item in entries
        if clean_text(item.get("city")) or clean_text(item.get("country"))
    )


def _is_high_confidence_summary_rejection(reason: str) -> bool:
    return reason in {
        "known_non_city",
        "prose_or_navigation",
        "css_fragment",
        "time_fragment",
    }


def _append_location_guardrail_example(
    examples: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    field: str,
    reason: str,
    value: Any,
) -> None:
    if len(examples) >= 20:
        return
    examples.append(
        {
            "company": clean_text(row.get("company")),
            "title": clean_text(row.get("title")),
            "source": clean_text(row.get("source")),
            "jobLink": clean_text(row.get("jobLink")),
            "field": field,
            "reason": reason,
            "value": clean_text(value),
        }
    )


def _apply_final_locations_list_guardrail(
    row: dict[str, Any],
    raw_locations: list[Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    cleaned_locations: list[dict[str, str]] = []
    seen_locations: set[str] = set()
    for item in raw_locations:
        if not isinstance(item, dict):
            continue
        cleaned_items, item_reasons = _clean_final_location_entry(item)
        for nested_field, reason in item_reasons.items():
            field_name = f"locations.{nested_field}"
            field_counts[field_name] += 1
            reason_counts[reason] += 1
            _append_location_guardrail_example(
                examples,
                row,
                field=field_name,
                reason=reason,
                value=item.get(nested_field),
            )
        for cleaned_item in cleaned_items:
            key = "|".join(
                [norm_text(cleaned_item.get("city")), norm_text(cleaned_item.get("country"))]
            )
            if key in seen_locations:
                continue
            seen_locations.add(key)
            cleaned_locations.append(cleaned_item)
    if cleaned_locations != raw_locations:
        row["locations"] = cleaned_locations
    rebuilt_summary = _location_summary_from_clean_entries(cleaned_locations)
    if clean_text(row.get("locationSummary")) == rebuilt_summary:
        return
    if clean_text(row.get("locationSummary")):
        field_counts["locationSummary"] += 1
        reason_counts["rebuilt_from_clean_locations"] += 1
    row["locationSummary"] = rebuilt_summary


def _apply_final_location_scalar_guardrail(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    for field_name in ("city", "country"):
        if field_name == "country" and _is_missing_country_placeholder(row.get(field_name)):
            continue
        value, reason = sanitize_location_text(row.get(field_name), field_name=field_name)
        if field_name == "city" and value:
            filter_reason = classify_city_filter_rejection(value)
            if filter_reason:
                value = ""
                reason = filter_reason
        if not reason:
            continue
        _append_location_guardrail_example(
            examples,
            row,
            field=field_name,
            reason=reason,
            value=row.get(field_name),
        )
        row[field_name] = value
        field_counts[field_name] += 1
        reason_counts[reason] += 1


def _apply_final_location_summary_guardrail(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    summary_reason = classify_city_filter_rejection(row.get("locationSummary"))
    if not _is_high_confidence_summary_rejection(summary_reason):
        return
    replacement_summary = ", ".join(
        part
        for part in [clean_text(row.get("city")), clean_text(row.get("country"))]
        if part and not _is_missing_country_placeholder(part)
    )
    if clean_text(row.get("locationSummary")) == replacement_summary:
        return
    _append_location_guardrail_example(
        examples,
        row,
        field="locationSummary",
        reason=summary_reason,
        value=row.get("locationSummary"),
    )
    row["locationSummary"] = replacement_summary
    field_counts["locationSummary"] += 1
    reason_counts[summary_reason] += 1


def _apply_final_location_quality_guardrail(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_locations = row.get("locations")
        if isinstance(raw_locations, list):
            _apply_final_locations_list_guardrail(
                row,
                raw_locations,
                field_counts=field_counts,
                reason_counts=reason_counts,
                examples=examples,
            )
        _apply_final_location_scalar_guardrail(
            row,
            field_counts=field_counts,
            reason_counts=reason_counts,
            examples=examples,
        )
        if not isinstance(raw_locations, list):
            _apply_final_location_summary_guardrail(
                row,
                field_counts=field_counts,
                reason_counts=reason_counts,
                examples=examples,
            )
    return {
        "totalRows": len(rows),
        "invalidLocationFieldCount": int(sum(field_counts.values())),
        "fieldCounts": dict(field_counts),
        "reasonCounts": dict(reason_counts),
        "examples": examples,
    }
