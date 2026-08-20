"""Source bundle and location accumulation state.

AI boundary owns: building ``sourceBundle`` and location state across rows during a
dedup pass, including working samples and elevation staleness rules.
AI boundary implement in: this leaf for state accumulation; record merging and
targeting live in sibling dedup_* leaves.
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.canonicalize import (
    clean_text,
    norm_text,
    normalize_url,
    to_iso,
)
from src.jobs.dedup_identity import (
    _LOCATION_DEDUP_WORKING_SAMPLE_LIMIT,
    _SOURCE_BUNDLE_OUTPUT_SAMPLE_LIMIT,
    _is_elevato_static_row,
    _is_elevato_url,
    _is_meaningful_location_value,
)
from src.jobs.text_utils import get_city_filter_option_values


def _normalized_bundle_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": clean_text(item.get("source")),
        "sourceJobId": clean_text(item.get("sourceJobId")),
        "jobLink": normalize_url(item.get("jobLink")),
        "postedAt": to_iso(item.get("postedAt")),
        "adapter": clean_text(item.get("adapter")),
        "studio": clean_text(item.get("studio")),
    }


def _bundle_key(item: dict[str, Any]) -> str:
    return "|".join(
        [
            norm_text(item.get("source")),
            norm_text(item.get("sourceJobId")),
            norm_text(item.get("jobLink")),
        ]
    )


def _merge_source_bundle(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bundle: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        entries = row.get("sourceBundle")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = _normalized_bundle_item(item)
            key = _bundle_key(normalized_item)
            if key in seen:
                continue
            seen.add(key)
            bundle.append(normalized_item)
    return bundle


def _source_bundle_count_from_row(row: dict[str, Any], bundle: list[dict[str, Any]]) -> int:
    try:
        count = int(row.get("sourceBundleCount") or 0)
    except (TypeError, ValueError):
        count = 0
    return max(0, count, len(bundle))


def _should_hide_stale_elevato_bundle_item(
    *, primary: dict[str, Any], item: dict[str, Any]
) -> bool:
    return (
        _is_elevato_static_row(primary)
        and clean_text(item.get("source")).startswith("google_sheets")
        and _is_elevato_url(item.get("jobLink"))
    )


def _source_bundle_working_sample(
    bundle: list[dict[str, Any]], *, primary: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    limit = max(0, int(_SOURCE_BUNDLE_OUTPUT_SAMPLE_LIMIT))
    filtered: list[dict[str, Any]] = []
    primary_payload = primary or {}
    for item in bundle:
        if primary_payload and _should_hide_stale_elevato_bundle_item(
            primary=primary_payload, item=item
        ):
            continue
        filtered.append(dict(item))
        if len(filtered) >= limit:
            break
    return filtered


def _source_bundle_state_from_payload(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str], int]:
    bundle = _merge_source_bundle([payload])
    keys = {_bundle_key(item) for item in bundle}
    return bundle, keys, _source_bundle_count_from_row(payload, bundle)


def _extend_source_bundle_state(
    *,
    bundle: list[dict[str, Any]],
    keys: set[str],
    count: int,
    incoming: dict[str, Any],
) -> int:
    incoming_bundle = _merge_source_bundle([incoming])
    incoming_count = _source_bundle_count_from_row(incoming, incoming_bundle)
    added = 0
    for item in incoming_bundle:
        key = _bundle_key(item)
        if key in keys:
            continue
        keys.add(key)
        bundle.append(item)
        added += 1
    hidden_incoming_count = max(0, incoming_count - len(incoming_bundle))
    return max(count + added + hidden_incoming_count, len(keys), count)


def _normalized_location_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "city": clean_text(item.get("city")),
        "country": clean_text(item.get("country")),
    }


def _location_key(item: dict[str, Any]) -> str:
    return "|".join([norm_text(item.get("city")), norm_text(item.get("country"))])


def _collect_location_entries(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    location_entries: list[dict[str, Any]] = []
    placeholder_location_entries: list[dict[str, Any]] = []
    for row in rows:
        entries = row.get("locations")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = _normalized_location_item(item)
            city_options = (
                get_city_filter_option_values(
                    normalized_item.get("city"),
                    normalized_item.get("country"),
                )
                if normalized_item.get("city")
                else []
            )
            normalized_items = [
                {**normalized_item, "city": city_option} for city_option in city_options
            ]
            if normalized_item.get("city") and not normalized_items:
                normalized_items = [{**normalized_item, "city": ""}]
            if not normalized_items:
                normalized_items = [normalized_item]
            for normalized_item in normalized_items:
                if not _is_meaningful_location_value(
                    normalized_item.get("city")
                ) and not _is_meaningful_location_value(normalized_item.get("country")):
                    if not placeholder_location_entries:
                        placeholder_location_entries.append(normalized_item)
                    continue
                location_entries.append(normalized_item)
    return location_entries, placeholder_location_entries


def _fallback_merged_locations(
    *,
    normalized_locations: dict[str, Any],
    merged_locations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if normalized_locations.get("locations"):
        return merged_locations
    fallback_city = clean_text(normalized_locations.get("city"))
    fallback_country = clean_text(normalized_locations.get("country"))
    if not any(
        clean_text(item.get("city")) or clean_text(item.get("country")) for item in merged_locations
    ):
        return []
    if not merged_locations and (fallback_city or fallback_country):
        return [{"city": fallback_city, "country": fallback_country}]
    return merged_locations


def _location_summary_from_entries(entries: list[dict[str, Any]]) -> str:
    return " | ".join(
        ", ".join(
            part for part in [clean_text(item.get("city")), clean_text(item.get("country"))] if part
        )
        for item in entries
        if clean_text(item.get("city")) or clean_text(item.get("country"))
    )


def _location_state_from_payload(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    location_entries, placeholder_location_entries = _collect_location_entries([payload])
    keys = {_location_key(item) for item in location_entries}
    return location_entries, placeholder_location_entries, keys


def _extend_location_state(
    *,
    location_entries: list[dict[str, Any]],
    placeholder_location_entries: list[dict[str, Any]],
    location_keys: set[str],
    incoming: dict[str, Any],
) -> None:
    incoming_locations, incoming_placeholders = _collect_location_entries([incoming])
    for item in incoming_locations:
        key = _location_key(item)
        if key in location_keys:
            continue
        location_keys.add(key)
        location_entries.append(item)
    if not location_entries and not placeholder_location_entries and incoming_placeholders:
        placeholder_location_entries.extend(incoming_placeholders[:1])


def _apply_location_state_sample(
    *,
    merged: dict[str, Any],
    location_entries: list[dict[str, Any]],
    placeholder_location_entries: list[dict[str, Any]],
) -> None:
    if location_entries:
        sample = [
            dict(item)
            for item in location_entries[: max(0, int(_LOCATION_DEDUP_WORKING_SAMPLE_LIMIT))]
        ]
        merged["locations"] = sample
        merged["locationSummary"] = _location_summary_from_entries(sample)
    elif placeholder_location_entries:
        merged["locations"] = [dict(item) for item in placeholder_location_entries[:1]]


def _apply_location_state(
    *,
    merged: dict[str, Any],
    location_entries: list[dict[str, Any]],
    placeholder_location_entries: list[dict[str, Any]],
) -> None:
    if location_entries:
        normalized_locations = normalize_location_details(location_entries)
        merged_locations = normalized_locations.get("locations") or location_entries
        merged_locations = _fallback_merged_locations(
            normalized_locations=normalized_locations,
            merged_locations=merged_locations,
        )
        merged["locations"] = merged_locations
        merged["locationSummary"] = clean_text(normalized_locations.get("locationSummary"))
        if not merged["locationSummary"] and merged_locations:
            merged["locationSummary"] = _location_summary_from_entries(merged_locations)
    elif placeholder_location_entries:
        merged["locations"] = [dict(item) for item in placeholder_location_entries[:1]]
