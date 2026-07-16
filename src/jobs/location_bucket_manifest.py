from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.text_utils import clean_text, resolve_country_acceptance_value


def _is_unknown_country(value: Any) -> bool:
    return clean_text(value) in {"", "Unknown"}


def _city_unknown_country_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for row in rows:
        city = clean_text(row.get("city") or row.get("City"))
        country = clean_text(row.get("country") or row.get("Country"))
        if not city or city == "Unknown" or not _is_unknown_country(country):
            continue
        filtered.append(
            {
                "title": clean_text(row.get("title") or row.get("Title")),
                "company": clean_text(row.get("company") or row.get("Company")),
                "city": city,
                "country": country,
                "source": clean_text(row.get("source") or row.get("Source")),
                "jobLink": clean_text(row.get("jobLink") or row.get("JobLink")),
            }
        )
    return filtered


def classify_bucket_family(city: str) -> str:
    token = clean_text(city)
    if not token:
        return "unknown"
    if classify_city_garbage(token):
        return "garbage"
    if resolve_country_acceptance_value(token):
        return "country_in_city"
    location_details = normalize_location_details(token)
    normalized_city = clean_text(location_details.get("city"))
    normalized_country = clean_text(location_details.get("country"))
    if normalized_country not in {"", "Unknown"}:
        if normalized_city and normalized_city != token:
            return "city_blob"
        return "city_only"
    if any(separator in token for separator in (",", "/", "|")):
        return "city_blob"
    return "source_specific"


def build_unknown_country_bucket_manifest(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    filtered = _city_unknown_country_rows(rows)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in filtered:
        grouped[row["city"]].append(row)

    manifest: list[dict[str, Any]] = []
    for city, bucket_rows in grouped.items():
        representative = sorted(
            bucket_rows,
            key=lambda item: (
                item["source"],
                item["company"],
                item["title"],
                item["jobLink"],
            ),
        )[0]
        source_counts = Counter(row["source"] for row in bucket_rows if row["source"])
        manifest.append(
            {
                "city": city,
                "count": len(bucket_rows),
                "family": classify_bucket_family(city),
                "representative": {
                    "title": representative["title"],
                    "company": representative["company"],
                    "source": representative["source"],
                    "jobLink": representative["jobLink"],
                    "jobHost": urlparse(representative["jobLink"]).netloc.lower(),
                },
                "topSources": [
                    {"source": source, "count": count}
                    for source, count in source_counts.most_common(3)
                ],
            }
        )
    manifest.sort(key=lambda item: (-int(item["count"]), item["city"].lower()))
    return manifest


def load_rows_from_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("jobs") if isinstance(payload, dict) else payload
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def load_manifest(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def check_manifest_against_rows(
    manifest: Iterable[Mapping[str, Any]],
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        title = clean_text(row.get("title") or row.get("Title"))
        job_link = clean_text(row.get("jobLink") or row.get("JobLink"))
        if title and job_link:
            index[(title, job_link)] = {
                "title": title,
                "company": clean_text(row.get("company") or row.get("Company")),
                "city": clean_text(row.get("city") or row.get("City")),
                "country": clean_text(row.get("country") or row.get("Country")),
                "source": clean_text(row.get("source") or row.get("Source")),
                "jobLink": job_link,
            }

    results: list[dict[str, Any]] = []
    for item in manifest:
        representative = item.get("representative") if isinstance(item, Mapping) else {}
        title = clean_text((representative or {}).get("title"))
        job_link = clean_text((representative or {}).get("jobLink"))
        candidate = index.get((title, job_link))
        status = "missing"
        if candidate:
            country = clean_text(candidate.get("country"))
            city = clean_text(candidate.get("city"))
            if country not in {"", "Unknown"}:
                status = "resolved"
            elif not city:
                status = "cleared"
            else:
                status = "still_unknown"
        results.append(
            {
                "city": clean_text(item.get("city")),
                "family": clean_text(item.get("family")),
                "count": int(item.get("count") or 0),
                "status": status,
                "candidate": candidate or {},
            }
        )
    return results
