"""Print a lightweight release data-quality report for the generated jobs feed."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.jobs.page_gating import looks_like_source_specific_static_noise_row
from src.jobs.text_utils import sanitize_location_text, sanitize_public_text

DEFAULT_FEED_PATH = Path("data/jobs-unified-light.json")

AMBIGUOUS_GENERIC_TITLES = {
    "creative",
    "finance",
    "legal",
    "marketing",
    "operations",
    "product",
    "sales",
}


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _city_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    unique_cities = sorted(
        {
            sanitize_public_text(row.get("city"))
            for row in rows
            if sanitize_public_text(row.get("city"))
        }
    )
    rejected = []
    accepted = []
    for city in unique_cities:
        value, reason = sanitize_location_text(city, field_name="city")
        if value and not reason:
            accepted.append(city)
        else:
            rejected.append({"city": city, "reason": reason or "empty"})
    return {
        "uniqueCityCount": len(unique_cities),
        "acceptedCityCount": len(accepted),
        "rejectedCityCount": len(rejected),
        "rejectedExamples": rejected[:50],
    }


def _non_job_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    high_confidence = []
    ambiguous = []
    ambiguous_counts: Counter[str] = Counter()
    for row in rows:
        title = sanitize_public_text(row.get("title"))
        source = sanitize_public_text(row.get("source"))
        job_link = sanitize_public_text(row.get("jobLink"))
        company = sanitize_public_text(row.get("company"))
        if looks_like_source_specific_static_noise_row(
            title=title,
            job_link=job_link,
            source_name=source,
        ):
            high_confidence.append(
                {
                    "title": title,
                    "company": company,
                    "source": source,
                    "jobLink": job_link,
                }
            )
            continue
        if title.strip().lower() in AMBIGUOUS_GENERIC_TITLES:
            ambiguous_counts[title.strip().lower()] += 1
            if len(ambiguous) < 50:
                ambiguous.append(
                    {
                        "title": title,
                        "company": company,
                        "source": source,
                        "jobLink": job_link,
                    }
                )
    return {
        "highConfidenceNonJobCount": len(high_confidence),
        "highConfidenceExamples": high_confidence[:50],
        "ambiguousGenericTitleCounts": dict(sorted(ambiguous_counts.items())),
        "ambiguousExamples": ambiguous,
    }


def build_release_data_quality_report(path: Path = DEFAULT_FEED_PATH) -> dict[str, Any]:
    rows = _load_rows(path)
    return {
        "feedPath": str(path),
        "rowCount": len(rows),
        "cityFilter": _city_report(rows),
        "nonJobSignals": _non_job_report(rows),
    }


def main() -> None:
    print(json.dumps(build_release_data_quality_report(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
