#!/usr/bin/env python3
"""Read-only shipped-artifact quality gate for jobs feed outputs."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.common.config import UNKNOWN_COMPANY_LABEL
from src.jobs.common.exact_category_titles import (
    has_static_container_artifact_evidence,
    is_exact_category_title,
    looks_like_category_container_url,
    looks_like_static_container_url,
)
from src.jobs.job_link_company import company_from_job_link
from src.jobs.page_gating import looks_like_static_parser_noise_title
from src.jobs.text_utils import (
    classify_city_filter_rejection,
    clean_text,
    get_city_filter_option_values,
    norm_text,
    normalize_url,
)


def _resolve_json_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "jobs-unified.json"
    return path


def _load_rows(value: str) -> list[dict[str, str]]:
    payload = json.loads(_resolve_json_path(value).read_text(encoding="utf-8"))
    rows = payload.get("jobs") if isinstance(payload, dict) else payload
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _parsed_bundle(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    if not clean_text(value):
        return []
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _gracklehq_bundle_urls(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for item in _parsed_bundle(row.get("sourceBundle")):
        url = normalize_url(item.get("jobLink"))
        if "gracklehq.com/rd/" in url:
            urls.append(url)
    if "gracklehq.com/rd/" in clean_text(row.get("jobLink")):
        urls.append(normalize_url(row.get("jobLink")))
    return [url for url in urls if url]


def _host(value: Any) -> str:
    normalized = normalize_url(value)
    if not normalized:
        return ""
    return urlparse(normalized).netloc.lower()


def _location(row: dict[str, Any]) -> str:
    return ", ".join(
        part for part in [clean_text(row.get("city")), clean_text(row.get("country"))] if part
    )


def _parsed_locations(value: Any) -> list[dict[str, Any]]:
    payload = _parsed_bundle(value)
    return payload if payload else []


def _city_filter_candidate_hits(
    row: dict[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    hits: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    def _add_structured_city_hit(field_name: str, value: Any, country: Any) -> None:
        text = clean_text(value)
        if not text:
            return
        if get_city_filter_option_values(text, country):
            return
        reason = classify_city_filter_rejection(text)
        if not reason:
            return
        target = warnings if reason == "compound_non_city" else hits
        target.append({"field": field_name, "value": text, "reason": reason})

    _add_structured_city_hit("city", row.get("city"), row.get("country"))
    summary_text = clean_text(row.get("locationSummary"))
    summary_reason = classify_city_filter_rejection(summary_text) if summary_text else ""
    if summary_reason in {
        "known_non_city",
        "prose_or_navigation",
        "css_fragment",
        "time_fragment",
    }:
        hits.append({"field": "locationSummary", "value": summary_text, "reason": summary_reason})
    for item in _parsed_locations(row.get("locations")):
        _add_structured_city_hit("locations.city", item.get("city"), item.get("country"))
    return hits, warnings


def _example(
    row: dict[str, Any],
    *,
    evidence: str = "",
    resolved_company: str = "",
    city_filter_hits: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    example: dict[str, Any] = {
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "location": _location(row),
        "jobLink": normalize_url(row.get("jobLink")),
        "host": _host(row.get("jobLink")),
        "source": clean_text(row.get("source")),
        "sourceJobId": clean_text(row.get("sourceJobId")),
        "companyEvidence": evidence,
        "resolvedCompany": resolved_company,
    }
    if city_filter_hits:
        example["cityFilterHits"] = city_filter_hits
    return example


def _has_exact_category_evidence(row: dict[str, Any]) -> bool:
    if not is_exact_category_title(row.get("title")):
        return False
    source = clean_text(row.get("source"))
    if source.startswith("google_sheets") or source == "scrapy_static_sources":
        return True
    if source.startswith("static_source::"):
        return True
    if looks_like_category_container_url(row.get("jobLink")):
        return True
    for item in _parsed_bundle(row.get("sourceBundle")):
        bundle_source = clean_text(item.get("source"))
        if bundle_source.startswith("google_sheets") or bundle_source.startswith("static_source::"):
            return True
        if clean_text(item.get("adapter")) in {
            "static",
            "scrapy_static",
        } and looks_like_category_container_url(item.get("jobLink")):
            return True
    return False


def _static_container_evidence_kind(value: Any, adapter: Any = "") -> str:
    if clean_text(adapter) in {"static", "scrapy_static"}:
        return "static"
    source = clean_text(value)
    if source.startswith("static_source::") or source == "scrapy_static_sources":
        return "static"
    if source.startswith("google_sheets"):
        return "sheet"
    return ""


def _has_static_container_artifact_for_source(
    title: Any,
    url: Any,
    *,
    source: Any = "",
    adapter: Any = "",
) -> bool:
    if not has_static_container_artifact_evidence(title, url):
        return False
    evidence_kind = _static_container_evidence_kind(source, adapter)
    if evidence_kind == "static":
        return True
    if evidence_kind == "sheet":
        return looks_like_static_container_url(url) or looks_like_category_container_url(url)
    return False


def _has_static_container_artifact_evidence(row: dict[str, Any]) -> bool:
    if is_exact_category_title(row.get("title")):
        return False
    if _has_static_container_artifact_for_source(
        row.get("title"),
        row.get("jobLink"),
        source=row.get("source"),
    ):
        return True
    for item in _parsed_bundle(row.get("sourceBundle")):
        if _has_static_container_artifact_for_source(
            row.get("title"),
            item.get("jobLink") or row.get("jobLink"),
            source=item.get("source"),
            adapter=item.get("adapter"),
        ):
            return True
    return False


def analyze_jobs_artifact(value: str) -> dict[str, Any]:
    rows = _load_rows(value)
    exact_category_examples: list[dict[str, str]] = []
    static_container_examples: list[dict[str, str]] = []
    parser_noise_examples: list[dict[str, str]] = []
    gracklehq_known_company_by_url: dict[str, set[str]] = defaultdict(set)

    for row in rows:
        company = clean_text(row.get("company"))
        if company and norm_text(company) not in {norm_text(UNKNOWN_COMPANY_LABEL), "unknown"}:
            for url in _gracklehq_bundle_urls(row):
                gracklehq_known_company_by_url[url].add(company)

    exact_category_rows = [row for row in rows if _has_exact_category_evidence(row)]
    for row in exact_category_rows[:20]:
        exact_category_examples.append(_example(row, evidence="exact_source_category_title"))
    static_container_rows = [row for row in rows if _has_static_container_artifact_evidence(row)]
    for row in static_container_rows[:20]:
        static_container_examples.append(_example(row, evidence="static_container_artifact_title"))
    parser_noise_rows = [
        row for row in rows if looks_like_static_parser_noise_title(clean_text(row.get("title")))
    ]
    for row in parser_noise_rows[:20]:
        parser_noise_examples.append(_example(row, evidence="static_parser_noise_title"))
    city_filter_rows: list[tuple[dict[str, str], list[dict[str, str]]]] = []
    city_filter_warning_rows: list[tuple[dict[str, str], list[dict[str, str]]]] = []
    city_filter_reason_counts: Counter[str] = Counter()
    city_filter_field_counts: Counter[str] = Counter()
    city_filter_warning_reason_counts: Counter[str] = Counter()
    city_filter_warning_field_counts: Counter[str] = Counter()
    for row in rows:
        hits, warnings = _city_filter_candidate_hits(row)
        if hits:
            city_filter_rows.append((row, hits))
            city_filter_reason_counts.update(hit["reason"] for hit in hits)
            city_filter_field_counts.update(hit["field"] for hit in hits)
        if warnings:
            city_filter_warning_rows.append((row, warnings))
            city_filter_warning_reason_counts.update(hit["reason"] for hit in warnings)
            city_filter_warning_field_counts.update(hit["field"] for hit in warnings)
    city_filter_examples = [
        _example(
            row,
            evidence="city_filter_non_city_candidate",
            city_filter_hits=hits[:5],
        )
        for row, hits in city_filter_rows[:20]
    ]
    city_filter_warning_examples = [
        _example(
            row,
            evidence="city_filter_compound_display_only",
            city_filter_hits=hits[:5],
        )
        for row, hits in city_filter_warning_rows[:20]
    ]

    strong_unknown_examples: list[dict[str, str]] = []
    weak_unknown_examples: list[dict[str, str]] = []
    weak_unknown_host_counts: Counter[str] = Counter()

    for row in rows:
        company = clean_text(row.get("company"))
        if norm_text(company) not in {norm_text(UNKNOWN_COMPANY_LABEL), "unknown"}:
            continue
        inferred_company = clean_text(company_from_job_link(row.get("jobLink") or ""))
        if inferred_company:
            strong_unknown_examples.append(
                _example(
                    row,
                    evidence="structured_job_link_company",
                    resolved_company=inferred_company,
                )
            )
            continue
        bundle_urls = _gracklehq_bundle_urls(row)
        if not bundle_urls:
            continue
        known_companies = sorted(
            {
                known_company
                for url in bundle_urls
                for known_company in gracklehq_known_company_by_url.get(url, set())
            }
        )
        if known_companies:
            strong_unknown_examples.append(
                _example(
                    row,
                    evidence="same_gracklehq_bundle_company",
                    resolved_company=" | ".join(known_companies),
                )
            )
            continue
        weak_unknown_examples.append(_example(row, evidence="no_strong_company_evidence"))
        weak_unknown_host_counts[_host(row.get("jobLink")) or ""] += 1

    blocked = (
        len(exact_category_rows)
        + len(static_container_rows)
        + len(parser_noise_rows)
        + len(strong_unknown_examples)
        + len(city_filter_rows)
    )
    status = "blocked" if blocked else ("warning" if weak_unknown_examples else "pass")
    if not blocked and city_filter_warning_rows:
        status = "warning"
    return {
        "status": status,
        "ok": blocked == 0,
        "artifactPath": str(_resolve_json_path(value)),
        "counts": {
            "rows": len(rows),
            "exactCategoryTitleLeaks": len(exact_category_rows),
            "staticContainerTitleLeaks": len(static_container_rows),
            "parserNoiseTitleLeaks": len(parser_noise_rows),
            "cityFilterCandidateLeaks": len(city_filter_rows),
            "cityFilterCompoundWarnings": len(city_filter_warning_rows),
            "unknownCompanyStrongEvidenceLeaks": len(strong_unknown_examples),
            "unknownCompanyWeakEvidenceWarnings": len(weak_unknown_examples),
            "cityFilterCandidateReasonCounts": dict(city_filter_reason_counts),
            "cityFilterCandidateFieldCounts": dict(city_filter_field_counts),
            "cityFilterCompoundWarningReasonCounts": dict(city_filter_warning_reason_counts),
            "cityFilterCompoundWarningFieldCounts": dict(city_filter_warning_field_counts),
        },
        "blocked": {
            "exactCategoryTitleExamples": exact_category_examples,
            "staticContainerTitleExamples": static_container_examples,
            "parserNoiseTitleExamples": parser_noise_examples,
            "cityFilterCandidateExamples": city_filter_examples,
            "unknownCompanyExamples": strong_unknown_examples[:20],
        },
        "warnings": {
            "cityFilterCompoundExamples": city_filter_warning_examples,
            "unknownCompanyExamples": weak_unknown_examples[:20],
            "unknownCompanyHostCounts": dict(weak_unknown_host_counts.most_common(20)),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate jobs feed artifacts for shipped-title/company leaks."
    )
    parser.add_argument("path", help="jobs-unified.json file or directory containing it")
    parser.add_argument("--json", action="store_true", help="Print full JSON output")
    args = parser.parse_args(argv)

    report = analyze_jobs_artifact(args.path)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        counts = report["counts"]
        print(
            "status={status} rows={rows} exactCategoryTitleLeaks={titles} "
            "staticContainerTitleLeaks={containers} "
            "parserNoiseTitleLeaks={noise} "
            "cityFilterCandidateLeaks={city_filter} "
            "cityFilterCompoundWarnings={city_compound} "
            "unknownCompanyStrongEvidenceLeaks={strong} "
            "unknownCompanyWeakEvidenceWarnings={weak}".format(
                status=report["status"],
                rows=counts["rows"],
                titles=counts["exactCategoryTitleLeaks"],
                containers=counts["staticContainerTitleLeaks"],
                noise=counts["parserNoiseTitleLeaks"],
                city_filter=counts["cityFilterCandidateLeaks"],
                city_compound=counts["cityFilterCompoundWarnings"],
                strong=counts["unknownCompanyStrongEvidenceLeaks"],
                weak=counts["unknownCompanyWeakEvidenceWarnings"],
            )
        )
    return 1 if report["status"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
