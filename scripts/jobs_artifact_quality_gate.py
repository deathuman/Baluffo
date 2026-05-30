#!/usr/bin/env python3
"""Read-only shipped-artifact quality gate for jobs feed outputs."""

from __future__ import annotations

import argparse
import csv
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
from src.jobs.text_utils import clean_text, norm_text, normalize_url


def _resolve_csv_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "jobs-unified.csv"
    return path


def _load_rows(value: str) -> list[dict[str, str]]:
    path = _resolve_csv_path(value)
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _parsed_bundle(value: Any) -> list[dict[str, Any]]:
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


def _example(
    row: dict[str, Any],
    *,
    evidence: str = "",
    resolved_company: str = "",
) -> dict[str, str]:
    return {
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

    strong_unknown_examples: list[dict[str, str]] = []
    weak_unknown_examples: list[dict[str, str]] = []
    weak_unknown_host_counts: Counter[str] = Counter()

    for row in rows:
        company = clean_text(row.get("company"))
        if norm_text(company) not in {norm_text(UNKNOWN_COMPANY_LABEL), "unknown"}:
            continue
        bundle_urls = _gracklehq_bundle_urls(row)
        if not bundle_urls:
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

    blocked = len(exact_category_rows) + len(static_container_rows) + len(strong_unknown_examples)
    status = "blocked" if blocked else ("warning" if weak_unknown_examples else "pass")
    return {
        "status": status,
        "ok": blocked == 0,
        "artifactPath": str(_resolve_csv_path(value)),
        "counts": {
            "rows": len(rows),
            "exactCategoryTitleLeaks": len(exact_category_rows),
            "staticContainerTitleLeaks": len(static_container_rows),
            "unknownCompanyStrongEvidenceLeaks": len(strong_unknown_examples),
            "unknownCompanyWeakEvidenceWarnings": len(weak_unknown_examples),
        },
        "blocked": {
            "exactCategoryTitleExamples": exact_category_examples,
            "staticContainerTitleExamples": static_container_examples,
            "unknownCompanyExamples": strong_unknown_examples[:20],
        },
        "warnings": {
            "unknownCompanyExamples": weak_unknown_examples[:20],
            "unknownCompanyHostCounts": dict(weak_unknown_host_counts.most_common(20)),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate jobs feed artifacts for shipped-title/company leaks."
    )
    parser.add_argument("path", help="jobs-unified.csv file or directory containing it")
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
            "unknownCompanyStrongEvidenceLeaks={strong} "
            "unknownCompanyWeakEvidenceWarnings={weak}".format(
                status=report["status"],
                rows=counts["rows"],
                titles=counts["exactCategoryTitleLeaks"],
                containers=counts["staticContainerTitleLeaks"],
                strong=counts["unknownCompanyStrongEvidenceLeaks"],
                weak=counts["unknownCompanyWeakEvidenceWarnings"],
            )
        )
    return 1 if report["status"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
