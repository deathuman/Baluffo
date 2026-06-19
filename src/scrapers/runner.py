#!/usr/bin/env python3
"""Scrapy subprocess runner for Baluffo static HTML crawling."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from collections import Counter
from pathlib import Path

# Allow importing src when runner is executed as script (e.g. by static adapter subprocess).
# Use repo root (parents[2]) so "from src.shared.regex" resolves; parents[1] would be src/ and would not contain a package "src".
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from typing import Any

from src.scrapers import domain_profiles
from src.scrapers.helpers import clean_text as _clean_text
from src.scrapers.helpers import safe_id as _safe_id
from src.scrapers.helpers import to_float as _to_float
from src.scrapers.helpers import to_int as _to_int
from src.scrapers.providers.jobylon_v1 import extract_jobylon_v1_jobs
from src.scrapers.settings import SCRAPY_PLAYWRIGHT_SETTINGS, SCRAPY_SETTINGS_DEFAULTS
from src.scrapers.spiders.generic_careers import GenericCareersSpider

_EXPECTED_CRAWL_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError)


def _source_id(name: str, studio: str, pages: list[str]) -> str:
    seed = "|".join([_clean_text(name), _clean_text(studio), *[_clean_text(p) for p in pages]])
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _classify_result(
    *,
    ok: bool,
    fetched_count: int,
    kept_count: int,
    partial_errors: list[str],
    reject_reasons: Counter[str] | None = None,
) -> str:
    if not ok:
        return "parse_error"
    if kept_count > 0:
        return "ok_with_jobs"
    if fetched_count <= 0:
        return "blocked_or_challenge"
    reject_reasons = reject_reasons or Counter()
    if int(reject_reasons.get("dead_listing_page") or 0) > 0:
        return "dead_listing_page"
    lower_errors = " ".join(item.lower() for item in partial_errors)
    if (
        "captcha" in lower_errors
        or "cloudflare" in lower_errors
        or "challenge" in lower_errors
        or "403" in lower_errors
    ):
        return "blocked_or_challenge"
    if fetched_count > 0 and kept_count == 0:
        return "needs_review"
    return "needs_review"


def _stats_subset(stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "downloader/request_count": _to_int(stats.get("downloader/request_count")),
        "downloader/response_count": _to_int(stats.get("downloader/response_count")),
        "downloader/response_status_count/200": _to_int(
            stats.get("downloader/response_status_count/200")
        ),
        "retry/count": _to_int(stats.get("retry/count")),
        "autothrottle/current_delay": _to_float(stats.get("autothrottle/current_delay")),
        "autothrottle/start_delay": _to_float(stats.get("autothrottle/start_delay")),
        "item_scraped_count": _to_int(stats.get("item_scraped_count")),
        "finish_reason": _clean_text(stats.get("finish_reason")),
        "candidate_links_found": _to_int(stats.get("candidate_links_found")),
        "detail_pages_visited": _to_int(stats.get("detail_pages_visited")),
        "jobs_emitted": _to_int(stats.get("jobs_emitted")),
        "jobs_rejected_validation": _to_int(stats.get("jobs_rejected_validation")),
        "dead_listing_pages_rejected": _to_int(stats.get("dead_listing_pages_rejected")),
    }


def _json_error_envelope(error: str, *, source_name: str, studio: str) -> dict[str, Any]:
    sid = _source_id(source_name, studio, [])
    return {
        "ok": False,
        "jobs": [],
        "details": [
            {
                "adapter": "scrapy_static",
                "studio": studio or "unknown",
                "name": source_name or "unknown",
                "status": "error",
                "fetchedCount": 0,
                "keptCount": 0,
                "error": _clean_text(error),
                "classification": "parse_error",
                "browserFallbackRecommended": False,
                "top_reject_reasons": ["parse_error:1"],
                "sourceId": sid,
                "pages": [],
            }
        ],
        "partialErrors": [_clean_text(error)],
        "stats": _stats_subset({}),
    }


def _make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {_clean_text(key): _make_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, str):
        return value.encode("utf-8", "replace").decode("utf-8")
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _clean_text(value)


def _emit_envelope(envelope: dict[str, Any]) -> None:
    safe_envelope = _make_json_safe(envelope)
    try:
        payload = json.dumps(safe_envelope, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        details = safe_envelope.get("details") if isinstance(safe_envelope, dict) else []
        first_detail = (
            details[0]
            if isinstance(details, list) and details and isinstance(details[0], dict)
            else {}
        )
        fallback = _json_error_envelope(
            f"Envelope serialization failed: {exc}",
            source_name=_clean_text(first_detail.get("name")) or "unknown",
            studio=_clean_text(first_detail.get("studio")) or "unknown",
        )
        payload = json.dumps(fallback, ensure_ascii=False)
    try:
        print(payload, flush=True)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        sys.stdout.buffer.write(payload.encode(encoding, "replace") + b"\n")
        sys.stdout.flush()


def _validate_input(payload: Any) -> tuple[dict[str, Any] | None, str]:
    if not isinstance(payload, dict):
        return None, "Invalid schema: top-level JSON object required"
    source = payload.get("source")
    runtime = payload.get("runtime")
    if not isinstance(source, dict):
        return None, "Invalid schema: 'source' object is required"
    if not isinstance(runtime, dict):
        return None, "Invalid schema: 'runtime' object is required"

    name = _clean_text(source.get("name"))
    studio = _clean_text(source.get("studio"))
    pages = source.get("pages")
    if not name:
        return None, "Invalid schema: source.name is required"
    if not studio:
        return None, "Invalid schema: source.studio is required"
    if not isinstance(pages, list) or not pages:
        return None, "Invalid schema: source.pages must be a non-empty array"
    if not all(_clean_text(item) for item in pages):
        return None, "Invalid schema: source.pages entries must be non-empty strings"

    if "timeout_s" not in runtime:
        return None, "Invalid schema: runtime.timeout_s is required"
    if "retries" not in runtime:
        return None, "Invalid schema: runtime.retries is required"
    if "backoff_s" not in runtime:
        return None, "Invalid schema: runtime.backoff_s is required"

    use_browser = bool(runtime.get("use_browser", False))

    timeout_s = _to_int(runtime.get("timeout_s"), -1)
    retries = _to_int(runtime.get("retries"), -1)
    backoff_s = _to_float(runtime.get("backoff_s"), -1.0)
    download_delay = _to_float(runtime.get("download_delay"), 1.0)

    if timeout_s <= 0:
        return None, "Invalid schema: runtime.timeout_s must be > 0"
    if retries < 0:
        return None, "Invalid schema: runtime.retries must be >= 0"
    if backoff_s < 0:
        return None, "Invalid schema: runtime.backoff_s must be >= 0"
    if download_delay < 0:
        return None, "Invalid schema: runtime.download_delay must be >= 0"

    return {
        "source": {
            "name": name,
            "studio": studio,
            "pages": [_clean_text(item) for item in pages if _clean_text(item)],
            "nlPriority": bool(source.get("nlPriority", False)),
        },
        "runtime": {
            "timeout_s": timeout_s,
            "retries": retries,
            "backoff_s": backoff_s,
            "download_delay": download_delay,
            "use_browser": use_browser,
        },
    }, ""


def _run_scrapy(validated: dict[str, Any]) -> dict[str, Any]:
    source = validated["source"]
    runtime = validated["runtime"]
    source_name = _clean_text(source.get("name")) or "scrapy_source"
    studio = _clean_text(source.get("studio")) or "unknown"
    pages = [str(page) for page in source.get("pages") or []]
    source_id_value = _source_id(source_name, studio, list(pages))
    domain_profile = domain_profiles.domain_profile_for_url(_clean_text(pages[0]) if pages else "")
    partial_errors: list[str] = []
    jobs: list[dict[str, Any]] = []
    seen_links: set[str] = set()
    reject_reasons: Counter[str] = Counter()
    extraction_stats: dict[str, int] = {
        "candidate_links_found": 0,
        "detail_pages_visited": 0,
        "jobs_emitted": 0,
        "jobs_rejected_validation": 0,
        "dead_listing_pages_rejected": 0,
    }
    dead_listing_page_examples: list[str] = []

    # Test-only deterministic path that avoids network and Scrapy runtime.
    if _clean_text(os.getenv("BALUFFO_SCRAPY_RUNNER_SELFTEST")) == "1":
        return {
            "ok": True,
            "jobs": [],
            "details": [
                {
                    "adapter": "scrapy_static",
                    "studio": studio,
                    "name": source_name,
                    "status": "ok",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": False,
                    "top_reject_reasons": [],
                    "deadListingPageCount": 0,
                    "deadListingPageExamples": [],
                    "sourceId": source_id_value,
                    "pages": list(pages),
                }
            ],
            "partialErrors": [],
            "stats": _stats_subset({"finish_reason": "selftest"}),
        }

    container = {
        "jobs": jobs,
        "seen_links": seen_links,
        "reject_reasons": reject_reasons,
        "extraction_stats": extraction_stats,
        "partial_errors": partial_errors,
        "dead_listing_page_examples": dead_listing_page_examples,
    }

    job_provider = _clean_text(domain_profile.get("job_provider"))
    if job_provider == "jobylon_v1":
        for page_url in pages:
            provider_jobs, provider_stats, provider_errors, provider_rejects = (
                extract_jobylon_v1_jobs(
                    source_name=source_name,
                    studio=studio,
                    page_url=_clean_text(page_url),
                    timeout_s=_to_int(runtime.get("timeout_s"), 20),
                )
            )
            partial_errors.extend(provider_errors)
            for key, value in provider_stats.items():
                if key in {"jobs_emitted", "jobs_rejected_validation"}:
                    continue
                extraction_stats[key] = int(extraction_stats.get(key, 0)) + _to_int(value)
            for reason, count in provider_rejects.items():
                reject_reasons[reason] += int(count)
            for job in provider_jobs:
                job_link = _clean_text(job.get("jobLink"))
                title = _clean_text(job.get("title"))
                company = _clean_text(job.get("company"))
                source_job_id = _clean_text(job.get("sourceJobId"))
                if not title or not company or not job_link:
                    extraction_stats["jobs_rejected_validation"] += 1
                    reject_reasons["missing_required_fields"] += 1
                    continue
                if not source_job_id:
                    job["sourceJobId"] = _safe_id(f"{job_link}|{title}|{company}")
                if job_link in seen_links:
                    extraction_stats["jobs_rejected_validation"] += 1
                    reject_reasons["duplicate_job_link"] += 1
                    continue
                if not domain_profiles.is_probable_job_detail_url(job_link, domain_profile):
                    extraction_stats["jobs_rejected_validation"] += 1
                    reject_reasons["non_job_url"] += 1
                    continue
                seen_links.add(job_link)
                jobs.append(job)
                extraction_stats["jobs_emitted"] += 1

    try:
        from scrapy.crawler import CrawlerProcess
        from scrapy.settings import Settings
    except ImportError as exc:
        return _json_error_envelope(
            f"Scrapy import failed: {exc}", source_name=source_name, studio=studio
        )

    settings_dict: dict[str, Any] = dict(SCRAPY_SETTINGS_DEFAULTS)
    if runtime.get("download_delay") is not None:
        settings_dict["DOWNLOAD_DELAY"] = runtime.get("download_delay")
    if runtime.get("timeout_s") is not None:
        settings_dict["DOWNLOAD_TIMEOUT"] = runtime.get("timeout_s")
    if runtime.get("retries") is not None:
        settings_dict["RETRY_TIMES"] = runtime.get("retries")

    use_browser = bool(runtime.get("use_browser", False))
    if use_browser:
        if importlib.util.find_spec("scrapy_playwright") is not None:
            for setting_key, setting_value in SCRAPY_PLAYWRIGHT_SETTINGS.items():
                settings_dict[setting_key] = setting_value
        else:
            use_browser = False

    settings = Settings(settings_dict)
    crawler_process = CrawlerProcess(settings=settings)
    crawler = crawler_process.create_crawler(GenericCareersSpider)

    ok = True
    error_text = ""
    try:
        crawler_process.crawl(
            crawler,
            start_urls=list(pages),
            studio_name=studio,
            source_name_value=source_name,
            profile=domain_profile,
            container=container,
            use_browser=use_browser,
        )
        crawler_process.start(stop_after_crawl=True)
    except _EXPECTED_CRAWL_EXCEPTIONS as exc:
        ok = False
        error_text = f"{source_name}: crawl failed: {exc}"
        partial_errors.append(error_text)

    crawler_stats = crawler.stats.get_stats() if crawler.stats is not None else {}
    for key, value in extraction_stats.items():
        crawler_stats[key] = int(value)
    stats = _stats_subset(crawler_stats)
    fetched_count = _to_int(stats.get("downloader/response_count"))
    kept_count = len(jobs)
    classification = _classify_result(
        ok=ok,
        fetched_count=fetched_count,
        kept_count=kept_count,
        partial_errors=partial_errors,
        reject_reasons=reject_reasons,
    )
    top_reject_reasons = [f"{key}:{count}" for key, count in reject_reasons.most_common(5)]
    browser_fallback_recommended = classification in {
        "blocked_or_challenge",
    }

    details = [
        {
            "adapter": "scrapy_static",
            "studio": studio,
            "name": source_name,
            "status": "ok" if ok else "error",
            "fetchedCount": fetched_count,
            "keptCount": kept_count,
            "error": error_text,
            "classification": classification,
            "browserFallbackRecommended": browser_fallback_recommended,
            "top_reject_reasons": top_reject_reasons,
            "deadListingPageCount": int(reject_reasons.get("dead_listing_page") or 0),
            "deadListingPageExamples": list(dead_listing_page_examples[:5]),
            "sourceId": source_id_value,
            "pages": list(pages),
        }
    ]
    return {
        "ok": ok,
        "jobs": jobs,
        "details": details,
        "partialErrors": partial_errors,
        "stats": stats,
    }


def main() -> int:
    source_name = "unknown"
    studio = "unknown"
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        _emit_envelope(
            _json_error_envelope(
                f"Failed to parse stdin JSON: {exc}", source_name=source_name, studio=studio
            )
        )
        return 1

    validated, error = _validate_input(payload)
    if not validated:
        if isinstance(payload, dict):
            source = payload.get("source")
            if isinstance(source, dict):
                source_name = _clean_text(source.get("name")) or source_name
                studio = _clean_text(source.get("studio")) or studio
        _emit_envelope(_json_error_envelope(error, source_name=source_name, studio=studio))
        return 1

    source_name = _clean_text(validated["source"].get("name")) or source_name
    studio = _clean_text(validated["source"].get("studio")) or studio
    envelope = _run_scrapy(validated)
    _emit_envelope(envelope)
    return 0 if bool(envelope.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
