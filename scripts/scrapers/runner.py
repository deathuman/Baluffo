#!/usr/bin/env python3
"""Scrapy subprocess runner for Baluffo static HTML crawling."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from html import unescape
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _stats_subset(stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "downloader/request_count": _to_int(stats.get("downloader/request_count")),
        "downloader/response_count": _to_int(stats.get("downloader/response_count")),
        "downloader/response_status_count/200": _to_int(stats.get("downloader/response_status_count/200")),
        "retry/count": _to_int(stats.get("retry/count")),
        "item_scraped_count": _to_int(stats.get("item_scraped_count")),
        "finish_reason": _clean_text(stats.get("finish_reason")),
    }


def _json_error_envelope(error: str, *, source_name: str, studio: str) -> Dict[str, Any]:
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
            }
        ],
        "partialErrors": [_clean_text(error)],
        "stats": _stats_subset({}),
    }


def _emit_envelope(envelope: Dict[str, Any]) -> None:
    print(json.dumps(envelope, ensure_ascii=False), flush=True)


def _validate_input(payload: Any) -> Tuple[Dict[str, Any] | None, str]:
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
            "remoteFriendly": bool(source.get("remoteFriendly", True)),
        },
        "runtime": {
            "timeout_s": timeout_s,
            "retries": retries,
            "backoff_s": backoff_s,
            "download_delay": download_delay,
        },
    }, ""


def _build_job(
    *,
    source_name: str,
    studio: str,
    title: str,
    company: str,
    job_link: str,
    source_job_id: str,
    city: str = "",
    country: str = "Unknown",
    work_type: str = "",
    contract_type: str = "",
    posted_at: str = "",
) -> Dict[str, Any]:
    return {
        "sourceJobId": source_job_id,
        "title": title,
        "company": company,
        "city": city,
        "country": country or "Unknown",
        "workType": work_type,
        "contractType": contract_type,
        "jobLink": job_link,
        "sector": "Game",
        "postedAt": posted_at,
        "source": source_name,
        "studio": studio,
        "adapter": "scrapy_static",
        "sourceBundle": [
            {
                "source": source_name,
                "sourceJobId": source_job_id,
                "jobLink": job_link,
                "postedAt": posted_at,
                "adapter": "scrapy_static",
                "studio": studio,
            }
        ],
    }


def _safe_id(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


def _run_scrapy(validated: Dict[str, Any]) -> Dict[str, Any]:
    source = validated["source"]
    runtime = validated["runtime"]
    source_name = _clean_text(source.get("name")) or "scrapy_source"
    studio = _clean_text(source.get("studio")) or "unknown"
    pages = source.get("pages") or []
    partial_errors: List[str] = []
    jobs: List[Dict[str, Any]] = []
    seen_links = set()

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
                }
            ],
            "partialErrors": [],
            "stats": _stats_subset({"finish_reason": "selftest"}),
        }

    try:
        import scrapy
        from scrapy.crawler import CrawlerProcess
        from scrapy.settings import Settings
    except Exception as exc:  # noqa: BLE001
        return _json_error_envelope(f"Scrapy import failed: {exc}", source_name=source_name, studio=studio)

    class GenericCareersSpider(scrapy.Spider):
        name = "generic_careers"

        def __init__(self, *, start_urls: List[str], studio_name: str, source_name_value: str, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.start_urls = start_urls
            self.studio_name = studio_name
            self.source_name_value = source_name_value
            self._detail_seen = set()

        def parse(self, response: scrapy.http.Response):  # type: ignore[name-defined]
            for script in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    payload = json.loads(unescape(script))
                except json.JSONDecodeError:
                    continue
                for item in self._flatten_jobposting_items(payload):
                    job = self._jsonld_to_job(item=item, page_url=response.url)
                    if job:
                        self._append_job(job)

            for href in self._extract_job_links(response):
                if href in self._detail_seen:
                    continue
                self._detail_seen.add(href)
                yield scrapy.Request(url=href, callback=self.parse_job_detail)

        def parse_job_detail(self, response: scrapy.http.Response):  # type: ignore[name-defined]
            for script in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    payload = json.loads(unescape(script))
                except json.JSONDecodeError:
                    continue
                for item in self._flatten_jobposting_items(payload):
                    job = self._jsonld_to_job(item=item, page_url=response.url)
                    if job:
                        self._append_job(job)
                        return

            raw_title = response.css("h1::text, [class*='title']::text").get("")
            title = _clean_text(raw_title)
            if not title:
                return
            job_link = _clean_text(response.url)
            if not self._is_probable_job_detail_url(job_link):
                return
            self._append_job(
                _build_job(
                    source_name=self.source_name_value,
                    studio=self.studio_name,
                    title=title,
                    company=self.studio_name,
                    job_link=job_link,
                    source_job_id=_safe_id(job_link),
                )
            )

        def _flatten_jobposting_items(self, payload: Any) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            if isinstance(payload, dict):
                if _clean_text(payload.get("@type")) == "JobPosting":
                    rows.append(payload)
                graph = payload.get("@graph")
                if isinstance(graph, list):
                    for item in graph:
                        if isinstance(item, dict) and _clean_text(item.get("@type")) == "JobPosting":
                            rows.append(item)
            elif isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict) and _clean_text(item.get("@type")) == "JobPosting":
                        rows.append(item)
            return rows

        def _jsonld_to_job(self, *, item: Dict[str, Any], page_url: str) -> Dict[str, Any] | None:
            org = item.get("hiringOrganization")
            org = org if isinstance(org, dict) else {}
            loc = item.get("jobLocation")
            if isinstance(loc, list):
                loc = loc[0] if loc else {}
            loc = loc if isinstance(loc, dict) else {}
            addr = loc.get("address")
            addr = addr if isinstance(addr, dict) else {}
            job_url = urljoin(page_url, _clean_text(item.get("url")))
            if not job_url:
                job_url = page_url
            title = _clean_text(item.get("title"))
            if not title:
                return None
            source_job_id = _clean_text((item.get("identifier") or {}).get("value") if isinstance(item.get("identifier"), dict) else "")
            if not source_job_id:
                source_job_id = _safe_id(job_url or title)
            return _build_job(
                source_name=self.source_name_value,
                studio=self.studio_name,
                title=title,
                company=_clean_text(org.get("name")) or self.studio_name,
                city=_clean_text(addr.get("addressLocality")),
                country=_clean_text(addr.get("addressCountry")) or "Unknown",
                work_type=_clean_text(item.get("jobLocationType")),
                contract_type=_clean_text(item.get("employmentType")),
                job_link=job_url,
                source_job_id=source_job_id,
                posted_at=_clean_text(item.get("datePosted")),
            )

        def _extract_job_links(self, response: scrapy.http.Response) -> List[str]:  # type: ignore[name-defined]
            patterns = [
                'a[href*="/job"]::attr(href)',
                'a[href*="/jobs/"]::attr(href)',
                '[class*="job-listing"] a::attr(href)',
            ]
            links = set()
            for pattern in patterns:
                for href in response.css(pattern).getall():
                    absolute = urljoin(response.url, _clean_text(href))
                    if not absolute:
                        continue
                    if urlparse(absolute).netloc != urlparse(response.url).netloc:
                        continue
                    if not self._is_probable_job_detail_url(absolute):
                        continue
                    links.add(absolute)
            return sorted(links)

        def _is_probable_job_detail_url(self, url: str) -> bool:
            parsed = urlparse(url)
            path = _clean_text(parsed.path).lower()
            query = _clean_text(parsed.query).lower()
            if not path:
                return False
            if "/jobs/" in path or "/job/" in path or "/jobdetail/" in path:
                return True
            if "job_id=" in query:
                return True
            # Many sites use UUIDs for job detail pages under generic careers paths.
            if "/careers/" in path and re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}$", path):
                return True
            # Filter known non-job taxonomy/listing pages.
            if "/careers/location/" in path or "/careers/locations/" in path:
                return False
            if "location=" in query:
                return False
            return False

        def _append_job(self, job: Dict[str, Any]) -> None:
            job_link = _clean_text(job.get("jobLink"))
            title = _clean_text(job.get("title"))
            company = _clean_text(job.get("company"))
            source_job_id = _clean_text(job.get("sourceJobId"))
            if not title or not company or not job_link:
                partial_errors.append(f"{self.source_name_value}: dropped incomplete job payload")
                return
            if not source_job_id:
                job["sourceJobId"] = _safe_id(f"{job_link}|{title}|{company}")
            if job_link in seen_links:
                return
            seen_links.add(job_link)
            jobs.append(job)

    settings = Settings(
        {
            "ROBOTSTXT_OBEY": True,
            "DOWNLOAD_DELAY": runtime.get("download_delay", 1.0),
            "DOWNLOAD_TIMEOUT": runtime.get("timeout_s"),
            "RETRY_TIMES": runtime.get("retries"),
            "LOG_LEVEL": "WARNING",
            "TELNETCONSOLE_ENABLED": False,
            "WEBSOCKETS_ENABLED": False,
            "REACTOR_THREADPOOL_MAXSIZE": 10,
        }
    )

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
        )
        crawler_process.start(stop_after_crawl=True)
    except Exception as exc:  # noqa: BLE001
        ok = False
        error_text = f"{source_name}: crawl failed: {exc}"
        partial_errors.append(error_text)

    stats = _stats_subset(crawler.stats.get_stats() if getattr(crawler, "stats", None) else {})
    fetched_count = _to_int(stats.get("downloader/response_count"))
    kept_count = len(jobs)

    details = [
        {
            "adapter": "scrapy_static",
            "studio": studio,
            "name": source_name,
            "status": "ok" if ok else "error",
            "fetchedCount": fetched_count,
            "keptCount": kept_count,
            "error": error_text,
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
    except Exception as exc:  # noqa: BLE001
        _emit_envelope(_json_error_envelope(f"Failed to parse stdin JSON: {exc}", source_name=source_name, studio=studio))
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
