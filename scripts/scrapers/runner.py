#!/usr/bin/env python3
"""Scrapy subprocess runner for Baluffo static HTML crawling."""

from __future__ import annotations

from collections import Counter
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


DOMAIN_PROFILES: Dict[str, Dict[str, Any]] = {
    "www.valvesoftware.com": {
        "include_query_keys": ["job_id"],
        "exclude_path_tokens": ["/faq", "/team", "/about"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 80,
    },
    "www.riotgames.com": {
        "include_path_tokens": ["/jobs", "/job"],
        "exclude_path_tokens": ["/internships", "/events", "/news", "/esports"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 50,
    },
    "cdprojektred.com": {
        "include_path_tokens": ["/jobs", "/careers"],
        "exclude_path_tokens": ["/news", "/about"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "supercell.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/blog", "/news"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 50,
    },
    "larian.com": {
        "include_path_tokens": ["/careers/"],
        "exclude_path_tokens": ["/careers/location/"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 40,
    },
    "www.remedygames.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/news", "/blog"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 40,
    },
    "www.ubisoft.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/locations", "/teams"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "www.epicgames.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/newsroom", "/store", "/site/en-us/home"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
}


def _domain_profile_for_url(url: str) -> Dict[str, Any]:
    host = _clean_text(urlparse(url).netloc).lower()
    return dict(DOMAIN_PROFILES.get(host) or {})


def _source_id(name: str, studio: str, pages: List[str]) -> str:
    seed = "|".join([_clean_text(name), _clean_text(studio), *[_clean_text(p) for p in pages]])
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _is_probable_job_detail_url(url: str, profile: Dict[str, Any]) -> bool:
    parsed = urlparse(url)
    path = _clean_text(parsed.path).lower()
    query = _clean_text(parsed.query).lower()
    if not path:
        return False
    exclude_path_tokens = [str(token).lower() for token in (profile.get("exclude_path_tokens") or [])]
    for token in exclude_path_tokens:
        if token and token in path:
            return False
    if "/jobs/" in path or "/job/" in path or "/jobdetail/" in path:
        return True
    if "job_id=" in query or "gh_jid=" in query or "lever-via=" in query:
        return True
    include_query_keys = [str(token).lower() for token in (profile.get("include_query_keys") or [])]
    for key in include_query_keys:
        if key and f"{key}=" in query:
            return True
    include_path_tokens = [str(token).lower() for token in (profile.get("include_path_tokens") or [])]
    for token in include_path_tokens:
        if token and token in path and (re.search(r"/[0-9]+", path) or re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}", path)):
            return True
    if "/careers/" in path and re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}$", path):
        return True
    if "/careers/location/" in path or "/careers/locations/" in path:
        return False
    if "location=" in query:
        return False
    return False


def _classify_result(*, ok: bool, fetched_count: int, kept_count: int, partial_errors: List[str]) -> str:
    if not ok:
        return "parse_error"
    if kept_count > 0:
        return "ok_with_jobs"
    if fetched_count <= 0:
        return "blocked_or_challenge"
    lower_errors = " ".join(item.lower() for item in partial_errors)
    if "captcha" in lower_errors or "cloudflare" in lower_errors or "challenge" in lower_errors or "403" in lower_errors:
        return "blocked_or_challenge"
    if fetched_count > 0 and kept_count == 0:
        return "fetch_ok_extract_zero"
    return "ok_no_jobs"


def _stats_subset(stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "downloader/request_count": _to_int(stats.get("downloader/request_count")),
        "downloader/response_count": _to_int(stats.get("downloader/response_count")),
        "downloader/response_status_count/200": _to_int(stats.get("downloader/response_status_count/200")),
        "retry/count": _to_int(stats.get("retry/count")),
        "item_scraped_count": _to_int(stats.get("item_scraped_count")),
        "finish_reason": _clean_text(stats.get("finish_reason")),
        "candidate_links_found": _to_int(stats.get("candidate_links_found")),
        "detail_pages_visited": _to_int(stats.get("detail_pages_visited")),
        "jobs_emitted": _to_int(stats.get("jobs_emitted")),
        "jobs_rejected_validation": _to_int(stats.get("jobs_rejected_validation")),
    }


def _json_error_envelope(error: str, *, source_name: str, studio: str) -> Dict[str, Any]:
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
    source_id_value = _source_id(source_name, studio, list(pages))
    domain_profile = _domain_profile_for_url(_clean_text(pages[0]) if pages else "")
    partial_errors: List[str] = []
    jobs: List[Dict[str, Any]] = []
    seen_links = set()
    reject_reasons: Counter[str] = Counter()
    extraction_stats: Dict[str, int] = {
        "candidate_links_found": 0,
        "detail_pages_visited": 0,
        "jobs_emitted": 0,
        "jobs_rejected_validation": 0,
    }

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
                    "classification": "ok_no_jobs",
                    "browserFallbackRecommended": False,
                    "top_reject_reasons": [],
                    "sourceId": source_id_value,
                    "pages": list(pages),
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

        def __init__(
            self,
            *,
            start_urls: List[str],
            studio_name: str,
            source_name_value: str,
            profile: Dict[str, Any],
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self.start_urls = start_urls
            self.studio_name = studio_name
            self.source_name_value = source_name_value
            self.profile = profile or {}
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
                extraction_stats["candidate_links_found"] += 1
                yield scrapy.Request(url=href, callback=self.parse_job_detail)

        def parse_job_detail(self, response: scrapy.http.Response):  # type: ignore[name-defined]
            extraction_stats["detail_pages_visited"] += 1
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
            if not _clean_text(raw_title):
                selectors = self.profile.get("title_selectors") if isinstance(self.profile, dict) else []
                if isinstance(selectors, list):
                    for selector in selectors:
                        raw_title = response.css(_clean_text(selector)).get("")
                        if _clean_text(raw_title):
                            break
            title = _clean_text(raw_title)
            if not title:
                reject_reasons["missing_title"] += 1
                return
            job_link = _clean_text(response.url)
            if not _is_probable_job_detail_url(job_link, self.profile):
                reject_reasons["non_job_url"] += 1
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
                'a[href*="/careers"]::attr(href)',
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
                    if not _is_probable_job_detail_url(absolute, self.profile):
                        continue
                    links.add(absolute)
            # Also mine raw URLs in scripts/html for query-based job links.
            for raw in re.findall(r'https?://[^\s"\'<>]+', response.text or "", flags=re.I):
                absolute = _clean_text(raw)
                if urlparse(absolute).netloc != urlparse(response.url).netloc:
                    continue
                if not _is_probable_job_detail_url(absolute, self.profile):
                    continue
                links.add(absolute)
            max_detail_links = _to_int(self.profile.get("max_detail_links"), 60)
            return sorted(links)[: max(1, max_detail_links)]

        def _append_job(self, job: Dict[str, Any]) -> None:
            job_link = _clean_text(job.get("jobLink"))
            title = _clean_text(job.get("title"))
            company = _clean_text(job.get("company"))
            source_job_id = _clean_text(job.get("sourceJobId"))
            if not title or not company or not job_link:
                extraction_stats["jobs_rejected_validation"] += 1
                reject_reasons["missing_required_fields"] += 1
                partial_errors.append(f"{self.source_name_value}: dropped incomplete job payload")
                return
            if not source_job_id:
                job["sourceJobId"] = _safe_id(f"{job_link}|{title}|{company}")
            if job_link in seen_links:
                reject_reasons["duplicate_job_link"] += 1
                return
            if not _is_probable_job_detail_url(job_link, self.profile):
                extraction_stats["jobs_rejected_validation"] += 1
                reject_reasons["non_job_url"] += 1
                return
            seen_links.add(job_link)
            jobs.append(job)
            extraction_stats["jobs_emitted"] += 1

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
            "DEPTH_LIMIT": 1,
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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
            profile=domain_profile,
        )
        crawler_process.start(stop_after_crawl=True)
    except Exception as exc:  # noqa: BLE001
        ok = False
        error_text = f"{source_name}: crawl failed: {exc}"
        partial_errors.append(error_text)

    crawler_stats = crawler.stats.get_stats() if getattr(crawler, "stats", None) else {}
    for key, value in extraction_stats.items():
        crawler_stats[key] = int(value)
    stats = _stats_subset(crawler_stats)
    fetched_count = _to_int(stats.get("downloader/response_count"))
    kept_count = len(jobs)
    classification = _classify_result(ok=ok, fetched_count=fetched_count, kept_count=kept_count, partial_errors=partial_errors)
    top_reject_reasons = [f"{key}:{count}" for key, count in reject_reasons.most_common(5)]
    browser_fallback_recommended = classification in {"fetch_ok_extract_zero", "blocked_or_challenge"}

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
