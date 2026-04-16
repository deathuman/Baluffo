"""Generic careers spider: JSON-LD + listing/detail link following for career pages."""

from __future__ import annotations

import json
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

import scrapy

from src.jobs.page_gating import classify_job_page
from src.scrapers import domain_profiles
from src.scrapers.helpers import build_job, clean_text, safe_id, to_int
from src.scrapers.items import JobItem, item_to_job_dict
from src.shared.regex import find_urls_in_text


class GenericCareersSpider(scrapy.Spider):
    """Spider that writes into a passed-in container (jobs, seen_links, reject_reasons, etc.)."""

    name = "generic_careers"

    def start_requests(self) -> Any:
        for url in self.start_urls:
            if not url:
                continue
            req = scrapy.Request(url, callback=self.parse)
            if self._use_browser:
                req.meta["playwright"] = True
                req.meta["playwright_page_goto_kwargs"] = {"wait_until": "load", "timeout": 15000}
                profile = self.profile or {}
                wait_selector = profile.get("playwright_wait_selector")
                wait_timeout = int(profile.get("playwright_wait_timeout") or 8000)
                if wait_selector:
                    try:
                        from scrapy_playwright.page import PageMethod

                        req.meta["playwright_page_methods"] = [
                            PageMethod("wait_for_selector", wait_selector, timeout=wait_timeout),
                        ]
                    except ImportError:
                        pass
            yield req

    def __init__(
        self,
        *,
        start_urls: list[str],
        studio_name: str,
        source_name_value: str,
        profile: dict[str, Any],
        container: dict[str, Any],
        use_browser: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.start_urls = start_urls
        self.studio_name = studio_name
        self.source_name_value = source_name_value
        self.profile = profile or {}
        self._container = container
        self._detail_seen = set()
        self._use_browser = bool(use_browser)

    def parse(self, response: scrapy.http.Response) -> Any:
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
            self._container["extraction_stats"]["candidate_links_found"] += 1
            req = scrapy.Request(url=href, callback=self.parse_job_detail)
            if self._use_browser:
                req.meta["playwright"] = True
            yield req

    def parse_job_detail(self, response: scrapy.http.Response) -> Any:
        self._container["extraction_stats"]["detail_pages_visited"] += 1
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

        loaded = self._build_detail_item(response)
        title = clean_text(loaded.get("title"))
        if not title:
            self._container["reject_reasons"]["missing_title"] += 1
            return
        job_link = clean_text(response.url)
        job_like, gate_reason = classify_job_page(
            response.text or "",
            job_link,
            page_title=title,
            profile=self.profile,
        )
        if not job_like:
            reject_reason = "no_openings" if gate_reason == "no_openings" else "dead_listing_page"
            self._container["reject_reasons"][reject_reason] += 1
            self._container["extraction_stats"]["dead_listing_pages_rejected"] += 1
            if reject_reason == "dead_listing_page":
                examples = self._container.get("dead_listing_page_examples")
                if isinstance(examples, list) and len(examples) < 5:
                    examples.append(f"{job_link} | {title}")
            return
        if not domain_profiles.is_probable_job_detail_url(job_link, self.profile):
            self._container["reject_reasons"]["non_job_url"] += 1
            return
        job_dict = item_to_job_dict(
            loaded, source_name=self.source_name_value, studio=self.studio_name
        )
        job_dict["jobLink"] = job_link
        job_dict["sourceJobId"] = job_dict.get("sourceJobId") or safe_id(job_link)
        self._append_job(job_dict)

    def _flatten_jobposting_items(self, payload: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            if clean_text(payload.get("@type")) == "JobPosting":
                rows.append(payload)
            graph = payload.get("@graph")
            if isinstance(graph, list):
                for item in graph:
                    if isinstance(item, dict) and clean_text(item.get("@type")) == "JobPosting":
                        rows.append(item)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and clean_text(item.get("@type")) == "JobPosting":
                    rows.append(item)
        return rows

    def _jsonld_to_job(self, *, item: dict[str, Any], page_url: str) -> dict[str, Any] | None:
        org = item.get("hiringOrganization")
        org = org if isinstance(org, dict) else {}
        loc = item.get("jobLocation")
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        loc = loc if isinstance(loc, dict) else {}
        addr = loc.get("address")
        addr = addr if isinstance(addr, dict) else {}
        job_url = urljoin(page_url, clean_text(item.get("url")))
        if not job_url:
            job_url = page_url
        title = clean_text(item.get("title"))
        if not title:
            return None
        source_job_id = clean_text(
            (item.get("identifier") or {}).get("value")
            if isinstance(item.get("identifier"), dict)
            else ""
        )
        if not source_job_id:
            source_job_id = safe_id(job_url or title)
        return build_job(
            source_name=self.source_name_value,
            studio=self.studio_name,
            title=title,
            company=clean_text(org.get("name")) or self.studio_name,
            city=clean_text(addr.get("addressLocality")),
            country=clean_text(addr.get("addressCountry")) or "Unknown",
            work_type=clean_text(item.get("jobLocationType")),
            contract_type=clean_text(item.get("employmentType")),
            job_link=job_url,
            source_job_id=source_job_id,
            posted_at=clean_text(item.get("datePosted")),
        )

    def _build_detail_item(self, response: scrapy.http.Response) -> JobItem:
        selectors = ["h1::text", "[class*='title']::text"]
        profile_selectors = self.profile.get("title_selectors") if isinstance(self.profile, dict) else []
        if isinstance(profile_selectors, list):
            selectors.extend(clean_text(selector) for selector in profile_selectors if clean_text(selector))

        item = JobItem()
        title = self._first_css_text(response, selectors)
        if title:
            item["title"] = title
        item["jobLink"] = response.url
        item["company"] = self.studio_name
        item["source"] = self.source_name_value
        item["studio"] = self.studio_name
        item["adapter"] = "scrapy_static"
        item["sourceBundle"] = []
        return item

    def _first_css_text(self, response: scrapy.http.Response, selectors: list[str]) -> str:
        for selector in selectors:
            for value in response.css(selector).getall():
                candidate = clean_text(value)
                if candidate:
                    return candidate
        return ""

    def _extract_job_links(self, response: scrapy.http.Response) -> list[str]:
        patterns = [
            'a[href*="/job"]::attr(href)',
            'a[href*="/jobs/"]::attr(href)',
            'a[href*="/careers"]::attr(href)',
            '[class*="job-listing"] a::attr(href)',
        ]
        links = set()
        for pattern in patterns:
            for href in response.css(pattern).getall():
                absolute = urljoin(response.url, clean_text(href))
                if not absolute:
                    continue
                if urlparse(absolute).netloc != urlparse(response.url).netloc:
                    continue
                if not domain_profiles.is_probable_job_detail_url(absolute, self.profile):
                    continue
                links.add(absolute)
        for raw in find_urls_in_text(response.text or ""):
            absolute = clean_text(raw)
            if urlparse(absolute).netloc != urlparse(response.url).netloc:
                continue
            if not domain_profiles.is_probable_job_detail_url(absolute, self.profile):
                continue
            links.add(absolute)
        max_detail_links = to_int(self.profile.get("max_detail_links"), 60)
        return sorted(links)[: max(1, max_detail_links)]

    def _append_job(self, job: dict[str, Any]) -> None:
        job_link = clean_text(job.get("jobLink"))
        title = clean_text(job.get("title"))
        company = clean_text(job.get("company"))
        source_job_id = clean_text(job.get("sourceJobId"))
        if not title or not company or not job_link:
            self._container["extraction_stats"]["jobs_rejected_validation"] += 1
            self._container["reject_reasons"]["missing_required_fields"] += 1
            self._container["partial_errors"].append(
                f"{self.source_name_value}: dropped incomplete job payload"
            )
            return
        if not source_job_id:
            job["sourceJobId"] = safe_id(f"{job_link}|{title}|{company}")
        if job_link in self._container["seen_links"]:
            self._container["reject_reasons"]["duplicate_job_link"] += 1
            return
        if not domain_profiles.is_probable_job_detail_url(job_link, self.profile):
            self._container["extraction_stats"]["jobs_rejected_validation"] += 1
            self._container["reject_reasons"]["non_job_url"] += 1
            return
        self._container["seen_links"].add(job_link)
        self._container["jobs"].append(job)
        self._container["extraction_stats"]["jobs_emitted"] += 1
