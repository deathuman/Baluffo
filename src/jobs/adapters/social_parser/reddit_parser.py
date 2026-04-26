from __future__ import annotations

"""Reddit JSON, HTML, and RSS parser leaves."""

import hashlib
import re
from html import unescape
from typing import Any
from xml.etree import ElementTree as ET

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.models import RawJob
from src.jobs.text_utils import normalize_url

from .signals import (
    _clean_text,
    _increment_reason,
    _norm_text,
    social_evaluate_post,
    social_extract_apply_url,
    social_infer_company,
    social_is_content_only_url,
    social_should_reject_non_job_reddit_post,
)


def parse_reddit_json_payload(
    payload: Any,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        children = (
            ((payload.get("data") or {}).get("children"))
            if isinstance(payload.get("data"), dict)
            else []
        )
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict) and isinstance(child.get("data"), dict):
                    rows.append(child["data"])
    out: list[RawJob] = []
    low_conf_count = 0
    for item in rows:
        title = _clean_text(item.get("title"))
        body = _clean_text(item.get("selftext"))
        flair = _clean_text(item.get("link_flair_text"))
        post_id = _clean_text(item.get("id"))
        permalink = (
            normalize_url(f"https://www.reddit.com{_clean_text(item.get('permalink'))}")
            if _clean_text(item.get("permalink"))
            else ""
        )
        external_url = normalize_url(item.get("url"))
        apply_url = social_extract_apply_url(body, external_url)
        keep, confidence, reject_reason = social_evaluate_post(
            title=title,
            text=f"{body} {flair}",
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            if reject_reason in {
                "missing_apply_url",
                "missing_valid_apply_url",
                "social_repost_or_commentary",
            } and social_is_content_only_url(external_url):
                reject_reason = "non_job_destination_url"
            _increment_reason(reject_reasons, reject_reason)
            continue
        job_link = apply_url or permalink or external_url
        if not title or not job_link:
            continue
        fallback_company = _clean_text(item.get("author"))
        company = social_infer_company(title, body, fallback=fallback_company)
        reject_reason = social_should_reject_non_job_reddit_post(
            title=title,
            text=f"{body} {flair}",
            apply_url=job_link,
            company=company,
            fallback_company=fallback_company,
        )
        if reject_reason:
            low_conf_count += 1
            _increment_reason(reject_reasons, reject_reason)
            continue
        post_source_id = f"reddit:{_clean_text(subreddit)}:{post_id or hashlib.sha1(job_link.encode('utf-8')).hexdigest()[:12]}"
        out.append(
            {
                "sourceJobId": post_source_id,
                "title": title,
                "company": company,
                "city": "Remote" if "remote" in _norm_text(f"{title} {body}") else "",
                "country": "Remote" if "remote" in _norm_text(f"{title} {body}") else "Unknown",
                "workType": "Remote" if "remote" in _norm_text(f"{title} {body}") else "",
                "contractType": _clean_text(flair),
                "jobLink": job_link,
                "sector": "Game",
                "postedAt": item.get("created_utc"),
                "adapter": "social",
                "studio": f"reddit/{_clean_text(subreddit)}",
                "sourceBundle": [
                    {
                        "source": "social_reddit",
                        "sourceJobId": post_source_id,
                        "jobLink": permalink or job_link,
                        "postedAt": item.get("created_utc"),
                        "adapter": "social",
                        "studio": _clean_text(subreddit),
                    }
                ],
            }
        )
    return out, low_conf_count


def parse_reddit_html_payload(
    html_text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    """Parse Reddit HTML content for job posts when JSON and RSS fail."""
    out: list[RawJob] = []
    low_conf_count = 0

    try:
        block_pattern = re.compile(r"(?is)<(?:article|div)\b[^>]*>(.*?)</(?:article|div)>")
        anchor_pattern = re.compile(r"(?is)<a\b[^>]*href\s*=\s*(['\"])(.*?)\1[^>]*>(.*?)</a>")
        title_pattern = re.compile(
            r"(?is)<(?:h1|h2|h3|h4|h5|h6)\b[^>]*>(.*?)</(?:h1|h2|h3|h4|h5|h6)>"
        )

        post_containers = [
            match.group(1) or "" for match in block_pattern.finditer(html_text or "")
        ]
        if not post_containers:
            post_containers = [html_text or ""]

        for container in post_containers:
            title_match = title_pattern.search(container)
            if title_match:
                title = _clean_text(strip_html_text(title_match.group(1)))
            else:
                first_anchor = anchor_pattern.search(container)
                title = _clean_text(strip_html_text(first_anchor.group(3))) if first_anchor else ""
            if not title:
                continue

            link = ""
            for anchor_match in anchor_pattern.finditer(container):
                href = _clean_text(anchor_match.group(2))
                if href and (href.startswith("http") or href.startswith("/")):
                    link = href if href.startswith("http") else f"https://www.reddit.com{href}"
                    break

            posted_match = re.search(r"(?is)<time\b[^>]*>(.*?)</time>", container)
            posted_at = _clean_text(strip_html_text(posted_match.group(1))) if posted_match else ""

            apply_url = social_extract_apply_url(container, link)
            keep, confidence, reject_reason = social_evaluate_post(
                title=title,
                text=strip_html_text(container),
                min_confidence=min_confidence,
                reject_for_hire_posts=reject_for_hire_posts,
                has_apply_url=bool(apply_url),
            )
            if not keep:
                low_conf_count += 1
                if reject_reason in {
                    "missing_apply_url",
                    "missing_valid_apply_url",
                    "social_repost_or_commentary",
                } and social_is_content_only_url(link):
                    reject_reason = "non_job_destination_url"
                _increment_reason(reject_reasons, reject_reason)
                continue

            fallback_company = link
            company = social_infer_company(
                title, strip_html_text(container), fallback=fallback_company
            )
            reject_reason = social_should_reject_non_job_reddit_post(
                title=title,
                text=strip_html_text(container),
                apply_url=apply_url or link,
                company=company,
                fallback_company=fallback_company,
            )
            if reject_reason:
                low_conf_count += 1
                _increment_reason(reject_reasons, reject_reason)
                continue

            job_entry = {
                "title": title,
                "company": company,
                "jobLink": apply_url or link,
                "source": "social_reddit",
                "sourceJobId": f"html:{subreddit}:{hash(title)}",
                "postedAt": posted_at,
                "adapter": "social",
                "studio": subreddit,
                "sector": "Game",
            }
            out.append(job_entry)

    except Exception:
        pass

    return out, low_conf_count


def parse_reddit_rss_payload(
    rss_text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    try:
        root = ET.fromstring(_clean_text(rss_text).lstrip())
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: list[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = _clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(unescape(_clean_text(item.findtext("description"))))
        apply_url = social_extract_apply_url(description, link)
        keep, confidence, reject_reason = social_evaluate_post(
            title=title,
            text=description,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            if reject_reason in {
                "missing_apply_url",
                "missing_valid_apply_url",
                "social_repost_or_commentary",
            } and social_is_content_only_url(link):
                reject_reason = "non_job_destination_url"
            _increment_reason(reject_reasons, reject_reason)
            continue
        if not title or not link:
            continue
        fallback_company = _clean_text(subreddit)
        company = social_infer_company(title, description, fallback=fallback_company)
        reject_reason = social_should_reject_non_job_reddit_post(
            title=title,
            text=description,
            apply_url=apply_url or link,
            company=company,
            fallback_company=fallback_company,
        )
        if reject_reason:
            low_conf_count += 1
            _increment_reason(reject_reasons, reject_reason)
            continue
        post_source_id = (
            f"reddit:{_clean_text(subreddit)}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:12]}"
        )
        out.append(
            {
                "sourceJobId": post_source_id,
                "title": title,
                "company": company,
                "city": "Remote" if "remote" in _norm_text(f"{title} {description}") else "",
                "country": "Remote"
                if "remote" in _norm_text(f"{title} {description}")
                else "Unknown",
                "workType": "Remote" if "remote" in _norm_text(f"{title} {description}") else "",
                "contractType": "Unknown",
                "jobLink": apply_url or link,
                "sector": "Game",
                "postedAt": _clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": f"reddit/{_clean_text(subreddit)}",
                "sourceBundle": [
                    {
                        "source": "social_reddit",
                        "sourceJobId": post_source_id,
                        "jobLink": link,
                        "postedAt": _clean_text(item.findtext("pubDate")),
                        "adapter": "social",
                        "studio": _clean_text(subreddit),
                    }
                ],
            }
        )
    return out, low_conf_count
