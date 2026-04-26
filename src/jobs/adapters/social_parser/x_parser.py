from __future__ import annotations

"""X/Twitter JSON and RSS parser leaves."""

import hashlib
import re
from html import unescape
from typing import Any
from xml.etree import ElementTree as ET

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.models import RawJob
from src.jobs.text_utils import normalize_url

from .signals import (
    _as_dict,
    _as_list,
    _clean_text,
    _increment_reason,
    _norm_text,
    social_evaluate_post,
    social_extract_apply_url,
    social_infer_company,
)


def parse_x_payload(
    payload: Any,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    payload_dict = _as_dict(payload)
    rows = _as_list(payload_dict.get("data"))
    out: list[RawJob] = []
    low_conf_count = 0
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        text = _clean_text(row.get("text"))
        post_id = _clean_text(row.get("id"))
        entities = _as_dict(row.get("entities"))
        entity_urls = _as_list(entities.get("urls"))
        expanded_urls = [
            _clean_text(_as_dict(item).get("expanded_url"))
            for item in entity_urls
            if isinstance(item, dict)
        ]
        apply_url = social_extract_apply_url(text, " ".join(expanded_urls))
        keep, confidence, reject_reason = social_evaluate_post(
            title=text,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            _increment_reason(reject_reasons, reject_reason)
            continue
        permalink = normalize_url(f"https://x.com/i/web/status/{post_id}") if post_id else ""
        company = social_infer_company(text, fallback="Unknown Studio")
        post_source_id = f"x:{post_id or hashlib.sha1(text.encode('utf-8')).hexdigest()[:12]}"
        out.append(
            {
                "sourceJobId": post_source_id,
                "title": _clean_text(text[:180]),
                "company": company,
                "city": "Remote" if "remote" in _norm_text(text) else "",
                "country": "Remote" if "remote" in _norm_text(text) else "Unknown",
                "workType": "Remote" if "remote" in _norm_text(text) else "",
                "contractType": _clean_text(query_label),
                "jobLink": apply_url or permalink,
                "sector": "Game",
                "postedAt": _clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": "x",
                "sourceBundle": [
                    {
                        "source": "social_x",
                        "sourceJobId": post_source_id,
                        "jobLink": permalink or apply_url,
                        "postedAt": _clean_text(row.get("created_at")),
                        "adapter": "social",
                        "studio": "x",
                    }
                ],
            }
        )
    return out, low_conf_count


def parse_x_rss_payload(
    rss_text: str,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    raw_text = _clean_text(rss_text).lstrip()
    safe_text = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)", "&amp;", raw_text)
    try:
        root = ET.fromstring(safe_text)
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: list[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = _clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(unescape(_clean_text(item.findtext("description"))))
        banner_text = _norm_text(f"{title} {description}")
        if "not yet whitelisted" in banner_text or "rss reader" in banner_text:
            low_conf_count += 1
            _increment_reason(reject_reasons, "rss_banner_or_whitelist")
            continue
        text = f"{title} {description}"
        apply_url = social_extract_apply_url(text, link)
        keep, confidence, reject_reason = social_evaluate_post(
            title=title,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            _increment_reason(reject_reasons, reject_reason)
            continue
        if not title or not link:
            continue
        post_id = hashlib.sha1(link.encode("utf-8")).hexdigest()[:12]
        company = social_infer_company(title, description, fallback="Unknown Studio")
        source_job_id = f"x:{post_id}"
        out.append(
            {
                "sourceJobId": source_job_id,
                "title": _clean_text(title[:180]),
                "company": company,
                "city": "Remote" if "remote" in _norm_text(text) else "",
                "country": "Remote" if "remote" in _norm_text(text) else "Unknown",
                "workType": "Remote" if "remote" in _norm_text(text) else "",
                "contractType": _clean_text(query_label),
                "jobLink": apply_url or link,
                "sector": "Game",
                "postedAt": _clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": "x",
                "sourceBundle": [
                    {
                        "source": "social_x",
                        "sourceJobId": source_job_id,
                        "jobLink": link,
                        "postedAt": _clean_text(item.findtext("pubDate")),
                        "adapter": "social",
                        "studio": "x",
                    }
                ],
            }
        )
    return out, low_conf_count
