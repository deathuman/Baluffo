from __future__ import annotations

"""Mastodon parser leaf."""

import hashlib
from html import unescape
from typing import Any
from urllib.parse import urlparse

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


def parse_mastodon_payload(
    payload: Any,
    *,
    instance: str,
    tag: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reasons: dict[str, int] | None = None,
) -> tuple[list[RawJob], int]:
    rows = _as_list(payload)
    out: list[RawJob] = []
    low_conf_count = 0
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        html_text = _clean_text(row.get("content"))
        text = strip_html_text(unescape(html_text))
        post_url = normalize_url(row.get("url"))
        card = _as_dict(row.get("card"))
        apply_url = social_extract_apply_url(text, _clean_text(card.get("url")))
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
        post_id = _clean_text(row.get("id"))
        account = _as_dict(row.get("account"))
        account_name = _clean_text(account.get("display_name") or account.get("acct"))
        company = social_infer_company(text, fallback=account_name)
        post_source_id = f"mastodon:{_clean_text(urlparse(instance).netloc)}:{post_id or hashlib.sha1((post_url or text).encode('utf-8')).hexdigest()[:12]}"
        out.append(
            {
                "sourceJobId": post_source_id,
                "title": _clean_text(text[:180]),
                "company": company,
                "city": "Remote" if "remote" in _norm_text(text) else "",
                "country": "Remote" if "remote" in _norm_text(text) else "Unknown",
                "workType": "Remote" if "remote" in _norm_text(text) else "",
                "contractType": _clean_text(tag),
                "jobLink": apply_url or post_url,
                "sector": "Game",
                "postedAt": _clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": f"mastodon/{_clean_text(urlparse(instance).netloc)}",
                "sourceBundle": [
                    {
                        "source": "social_mastodon",
                        "sourceJobId": post_source_id,
                        "jobLink": post_url or apply_url,
                        "postedAt": _clean_text(row.get("created_at")),
                        "adapter": "social",
                        "studio": _clean_text(urlparse(instance).netloc),
                    }
                ],
            }
        )
    return out, low_conf_count
