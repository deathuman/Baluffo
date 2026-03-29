from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    iter_anchor_fragments,
    iter_block_fragments,
    strip_html_text,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_IGNORED_TOKENS = frozenset(
    {
        "apply",
        "apply now",
        "details",
        "learn more",
        "read more",
        "view job",
        "view details",
        "more details",
        "open positions",
        "open positions ›",
        "open positions ->",
    }
)
_CTA_TOKENS = frozenset(
    {
        "apply",
        "apply now",
        "details",
        "learn more",
        "read more",
        "view job",
        "view details",
        "more details",
    }
)
_JOB_HINT_TOKENS = frozenset(
    {
        "job",
        "jobs",
        "career",
        "careers",
        "position",
        "positions",
        "opening",
        "openings",
        "role",
        "roles",
        "vacanc",
        "opportunity",
        "opportunities",
        "hiring",
    }
)
_LOCATION_HINTS = (
    ",",
    "remote",
    "hybrid",
    "onsite",
    "permanent",
    "temporary",
    "fixed-term",
    "full-time",
    "part-time",
    "contract",
)


def _normalize_title_candidate(text: str) -> str:
    candidate = clean_text(text)
    if not candidate:
        return ""
    lower = candidate.lower()
    if lower in _IGNORED_TOKENS:
        return ""
    if "placeholder" in lower:
        candidate = candidate.split("Placeholder", 1)[0].strip()
    candidate = re.sub(r"(?is)\b(?:apply now|learn more|details|read more|view job|view details)\b.*$", "", candidate)
    hiring_match = re.search(
        r"(?is)^(?:.*?\bis hiring (?:a|an)\s+)(.+?)(?:\s+(?:to|for|from|in|at|on)\b.*)?$",
        candidate,
    )
    if hiring_match:
        candidate = hiring_match.group(1)
    candidate = clean_text(candidate)
    return "" if candidate.lower() in _IGNORED_TOKENS else candidate


def _pick_title(block_html: str, anchor_body: str) -> str:
    anchor_title = _normalize_title_candidate(strip_html_text(anchor_body))
    if anchor_title:
        return anchor_title
    for match in re.finditer(r"(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>", block_html or ""):
        heading = _normalize_title_candidate(strip_html_text(match.group(1) or ""))
        if heading:
            return heading
    for line in html_fragment_lines(block_html):
        candidate = _normalize_title_candidate(line)
        if not candidate:
            continue
        lower = candidate.lower()
        if lower in _IGNORED_TOKENS:
            continue
        if any(hint in lower for hint in ("location", "term", "type", "contract", "department", "team")):
            continue
        return candidate
    return ""


def _pick_location_and_terms(block_html: str, title: str) -> tuple[str, str, str]:
    location = ""
    work_type = ""
    contract_type = ""
    for line in html_fragment_lines(block_html):
        candidate = clean_text(strip_html_text(line))
        if not candidate or candidate == title or candidate.lower() in _IGNORED_TOKENS:
            continue
        lower = candidate.lower()
        if not location and any(hint in lower for hint in _LOCATION_HINTS):
            location = candidate
            continue
        if not work_type and any(token in lower for token in ("full time", "part time", "remote", "hybrid")):
            work_type = candidate
            continue
        if not contract_type and any(
            token in lower for token in ("permanent", "contract", "temporary", "fixed term", "fixed-term")
        ):
            contract_type = candidate
            continue
    return location, work_type, contract_type


def _pick_job_anchor(
    anchors: list[dict[str, str]],
    *,
    href_tokens: tuple[str, ...],
    allow_any_anchor: bool,
) -> dict[str, str] | None:
    if not anchors:
        return None

    def _score(anchor: dict[str, str]) -> tuple[int, int]:
        href = clean_text(anchor.get("href"))
        text = clean_text(anchor.get("text"))
        haystack = f"{href} {text}".lower()
        if not href:
            return (-999, -999)
        if any(token in haystack for token in ("privacy", "terms", "cookie", "mailto:", "tel:")):
            return (-999, -999)
        score = 0
        if any(token in haystack for token in href_tokens):
            score += 10
        if any(token in haystack for token in _JOB_HINT_TOKENS):
            score += 5
        if any(token in haystack for token in _CTA_TOKENS):
            score += 3
        if allow_any_anchor:
            score += 1
        return (score, len(haystack))

    ranked = sorted(anchors, key=_score, reverse=True)
    best = ranked[0] if ranked else None
    if best and _score(best)[0] > 0:
        return best
    return best if allow_any_anchor else None


def extract_rendered_card_jobs(
    html: str,
    *,
    page_url: str,
    company: str,
    source_id: str,
    href_tokens: tuple[str, ...] = (),
    allow_any_anchor: bool = False,
    block_tags: tuple[str, ...] = ("tr", "td", "li", "article", "section", "div"),
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links: set[str] = set()
    for tag in block_tags:
        for block_html in iter_block_fragments(html or "", tag):
            block_text = clean_text(strip_html_text(block_html))
            if not block_text:
                continue
            lower = block_text.lower()
            if not allow_any_anchor and not any(token in lower for token in _JOB_HINT_TOKENS | _CTA_TOKENS):
                continue
            anchors = list(iter_anchor_fragments(block_html))
            if not anchors:
                continue
            anchor = _pick_job_anchor(anchors, href_tokens=href_tokens, allow_any_anchor=allow_any_anchor)
            if not anchor:
                continue
            href = clean_text(anchor.get("href"))
            if not href:
                continue
            link = normalize_url(urljoin(page_url, href))
            if not link or link in seen_links:
                continue
            title = _pick_title(block_html, anchor.get("body") or anchor.get("text") or "")
            if not title:
                continue
            location, work_type, contract_type = _pick_location_and_terms(block_html, title)
            seen_links.add(link)
            jobs.append(
                {
                    "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                    "title": title,
                    "company": company,
                    "city": location,
                    "country": "Unknown",
                    "workType": work_type,
                    "contractType": contract_type,
                    "jobLink": link,
                    "sector": "Game",
                    "postedAt": "",
                    "adapter": "static",
                    "studio": company,
                    "source": "",
                }
            )
    return jobs
