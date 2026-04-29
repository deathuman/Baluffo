from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import (
    extract_tag_texts,
    html_fragment_lines,
    iter_anchor_fragments,
    iter_block_fragments,
    strip_html_text,
)
from src.jobs.adapters.location_rules import (
    _WORK_TYPE_NOISE_TOKENS,
    _looks_like_location_name,
    classify_city_garbage,
    is_plausibly_location_candidate,
)
from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.adapters.plugins.static._runner import static_listing_job_row
from src.jobs.adapters.provider_parsers import parse_generic_location_fields
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
        "role",
        "opening",
        "openings",
        "roles",
        "vacanc",
        "opportunity",
        "opportunities",
        "hiring",
    }
)
_OPEN_POSITION_HINTS = frozenset(
    {
        "open position",
        "open positions",
        "open role",
        "open roles",
    }
)

_TITLE_CAMPAIGN_NOISE_PHRASES = (
    "student and recent graduates",
    "students and recent graduates",
    "explore internship and apprenticeship roles",
    "apprenticeship roles across exciting teams",
    "across exciting teams including",
)
_JOB_TITLE_HINT_TOKENS = frozenset(
    {
        "artist",
        "designer",
        "engineer",
        "programmer",
        "developer",
        "manager",
        "director",
        "producer",
        "specialist",
        "analyst",
        "scientist",
        "writer",
        "animator",
        "coordinator",
        "recruiter",
        "architect",
        "consultant",
        "assistant",
        "lead",
        "principal",
        "intern",
        "technician",
        "technical",
        "qa",
        "quality assurance",
        "operations",
        "marketing",
        "community",
        "data",
        "security",
        "devops",
        "infrastructure",
        "finance",
        "legal",
        "hr",
        "talent",
        "mobile",
        "web",
        "backend",
        "frontend",
        "full stack",
        "fullstack",
        "ui",
        "ux",
        "audio",
        "content",
        "training",
        "sales",
        "account",
        "partnership",
        "research",
        "software",
        "systems",
        "localization",
        "monetization",
        "retention",
    }
)
_NON_JOB_TITLE_EXACT_TOKENS = frozenset(
    {
        "about",
        "art",
        "blog",
        "career",
        "careers",
        "contact",
        "games",
        "home",
        "join us",
        "news",
        "privacy",
        "products",
        "search faq",
        "support",
        "tech",
        "terms",
    }
)
_NON_JOB_TITLE_PHRASE_TOKENS = frozenset(
    {
        "about us",
        "all games",
        "cookie policy",
        "cookies policy",
        "our games",
        "privacy policy",
    }
)
_JOB_URL_HINT_TOKENS = (
    "jobs.ashbyhq.com",
    "/job/",
    "/jobs/",
    "/career/",
    "/careers/",
    "/position/",
    "/positions/",
    "/opening/",
    "/openings/",
    "/vacanc",
    "/apply/",
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
_LEADING_POSTED_DATE_RE = re.compile(
    r"(?is)^\s*(?:"
    r"(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?\s+\d{1,2}(?:,\s*\d{2,4})?"
    r"|\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?"
    r"|\d{4}-\d{2}-\d{2}"
    r")\s*(?:[|\-–—:]\s*|\s+)"
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
    candidate = re.sub(
        r"(?is)\b(?:apply now|learn more|details|read more|view job|view details)\b.*$",
        "",
        candidate,
    )
    hiring_match = re.search(
        r"(?is)^(?:.*?\bis hiring (?:a|an)\s+)(.+?)(?:\s+(?:to|for|from|in|at|on)\b.*)?$",
        candidate,
    )
    if hiring_match:
        candidate = hiring_match.group(1)
    candidate = clean_text(candidate)
    candidate = _strip_leading_posted_date_prefix(candidate)
    lowered = candidate.lower()
    if any(phrase in lowered for phrase in _TITLE_CAMPAIGN_NOISE_PHRASES):
        return ""
    return "" if candidate.lower() in _IGNORED_TOKENS else candidate


def _strip_leading_posted_date_prefix(text: str) -> str:
    candidate = clean_text(text)
    if not candidate:
        return ""
    match = _LEADING_POSTED_DATE_RE.match(candidate)
    if not match:
        return candidate
    remainder = clean_text(candidate[match.end() :])
    if not remainder:
        return candidate
    if _looks_like_job_title(remainder):
        return remainder
    return candidate


def _extract_structured_cell_texts(fragment: str) -> list[str]:
    return [
        text for text in extract_tag_texts(fragment or "", ("div", "td", "span", "p", "li")) if text
    ]


def _pick_title(block_html: str, anchor_body: str) -> str:
    for candidate in _extract_structured_cell_texts(anchor_body):
        normalized = _normalize_title_candidate(candidate)
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in _IGNORED_TOKENS:
            continue
        if any(
            hint in lowered
            for hint in ("location", "term", "type", "contract", "department", "team")
        ):
            continue
        if _looks_like_non_job_title(lowered):
            continue
        if _looks_like_job_title(normalized):
            return normalized
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
        if any(
            hint in lower for hint in ("location", "term", "type", "contract", "department", "team")
        ):
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
        normalized = re.sub(r"[\s_-]+", " ", lower).strip()
        if normalized in {"full", "part"}:
            continue
        if not location:
            parsed_city, parsed_country, _ = parse_generic_location_fields(candidate)
            if parsed_city or parsed_country != "Unknown":
                location = candidate
                continue
        if not work_type and (normalized in _WORK_TYPE_NOISE_TOKENS or normalized in {"remote"}):
            work_type = candidate
            continue
        if not contract_type and any(
            token in lower
            for token in ("permanent", "contract", "temporary", "fixed term", "fixed-term")
        ):
            contract_type = candidate
            continue
    return location, work_type, contract_type


def _looks_like_location_cell(text: str) -> bool:
    candidate = clean_text(text)
    if not candidate or candidate.lower() in _IGNORED_TOKENS:
        return False
    lowered = candidate.lower()
    if lowered in _NON_JOB_TITLE_EXACT_TOKENS or lowered in _NON_JOB_TITLE_PHRASE_TOKENS:
        return False
    normalized = re.sub(r"[\s_-]+", " ", lowered).strip()
    if normalized in {"full", "part"}:
        return False
    if normalized in _WORK_TYPE_NOISE_TOKENS:
        return False
    city, country, work_type = parse_generic_location_fields(candidate)
    if city or country != "Unknown":
        if any(token in lowered for token in _JOB_TITLE_HINT_TOKENS) and not any(
            delimiter in candidate for delimiter in (",", "/", "-")
        ):
            return False
        words = re.findall(r"[A-Za-zÀ-ÿ0-9']+", candidate)
        if len(words) > 1 and not any(delimiter in candidate for delimiter in (",", "/", "-", "|")):
            if not _looks_like_location_name(candidate, words):
                return False
        return True
    if work_type:
        return False
    return is_plausibly_location_candidate(candidate)


def _parse_structured_locations(
    structured_cells: list[str], title: str
) -> tuple[list[dict[str, str]], str, str]:
    work_type = ""
    contract_type = ""
    location_candidates: list[str] = []
    for candidate in structured_cells[1:]:
        candidate = clean_text(candidate)
        if not candidate or candidate == title or candidate.lower() in _IGNORED_TOKENS:
            continue
        lower = candidate.lower()
        normalized = re.sub(r"[\s_-]+", " ", lower).strip()
        if normalized in {"full", "part"}:
            continue
        if normalized in _WORK_TYPE_NOISE_TOKENS:
            if not work_type and normalized in {"full time", "part time", "remote", "hybrid"}:
                work_type = candidate
            continue
        if _looks_like_location_cell(candidate):
            location_candidates.append(candidate)
            continue
        if not work_type and normalized in {"full time", "part time", "remote", "hybrid"}:
            work_type = candidate
            continue
        if not contract_type and any(
            token in lower
            for token in ("permanent", "contract", "temporary", "fixed term", "fixed-term")
        ):
            contract_type = candidate
            continue
    locations = normalize_location_details(location_candidates).get("locations") or []
    return locations, work_type, contract_type


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
        return (score, len(haystack))

    ranked = sorted(anchors, key=_score, reverse=True)
    best = ranked[0] if ranked else None
    if best and _score(best)[0] > 0:
        return best
    return None


def _looks_like_job_title(title: str) -> bool:
    lowered = clean_text(title).lower()
    if not lowered or lowered in _IGNORED_TOKENS:
        return False
    if any(phrase in lowered for phrase in _TITLE_CAMPAIGN_NOISE_PHRASES):
        return False
    return any(token in lowered for token in _JOB_TITLE_HINT_TOKENS)


def _looks_like_non_job_title(title: str) -> bool:
    lowered = clean_text(title).lower()
    if not lowered:
        return False
    if lowered in _NON_JOB_TITLE_EXACT_TOKENS:
        return True
    return any(token in lowered for token in _NON_JOB_TITLE_PHRASE_TOKENS)


def _has_job_entry_evidence(
    *,
    href: str,
    anchor_text: str,
    block_text: str,
    title: str,
    location: str,
    work_type: str,
    contract_type: str,
    href_tokens: tuple[str, ...],
) -> bool:
    href_lower = clean_text(href).lower()
    anchor_lower = clean_text(anchor_text).lower()
    block_lower = clean_text(block_text).lower()
    title_lower = clean_text(title).lower()
    title_is_job_like = _looks_like_job_title(title_lower)
    if not href_lower:
        return False
    href_has_job_signal = any(token in href_lower for token in href_tokens) or any(
        token in href_lower for token in _JOB_URL_HINT_TOKENS
    )
    open_position_block = any(token in block_lower for token in _OPEN_POSITION_HINTS)
    if _looks_like_non_job_title(title_lower):
        return bool(open_position_block and href_has_job_signal)
    if href_has_job_signal:
        return True
    if title_is_job_like:
        return True
    if any(token in block_lower for token in _JOB_HINT_TOKENS):
        if open_position_block and href_has_job_signal:
            return True
        return title_is_job_like and (
            href_has_job_signal
            or bool(location)
            or bool(work_type)
            or bool(contract_type)
            or any(token in anchor_lower for token in _CTA_TOKENS)
        )
    if title_is_job_like and (
        href_has_job_signal
        or bool(location)
        or bool(work_type)
        or bool(contract_type)
        or any(token in anchor_lower for token in _CTA_TOKENS)
    ):
        return True
    return False


def _rendered_card_location_fields(
    *,
    block_html: str,
    title: str,
    structured_cells: list[str],
) -> tuple[str, str, str, str, list[dict[str, str]], str]:
    locations: list[dict[str, str]] = []
    structured_work_type = ""
    structured_contract_type = ""
    if structured_cells:
        locations, structured_work_type, structured_contract_type = _parse_structured_locations(
            structured_cells, title
        )
    location, work_type, contract_type = _pick_location_and_terms(block_html, title)
    if locations:
        location = " | ".join(
            ", ".join(
                part
                for part in [
                    clean_text(item.get("city", "")),
                    clean_text(item.get("country", "")),
                ]
                if part
            )
            for item in locations
            if clean_text(item.get("city", "")) or clean_text(item.get("country", ""))
        )
        location_details = normalize_location_details(locations)
        primary_location: dict[str, Any] = next(
            (
                item
                for item in location_details.get("locations", [])
                if clean_text(item.get("city", "")) or clean_text(item.get("country", ""))
            ),
            {},
        )
        city = (
            clean_text(primary_location.get("city", ""))
            or clean_text(location_details.get("city", ""))
            or location
        )
        country = (
            clean_text(primary_location.get("country", ""))
            or clean_text(location_details.get("country", ""))
            or "Unknown"
        )
        return (
            city,
            country,
            structured_work_type or work_type,
            structured_contract_type or contract_type,
            locations,
            location,
        )
    location_details = normalize_location_details(location)
    city = clean_text(location_details.get("city", ""))
    country = clean_text(location_details.get("country", "")) or "Unknown"
    locations = location_details.get("locations") or []
    location = clean_text(location_details.get("locationSummary")) or location
    if not city and country == "Unknown":
        location = ""
    return city, country, work_type, contract_type, locations, location


def _rendered_location_hint(block_html: str, title: str) -> str:
    for line in html_fragment_lines(block_html):
        candidate = clean_text(strip_html_text(line))
        if not candidate or candidate == title or candidate.lower() in _IGNORED_TOKENS:
            continue
        if classify_city_garbage(candidate):
            return candidate
    return ""


def _append_rendered_anchor_candidate(
    *,
    jobs: list[RawJob],
    seen_links: set[str],
    anchor: dict[str, str],
    block_html: str,
    block_text: str,
    mode: str,
    page_url: str,
    company: str,
    source_id: str,
    href_tokens: tuple[str, ...],
) -> None:
    href = clean_text(anchor.get("href"))
    if not href:
        return
    link = normalize_url(urljoin(page_url, href))
    if not link or link in seen_links:
        return
    anchor_body = anchor.get("body") or anchor.get("text") or ""
    structured_cells = _extract_structured_cell_texts(anchor_body)
    title = _pick_title(block_html, anchor_body)
    if not title:
        return
    location_hint = _rendered_location_hint(block_html, title)
    city, country, work_type, contract_type, locations, location = _rendered_card_location_fields(
        block_html=block_html,
        title=title,
        structured_cells=structured_cells,
    )
    if not _has_job_entry_evidence(
        href=href,
        anchor_text=anchor.get("text") or "",
        block_text=block_text,
        title=title,
        location=location,
        work_type=work_type,
        contract_type=contract_type,
        href_tokens=href_tokens,
    ):
        return
    seen_links.add(link)
    jobs.append(
        static_listing_job_row(
            source_id=source_id,
            link=link,
            title=title,
            company=company,
            city=city,
            country=country,
            work_type=work_type,
            contract_type=contract_type,
            locations=locations,
            location_summary=location,
            _locationHint=location_hint,
            _renderedCardMode=mode,
        )
    )


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
            if not allow_any_anchor and not any(
                token in lower for token in _JOB_HINT_TOKENS | _CTA_TOKENS
            ):
                continue
            anchors = list(iter_anchor_fragments(block_html))
            if not anchors:
                continue
            anchor_candidates = anchors
            if not allow_any_anchor:
                anchor = _pick_job_anchor(
                    anchors, href_tokens=href_tokens, allow_any_anchor=allow_any_anchor
                )
                anchor_candidates = [anchor] if anchor else []
            for anchor in anchor_candidates:
                if not anchor:
                    continue
                _append_rendered_anchor_candidate(
                    jobs=jobs,
                    seen_links=seen_links,
                    anchor=anchor,
                    block_html=block_html,
                    block_text=block_text,
                    mode="block",
                    page_url=page_url,
                    company=company,
                    source_id=source_id,
                    href_tokens=href_tokens,
                )
    if allow_any_anchor and not jobs:
        page_text = clean_text(strip_html_text(html or ""))
        for anchor in iter_anchor_fragments(html or ""):
            _append_rendered_anchor_candidate(
                jobs=jobs,
                seen_links=seen_links,
                anchor=anchor,
                block_html=html or "",
                block_text=page_text,
                mode="fallback",
                page_url=page_url,
                company=company,
                source_id=source_id,
                href_tokens=href_tokens,
            )
    return jobs
