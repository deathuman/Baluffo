"""Reddit, X (Twitter), and Mastodon job post parsers for social adapters."""

from __future__ import annotations

import hashlib
import re
from html import unescape
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text as _clean_text_impl
from src.jobs.text_utils import norm_text as _norm_text_impl
from src.jobs.text_utils import normalize_url
from src.shared.regex import find_urls_in_text

SOCIAL_HIRING_KEYWORDS = {
    "hiring",
    "we're hiring",
    "we are hiring",
    "is hiring",
    "job opening",
    "open role",
    "join our team",
    "looking for",
    "vacancy",
    "position",
    "apply now",
    "paid",
}
SOCIAL_FOR_HIRE_KEYWORDS = {
    "for hire",
    "available for work",
    "looking for work",
    "hire me",
    "open to work",
}
SOCIAL_EXPLICIT_OPENING_PHRASES = {
    "we're hiring",
    "we are hiring",
    "is hiring",
    "job opening",
    "job openings",
    "open role",
    "open roles",
    "open position",
    "open positions",
    "apply now",
    "join our team",
    "hiring for",
}
SOCIAL_NEGATIVE_NOT_HIRING_PHRASES = {
    "we're not hiring",
    "we are not hiring",
    "not hiring",
    "wish we were hiring",
    "wish we could hire",
    "nobody is hiring",
    "why is nobody hiring",
    "why is no one hiring",
    "laid off",
    "layoff",
    "layoffs",
}
SOCIAL_DISCUSSION_PHRASES = {
    "anyone hiring",
    "who is hiring",
    "how do i get a job",
    "how do i get hired",
    "how do i find a job",
    "jobs are bad",
    "job market",
    "why are jobs",
    "why is hiring",
    "rant",
    "beware",
    "avoid",
}
SOCIAL_BLOCKED_HOSTS = {
    "reddit.com",
    "www.reddit.com",
    "old.reddit.com",
    "x.com",
    "www.x.com",
    "twitter.com",
    "www.twitter.com",
    "t.co",
    "mastodon.gamedev.place",
    "xcancel.com",
    "rss.xcancel.com",
    "nitter.net",
    "nitter.poast.org",
    "bsky.app",
    "www.linkedin.com",
    "linkedin.com",
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "twitch.tv",
    "www.twitch.tv",
    "discord.gg",
}
SOCIAL_APPLY_HOST_HINTS = (
    "boards.greenhouse.io",
    "jobs.ashbyhq.com",
    "ashbyhq.com",
    "jobs.lever.co",
    "lever.co",
    "myworkdayjobs.com",
    "workday.com",
    "jobs.smartrecruiters.com",
    "smartrecruiters.com",
    "teamtailor.com",
    "job-boards.greenhouse.io",
    "jobvite.com",
    "breezy.hr",
    "pinpointhq.com",
    "personio",
)
SOCIAL_APPLY_PATH_HINTS = (
    "/job",
    "/jobs",
    "/career",
    "/careers",
    "/apply",
    "/application",
    "/opening",
    "/openings",
    "/position",
    "/positions",
    "/vacan",
)
SOCIAL_CONTENT_ONLY_HOST_HINTS = (
    "medium.com",
    "substack.com",
    "wordpress.com",
    "blogspot.com",
    "notion.site",
)
SOCIAL_BLOCKED_PATH_HINTS = (
    "/status/",
    "/statuses/",
    "/post/",
    "/posts/",
    "/comment/",
    "/comments/",
    "/thread/",
    "/threads/",
    "/blog",
    "/news",
    "/article",
    "/articles",
    "/watch",
    "/video",
    "/videos",
    "/podcast",
)
SOCIAL_CONTENT_ONLY_PATH_HINTS = (
    "/blog",
    "/blogs",
    "/article",
    "/articles",
    "/news",
    "/post",
    "/posts",
    "/study",
    "/studies",
    "/research",
    "/technical",
    "/tutorial",
    "/guide",
)


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return _clean_text_impl(value)


def _norm_text(value: Any) -> str:
    return _norm_text_impl(value)


def social_extract_urls(text: str) -> list[str]:
    return [
        normalize_url(url) for url in find_urls_in_text(_clean_text(text)) if normalize_url(url)
    ]


def _increment_reason(counter: dict[str, int] | None, reason: str) -> None:
    if counter is None or not reason:
        return
    counter[reason] = int(counter.get(reason) or 0) + 1


def social_has_explicit_opening_signal(*values: Any) -> bool:
    text = " ".join(_norm_text(value) for value in values if value is not None)
    if any(token in text for token in SOCIAL_EXPLICIT_OPENING_PHRASES):
        return True
    return bool(re.search(r"\b[a-z0-9][a-z0-9& .'\-]{1,50}\s+is hiring\b", text))


def social_has_negative_hiring_signal(*values: Any) -> str:
    text = " ".join(_norm_text(value) for value in values if value is not None)
    if any(token in text for token in SOCIAL_NEGATIVE_NOT_HIRING_PHRASES):
        return "not_hiring_or_layoff"
    if any(token in text for token in SOCIAL_DISCUSSION_PHRASES):
        return "discussion_or_question"
    if text.endswith("?") and not social_has_explicit_opening_signal(text):
        return "discussion_or_question"
    return ""


def social_is_job_destination_url(url: str, *, context_text: str = "") -> bool:
    normalized_url = normalize_url(url)
    if not normalized_url:
        return False
    parsed = urlparse(normalized_url)
    host = _clean_text(parsed.netloc).lower()
    path = _clean_text(parsed.path).lower()
    query = _clean_text(parsed.query).lower()
    if not host or host in SOCIAL_BLOCKED_HOSTS:
        return False
    if any(blocked in path for blocked in SOCIAL_BLOCKED_PATH_HINTS):
        return False
    if any(host_hint in host for host_hint in SOCIAL_APPLY_HOST_HINTS):
        return True
    if host.startswith(("jobs.", "careers.", "apply.")):
        return True
    if any(path_hint in path for path_hint in SOCIAL_APPLY_PATH_HINTS):
        return True
    if any(path_hint.strip("/") in query for path_hint in SOCIAL_APPLY_PATH_HINTS):
        return True
    normalized_context = _norm_text(context_text)
    return bool(normalized_context) and (
        "apply" in normalized_context
        or "careers" in normalized_context
        or "application" in normalized_context
    )


def social_is_content_only_url(url: str) -> bool:
    normalized_url = normalize_url(url)
    if not normalized_url:
        return False
    parsed = urlparse(normalized_url)
    host = _clean_text(parsed.netloc).lower()
    path = _clean_text(parsed.path).lower()
    if any(host_hint in host for host_hint in SOCIAL_CONTENT_ONLY_HOST_HINTS):
        return True
    return any(path_hint in path for path_hint in SOCIAL_CONTENT_ONLY_PATH_HINTS)


def social_has_social_repost_only(*values: Any) -> bool:
    urls = []
    for value in values:
        urls.extend(social_extract_urls(_clean_text(value)))
    if not urls:
        return False
    return not any(
        social_is_job_destination_url(url, context_text=" ".join(_clean_text(v) for v in values))
        for url in urls
    )


def social_extract_apply_url(*texts: Any) -> str:
    context = " ".join(_clean_text(text) for text in texts if _clean_text(text))
    for text in texts:
        for url in social_extract_urls(_clean_text(text)):
            if social_is_job_destination_url(url, context_text=context):
                return url
    return ""


def social_should_reject_non_job_reddit_post(
    *,
    title: str,
    text: str,
    apply_url: str,
    company: str,
    fallback_company: str,
) -> str:
    normalized_apply_url = normalize_url(apply_url)
    if not normalized_apply_url:
        return "missing_apply_url"
    if social_is_content_only_url(normalized_apply_url):
        return "non_job_destination_url"
    if company and fallback_company and _norm_text(company) == _norm_text(fallback_company):
        combined = _norm_text(f"{title} {text}")
        if not looks_like_game_job(title, text):
            return "discussion_or_commentary"
        if "apply" not in combined and "careers" not in combined and "job" not in combined:
            return "discussion_or_commentary"
    return ""


def social_infer_company(*texts: Any, fallback: str = "") -> str:
    corpus = " ".join(_clean_text(text) for text in texts if _clean_text(text))
    patterns = (
        r"\bat\s+([A-Z][A-Za-z0-9& .'\-]{2,})",
        r"\bjoin\s+([A-Z][A-Za-z0-9& .'\-]{2,})",
        r"\b([A-Z][A-Za-z0-9& .'\-]{2,})\s+is\s+hiring",
    )
    for pattern in patterns:
        match = re.search(pattern, corpus)
        if match:
            candidate = _clean_text(match.group(1)).strip(" .,:;")
            candidate = re.split(
                r"\b(remote|apply|role|position|job)\b", candidate, maxsplit=1, flags=re.IGNORECASE
            )[0].strip(" .,:;-")
            words = [part for part in candidate.split() if part]
            if len(words) > 6:
                candidate = " ".join(words[:6])
            if candidate:
                return candidate
    return _clean_text(fallback) or "Unknown Studio"


def social_compute_confidence(
    *values: Any, has_apply_url: bool = False, has_remote_hint: bool = False
) -> int:
    text = " ".join(_norm_text(value) for value in values if value is not None)
    score = 0
    if any(token in text for token in SOCIAL_HIRING_KEYWORDS):
        score += 35
    if looks_like_game_job(text):
        score += 30
    if "job" in text or "role" in text or "position" in text:
        score += 10
    if has_apply_url:
        score += 20
    if has_remote_hint:
        score += 5
    if any(token in text for token in SOCIAL_FOR_HIRE_KEYWORDS):
        score -= 40
    return max(0, min(100, score))


def social_evaluate_post(
    *,
    title: str,
    text: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    has_apply_url: bool,
) -> tuple[bool, int, str]:
    normalized = f"{_norm_text(title)} {_norm_text(text)}"
    if reject_for_hire_posts and any(token in normalized for token in SOCIAL_FOR_HIRE_KEYWORDS):
        return False, 0, "for_hire"
    negative_reason = social_has_negative_hiring_signal(title, text)
    if negative_reason:
        return False, 0, negative_reason
    if not social_has_explicit_opening_signal(title, text):
        confidence = social_compute_confidence(
            title, text, has_apply_url=has_apply_url, has_remote_hint=("remote" in normalized)
        )
        return False, confidence, "missing_explicit_opening"
    if not has_apply_url:
        confidence = social_compute_confidence(
            title, text, has_apply_url=False, has_remote_hint=("remote" in normalized)
        )
        if social_has_social_repost_only(title, text):
            return False, confidence, "social_repost_or_commentary"
        return False, confidence, "missing_valid_apply_url"
    confidence = social_compute_confidence(
        title, text, has_apply_url=has_apply_url, has_remote_hint=("remote" in normalized)
    )
    if confidence < max(0, min(100, int(min_confidence or 0))):
        return False, confidence, "low_confidence"
    return True, confidence, ""


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
