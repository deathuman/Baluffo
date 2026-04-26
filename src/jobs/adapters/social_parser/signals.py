from __future__ import annotations

"""Shared signal helpers for social job-post parsers."""

import re
from typing import Any
from urllib.parse import urlparse

from src.jobs.game_detection import looks_like_game_job
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
