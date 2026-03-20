"""Reddit, X (Twitter), and Mastodon job post parsers for social adapters."""
from __future__ import annotations

import hashlib
import re
from html import unescape
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from src.jobs.models import RawJob

from src.jobs.game_detection import looks_like_game_job
from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.text_utils import clean_text as _clean_text_impl, norm_text as _norm_text_impl, normalize_url
from src.shared.regex import find_urls_in_text

SOCIAL_HIRING_KEYWORDS = {
    "hiring",
    "we're hiring",
    "we are hiring",
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


def _clean_text(value: Any) -> str:
    return _clean_text_impl(value)


def _norm_text(value: Any) -> str:
    return _norm_text_impl(value)


def social_extract_urls(text: str) -> List[str]:
    return [
        normalize_url(url)
        for url in find_urls_in_text(_clean_text(text))
        if normalize_url(url)
    ]


def social_extract_apply_url(*texts: Any) -> str:
    blocked_hosts = {
        "reddit.com",
        "www.reddit.com",
        "x.com",
        "www.x.com",
        "twitter.com",
        "www.twitter.com",
        "t.co",
        "mastodon.gamedev.place",
        "xcancel.com",
        "rss.xcancel.com",
    }
    for text in texts:
        for url in social_extract_urls(_clean_text(text)):
            host = _clean_text(urlparse(url).netloc).lower()
            if host in blocked_hosts:
                continue
            return url
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


def social_should_keep_post(
    *,
    title: str,
    text: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    has_apply_url: bool,
) -> Tuple[bool, int]:
    normalized = f"{_norm_text(title)} {_norm_text(text)}"
    if reject_for_hire_posts and any(
        token in normalized for token in SOCIAL_FOR_HIRE_KEYWORDS
    ):
        return False, 0
    confidence = social_compute_confidence(
        title, text, has_apply_url=has_apply_url, has_remote_hint=("remote" in normalized)
    )
    return confidence >= max(0, min(100, int(min_confidence or 0))), confidence


def parse_reddit_json_payload(
    payload: Any,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows: List[Dict[str, Any]] = []
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
    out: List[RawJob] = []
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
        keep, confidence = social_should_keep_post(
            title=title,
            text=f"{body} {flair}",
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        job_link = apply_url or permalink or external_url
        if not title or not job_link:
            continue
        company = social_infer_company(title, body, fallback=_clean_text(item.get("author")))
        post_source_id = f"reddit:{_clean_text(subreddit)}:{post_id or hashlib.sha1(job_link.encode('utf-8')).hexdigest()[:12]}"
        out.append({
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
            "sourceBundle": [{
                "source": "social_reddit",
                "sourceJobId": post_source_id,
                "jobLink": permalink or job_link,
                "postedAt": item.get("created_utc"),
                "adapter": "social",
                "studio": _clean_text(subreddit),
            }],
        })
    return out, low_conf_count


def parse_reddit_html_payload(
    html_text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    """Parse Reddit HTML content for job posts when JSON and RSS fail."""
    out: List[RawJob] = []
    low_conf_count = 0

    try:
        block_pattern = re.compile(r"(?is)<(?:article|div)\b[^>]*>(.*?)</(?:article|div)>")
        anchor_pattern = re.compile(r"(?is)<a\b[^>]*href\s*=\s*(['\"])(.*?)\1[^>]*>(.*?)</a>")
        title_pattern = re.compile(r"(?is)<(?:h1|h2|h3|h4|h5|h6)\b[^>]*>(.*?)</(?:h1|h2|h3|h4|h5|h6)>")

        post_containers = [match.group(1) or "" for match in block_pattern.finditer(html_text or "")]
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

            confidence = social_compute_confidence(
                title,
                link,
                has_apply_url=bool(link),
                has_remote_hint=False
            )

            title_lower = title.lower()
            if any(keyword in title_lower for keyword in SOCIAL_HIRING_KEYWORDS):
                confidence += 20
            elif any(keyword in title_lower for keyword in SOCIAL_FOR_HIRE_KEYWORDS):
                if reject_for_hire_posts:
                    continue
                confidence -= 30
            
            if confidence < min_confidence:
                low_conf_count += 1
                continue

            company = social_infer_company(title, link)

            job_entry = RawJob(
                title=title,
                company=company,
                job_link=link,
                source="social_reddit",
                source_job_id=f"html:{subreddit}:{hash(title)}",
                posted_at=posted_at,
                adapter="social",
                studio=subreddit,
            )
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
) -> Tuple[List[RawJob], int]:
    try:
        root = ET.fromstring(_clean_text(rss_text).lstrip())
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: List[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = _clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(
            unescape(_clean_text(item.findtext("description")))
        )
        apply_url = social_extract_apply_url(description, link)
        keep, confidence = social_should_keep_post(
            title=title,
            text=description,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        if not title or not link:
            continue
        company = social_infer_company(title, description, fallback=_clean_text(subreddit))
        post_source_id = f"reddit:{_clean_text(subreddit)}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:12]}"
        out.append({
            "sourceJobId": post_source_id,
            "title": title,
            "company": company,
            "city": "Remote" if "remote" in _norm_text(f"{title} {description}") else "",
            "country": "Remote" if "remote" in _norm_text(f"{title} {description}") else "Unknown",
            "workType": "Remote" if "remote" in _norm_text(f"{title} {description}") else "",
            "contractType": "Unknown",
            "jobLink": apply_url or link,
            "sector": "Game",
            "postedAt": _clean_text(item.findtext("pubDate")),
            "adapter": "social",
            "studio": f"reddit/{_clean_text(subreddit)}",
            "sourceBundle": [{
                "source": "social_reddit",
                "sourceJobId": post_source_id,
                "jobLink": link,
                "postedAt": _clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": _clean_text(subreddit),
            }],
        })
    return out, low_conf_count


def parse_x_payload(
    payload: Any,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows = (
        payload.get("data")
        if isinstance(payload, dict) and isinstance(payload.get("data"), list)
        else []
    )
    out: List[RawJob] = []
    low_conf_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = _clean_text(row.get("text"))
        post_id = _clean_text(row.get("id"))
        entities = row.get("entities") if isinstance(row.get("entities"), dict) else {}
        entity_urls = entities.get("urls") if isinstance(entities.get("urls"), list) else []
        expanded_urls = [
            _clean_text(item.get("expanded_url"))
            for item in entity_urls
            if isinstance(item, dict)
        ]
        apply_url = social_extract_apply_url(text, " ".join(expanded_urls))
        keep, confidence = social_should_keep_post(
            title=text,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        permalink = (
            normalize_url(f"https://x.com/i/web/status/{post_id}") if post_id else ""
        )
        company = social_infer_company(text, fallback="Unknown Studio")
        post_source_id = f"x:{post_id or hashlib.sha1(text.encode('utf-8')).hexdigest()[:12]}"
        out.append({
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
            "sourceBundle": [{
                "source": "social_x",
                "sourceJobId": post_source_id,
                "jobLink": permalink or apply_url,
                "postedAt": _clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": "x",
            }],
        })
    return out, low_conf_count


def parse_x_rss_payload(
    rss_text: str,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    raw_text = _clean_text(rss_text).lstrip()
    safe_text = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)", "&amp;", raw_text)
    try:
        root = ET.fromstring(safe_text)
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: List[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = _clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(
            unescape(_clean_text(item.findtext("description")))
        )
        banner_text = _norm_text(f"{title} {description}")
        if "not yet whitelisted" in banner_text or "rss reader" in banner_text:
            low_conf_count += 1
            continue
        text = f"{title} {description}"
        apply_url = social_extract_apply_url(text, link)
        keep, confidence = social_should_keep_post(
            title=title,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        if not title or not link:
            continue
        post_id = hashlib.sha1(link.encode("utf-8")).hexdigest()[:12]
        company = social_infer_company(title, description, fallback="Unknown Studio")
        source_job_id = f"x:{post_id}"
        out.append({
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
            "sourceBundle": [{
                "source": "social_x",
                "sourceJobId": source_job_id,
                "jobLink": link,
                "postedAt": _clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": "x",
            }],
        })
    return out, low_conf_count


def parse_mastodon_payload(
    payload: Any,
    *,
    instance: str,
    tag: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows = payload if isinstance(payload, list) else []
    out: List[RawJob] = []
    low_conf_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        html_text = _clean_text(row.get("content"))
        text = strip_html_text(unescape(html_text))
        post_url = normalize_url(row.get("url"))
        card = row.get("card") if isinstance(row.get("card"), dict) else {}
        apply_url = social_extract_apply_url(text, _clean_text(card.get("url")))
        keep, confidence = social_should_keep_post(
            title=text,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        post_id = _clean_text(row.get("id"))
        account = row.get("account") if isinstance(row.get("account"), dict) else {}
        account_name = _clean_text(account.get("display_name") or account.get("acct"))
        company = social_infer_company(text, fallback=account_name)
        post_source_id = f"mastodon:{_clean_text(urlparse(instance).netloc)}:{post_id or hashlib.sha1((post_url or text).encode('utf-8')).hexdigest()[:12]}"
        out.append({
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
            "sourceBundle": [{
                "source": "social_mastodon",
                "sourceJobId": post_source_id,
                "jobLink": post_url or apply_url,
                "postedAt": _clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": _clean_text(urlparse(instance).netloc),
            }],
        })
    return out, low_conf_count
