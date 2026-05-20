"""Job link extraction from HTML (embedded URLs, script sources, jobylon, intervieweb, etc.)."""

from __future__ import annotations

import html as html_module
import json
import re
from collections.abc import Callable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from src.jobs.transport import normalize_url as normalize_job_url
from src.shared.regex import find_urls_in_text
from src.source_registry import normalize_source_url
from src.url_hosts import host_matches_domain


def _is_ignored_job_href(href: str) -> bool:
    clean = str(href or "").strip()
    return (
        not clean
        or clean.startswith("#")
        or clean.lower().startswith(("javascript:", "mailto:", "tel:"))
    )


def _is_ignored_job_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    path = (parsed.path or "").lower()
    if "/jobs/share_image/" in path:
        return True
    if _is_general_application_url(parsed):
        return True
    return False


def _is_general_application_url(parsed: object) -> bool:
    host = (getattr(parsed, "hostname", "") or "").lower()
    path = (getattr(parsed, "path", "") or "").lower()
    if any(
        token in path
        for token in (
            "open-application",
            "openapplication",
            "general-application",
            "generalapplication",
            "no-job-that-suits-you",
            "nojobthatsuitsyou",
            "talent-community",
            "talentcommunity",
            "spontaneous-application",
            "unsolicited-application",
        )
    ):
        return True
    if not host_matches_domain(host, "greenhouse.io"):
        return False
    parts = [part for part in path.strip("/").split("/") if part]
    return bool(len(parts) >= 3 and parts[1] == "jobs" and parts[0].endswith("oa"))


def _is_general_application_anchor(anchor_body: str) -> bool:
    text = re.sub(r"(?is)<[^>]+>", " ", str(anchor_body or ""))
    text = html_module.unescape(re.sub(r"\s+", " ", text).strip()).lower()
    return bool(
        re.search(
            r"\b(submit your application|open applications?|general applications?|no job that suits you|talent community)\b",
            text,
        )
    )


def _is_lever_posting_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    if parsed.netloc.lower() != "jobs.lever.co":
        return False
    parts = [part for part in (parsed.path or "").strip("/").split("/") if part]
    return len(parts) >= 2


def _is_job_like_path(path: str) -> bool:
    if "/job/" in path or "/jobs/" in path or "/career/posting/" in path:
        return True
    if path.startswith("/requisitions/view/"):
        return bool(re.search(r"/requisitions/view/\d+/?$", path))
    if "/careers/" in path:
        tail = path.rstrip("/")
        return not (
            tail == "/careers" or tail.endswith("/careers-category") or "/careers-category/" in tail
        )
    if "/career/" in path:
        return path.rstrip("/") != "/career"
    if path.startswith("/open-positions/"):
        return True
    if "/job-offers/" in path or path.rstrip("/") == "/job-offers":
        return True
    if path.startswith("/vacancy/"):
        return bool(re.search(r"/vacancy/\d+/?$", path))
    if path.startswith("/vacancies/") or path.rstrip("/") == "/vacancies":
        return True
    if path.startswith(("/join/", "/o/")):
        return bool(re.search(r"(?:/join/[^/]+/\d+|/o/[^/]+)/?$", path))
    return False


def _embedded_job_url_candidates(absolute: str) -> list[str]:
    low = absolute.lower()
    if _is_ignored_job_url(absolute):
        return []
    parsed = urlparse(absolute)
    host = (parsed.hostname or "").lower()
    if "jobs.lever.co/" in low:
        return [absolute] if _is_lever_posting_url(absolute) else []
    if host in {"boards.greenhouse.io", "jobs.ashbyhq.com"}:
        return [absolute]
    if host_matches_domain(host, "jobs.personio.de"):
        out = [absolute]
        search_url = normalize_job_url(absolute.rstrip("/") + "/search.json")
        if search_url and not low.endswith("/search.json"):
            out.append(search_url)
        return out
    if host in {"jobs.smartrecruiters.com", "apply.workable.com"}:
        return [absolute]
    path = (parsed.path or "").lower()
    return [absolute] if not _is_ignored_job_url(absolute) and _is_job_like_path(path) else []


def extract_job_like_links(html: str, base_url: str) -> list[str]:
    links: list[str] = []
    seen = set()
    for anchor in re.finditer(r'(?is)<a\b[^>]*href=(["\'])(.*?)\1[^>]*>(.*?)</a>', html):
        href = anchor.group(2)
        raw_href = str(href or "").strip()
        if _is_ignored_job_href(raw_href):
            continue
        if _is_general_application_anchor(anchor.group(3)):
            continue
        try:
            absolute = urljoin(base_url, raw_href)
        except Exception:  # noqa: BLE001
            absolute = raw_href
        parsed = urlparse(absolute)
        path = (parsed.path or "").lower()
        if _is_ignored_job_url(absolute):
            continue
        if not _is_job_like_path(path):
            continue
        normalized = normalize_job_url(absolute)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        links.append(normalized)
    return links


def extract_embedded_job_urls(html: str, base_url: str) -> list[str]:
    links: list[str] = []
    seen = set()
    for raw in find_urls_in_text(html):
        absolute = normalize_job_url(raw)
        if not absolute or absolute in seen:
            continue
        for candidate in _embedded_job_url_candidates(absolute):
            if candidate in seen:
                continue
            seen.add(candidate)
            links.append(candidate)
    for raw in re.findall(r'(?is)href=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(raw or "").strip()))
        if not absolute or absolute in seen:
            continue
        path = (urlparse(absolute).path or "").lower()
        if "/career/posting/" in path:
            seen.add(absolute)
            links.append(absolute)
    for raw in re.findall(r'(?is)["\'](/[^"\']{3,260})["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(raw or "").strip()))
        if not absolute or absolute in seen:
            continue
        path = (urlparse(absolute).path or "").lower()
        if _is_ignored_job_url(absolute):
            continue
        if not _is_job_like_path(path):
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def extract_workable_account(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if (parsed.hostname or "").lower() != "apply.workable.com":
        return ""
    parts = [part for part in (parsed.path or "").split("/") if part]
    if not parts:
        return ""
    account = str(parts[0] or "").strip()
    return account if re.match(r"^[a-z0-9][a-z0-9-]{1,80}$", account, flags=re.I) else ""


def count_workable_jobs(account: str, timeout_s: int, fetch_text: Callable[[str, int], str]) -> int:
    token = str(account or "").strip()
    if not token:
        return 0
    api_url = f"https://apply.workable.com/api/v1/widget/accounts/{token}?details=true"
    payload_text = fetch_text(api_url, timeout_s)
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return 0
    if not isinstance(payload, dict):
        return 0
    jobs = payload.get("jobs")
    return len(jobs) if isinstance(jobs, list) else 0


def parse_personio_search_count(text: str) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("data", "positions", "items", "results"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return len(rows)
        if isinstance(payload.get("jobs"), list):
            return len(payload.get("jobs") or [])
    return 0


def extract_jobylon_embed_urls(html: str) -> list[str]:
    out: list[str] = []
    seen = set()
    if "cdn.jobylon.com/embedder.js" not in html.lower():
        return out
    company_ids = re.findall(r"jbl_company_id\s*=\s*([0-9]+)", html, flags=re.I)
    versions = re.findall(r"jbl_version\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    page_sizes = re.findall(r"jbl_page_size\s*=\s*([0-9]+)", html, flags=re.I)
    version = versions[0].strip() if versions else "v2"
    page_size = page_sizes[0].strip() if page_sizes else "30"
    for company_id in company_ids:
        company = str(company_id or "").strip()
        if not company:
            continue
        url = (
            f"https://cdn.jobylon.com/jobs/companies/{company}/embed/{version}/"
            f"?target=jobylon-jobs-widget&page_size={page_size}"
        )
        normalized = normalize_job_url(url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def extract_script_sources(html: str, base_url: str) -> list[str]:
    out: list[str] = []
    seen = set()
    for src in re.findall(r'(?is)<script[^>]+src=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(src or "").strip()))
        if not absolute or absolute in seen:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


def build_intervieweb_iframe_url(script_url: str, page_url: str) -> str:
    parsed = urlparse(script_url)
    if "intervieweb.it" not in (parsed.netloc or "").lower():
        return ""
    if "announces_js.php" not in (parsed.path or "").lower():
        return ""
    query = parse_qs(parsed.query, keep_blank_values=True)
    k = (query.get("k") or [""])[0]
    lac = (query.get("LAC") or [""])[0]
    lang = (query.get("lang") or ["en"])[0] or "en"
    ann_type = (query.get("annType") or ["published"])[0] or "published"
    type_view = (query.get("typeView") or ["large"])[0] or "large"
    d_value = (query.get("d") or [""])[0] or (urlparse(page_url).netloc or "")
    if not k or not lac or not d_value:
        return ""
    params = {
        "module": "iframeAnnunci",
        "lang": lang,
        "k": k,
        "d": d_value,
        "LAC": lac,
        "utype": (query.get("utype") or [""])[0],
        "act1": "23",
        "defgroup": (query.get("defgroup") or ["name"])[0],
        "gnavenable": (query.get("gnavenable") or ["1"])[0],
        "desc": (query.get("desc") or ["1"])[0],
        "annType": ann_type,
        "h": (query.get("h") or [""])[0],
        "typeView": type_view,
    }
    return f"{parsed.scheme}://{parsed.netloc}/app.php?{urlencode(params)}"


def extract_intervieweb_job_links(html: str, base_url: str) -> list[str]:
    links: list[str] = []
    seen = set()
    for href in re.findall(r'(?is)href=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(href or "").strip()))
        if not absolute or absolute in seen:
            continue
        lower = absolute.lower()
        if "idannuncio=" in lower or ("module=iframeannunci" in lower and "act1=1" in lower):
            seen.add(absolute)
            links.append(absolute)
    return links


def extract_external_job_links_from_scripts(
    html: str,
    page_url: str,
    timeout_s: int,
    fetch_text: Callable[[str, int], str],
) -> tuple[list[str], list[str]]:
    job_links: list[str] = []
    errors: list[str] = []
    seen = set()
    script_sources = extract_script_sources(html, page_url)
    for script_url in script_sources:
        lower = script_url.lower()
        intervieweb_iframe = build_intervieweb_iframe_url(script_url, page_url)
        if intervieweb_iframe:
            try:
                iframe_html = fetch_text(intervieweb_iframe, timeout_s)
                for link in extract_intervieweb_job_links(iframe_html, intervieweb_iframe):
                    if link in seen:
                        continue
                    seen.add(link)
                    job_links.append(link)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{intervieweb_iframe}: {exc}")
            continue
        if not any(token in lower for token in ("career", "job", "vacanc", "recruit", "announc")):
            continue
        try:
            script_text = fetch_text(script_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{script_url}: {exc}")
            continue
        for raw in find_urls_in_text(script_text):
            absolute = normalize_job_url(raw)
            if not absolute or absolute in seen:
                continue
            low_abs = absolute.lower()
            if not any(
                token in low_abs for token in ("job", "career", "vacanc", "recruit", "annunci")
            ):
                continue
            seen.add(absolute)
            job_links.append(absolute)
    return job_links, errors


def extract_text_job_signals(html: str, page_url: str) -> list[str]:
    sanitized = re.sub(r"(?is)<script\b[^>]*>.*?</script\s*>", " ", html)
    sanitized = re.sub(r"(?is)<style\b[^>]*>.*?</style\s*>", " ", sanitized)
    text = re.sub(r"(?is)<[^>]+>", " ", sanitized)
    text = re.sub(r"\s+", " ", text).strip().lower()
    if not text:
        return []
    parsed = urlparse(page_url)
    path = (parsed.path or "").lower()
    on_careers_page = "/career" in path or "/careers" in path
    apply_count = len(re.findall(r"\bapply(?:\s+now)?\b", text))
    role_keywords = (
        "programmer",
        "engineer",
        "designer",
        "artist",
        "animator",
        "producer",
        "director",
        "qa",
        "tester",
        "technical",
    )
    role_count = sum(len(re.findall(rf"\b{re.escape(token)}\b", text)) for token in role_keywords)
    if not on_careers_page or apply_count < 4 or role_count < 4:
        return []
    signal_count = max(1, min(24, role_count // 2))
    page_norm = normalize_source_url(page_url) or page_url
    return [f"signal:text_jobs:{page_norm}:{idx}" for idx in range(signal_count)]


def extract_embedded_job_filter_signals(html: str, page_url: str) -> tuple[list[str], list[str]]:
    structured_links: list[str] = []
    weak_signals: list[str] = []
    seen_links = set()
    page_norm = normalize_source_url(page_url) or page_url
    matches = re.findall(r'(?is)<job-filter\b[^>]+:raw-data=["\'](.*?)["\']', html)
    for raw_payload in matches:
        payload_text = html_module.unescape(str(raw_payload or "").strip())
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        jobs = payload.get("jobs") if isinstance(payload, dict) else []
        if not isinstance(jobs, list):
            continue
        for idx, job in enumerate(jobs):
            if not isinstance(job, dict):
                continue
            raw_link = str(job.get("link") or job.get("url") or "").strip()
            if raw_link:
                absolute = normalize_job_url(urljoin(page_url, raw_link))
                if absolute and absolute not in seen_links:
                    seen_links.add(absolute)
                    structured_links.append(absolute)
                    continue
            job_id = str(job.get("id") or idx).strip() or str(idx)
            weak_signals.append(f"signal:embedded_job_filter:{page_norm}:{job_id}")
    return structured_links, weak_signals
