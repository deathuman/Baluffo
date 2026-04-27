from __future__ import annotations

"""Shared diagnostics for fetched discovery pages."""

from collections.abc import Callable
from urllib.parse import urlparse


def _host(url: str) -> str:
    try:
        return (urlparse(str(url or "")).netloc or "").lower().lstrip(".")
    except ValueError:
        return ""


def _host_in(host_value: str, hosts: set[str]) -> bool:
    normalized = str(host_value or "").lower().lstrip(".")
    return any(normalized == item or normalized.endswith(f".{item}") for item in hosts)


def looks_like_js_shell(
    html: str,
    *,
    short_html_threshold: int = 500,
    include_noscript_script_shell: bool = False,
) -> bool:
    text = str(html or "")
    lowered = text.lower()
    if len(text.strip()) < int(short_html_threshold) and "<script" in lowered:
        return True
    if ("<script" in lowered) and (
        'id="app"' in lowered
        or "id='app'" in lowered
        or 'id="root"' in lowered
        or "id='root'" in lowered
        or 'id="__next"' in lowered
        or "id='__next'" in lowered
    ):
        return True
    return bool(
        include_noscript_script_shell and "<noscript" in lowered and lowered.count("<script") >= 3
    )


def no_candidate_reason_detail(
    page_url: str,
    html: str,
    *,
    social_profile_hosts: set[str] | None = None,
    third_party_profile_hosts: set[str] | None = None,
    profile_hosts: set[str] | None = None,
    jobish_url_fn: Callable[[str, str], list[str]],
    short_html_threshold: int = 500,
    include_noscript_script_shell: bool = False,
) -> str:
    page_host = _host(page_url)
    if social_profile_hosts and _host_in(page_host, social_profile_hosts):
        return "social_profile_host"
    if third_party_profile_hosts and _host_in(page_host, third_party_profile_hosts):
        return "third_party_profile_host"
    if profile_hosts and _host_in(page_host, profile_hosts):
        return "profile_host"
    if looks_like_js_shell(
        html,
        short_html_threshold=short_html_threshold,
        include_noscript_script_shell=include_noscript_script_shell,
    ):
        return "js_shell"
    if not jobish_url_fn(page_url, html):
        return "no_jobish_links"
    return "homepage_links_no_candidate"


def browser_recoverable_error(error: str) -> bool:
    text = str(error or "").lower()
    return any(
        token in text
        for token in (
            "403",
            "429",
            "timeout",
            "timed out",
            "challenge",
            "cloudflare",
            "forbidden",
            "too many requests",
        )
    )
