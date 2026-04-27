from __future__ import annotations

"""Shared diagnostics for fetched discovery pages."""


def looks_like_js_shell(html: str, *, short_html_threshold: int = 500) -> bool:
    text = str(html or "")
    lowered = text.lower()
    if len(text.strip()) < int(short_html_threshold) and "<script" in lowered:
        return True
    return bool(
        ("<script" in lowered)
        and (
            'id="app"' in lowered
            or "id='app'" in lowered
            or 'id="root"' in lowered
            or "id='root'" in lowered
            or 'id="__next"' in lowered
            or "id='__next'" in lowered
        )
    )


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
