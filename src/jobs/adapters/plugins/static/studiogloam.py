"""Static plugin for Studio Gloam (studiogloam.com).

The site is a Vite + Base44 single-page app. Its ``#careers`` openings are not in
the served HTML at all: the shell is ~8.5KB with a single anchor and no job links,
roles are rendered as ``<button>`` accordions, and the apply step is an in-page form
or ``mailto:hello@studiogloam.com``. The openings are hardcoded in the JS bundle's
openings array literal.

So the deterministic read is the bundle the page itself names. The fetch is two
steps — ``index.html``, then the hashed ``/assets/index-*.js`` entry script it
references (resolved from the HTML, never hardcoded: a Vite rebuild renames it) — and
the extraction matches the *object shape*, not the minified array name, which the
minifier also renames every build.

The openings have no per-role URL to link to (there are no job anchors even after
render), so each row carries a synthesized per-role link. See
:func:`_studiogloam_role_link` for why it is a query parameter rather than the
``#careers-<slug>`` fragment the slice plan names.

This plugin does **not** short-circuit on ``static_plugin_blocked_by_js_shell``: the
page is a JS shell by construction, which is exactly why this plugin exists. It reads
the bundle instead of escalating to Playwright, and it only returns ``[]`` when the
bundle — or the openings inside it — cannot be found.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    first_static_page,
    record_static_plugin_empty_parse,
    stamp_static_plugin_rows,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import parse_generic_location_fields
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, sanitize_public_text

# The bundle references its entry script as a root-relative hashed build artifact.
_ENTRY_SCRIPT_RE = re.compile(r'src=["\'](?P<src>[^"\']*\.js)["\']')

# A JS string literal body: any run of non-quote/non-backslash characters, or an
# escape pair. Consuming escapes as a unit is what keeps a ``\\"`` inside a string
# from terminating the match early.
_JS_STRING = r'(?:[^"\\]|\\.)*'
# A minified object key: the minifier renames identifier keys, so the key cannot be
# matched literally except for the four fields the shape is defined by.
_JS_KEY = r"[A-Za-z_$][\w$]*"

# The openings array, matched by object shape rather than by name (the minified
# array identifier was ``QN`` on the 2026-10-14 recon and is renamed every build).
#
# Each opening is ``{title:"…",type:"…",location:"…",contract:"…",summary:"…",…}``:
# its first property is the literal ``title`` key, followed by ``type``, ``location``
# and ``contract`` in that order. Requiring the actual title key prevents unrelated
# objects with a ``name`` or other string field and matching metadata from becoming
# openings (the plugin's rows bypass the shared title classifier).
_OPENING_RE = re.compile(
    r'\{\s*title:"(?P<title>' + _JS_STRING + r')"\s*,'
    r"(?:" + _JS_KEY + r':(?:"' + _JS_STRING + r'"|\[[^\]]*\])\s*,\s*)*'
    r'type:"(?P<type>' + _JS_STRING + r')"\s*,'
    r'location:"(?P<location>' + _JS_STRING + r')"\s*,'
    r'contract:"(?P<contract>' + _JS_STRING + r')"',
)

_JS_ESCAPES = {
    '"': '"',
    "'": "'",
    "\\": "\\",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "b": "\b",
    "f": "\f",
    "/": "/",
}
# ``\uXXXX`` (and the escaped surrogate pair form) before the single-character map,
# so an escaped non-ASCII character survives rather than losing its ``u``. The live
# bundle emits ``·`` literally, but a rebuild that escapes it must not corrupt it.
_JS_UNICODE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")
_JS_ESCAPE_RE = re.compile(r"\\(.)", re.DOTALL)
_SLUG_RE = re.compile(r"[^a-z0-9]+")
_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9-]")


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("studiogloam.com", "www.studiogloam.com")


def _unescape_js_string(value: str) -> str:
    """Decode the JS string escapes this site's data carries.

    The titles/locations use ``·`` (U+00B7) and the summaries contain apostrophes, so
    the escapes that matter are the quote and backslash forms, the common control
    escapes, and ``\\uXXXX`` (a rebuild may encode the separator rather than emit it
    literally — dropping its ``u`` would publish ``u00b7``).
    """

    def _replace(match: re.Match[str]) -> str:
        char = match.group(1)
        return _JS_ESCAPES.get(char, char)

    decoded = _JS_UNICODE_RE.sub(lambda match: chr(int(match.group(1), 16)), value)
    return _JS_ESCAPE_RE.sub(_replace, decoded)


def _entry_script_url(*, html: str, page_url: str) -> str:
    """The bundle URL the shell names, resolved against the page URL.

    The recon page references ``/assets/index-<hash>.js``; the path is resolved
    rather than string-concatenated so an absolute or relative ``src`` both work.
    """
    for match in _ENTRY_SCRIPT_RE.finditer(html or ""):
        src = clean_text(match.group("src"))
        if not src:
            continue
        absolute = urljoin(page_url, src)
        if absolute and urlparse(absolute).path.lower().endswith(".js"):
            return absolute
    return ""


def _role_slug(title: str) -> str:
    """URL-safe per-role slug for :func:`_studiogloam_role_link` (``Senior Technical Artist``)."""
    return _SLUG_RE.sub("-", clean_text(title).lower()).strip("-")


def _studiogloam_role_link(page_url: str, title: str) -> str:
    """Synthesized per-role link: ``https://studiogloam.com/?careers=<slug>``.

    The plan specifies ``https://studiogloam.com/#careers-<slug>`` so url-based dedup
    keeps both rows. A **fragment** cannot do that job: ``clean_text.normalize_url``
    strips the fragment, so both roles normalize to the same URL and the second row
    is dropped as a duplicate — the same mechanism that makes ``?static-role=`` the
    anchor form in :func:`static_listing_anchor_link`. A query parameter survives
    normalization, stays on-domain, and resolves to the live single-page site, so it
    is the closest faithful equivalent. The ``careers`` key (rather than
    ``static-role``) keeps the link semantically the one the plan intends.
    """
    slug = _role_slug(title)
    if not slug:
        return urljoin(page_url, "/")
    return urljoin(page_url, "/?careers=" + _SLUG_UNSAFE_RE.sub("", slug))


def _opening_rows(*, js: str, page_url: str, company: str, source_id: str) -> list[RawJob]:
    """One row per opening object found in the bundle; ``[]`` when the shape is absent."""
    rows: list[RawJob] = []
    seen_titles: set[str] = set()
    for match in _OPENING_RE.finditer(js or ""):
        title = sanitize_public_text(_unescape_js_string(match.group("title")))
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        location = sanitize_public_text(_unescape_js_string(match.group("location")))
        contract = sanitize_public_text(_unescape_js_string(match.group("contract")))
        city, country, _ = parse_generic_location_fields(location)
        link = _studiogloam_role_link(page_url, title)
        rows.append(
            {
                "sourceJobId": f"static:{source_id}:{_role_slug(title)}",
                "title": title,
                "company": company,
                "city": city,
                "country": country or "Unknown",
                "workType": "",
                "contractType": contract,
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "locationSummary": location,
            }
        )
    return rows


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, parse_jobpostings_from_html, kwargs)
    if not pages:
        return []
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Studio Gloam",
        default_source_id="studiogloam",
        default_source_name="studiogloam",
    )
    html = fetch_static_plugin_html(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html:
        return []

    bundle_url = _entry_script_url(html=html, page_url=page_url)
    bundle_js = (
        fetch_static_plugin_html(
            fetch_text=fetch_text,
            page_url=bundle_url,
            timeout_s=timeout_s,
            source_row=source_row,
        )
        if bundle_url
        else ""
    )

    rows = _opening_rows(js=bundle_js, page_url=page_url, company=company, source_id=source_id)
    if not rows and callable(try_playwright):
        # The bundle shape is the site's real source of truth, so a render is only
        # worth trying when it yielded nothing at all. The rendered DOM carries the
        # same strings, so the same extractor reads it.
        rendered_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if rendered_html:
            rows = _opening_rows(
                js=rendered_html, page_url=page_url, company=company, source_id=source_id
            )

    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
    return cleaned
