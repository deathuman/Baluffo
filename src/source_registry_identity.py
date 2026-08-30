"""Identity and URL helpers for source registry rows."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunsplit

# Matches an optional leading ``www.`` directly after the https scheme.
_WWW_PREFIX_RE = re.compile(r"^(https://)www\.(.*)$", re.IGNORECASE)


def canonicalize_careers_url(url: str | None) -> str:
    """Collapse a careers URL to a canonical identity for twin detection.

    Normalizations applied, in order:
      * ``http://`` => ``https://``
      * drop a single leading ``www.``
      * drop the URL fragment (``#...``)
      * lowercase host and path, strip any trailing ``/``

    The query string is preserved (unnormalized) so that distinct
    parameterized pages (e.g. two different job-search queries) do not
    accidentally merge into a false twin; the real twins in this dataset
    differ only by scheme/www/slash/fragment and have no query.

    Returns ``""`` for blank/unsupported URLs so callers can skip them.

    This is the single authoritative rule shared by the commit-time repo
    guardrail (``tools/repo_health/source_registry_duplicate_url_policy.py``)
    and the runtime registry conflict automations: both must agree on what
    counts as the "same" careers URL so a twin demoted at commit time is also
    demoted automatically when live discovery re-introduces it.
    """
    if not url:
        return ""
    collapsed = url.strip().rstrip("/")
    if collapsed.lower().startswith("http://"):
        collapsed = "https://" + collapsed[len("http://") :]
    collapsed = collapsed.split("#", 1)[0]
    match = _WWW_PREFIX_RE.match(collapsed)
    if match:
        collapsed = match.group(1) + match.group(2)
    parsed = urlparse(collapsed)
    host = (parsed.hostname or "").lower()
    if not host:
        return ""
    path = (parsed.path or "").lower().rstrip("/")
    query = parsed.query
    return f"{host}{path}?{query}" if query else f"{host}{path}"


def source_careers_url_key(row: dict[str, Any]) -> str:
    """Canonical twin identity for a registry row.

    Mirrors the guardrail's row URL preference: ``board_url`` first, then
    ``listing_url``, then ``url``. Returns ``""`` when the row has no usable
    careers URL.
    """
    url = str(row.get("board_url") or row.get("listing_url") or row.get("url") or "").strip()
    return canonicalize_careers_url(url)


def source_identity(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    explicit_id = str(row.get("id") or "").strip()
    if explicit_id:
        return explicit_id.lower()
    for key in (
        "id",
        "slug",
        "account",
        "company_id",
        "api_url",
        "feed_url",
        "board_url",
        "site_path",
        "listing_url",
        "name",
    ):
        value = str(row.get(key) or "").strip().lower()
        if value:
            return f"{adapter}:{key}:{value}"
    digest = hashlib.sha1(
        json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"{adapter}:unknown:{digest}"


def ensure_source_id(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["id"] = source_identity(normalized)
    return normalized


_PROVIDER_ID_FIELDS_BY_ADAPTER = {
    "greenhouse": {"slug"},
    "lever": {"account"},
    "smartrecruiters": {"company_id"},
    "workable": {"account"},
    "jazzhr": {"board_url"},
    "oracle_hcm": {"listing_url", "site_path"},
}


def provider_fields_from_source_id(source_id: Any) -> dict[str, str]:
    text = str(source_id or "").strip()
    if not text:
        return {}
    parts = text.split(":", 2)
    if len(parts) != 3:
        return {}
    adapter, field, value = (part.strip() for part in parts)
    adapter = adapter.lower()
    field = field.lower()
    if field not in _PROVIDER_ID_FIELDS_BY_ADAPTER.get(adapter, set()):
        return {}
    value = value.strip()
    if not value:
        return {}
    return {"adapter": adapter, field: value}


def provider_fields_from_row_identity(row: dict[str, Any]) -> dict[str, str]:
    return provider_fields_from_source_id(row.get("id") or row.get("sourceId"))


def normalize_source_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return ""
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, host, path, "", ""))


_STATIC_ALIAS_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "msclkid",
}


def static_listing_url_alias(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = (parsed.scheme or "https").lower()
    if scheme not in {"http", "https"}:
        return ""
    netloc = _static_alias_netloc(parsed, scheme)
    if not netloc:
        return ""
    return urlunsplit(
        ("https", netloc, _static_alias_path(parsed.path), _static_alias_query(parsed.query), "")
    )


def _static_alias_netloc(parsed: Any, scheme: str) -> str:
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return ""
    host = host.removeprefix("www.")
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        return f"{host}:{port}"
    return host


def _static_alias_path(raw_path: str) -> str:
    path = "/" + ((raw_path or "/").strip() or "/").lstrip("/")
    path = path.lower()
    for suffix in ("/index.html", "/index.htm"):
        if path.endswith(suffix):
            path = path[: -len(suffix)] or "/"
            break
    return path.rstrip("/") or "/"


def _static_alias_query(raw_query: str) -> str:
    query_items: list[tuple[str, str]] = []
    for key, value in parse_qsl(raw_query, keep_blank_values=True):
        normalized_key = key.strip().lower()
        if _drop_static_alias_query_param(normalized_key, value):
            continue
        query_items.append((normalized_key, str(value).strip()))
    return urlencode(sorted(query_items), doseq=True)


def _drop_static_alias_query_param(key: str, value: Any) -> bool:
    if not key:
        return True
    if key.startswith("utm_") or key in _STATIC_ALIAS_TRACKING_PARAMS:
        return True
    return key == "page" and str(value).strip() in {"", "1"}


def static_listing_url_aliases(row: dict[str, Any]) -> set[str]:
    if str(row.get("adapter") or "").strip().lower() != "static":
        return set()
    aliases: set[str] = set()
    for key in ("id", "sourceId", "listing_url", "careersUrl", "url", "endpointUrl", "finalUrl"):
        value = str(row.get(key) or "").strip()
        if not value:
            continue
        if key in {"id", "sourceId"} and "http" in value:
            value = value[value.find("http") :]
        alias = static_listing_url_alias(value)
        if alias:
            aliases.add(alias)
    return aliases


def source_endpoint_url(row: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = str(value or "").strip()
            if text:
                return text
    return ""


def source_url_fingerprint(row: dict[str, Any]) -> str:
    return normalize_source_url(source_endpoint_url(row))


def _clean_family_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def source_family_key(row: dict[str, Any]) -> str:
    studio = _clean_family_token(row.get("studio"))
    if studio:
        return studio
    return _clean_family_token(row.get("name"))


def unique_sources(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(ensure_source_id(row))
    return out
