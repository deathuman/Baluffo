"""ATS signature detection and native provider routing for static sources.

The static runner sees the HTML of a board before it decides how to parse it.  A few
boards are served by an ATS but registered as static listings: the page carries the
vendor's signature, yet the static extractor cannot read the vendor's board.  Writing a
static parser for each of them would duplicate an adapter that already exists.

This leaf owns the two pure pieces that let the registry move those rows onto the
existing provider adapter:

* ``detect_ats_signature`` -- which supported ATS, if any, the HTML names.
* ``extract_ats_route`` -- the provider board URL and locator fields the provider
  loader needs, derived from the ATS links the page already contains.

Nothing here fetches a URL.  The caller supplies HTML or an ATS URL, and a route is
returned only when the host and tenant can be read without guessing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.jobs.text_utils import clean_text

# (adapter_name, html_substring) pairs.  Kept in the same order the static runner has
# always used so a page that mentions two vendors continues to report the first one.
ATS_SIGNATURES: tuple[tuple[str, str], ...] = (
    ("teamtailor", "teamtailor"),
    ("dover", "app.dover.com"),
    ("greenhouse", "greenhouse.io"),
    ("bamboohr", "bamboohr"),
    ("workday", "myworkdayjobs"),
    ("smartrecruiters", "smartrecruiters"),
    ("lever", "lever.co"),
    ("workable", "workable"),
)

# Adapters covered by this routing path.  Dover is deliberately absent: the static
# warning still recognises it, but its board identity is resolved by its own runner.
ROUTABLE_ADAPTERS = frozenset(
    {"teamtailor", "greenhouse", "bamboohr", "workday", "smartrecruiters", "lever", "workable"}
)

_ATS_HOST_SUFFIXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("greenhouse", ("greenhouse.io",)),
    ("teamtailor", ("teamtailor.com",)),
    ("bamboohr", ("bamboohr.com",)),
    ("workday", ("myworkdayjobs.com", "myworkday.com")),
    ("smartrecruiters", ("smartrecruiters.com",)),
    ("lever", ("lever.co",)),
    ("workable", ("workable.com",)),
    ("dover", ("dover.com",)),
)

_URL_RE = re.compile(r"https?://[^\s\"'<>\\]+", re.IGNORECASE)
_PROTOCOL_RELATIVE_RE = re.compile(r"//[a-z0-9][^\s\"'<>\\/]+\.[^\s\"'<>\\]+", re.IGNORECASE)
_WARNING_ADAPTER_RE = re.compile(r"HTML contains\s+([a-z0-9_-]+)\s+signature", re.IGNORECASE)
_TRAILING_URL_PUNCTUATION = ".,;:!?)]}'\""
_LOCALE_RE = re.compile(r"^[a-z]{2}(?:-[A-Z]{2})?$")


@dataclass(frozen=True)
class AtsSignature:
    """The ATS signature found in a listing page."""

    adapter: str
    token: str


def detect_ats_signature(html: str) -> AtsSignature | None:
    """Return the first known ATS signature in ``html``, if any."""
    lowered = str(html or "").lower()
    if not lowered:
        return None
    for adapter, token in ATS_SIGNATURES:
        if token in lowered:
            return AtsSignature(adapter=adapter, token=token)
    return None


def adapter_from_warning(value: str) -> str:
    """Read the adapter named by the static runner's signature warning.

    This is the report-side counterpart to :func:`detect_ats_signature`.  The runner
    writes ``HTML contains workday signature``; matching the word ``workday`` against
    page HTML would be too loose, so report parsing uses this explicit warning shape
    instead.
    """
    match = _WARNING_ADAPTER_RE.search(str(value or ""))
    if not match:
        return ""
    adapter = match.group(1).strip().lower()
    return adapter if adapter in {name for name, _token in ATS_SIGNATURES} else ""


def adapter_for_ats_host(host: str) -> str:
    """Return the ATS adapter for a host, or an empty string for an unknown host."""
    text = str(host or "").strip().lower().removeprefix("www.")
    if not text:
        return ""
    for adapter, suffixes in _ATS_HOST_SUFFIXES:
        for suffix in suffixes:
            if text == suffix or text.endswith("." + suffix):
                return adapter
    return ""


def _clean_url(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace("&amp;", "&").replace("\\/", "/")
    text = text.strip(_TRAILING_URL_PUNCTUATION)
    if text.startswith("//"):
        text = "https:" + text
    if not text.lower().startswith(("http://", "https://")):
        return ""
    return text


def _candidate_urls(html: str, page_url: str) -> list[str]:
    """Absolute URLs from the page, ordered with the shallowest path first.

    A careers page usually links its nav to the board root before it links an
    individual job.  Sorting by path depth makes the board link win without needing a
    vendor-specific selector.
    """
    text = str(html or "").replace("\\/", "/")
    found: list[str] = []
    if page_url:
        found.append(_clean_url(page_url))
    found.extend(_clean_url(match.group(0)) for match in _URL_RE.finditer(text))
    found.extend(
        _clean_url("https:" + match.group(0)) for match in _PROTOCOL_RELATIVE_RE.finditer(text)
    )
    unique: list[str] = []
    seen: set[str] = set()
    for url in found:
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(url)

    def _sort_key(url: str) -> tuple[int, int, int]:
        parsed = urlparse(url)
        segments = [segment for segment in parsed.path.split("/") if segment]
        detail = int(any(segment.lower() in {"job", "jobs"} for segment in segments[1:-1]))
        return (detail, len(segments), len(url))

    return sorted(unique, key=_sort_key)


def _first_segment(path: str, skip: set[str] | None = None) -> str:
    skip = skip or set()
    for segment in (piece for piece in path.split("/") if piece):
        lowered = segment.lower()
        if lowered not in skip:
            return segment
    return ""


def _greenhouse_fields(host: str, path: str, query: dict[str, list[str]]) -> dict[str, str] | None:
    for key in ("for", "for_slug", "board"):
        value = clean_text((query.get(key) or [""])[0])
        if value:
            slug = value
            break
    else:
        segments = [piece for piece in path.split("/") if piece]
        if "boards" in segments:
            index = segments.index("boards")
            slug = segments[index + 1] if index + 1 < len(segments) else ""
        else:
            slug = _first_segment(path, {"embed", "job_board", "jobs"})
    slug = clean_text(slug).strip("/").lower()
    if not slug:
        return None
    return {
        "slug": slug,
        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
    }


def _lever_fields(_host: str, path: str) -> dict[str, str] | None:
    segments = [piece for piece in path.split("/") if piece]
    if "postings" in segments:
        index = segments.index("postings")
        account = segments[index + 1] if index + 1 < len(segments) else ""
    else:
        account = _first_segment(path)
    account = clean_text(account).strip("/").lower()
    if not account:
        return None
    return {
        "account": account,
        "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
    }


def _workable_fields(host: str, path: str, query: dict[str, list[str]]) -> dict[str, str] | None:
    account = ""
    for key in ("account", "organization", "company"):
        account = clean_text((query.get(key) or [""])[0])
        if account:
            break
    if not account:
        segments = [piece for piece in path.split("/") if piece]
        if "accounts" in segments:
            index = segments.index("accounts")
            account = segments[index + 1] if index + 1 < len(segments) else ""
        elif host not in {"workable.com", "apply.workable.com", "jobs.workable.com"}:
            account = host.split(".workable.com", 1)[0]
        else:
            account = _first_segment(path, {"apply", "jobs", "api", "accounts", "j"})
    account = clean_text(account).strip("/").lower()
    if not account:
        return None
    return {
        "account": account,
        "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
    }


def _teamtailor_fields(host: str, path: str) -> dict[str, str] | None:
    if host.endswith(".teamtailor.com"):
        tenant = host.split(".teamtailor.com", 1)[0]
    else:
        tenant = _first_segment(path)
    tenant = clean_text(tenant).strip("/").lower()
    if not tenant:
        return None
    board_url = f"https://{tenant}.teamtailor.com"
    return {"listing_url": f"{board_url}/jobs", "base_url": board_url}


def _bamboohr_fields(host: str) -> dict[str, str] | None:
    if not host.endswith(".bamboohr.com"):
        return None
    tenant = host.split(".bamboohr.com", 1)[0]
    if not tenant:
        return None
    base_url = f"https://{tenant}.bamboohr.com"
    return {"listing_url": f"{base_url}/careers", "api_url": f"{base_url}/careers/list"}


def _workday_fields(host: str, path: str) -> dict[str, str] | None:
    tenant = host.split(".", 1)[0]
    if not tenant:
        return None
    segments = [piece for piece in path.split("/") if piece]
    locale = segments[0] if segments and _LOCALE_RE.fullmatch(segments[0]) else ""
    site = ""
    for segment in segments[1:] if locale else segments:
        if segment.lower() not in {"job", "jobs"}:
            site = segment
            break
    if not site:
        return None
    board_path = f"/{locale}/{site}" if locale else f"/{site}"
    return {"listing_url": f"https://{host}{board_path}"}


def _smartrecruiters_fields(host: str, path: str) -> dict[str, str] | None:
    segments = [piece for piece in path.split("/") if piece]
    if "companies" in segments:
        index = segments.index("companies")
        company_id = segments[index + 1] if index + 1 < len(segments) else ""
    else:
        company_id = _first_segment(path, {"company", "jobs"})
    company_id = clean_text(company_id).strip("/").lower()
    if not company_id:
        return None
    return {
        "company_id": company_id,
        "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
    }


def _route_fields(adapter: str, url: str) -> dict[str, str] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").strip().lower()
    path = parsed.path or ""
    query = parse_qs(parsed.query)
    if adapter == "greenhouse":
        return _greenhouse_fields(host, path, query)
    if adapter == "lever":
        return _lever_fields(host, path)
    if adapter == "workable":
        return _workable_fields(host, path, query)
    if adapter == "teamtailor":
        return _teamtailor_fields(host, path)
    if adapter == "bamboohr":
        return _bamboohr_fields(host)
    if adapter == "workday":
        return _workday_fields(host, path)
    if adapter == "smartrecruiters":
        return _smartrecruiters_fields(host, path)
    return None


def _board_url(adapter: str, host: str, fields: dict[str, str]) -> str:
    if adapter == "greenhouse":
        return f"https://job-boards.greenhouse.io/{fields['slug']}"
    if adapter == "lever":
        return f"https://jobs.lever.co/{fields['account']}"
    if adapter == "workable":
        return f"https://apply.workable.com/{fields['account']}"
    if adapter == "teamtailor":
        return fields["base_url"]
    if adapter == "bamboohr":
        return fields["listing_url"]
    if adapter == "workday":
        return fields["listing_url"]
    if adapter == "smartrecruiters":
        return f"https://jobs.smartrecruiters.com/{fields['company_id']}"
    return f"https://{host}/"


def _tenant_for(adapter: str, host: str, fields: dict[str, str]) -> str:
    if adapter == "greenhouse":
        return fields.get("slug", "")
    if adapter == "lever":
        return fields.get("account", "")
    if adapter == "workable":
        return fields.get("account", "")
    if adapter == "teamtailor":
        return host.split(".teamtailor.com", 1)[0]
    if adapter == "bamboohr":
        return host.split(".bamboohr.com", 1)[0]
    if adapter == "workday":
        return host.split(".", 1)[0]
    if adapter == "smartrecruiters":
        return fields.get("company_id", "")
    return ""


def extract_ats_route(
    html: str,
    page_url: str,
    *,
    expected_adapter: str = "",
    evidence: str = "",
) -> dict[str, Any] | None:
    """Derive a provider route from an ATS host or an ATS link in the page.

    ``expected_adapter`` narrows the search to the signature the caller already saw.
    Without it, the page URL's own ATS host is tried first, then links in the HTML.
    The return value is JSON-serialisable because the static runner stores it on the
    source report.
    """
    signature = detect_ats_signature(html)
    adapter = (
        clean_text(expected_adapter)
        or (signature.adapter if signature else "")
        or adapter_from_warning(html)
    )
    if adapter and adapter not in ROUTABLE_ADAPTERS:
        return None

    page_host = (urlparse(_clean_url(page_url)).hostname or "").strip().lower()
    page_adapter = adapter_for_ats_host(page_host)
    if not adapter and page_adapter:
        adapter = page_adapter
    candidates = _candidate_urls(html, page_url)
    if page_adapter and page_adapter == adapter and page_url:
        candidates.insert(0, _clean_url(page_url))

    for candidate in candidates:
        parsed = urlparse(candidate)
        host = (parsed.hostname or "").strip().lower()
        candidate_adapter = adapter_for_ats_host(host)
        if not candidate_adapter:
            continue
        if adapter and candidate_adapter != adapter:
            continue
        fields = _route_fields(candidate_adapter, candidate)
        if not fields:
            continue
        board_url = _board_url(candidate_adapter, host, fields)
        tenant = _tenant_for(candidate_adapter, host, fields)
        if not board_url or not tenant:
            continue
        return {
            "adapter": candidate_adapter,
            "signature": signature.token if signature else "",
            "boardUrl": board_url,
            "host": host,
            "tenant": tenant,
            "fields": fields,
            "evidence": evidence or ("page_host" if candidate == page_url else "html_link"),
        }
    return None


def reclassified_registry_row(
    row: dict[str, Any],
    route: dict[str, Any],
    *,
    at: str,
    actor: str = "ats_signature_routing",
) -> dict[str, Any]:
    """Return a static row reclassified onto its native provider adapter.

    The row id and state history stay put.  Provider loaders select by ``adapter`` and
    the locator fields below; preserving the id keeps source-state continuity while the
    ``atsRouting`` evidence records why the adapter changed.
    """
    adapter = clean_text(route.get("adapter"))
    fields = route.get("fields") if isinstance(route.get("fields"), dict) else {}
    if not adapter or not fields:
        raise ValueError("an ATS route needs an adapter and locator fields")
    updated = dict(row)
    previous_listing_url = clean_text(updated.get("listing_url")) or clean_text(
        updated.get("careersUrl")
    )
    updated["adapter"] = adapter
    updated.update({str(key): value for key, value in fields.items() if value})
    board_url = clean_text(route.get("boardUrl"))
    listing_url = clean_text(fields.get("listing_url")) or board_url
    if listing_url:
        updated["listing_url"] = listing_url
        updated["careersUrl"] = board_url or listing_url
        updated["pages"] = [listing_url]
    updated["atsRouting"] = {
        "routedFromAdapter": clean_text(row.get("adapter")),
        "routedFromListingUrl": previous_listing_url,
        "boardUrl": board_url,
        "host": clean_text(route.get("host")),
        "tenant": clean_text(route.get("tenant")),
        "evidence": clean_text(route.get("evidence")),
        "routedAt": at,
    }
    updated["stateChangedAt"] = at
    updated["stateChangedBy"] = actor
    updated.setdefault("enabledByDefault", True)
    return updated
