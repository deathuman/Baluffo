"""Rows from JSON embedded in a listing page, when anchors carry none.

Host-agnostic by design. The shapes are the ones boards actually embed: schema.org
``JobPosting`` blocks, a ``__NEXT_DATA__`` payload (Bungie's careers page carries its
Greenhouse rows verbatim inside one), and job-shaped objects in generic script arrays.
No host rule is needed, so a board that changes platform keeps working.

A row is emitted only when it carries a title and a fetchable URL. CMS assets in the same
payloads carry both too, which is why the posting-shape guard also rejects asset keys
(``fileName``, ``contentType``, ...) and non-``JobPosting`` ``@type`` nodes -- and why
``name`` alone is not accepted as a title (nav and metadata entries pair ``name`` with
``url``).

AI boundary owns: embedded-JSON row extraction for the static listing lane.
AI boundary implement in: this file for the extraction; the lane that emits rows lives in
``static_listing_rows.py`` and the JSON primitive in ``src/shared/json_extract.py``.
AI boundary verify: `npm run lint:repo-guardrails` plus focused embedded-JSON tests.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from html import unescape
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url
from src.shared.json_extract import extract_json_array, next_data_payload
from src.shared.json_shapes import as_json_object as _as_dict

# Measured key names, not guesses. `RecruitPostName` / `PostURL` are Tencent's
# (`/tencentcareer/api/post/Query`), `job_post_url` Feishu's, `absolute_url` Greenhouse's,
# `title`/`url` the common case. A board's platform names the field its own API uses, and
# there is no way to know that name in advance -- one key per platform is the per-platform
# parser this avoids. Extra keys are harmless: a node still has to carry a fetchable URL and
# pass the asset / `@type` / navigation guards.
_TITLE_KEYS = (
    "title",
    "position",
    "role",
    "jobtitle",
    "jobtitlename",
    "recruitpostname",
    "positionname",
    "jobname",
    "recruitposttitle",
    "positiontitle",
)
# Deliberately many spellings of "the link to this posting". A board that ships its listings
# in its own XHR names the field its platform uses, and there is no way to know that name in
# advance -- guessing one per platform is the per-platform parser this avoids. Extra keys cost
# nothing: a node still has to pass the asset and `@type` guards and carry a title.
_URL_KEYS = (
    "absolute_url",
    "joblink",
    "joburl",
    "job_url",
    "job_post_url",
    "jobposturl",
    "posturl",
    "position_url",
    "positionurl",
    "posting_url",
    "postingurl",
    "career_url",
    "careerurl",
    "careers_url",
    "vacancy_url",
    "vacancyurl",
    "applyurl",
    "apply_url",
    "hostedurl",
    "hosted_url",
    "externalpath",
    "external_path",
    "url",
    "link",
    "href",
)
_ASSET_KEYS = frozenset({"filename", "contenttype", "contenttypeid", "filesize", "width", "height"})
_EMPLOYER_KEYS = ("company_name", "company", "employer", "comname", "companyname")
_ID_KEYS = (
    "id",
    "internal_job_id",
    "requisition_id",
    "jobid",
    "job_id",
    "postingid",
    "posting_id",
    "postid",
    "recruitpostid",
)
# `LocationName` is Tencent's; `location` a string or object is the common case; `city` alone is
# a weaker hint but is all some APIs carry.
_LOCATION_KEYS = (
    "location",
    "joblocation",
    "locationname",
    "locations",
    "workplace",
    "city",
    "workcity",
    "worklocation",
)
_POSTED_KEYS = (
    "first_published",
    "dateposted",
    "date_posted",
    "publisheddate",
    "published_at",
    "updated_at",
    "updatedat",
)
_EMPLOYMENT_KEYS = ("employmenttype", "employment_type", "type", "contracttype")

_JSON_LD_RE = re.compile(
    r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
)
_SCRIPT_RE = re.compile(r"(?is)<script\b[^>]*>(.*?)</script>")
_ARRAY_START_RE = re.compile(r"[=:]\s*(\[)")

# A payload can nest arbitrarily deep; the walk is iterative and budgeted so a pathological
# document costs a bounded amount of work rather than a recursion error.
_WALK_BUDGET = 20000
_MAX_ARRAY_STARTS_PER_SCRIPT = 12


def rows_from_json_payload(
    payload_text: str, *, board_url: str, fallback_company: str = ""
) -> list[RawJob]:
    """Rows from a standalone JSON body, by the same posting-shape rule.

    The rendered-JSON lane captures the responses a page's own app fetches -- bodies that are
    JSON documents rather than HTML with JSON inside it -- so this is the entry point that
    skips the script-tag scan and hands the document straight to the walker. One definition of
    "this object is a posting" for both lanes: the payload is a different *container*, not a
    different rule, and a second rule is how the two would drift.
    """
    text = str(payload_text or "").strip()
    if not text:
        return []
    try:
        document = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return []
    rows: list[RawJob] = []
    seen: set[str] = set()
    _collect_rows(
        document,
        rows=rows,
        seen=seen,
        board_url=board_url,
        fallback_company=fallback_company,
        budget=_WALK_BUDGET,
    )
    return rows


def extract_embedded_json_rows(
    html_text: str, *, board_url: str, fallback_company: str = ""
) -> list[RawJob]:
    """Posting rows embedded in ``html_text``, deduplicated by link."""
    rows: list[RawJob] = []
    seen: set[str] = set()
    budget = _WALK_BUDGET
    for payload in _embedded_payloads(html_text):
        if budget <= 0:
            break
        budget = _collect_rows(
            payload,
            rows=rows,
            seen=seen,
            board_url=board_url,
            fallback_company=fallback_company,
            budget=budget,
        )
    return rows


def _embedded_payloads(html_text: str) -> Iterator[Any]:
    yield from _json_ld_payloads(html_text)
    next_data = next_data_payload(html_text)
    if next_data is not None:
        yield next_data
    yield from _generic_script_arrays(html_text)


def _json_ld_payloads(html_text: str) -> Iterator[Any]:
    for match in _JSON_LD_RE.finditer(str(html_text or "")):
        try:
            yield json.loads(unescape(match.group(1).strip()))
        except json.JSONDecodeError:
            continue


def _generic_script_arrays(html_text: str) -> Iterator[list[Any]]:
    for match in _SCRIPT_RE.finditer(str(html_text or "")):
        whole = match.group(0)
        if "__NEXT_DATA__" in whole or "application/ld+json" in whole:
            continue
        body = match.group(1)
        starts = [found.start(1) for found in _ARRAY_START_RE.finditer(body)]
        stripped = body.lstrip()
        if stripped.startswith("["):
            starts.insert(0, len(body) - len(stripped))
        for start in starts[:_MAX_ARRAY_STARTS_PER_SCRIPT]:
            payload = extract_json_array(body, start)
            if payload:
                yield payload


def _collect_rows(
    root: Any,
    *,
    rows: list[RawJob],
    seen: set[str],
    board_url: str,
    fallback_company: str,
    budget: int,
) -> int:
    stack: list[Any] = [root]
    while stack and budget > 0:
        budget -= 1
        node = stack.pop()
        if isinstance(node, dict):
            row = _row_from_node(
                node,
                board_url=board_url,
                fallback_company=fallback_company,
            )
            if row is not None and row["jobLink"] not in seen:
                seen.add(row["jobLink"])
                rows.append(row)
            # Reversed push keeps the walk in document order despite the LIFO stack.
            stack.extend(reversed(list(node.values())))
        elif isinstance(node, list):
            stack.extend(reversed(node))
    return budget


def _row_from_node(node: dict[str, Any], *, board_url: str, fallback_company: str) -> RawJob | None:
    lower = {str(key).lower(): value for key, value in node.items()}
    if any(key in lower for key in _ASSET_KEYS):
        return None
    node_type = clean_text(lower.get("@type"))
    if node_type and node_type.lower() != "jobposting":
        return None
    title = _first_text(lower, _TITLE_KEYS)
    link = _first_text(lower, _URL_KEYS)
    if not title or not link:
        return None
    link = normalize_url(urljoin(board_url, link))
    if not link.startswith(("http://", "https://")):
        return None
    company = _first_text(lower, _EMPLOYER_KEYS)
    if not company:
        for org_key in ("hiringorganization", "organization"):
            company = clean_text(_as_dict(lower.get(org_key)).get("name"))
            if company:
                break
    company = company or clean_text(fallback_company) or "Unknown"
    location_text = _location_text(lower)
    location_details = normalize_location_details(location_text)
    identifier = _first_text(lower, _ID_KEYS)
    if identifier in {"0", "-1"}:
        # Tencent's `/post/query` answers `Id: 0` on every post and carries the real
        # identifier in `PostId` / `RecruitPostId`. Accepting `0` would give every row on the
        # board the same `sourceJobId`, which dedupes them into one.
        identifier = _first_text(
            lower, ("postid", "recruitpostid", "jobid", "job_id", "postingid", "posting_id")
        )
    return {
        "sourceJobId": f"embedded:{identifier or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": company,
        "city": clean_text(location_details.get("city")) or location_text,
        "country": clean_text(location_details.get("country")) or "Unknown",
        "workType": "",
        "contractType": _first_text(lower, _EMPLOYMENT_KEYS),
        "jobLink": link,
        "sector": "Game",
        "postedAt": _first_text(lower, _POSTED_KEYS),
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def _first_text(lower: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = clean_text(lower.get(key))
        if text:
            return text
    return ""


def _location_text(lower: dict[str, Any]) -> str:
    for key in _LOCATION_KEYS:
        value = lower.get(key)
        if isinstance(value, dict):
            text = clean_text(value.get("name") or value.get("locationName"))
            if not text:
                address = _as_dict(value.get("address"))
                text = ", ".join(
                    part
                    for part in (
                        clean_text(address.get("addressLocality")),
                        clean_text(address.get("addressCountry")),
                    )
                    if part
                )
        elif isinstance(value, list):
            parts = [
                clean_text(item.get("name") or item.get("locationName"))
                if isinstance(item, dict)
                else clean_text(item)
                for item in value
            ]
            text = "; ".join(part for part in parts if part)
        else:
            text = clean_text(value)
        if text:
            return text
    return ""
