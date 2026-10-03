#!/usr/bin/env python3
"""Board identity: the shared rules for turning a job URL into a board.

This is a leaf on purpose. ``coverage_audit``, ``coverage_boards``,
``coverage_verify`` and ``coverage_probe`` all need the same answers -- which vendor
this host is, where that vendor keeps its tenant, and whether a registry row already
covers this board -- and they need them to agree exactly. A disagreement between the
audit's registry join and the board tool's candidate builder is not a cosmetic
mismatch: it puts an opening in one bucket during measurement and the other bucket
during registration, and the phase that was supposed to fix it goes looking in the
wrong place. That is not hypothetical -- it is how 4,589 rows were labelled a
collection failure when the boards were never registered.

Three rules are load-bearing, and each is measured rather than assumed.

**Identity is host + tenant, never the id string.** The registry is internally
inconsistent about trailing slashes: of 2,030 active static rows, 778 end in ``/``
and 1,252 do not. Comparing id strings calls one real board two boards, so one of
them gets proposed for registration while the other already serves the openings.

**Identity is never the studio label.** One board is registered both as "Lost Boys
Interactive" and "Lost Boys Interactive (Embracer Group)". A label-keyed join reports
that covered board as missing, which reads as a coverage bug and is a data-modelling
one.

**Tenant position is per platform, and guessing it is the worst available outcome.**
A multi-tenant platform collapsed onto one host-root row leaves the registry looking
served while every tenant's openings stay missing. ``hrmos.co`` is 31 tenants and 845
openings on the apex host -- Capcom, Square Enix, Cygames, Nexon, Game Freak, Spike
Chunsoft -- and a host-root rule reports all 31 as one board.

Also kept here: URL normalisation for cross-feed matching, which is not the same
normalisation as board identity. Match keys keep the query string, because for
several vendors the job id *is* the query (``?ashby_jid=``) and dropping it collapses
up to 218 distinct openings onto one URL.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

# --- URL normalisation -----------------------------------------------------


def normalize_url(value: Any) -> str:
    """Normalised posting URL for exact cross-feed matching.

    The query string is **kept**: for several vendors it is the job identifier
    (``?ashby_jid=``), so dropping it collapses every opening at a studio onto one
    URL -- measured at 218 distinct jobs sharing a single base URL on one board.
    Only the scheme, a leading ``www.``, and the fragment are dropped.
    """
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text).split("#", 1)[0].rstrip("/")
    return re.sub(r"^www\.", "", text)


def host_of(url: Any) -> str:
    """Host of a URL, lowercased and without a leading ``www.``."""
    parsed = urlparse(str(url or "") if "//" in str(url or "") else f"//{url}")
    return (parsed.netloc or "").lower().removeprefix("www.")


def board_root(url: str) -> str:
    """The board's own root, so a 404 on a job path is not read as a dead board."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"


def board_key(url: str) -> str:
    """Grouping key for "which board does this job belong to"."""
    parsed = urlparse(url)
    host = host_of(url)
    if host.endswith(".recruitee.com"):
        return f"{host}/api/offers/"
    if host.endswith(".bamboohr.com"):
        return f"{host}/careers"
    if host.endswith(".breezy.hr"):
        return f"{host}/"
    if host.endswith(".pinpointhq.com"):
        return f"{host}/postings.json"
    if host in {"job-boards.greenhouse.io", "jobs.lever.co", "jobs.ashbyhq.com"}:
        segments = [s for s in parsed.path.split("/") if s]
        return f"{host}/{segments[0]}" if segments else host
    return board_root(url)


def segments_of(url: str) -> list[str]:
    return [segment for segment in urlparse(url).path.split("/") if segment]


# --- Adapters the runtime can actually fetch -------------------------------
#
# A vendor outside this set is reported as `unsupported_vendor` rather than silently
# dropped, because the opening is still real and the adapter gap is then a visible,
# countable backlog instead of a silent gap in the numbers.

ADAPTER_ID_FORMAT = {
    "ashby": "board_url",
    # Anything without a structured adapter is still reachable: the registry holds
    # 2,030 active static rows, and a scraped careers page is how Baluffo covers
    # most studios. Without this fallback the 697 unknown-host boards would be
    # discarded, which is exactly the "leaving openings behind" failure the sweep
    # exists to prevent.
    "static": "listing_url",
    "bamboohr": "listing_url",
    "breezy": "board_url",
    "greenhouse": "slug",
    "jazzhr": "board_url",
    "lever": "account",
    "oracle_hcm": "listing_url",
    "personio": "feed_url",
    "pinpoint": "api_url",
    "recruitee": "api_url",
    "smartrecruiters": "company_id",
    "teamtailor": "listing_url",
    "workable": "account",
    "workday": "listing_url",
}

# Host patterns that identify a vendor. Ordered: first match wins, so put the
# specific multi-tenant patterns above the generic ones.
HOST_RULES: tuple[tuple[str, str], ...] = (
    (r"(^|\.)job-boards\.greenhouse\.io$", "greenhouse"),
    (r"(^|\.)boards\.greenhouse\.io$", "greenhouse"),
    (r"^jobs\.lever\.co$", "lever"),
    (r"(^|\.)jobs\.ashbyhq\.com$", "ashby"),
    (r"(^|\.)jobs\.smartrecruiters\.com$", "smartrecruiters"),
    (r"(^|\.)recruitee\.com$", "recruitee"),
    (r"(^|\.)bamboohr\.com$", "bamboohr"),
    (r"(^|\.)breezy\.hr$", "breezy"),
    (r"(^|\.)teamtailor\.com$", "teamtailor"),
    (r"(^|\.)apply\.workable\.com$", "workable"),
    (r"(^|\.)pinpointhq\.com$", "pinpoint"),
    (r"(^|\.)myworkdayjobs\.com$", "workday"),
    (r"(^|\.)oraclecloud\.com$", "oracle_hcm"),
    (r"(^|\.)personio\.(com|de)$", "personio"),
    (r"(^|\.)applytojob\.com$", "jazzhr"),
    (r"(^|\.)jobs\.feishu\.cn$", "feishu"),
    (r"(^|\.)jobs\.hiretalent\.com$", "hirentalent"),
    (r"(^|\.)hrmos\.co$", "hrmos"),
    (r"(^|\.)csod\.com$", "csod"),
)

# Canonical host per adapter, for reading a registry id that stores only a slug.
ADAPTER_CANONICAL_HOST = {
    "greenhouse": "job-boards.greenhouse.io",
    "lever": "jobs.lever.co",
    "ashby": "jobs.ashbyhq.com",
    "smartrecruiters": "jobs.smartrecruiters.com",
    "workable": "apply.workable.com",
}

# Where a platform keeps its tenant. Four shapes occur in the data:
#
#   subdomain  kurogame.jobs.feishu.cn, nintendoeurope.csod.com
#   seg0       job-boards.greenhouse.io/2kczech, jobs.jobvite.com/asus/job/...
#   after      hrmos.co/pages/capcom/..., herp.careers/v1/charabank/...
#   host       blooberteam.recruitee.com, jobs.ea.com
TENANT_SOURCE = {
    r"(^|\.)recruitee\.com$": "host",
    r"(^|\.)bamboohr\.com$": "host",
    r"(^|\.)breezy\.hr$": "host",
    r"(^|\.)pinpointhq\.com$": "host",
    r"(^|\.)personio\.(com|de)$": "host",
    r"(^|\.)applytojob\.com$": "host",
    r"^jobs\.lever\.co$": "seg0",
    r"(^|\.)job-boards\.greenhouse\.io$": "seg0",
    r"(^|\.)boards\.greenhouse\.io$": "seg0",
    r"(^|\.)jobs\.smartrecruiters\.com$": "seg0",
    r"(^|\.)jobs\.ashbyhq\.com$": "seg0",
    r"(^|\.)teamtailor\.com$": "seg0",
    r"(^|\.)myworkdayjobs\.com$": "seg0",
    r"(^|\.)jobs\.jobvite\.com$": "seg0",
    r"(^|\.)herp\.careers$": "after",
    r"(^|\.)app\.mokahr\.com$": "after",
    # hrmos.co/pages/<tenant>/jobs/<id> -- the tenant is the segment after "pages",
    # not the host. Measured 31 tenants on the apex host covering 845 openings,
    # including Capcom, Square Enix, Cygames, Nexon, Game Freak and Spike Chunsoft.
    r"(^|\.)hrmos\.co$": "after",
    r"(^|\.)jobs\.feishu\.cn$": "subdomain",
    r"(^|\.)jobs\.hiretalent\.com$": "subdomain",
    r"(^|\.)csod\.com$": "subdomain",
    r"(^|\.)apply\.workable\.com$": "seg0",
}

# Path segments that are platform chrome, never a tenant. Stored lowercased
# because lookup lowercases the candidate: "en_US" listed with its original case
# would never match, and the locale segment would be taken for a tenant.
_TENANT_SKIP = {
    "v1",
    "v2",
    "index",
    "ux",
    "ats",
    "careersite",
    "social-recruitment",
    "recruitment",
    "jobs",
    "career",
    "pages",
    "position",
    "requisition",
    "en_us",
    "en_gb",
}

STATUS_NEW = "new_candidate"
STATUS_ALREADY = "already_registered"
STATUS_UNSUPPORTED = "unsupported_vendor"
STATUS_NO_TENANT = "no_tenant_resolved"


def adapter_for_host(host: str) -> str:
    lowered = host.lower()
    for pattern, adapter in HOST_RULES:
        if re.search(pattern, lowered):
            return adapter
    return "static"


def tenant_source_for(host: str) -> str:
    lowered = host.lower()
    for pattern, source in TENANT_SOURCE.items():
        if re.search(pattern, lowered):
            return source
    return "host"


def resolve_tenant(host: str, segments: Sequence[str]) -> str:
    source = tenant_source_for(host)
    if source == "host":
        return host
    if source == "subdomain":
        return host.split(".", 1)[0]
    if source == "seg0":
        return segments[0] if segments else ""
    # "after": first segment that is not platform chrome.
    return next((s for s in segments if s.lower() not in _TENANT_SKIP), "")


def build_candidate(source_url: str, *, company: str = "") -> dict[str, Any]:
    """Derive one board descriptor from a missed opening's URL."""
    host = host_of(source_url)
    segments = segments_of(source_url)
    adapter = adapter_for_host(host)
    tenant = resolve_tenant(host, segments)
    if adapter == "oracle_hcm":
        tenant = (
            next((s for s in segments if s not in {"hcmUI", "CandidateExperience"}), "") or host
        )
    return describe_board(adapter=adapter, host=host, tenant=tenant, company=company)


def describe_board(*, adapter: str, host: str, tenant: str, company: str = "") -> dict[str, Any]:
    """Fill in a board's locator fields and registry id from adapter + host + tenant.

    Split out from :func:`build_candidate` so a caller holding a resolved board
    identity -- rather than a job URL -- can produce the same row. Regenerating the
    curated catalogue needs exactly that: audit output carries adapter, host and
    tenant but not the original job URL, and rebuilding from tenant is what keeps an
    ``api_url`` on every row where one exists.
    """
    scheme = "https"
    row: dict[str, Any] = {"adapter": adapter, "host": host, "tenant": tenant, "company": company}
    if adapter not in ADAPTER_ID_FORMAT:
        row["status"] = STATUS_UNSUPPORTED
        return row
    if not tenant:
        row["status"] = STATUS_NO_TENANT
        return row

    base = f"{scheme}://{host}"
    # `api_url` is load-bearing, not decoration: discovery's `endpoint_url` probes the
    # first of api_url, feed_url, board_url, listing_url, and auto-approval requires a
    # positive job count. A row carrying only `board_url` sends the probe at the
    # human-facing page, which for a single-page app ships no job links -- so the
    # board reads as healthy with zero jobs and sits in pending forever.
    #
    # Measured on this catalogue: the only two adapters that carried an api_url
    # (smartrecruiters, recruitee) were the only provider adapters where every board
    # landed, while greenhouse, lever, ashby, workday, bamboohr and breezy landed none
    # -- despite APIs that return 18-122 rows on a plain GET.
    if adapter == "greenhouse":
        row.update(
            {
                "slug": tenant,
                "api_url": f"https://boards-api.greenhouse.io/v1/boards/{tenant.lower()}/jobs?content=true",
                "id": f"greenhouse:slug:{tenant.lower()}",
            }
        )
    elif adapter == "lever":
        row.update(
            {
                "account": tenant,
                "api_url": f"https://api.lever.co/v0/postings/{tenant.lower()}?mode=json",
                "id": f"lever:account:{tenant.lower()}",
            }
        )
    elif adapter == "ashby":
        board_url = f"{base}/{tenant}"
        row.update(
            {
                "board_url": board_url,
                "api_url": f"https://api.ashbyhq.com/posting-api/job-board/{tenant}",
                "id": f"ashby:board_url:{board_url}",
            }
        )
    elif adapter == "smartrecruiters":
        org = re.sub(r"[^A-Za-z0-9]", "", tenant)
        row.update(
            {
                "company_id": org,
                "api_url": f"https://api.smartrecruiters.com/v1/companies/{org}/postings",
                "id": f"smartrecruiters:company_id:{org.lower()}",
            }
        )
    elif adapter == "recruitee":
        api = f"{base}/api/offers/"
        row.update({"api_url": api, "id": f"recruitee:api_url:{api}"})
    elif adapter == "personio":
        feed = f"{base}/xml"
        row.update({"feed_url": feed, "id": f"personio:feed_url:{feed}"})
    elif adapter == "pinpoint":
        api = f"{base}/postings.json"
        row.update({"api_url": api, "id": f"pinpoint:api_url:{api}"})
    elif adapter == "bamboohr":
        url = f"{base}/careers"
        row.update({"listing_url": url, "id": f"bamboohr:listing_url:{url}"})
    elif adapter == "breezy":
        url = f"{base}/"
        row.update(
            {
                "board_url": url,
                # Verified: https://feeds.breedy.hr/<tenant> does not resolve, while
                # https://<tenant>.breezy.hr/json returns the posting list.
                "api_url": f"{base}/json",
                "id": f"breezy:board_url:{url}",
            }
        )
    elif adapter == "jazzhr":
        url = f"{base}/apply"
        row.update({"board_url": url, "id": f"jazzhr:board_url:{url}"})
    elif adapter == "teamtailor":
        url = f"{base}/jobs"
        row.update({"listing_url": url, "id": f"teamtailor:listing_url:{url}"})
    elif adapter == "workable":
        row.update({"account": tenant, "id": f"workable:account:{tenant}"})
    elif adapter == "workday":
        # The tenant is the board's path segment, which is also the CXS site name.
        url = f"{base}/{tenant}"
        row.update({"listing_url": url, "id": f"workday:listing_url:{url}"})
    elif adapter == "oracle_hcm":
        row.update({"id": f"oracle_hcm:listing_url:{base}", "listing_url": base})
    elif adapter == "static":
        # Scraper candidate. A single-site careers host is its own board; a
        # multi-tenant platform needs one row per tenant or the whole tenant's
        # openings stay missing behind a row that looks served.
        if tenant == host:
            url = base
            row.update({"listing_url": url, "id": f"static:listing_url:{url}"})
        else:
            scoped = f"{base}/{tenant}"
            row.update(
                {"listing_url": scoped, "id": f"static:listing_url:{scoped}", "tenantScoped": True}
            )
    row["status"] = STATUS_NEW
    return row


def _normalise_tenant(host: str, tenant: str) -> str:
    """A tenant equal to the host is not a tenant; it means "single-site board".

    Both sides must apply this or the same board gets two identities: the registry
    row resolves its tenant from the URL's last path segment (``/jobs`` -> ``jobs``)
    while a candidate resolves from the job URL's host (``x.com``), and the two
    never compare equal. That would make every single-site static board look
    unregistered.
    """
    return "" if tenant.lower() == host.lower() else tenant.strip("/").lower()


def registry_identity(registry_id: str) -> tuple[str, str, str] | None:
    """Parse a registry id into ``(adapter, host, tenant)``.

    Identity is host + tenant, never the id string and never the studio label.
    The id string is not a safe key: the registry is internally inconsistent about
    trailing slashes -- of 2,030 active static rows, 778 end in ``/`` and 1,252 do
    not -- so ``static:listing_url:https://x.com/jobs`` and
    ``static:listing_url:https://x.com/jobs/`` are both real rows for one board.

    Tenant resolution reuses :func:`resolve_tenant`, the same function candidates
    use. Using a different rule here is what made the two sides disagree.
    """
    parts = str(registry_id or "").split(":", 2)
    if len(parts) < 3:
        return None
    adapter, value = parts[0].lower(), parts[2]
    if "/" in value or value.startswith("http"):
        host = host_of(value)
        tenant = resolve_tenant(host, segments_of(value))
    else:
        host = ADAPTER_CANONICAL_HOST.get(adapter, "")
        tenant = value
    return (adapter, host, _normalise_tenant(host, tenant))


def candidate_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str]:
    """The comparable identity of a candidate built by :func:`build_candidate`."""
    host = str(candidate.get("host") or "").lower()
    tenant = str(candidate.get("tenant") or "")
    return (str(candidate.get("adapter") or ""), host, _normalise_tenant(host, tenant))


def registry_identities(values: Any) -> set[tuple[str, str, str]]:
    """Accept either raw registry ids or already-parsed identities."""
    out: set[tuple[str, str, str]] = set()
    for value in values or ():
        if isinstance(value, tuple):
            out.add(value)
            continue
        parsed = registry_identity(str(value))
        if parsed is not None:
            out.add(parsed)
    return out


def covers_board(registry_id: str, candidate: Mapping[str, Any]) -> bool:
    """Whether a registry row already covers this board."""
    return registry_identity(registry_id) == candidate_identity(candidate)
