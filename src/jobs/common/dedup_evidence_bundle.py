"""Shared low-risk computation helpers for dedup evidence.

Pure functions used by multiple dedup evidence modules.
No side effects; no imports from reporting_dedup_evidence.

AI boundary owns: shared dedup evidence bundle computation and row aggregation helpers.
AI boundary implement in: this file for reusable evidence computations; report contracts and UI-facing rows stay in contract/reporting leaves.
AI boundary search before contracts: dedup evidence provider/static helpers, audit gate helpers, and evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.json_shapes import json_object_rows

OUTLIER_REASON_KEYS = (
    "multi_location_strong_identity",
    "location_divergence_without_strong_identity",
    "provider_static_disagreement",
    "large_other_source_bundle",
    "sparse_title_company_bundle",
    "unknown",
)
IDENTITY_SHAPE_KEYS = (
    "shared_job_detail_url",
    "shared_listing_or_category_url",
    "many_unique_urls_same_title",
    "provider_id_backed",
    "missing_url_and_ids",
    "mixed_or_unknown_identity",
)
IDENTITY_QUALITY_KEYS = (
    "provider_id_strong",
    "shared_detail_url_strong",
    "shared_listing_url_weak",
    "many_urls_same_host_weak",
    "many_urls_many_hosts_weak",
    "other_source_id_untrusted",
    "missing_identity",
    "unknown",
)
NON_PROVIDER_IDENTITY_PROVENANCE_KEYS = (
    "google_sheets_row_identity",
    "url_derived_identity",
    "category_or_directory_identity",
    "opaque_other_source_identity",
    "mixed_non_provider_identity",
    "none",
    "unknown",
)

CATEGORY_TITLE_TERMS = frozenset(
    {
        "accounting",
        "art",
        "design",
        "engineering",
        "finance",
        "hr",
        "human resources",
        "marketing",
        "operations",
        "production",
        "qa",
        "quality assurance",
        "research development",
        "research-development",
        "sales",
        "software development",
        "software development engineering",
        "software-development-&-engineering",
    }
)
GENERIC_ROLE_BUCKET_TERMS = frozenset(
    {
        "account",
        "account-management",
        "localization",
        "management",
        "product-management",
        "program-management",
        "programming",
        "system-design",
    }
)
ROLE_BUCKET_TITLE_TERMS = frozenset(
    {
        "account management",
        "account-management",
        "community management",
        "community-management",
        "localization",
        "product management",
        "product-management",
        "program management",
        "program-management",
        "programming",
        "project management",
        "project-management",
        "system design",
        "system-design",
    }
)
GENERIC_LISTING_PATH_SEGMENTS = frozenset(
    {
        "career",
        "careers",
        "departments",
        "jobs",
        "join",
        "join-us",
        "openings",
        "opportunities",
        "positions",
        "teams",
        "vacancies",
        "work-with-us",
    }
)
SPECULATIVE_TITLE_MARKERS = (
    "general application",
    "initiativbewerbung",
    "open application",
    "spontaneous application",
    "speculative application",
    "unsolicited application",
)
PROVIDER_ADAPTERS = frozenset(
    {
        "ashby",
        "bamboohr",
        "breezy",
        "greenhouse",
        "jazzhr",
        "lever",
        "personio",
        "pinpoint",
        "recruitee",
        "smartrecruiters",
        "teamtailor",
        "workable",
    }
)
SOCIAL_ADAPTERS = frozenset({"mastodon", "reddit", "social", "twitter", "x"})


def _limit_provider_static_examples(
    rows: Sequence[Mapping[str, Any]], limit: int
) -> list[Mapping[str, Any]]:
    capped_warning_slots = max(0, int(limit))
    blocked_count = sum(
        1 for row in rows if clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    warning_slots = max(0, capped_warning_slots - blocked_count)
    warnings_added = 0
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        if clean_text(row.get("disagreementGateDisposition")) == "blocked":
            selected.append(row)
            continue
        if warnings_added < warning_slots:
            selected.append(row)
            warnings_added += 1
    return selected


def _payload(row: CanonicalJob | Mapping[str, Any]) -> dict[str, Any]:
    return row.to_dict() if isinstance(row, CanonicalJob) else dict(row)


def _source_class(item: Mapping[str, Any]) -> str:
    adapter = norm_text(item.get("adapter"))
    source = norm_text(item.get("source"))
    if adapter == "static" or source.startswith(("static_source::", "static:listing_url:")):
        return "static"
    if adapter in SOCIAL_ADAPTERS or source.startswith(("social_", "reddit", "mastodon")):
        return "social"
    if adapter in PROVIDER_ADAPTERS or source.startswith(
        (
            "ashby:",
            "bamboohr:",
            "breezy:",
            "greenhouse:",
            "jazzhr:",
            "lever:",
            "personio:",
            "pinpoint:",
            "recruitee:",
            "smartrecruiters:",
            "teamtailor:",
            "workable:",
        )
    ):
        return "provider"
    return "other"


def _source_bundle(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    bundle = json_object_rows(row.get("sourceBundle"))
    if bundle:
        return bundle
    source = clean_text(row.get("source"))
    if not source:
        return []
    return [
        {
            "source": source,
            "sourceJobId": clean_text(row.get("sourceJobId")),
            "jobLink": normalize_url(row.get("jobLink")),
            "adapter": clean_text(row.get("adapter")),
            "studio": clean_text(row.get("studio")),
        }
    ]


def _source_class_counts(bundle: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = Counter(_source_class(item) for item in bundle)
    return {key: int(counts.get(key, 0)) for key in ("provider", "static", "social", "other")}


def _dominant_source_class(source_classes: Mapping[str, int]) -> str:
    ordered = ("provider", "static", "social", "other")
    ranked = sorted(
        ordered,
        key=lambda key: (-int(source_classes.get(key, 0) or 0), ordered.index(key)),
    )
    winner = ranked[0] if ranked else "unknown"
    return winner if int(source_classes.get(winner, 0) or 0) > 0 else "unknown"


def _meaningful_locations(row: Mapping[str, Any]) -> list[str]:
    values: set[str] = set()
    for item in json_object_rows(row.get("locations")):
        city = clean_text(item.get("city"))
        country = clean_text(item.get("country"))
        label = ", ".join(part for part in (city, country) if part)
        if label and norm_text(label) not in {"unknown", "n/a", "na", "none"}:
            values.add(norm_text(label))
    city = clean_text(row.get("city"))
    country = clean_text(row.get("country"))
    label = ", ".join(part for part in (city, country) if part)
    if label and norm_text(label) not in {"unknown", "n/a", "na", "none"}:
        values.add(norm_text(label))
    return sorted(values)


def _location_label_parts(label: str) -> tuple[str, str]:
    cleaned = clean_text(label)
    if not cleaned:
        return "", ""
    if "," not in cleaned:
        return cleaned, ""
    city, country = cleaned.rsplit(",", 1)
    return clean_text(city), clean_text(country)


def _provider_items_missing_ids(bundle: Sequence[Mapping[str, Any]]) -> bool:
    provider_items = [item for item in bundle if _source_class(item) == "provider"]
    return bool(provider_items) and any(
        not clean_text(item.get("sourceJobId")) for item in provider_items
    )


def _shared_primary_url(bundle: Sequence[Mapping[str, Any]]) -> bool:
    urls = {
        normalize_url(item.get("jobLink")) for item in bundle if normalize_url(item.get("jobLink"))
    }
    return len(urls) == 1


def _unique_job_links(bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            normalize_url(item.get("jobLink"))
            for item in bundle
            if normalize_url(item.get("jobLink"))
        }
    )


def _unique_job_link_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len(_unique_job_links(bundle))


def _shared_url(bundle: Sequence[Mapping[str, Any]]) -> str:
    urls = _unique_job_links(bundle)
    return urls[0] if len(urls) == 1 else ""


def _url_host(url: str) -> str:
    return norm_text(urlparse(url).netloc.removeprefix("www."))


def _url_path(url: str) -> str:
    return urlparse(url).path or "/"


def _path_prefix(url: str) -> str:
    segments = [segment for segment in _url_path(url).strip("/").split("/") if segment]
    return "/".join(segments[:2]).lower()


def _provider_source_job_id_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len(
        {
            clean_text(item.get("sourceJobId"))
            for item in bundle
            if _source_class(item) == "provider" and clean_text(item.get("sourceJobId"))
        }
    )


def _non_provider_source_job_id_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len(
        {
            clean_text(item.get("sourceJobId"))
            for item in bundle
            if _source_class(item) != "provider" and clean_text(item.get("sourceJobId"))
        }
    )


def _items_for_source_class(
    bundle: Sequence[Mapping[str, Any]], source_class: str
) -> list[Mapping[str, Any]]:
    return [item for item in bundle if _source_class(item) == source_class]


def _sample_clean_values(
    items: Sequence[Mapping[str, Any]], field: str, *, normalize_urls: bool = False
) -> list[str]:
    return _clean_values(items, field, normalize_urls=normalize_urls)[:5]


def _clean_values(
    items: Sequence[Mapping[str, Any]], field: str, *, normalize_urls: bool = False
) -> list[str]:
    values = {
        normalize_url(item.get(field)) if normalize_urls else clean_text(item.get(field))
        for item in items
    }
    return sorted(value for value in values if value)


def _identifier_tokens(values: Sequence[str]) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        normalized = norm_text(value.replace("-", " ").replace("_", " ").replace("/", " "))
        for token in normalized.split():
            if len(token) >= 6 or (len(token) >= 4 and any(char.isdigit() for char in token)):
                tokens.add(token)
    return tokens


def _concrete_identifier_tokens(values: Sequence[str]) -> set[str]:
    generic_tokens = {
        "application",
        "career",
        "careers",
        "department",
        "greenhouse",
        "job",
        "jobs",
        "listing",
        "opening",
        "openings",
        "position",
        "positions",
        "provider",
        "recruiting",
        "static",
        "studio",
        "work",
    }
    tokens: set[str] = set()
    for token in _identifier_tokens(values):
        if token in generic_tokens:
            continue
        if len(token) < 6:
            continue
        if not any(char.isdigit() for char in token):
            continue
        tokens.add(token)
    return tokens


def _concrete_shared_identifier_tokens(
    *,
    provider_ids: Sequence[str],
    provider_urls: Sequence[str],
    static_ids: Sequence[str],
    static_urls: Sequence[str],
) -> list[str]:
    provider_tokens = _concrete_identifier_tokens(
        [*provider_ids, *(_url_path(url) for url in provider_urls)]
    )
    static_tokens = _concrete_identifier_tokens(
        [*static_ids, *(_url_path(url) for url in static_urls)]
    )
    return sorted(provider_tokens & static_tokens)


def _non_provider_items(bundle: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [item for item in bundle if _source_class(item) != "provider"]


def _non_provider_source_job_ids(bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            clean_text(item.get("sourceJobId"))
            for item in _non_provider_items(bundle)
            if clean_text(item.get("sourceJobId"))
        }
    )


def _non_provider_source_job_id_values(bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        clean_text(item.get("sourceJobId"))
        for item in _non_provider_items(bundle)
        if clean_text(item.get("sourceJobId"))
    )


def _non_provider_source_names(bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            norm_text(item.get("source"))
            for item in _non_provider_items(bundle)
            if norm_text(item.get("source"))
        }
    )


def _looks_url_or_hash_identity(value: str) -> bool:
    normalized = clean_text(value).lower()
    if not normalized:
        return False
    if normalized.startswith(("http://", "https://", "url:")):
        return True
    if len(normalized) in {32, 40, 64} and all(char in "0123456789abcdef" for char in normalized):
        return True
    return False


def _source_job_id_shape(value: str) -> str:
    normalized = clean_text(value).lower()
    if not normalized:
        return "empty"
    if normalized.startswith(("http://", "https://")):
        return "url"
    if normalized.startswith("url:"):
        return "url_hash"
    if len(normalized) in {32, 40, 64} and all(char in "0123456789abcdef" for char in normalized):
        return "hex_hash"
    if any(char.isdigit() for char in normalized):
        return "opaque_with_digits"
    return "opaque"


def _source_job_id_prefix(value: str) -> str:
    normalized = clean_text(value).lower()
    if not normalized:
        return ""
    if normalized.startswith(("http://", "https://")):
        parsed = urlparse(normalized)
        return norm_text(parsed.netloc.removeprefix("www."))
    for separator in (":", "|", "#", "/", "\\"):
        if separator in normalized:
            return normalized.split(separator, 1)[0]
    return normalized[:12]


def _sheet_row_indexes(bundle: Sequence[Mapping[str, Any]]) -> list[int]:
    indexes: list[int] = []
    for value in _non_provider_source_job_id_values(bundle):
        normalized = clean_text(value).lower()
        if not normalized.startswith("sheet-"):
            continue
        suffix = normalized.removeprefix("sheet-")
        if suffix.isdigit():
            indexes.append(int(suffix))
    return sorted(indexes)


def _sheet_row_span(bundle: Sequence[Mapping[str, Any]]) -> int:
    indexes = _sheet_row_indexes(bundle)
    if not indexes:
        return 0
    return indexes[-1] - indexes[0] + 1


def _non_provider_identity_provenance(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    ids = _non_provider_source_job_ids(bundle)
    if not ids:
        return "none"
    source_names = _non_provider_source_names(bundle)
    if len(source_names) > 1:
        return "mixed_non_provider_identity"
    source = source_names[0] if source_names else ""
    if source == "google_sheets":
        return "google_sheets_row_identity"
    if all(_looks_url_or_hash_identity(value) for value in ids):
        return "url_derived_identity"
    if _title_shape(row) == "category_like" or any(
        token in source for token in ("category", "directory", "gamedevmap", "gameprog", "gamesmap")
    ):
        return "category_or_directory_identity"
    return "opaque_other_source_identity"


def _non_provider_identity_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_ids(bundle)
    source_names = _non_provider_source_names(bundle)
    shapes = sorted({_source_job_id_shape(value) for value in ids})
    prefixes = sorted(
        {_source_job_id_prefix(value) for value in ids if _source_job_id_prefix(value)}
    )
    provenance = _non_provider_identity_provenance(row, bundle)
    evidence = [
        f"provenance:{provenance}",
        f"source_count:{len(source_names)}",
        f"non_provider_ids:{len(ids)}",
        f"id_prefixes:{len(prefixes)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
    ]
    if source_names:
        evidence.append(f"dominant_source_name:{source_names[0]}")
    if len(source_names) == 1:
        evidence.append("single_non_provider_source")
    for shape in shapes[:3]:
        evidence.append(f"id_shape:{shape}")
    if _title_shape(row) == "category_like":
        evidence.append("title_category_like")
    return evidence[:12]


def _sample_url_paths(bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({_url_path(url) for url in _unique_job_links(bundle) if _url_path(url)})[:5]


def _path_tokens(path: str) -> list[str]:
    normalized = norm_text(path.replace("-", " ").replace("_", " ").replace("/", " "))
    return sorted({token for token in normalized.split() if token})


def _url_path_looks_listing_or_search(path: str) -> bool:
    normalized = norm_text(path)
    return any(
        token in normalized
        for token in ("career", "careers", "explore", "jobs", "openings", "search")
    ) and not any(char.isdigit() for char in normalized)


def _url_paths_look_job_detail(bundle: Sequence[Mapping[str, Any]]) -> bool:
    paths = _sample_url_paths(bundle)
    return bool(paths) and all(
        any(char.isdigit() for char in path) or "/details/" in path or "/job/" in path
        for path in paths
    )


def _title_tokens(row: Mapping[str, Any]) -> list[str]:
    normalized = norm_text(
        clean_text(row.get("title"))
        .replace("-", " ")
        .replace("_", " ")
        .replace("/", " ")
        .replace("&", " ")
    )
    return sorted({token for token in normalized.split() if token})


def _company_tokens(row: Mapping[str, Any]) -> list[str]:
    normalized = norm_text(clean_text(row.get("company")).replace("-", " ").replace("_", " "))
    return sorted({token for token in normalized.split() if token})


def _looks_role_bucket_title(row: Mapping[str, Any]) -> bool:
    title = norm_text(row.get("title"))
    normalized = norm_text(
        clean_text(row.get("title")).replace("-", " ").replace("_", " ").replace("&", " ")
    )
    if title in ROLE_BUCKET_TITLE_TERMS or normalized in ROLE_BUCKET_TITLE_TERMS:
        return True
    tokens = normalized.split()
    return 1 <= len(tokens) <= 2 and any(
        token in {"management", "programming", "design", "localization"} for token in tokens
    )


def _has_generic_role_bucket_title(row: Mapping[str, Any]) -> bool:
    title = norm_text(row.get("title"))
    normalized = norm_text(clean_text(row.get("title")).replace("-", " "))
    tokens = set(_title_tokens(row))
    if title in ROLE_BUCKET_TITLE_TERMS or normalized in ROLE_BUCKET_TITLE_TERMS:
        return True
    return bool(tokens & GENERIC_ROLE_BUCKET_TERMS)


def _unique_url_host_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len({_url_host(url) for url in _unique_job_links(bundle) if _url_host(url)})


def _unique_url_path_prefix_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len({_path_prefix(url) for url in _unique_job_links(bundle) if _path_prefix(url)})


def _has_any_strong_identity(bundle: Sequence[Mapping[str, Any]]) -> bool:
    if any(clean_text(item.get("sourceJobId")) for item in bundle):
        return True
    return _shared_primary_url(bundle)


def _risky_reasons(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    classes = _source_class_counts(bundle)
    if len(_meaningful_locations(row)) > 1:
        reasons.append("same_title_company_different_location")
    if classes["provider"] > 0 and classes["static"] > 0 and not _shared_primary_url(bundle):
        reasons.append("provider_static_duplicate_disagreement")
    if _provider_items_missing_ids(bundle) and not _shared_primary_url(bundle):
        reasons.append("missing_provider_ids")
    if len(bundle) > 1 and not _has_any_strong_identity(bundle):
        reasons.append("weak_title_company_only_evidence")
    return reasons


def _looks_listing_or_category_url(url: str) -> bool:
    path = _url_path(url).strip("/").lower()
    if not path:
        return True
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return True
    last = segments[-1].removesuffix(".html").removesuffix(".htm")
    if any(char.isdigit() for char in last):
        return False
    if last in GENERIC_LISTING_PATH_SEGMENTS:
        return True
    if len(segments) <= 2 and any(segment in GENERIC_LISTING_PATH_SEGMENTS for segment in segments):
        return True
    normalized_last = last.replace("-", " ").replace("_", " ").strip()
    return normalized_last in CATEGORY_TITLE_TERMS


def _title_shape(row: Mapping[str, Any]) -> str:
    title = norm_text(row.get("title"))
    if not title:
        return "empty_or_unknown"
    if any(marker in title for marker in SPECULATIVE_TITLE_MARKERS):
        return "speculative_or_open_application"
    normalized = norm_text(title.replace("-", " ").replace("_", " ").replace("&", " "))
    if title in CATEGORY_TITLE_TERMS or normalized in CATEGORY_TITLE_TERMS:
        return "category_like"
    return "role_like"


def _title_company_pollution_signals(row: Mapping[str, Any]) -> list[str]:
    signals: list[str] = []
    title = clean_text(row.get("title"))
    company = clean_text(row.get("company"))
    if title and title[-1:].isdigit():
        signals.append("title_numeric_suffix")
    if company and company[-1:].isdigit():
        signals.append("company_numeric_suffix")
    if title and company and title == company:
        signals.append("title_company_identical")
    if title and len(title.split()) <= 2 and any(char.isdigit() for char in title):
        signals.append("short_title_with_digits")
    if company and len(company.split()) <= 4 and any(char.isdigit() for char in company):
        signals.append("short_company_with_digits")
    return sorted(set(signals))


def _identity_shape(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> str:
    provider_ids = _provider_source_job_id_count(bundle)
    urls = _unique_job_links(bundle)
    if provider_ids > 0:
        return "provider_id_backed"
    if not urls:
        return "missing_url_and_ids"
    if len(urls) == 1:
        return (
            "shared_listing_or_category_url"
            if _looks_listing_or_category_url(urls[0]) or _title_shape(row) == "category_like"
            else "shared_job_detail_url"
        )
    if len(urls) > 1:
        return "many_unique_urls_same_title"
    return "mixed_or_unknown_identity"


def _identity_caveats(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> list[str]:
    caveats: list[str] = []
    identity_shape = _identity_shape(row, bundle)
    title_shape = _title_shape(row)
    source_classes = _source_class_counts(bundle)
    if identity_shape == "shared_listing_or_category_url":
        caveats.append("shared_url_looks_like_listing_or_category")
    if identity_shape == "many_unique_urls_same_title":
        caveats.append("many_unique_urls_same_title")
    if identity_shape == "missing_url_and_ids":
        caveats.append("missing_url_and_provider_ids")
    if title_shape == "category_like":
        caveats.append("category_like_title")
    if title_shape == "speculative_or_open_application":
        caveats.append("speculative_or_open_application_title")
    if source_classes["other"] > 0 and source_classes["other"] >= max(source_classes.values()):
        caveats.append("other_source_class_dominant")
    if _shared_primary_url(bundle) and _provider_source_job_id_count(bundle) == 0:
        caveats.append("shared_url_without_provider_ids")
    return caveats


def _identity_quality(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> str:
    provider_ids = _provider_source_job_id_count(bundle)
    non_provider_ids = _non_provider_source_job_id_count(bundle)
    urls = _unique_job_links(bundle)
    if provider_ids > 0:
        return "provider_id_strong"
    if non_provider_ids > 0:
        return "other_source_id_untrusted"
    if not urls:
        return "missing_identity"
    if len(urls) == 1:
        return (
            "shared_listing_url_weak"
            if _looks_listing_or_category_url(urls[0]) or _title_shape(row) == "category_like"
            else "shared_detail_url_strong"
        )
    if len(urls) > 1:
        return (
            "many_urls_same_host_weak"
            if _unique_url_host_count(bundle) == 1 and _unique_url_path_prefix_count(bundle) <= 1
            else "many_urls_many_hosts_weak"
        )
    return "unknown"


def _identity_quality_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    quality = _identity_quality(row, bundle)
    evidence = [
        f"quality:{quality}",
        f"provider_ids:{_provider_source_job_id_count(bundle)}",
        f"non_provider_ids:{_non_provider_source_job_id_count(bundle)}",
        f"urls:{_unique_job_link_count(bundle)}",
        f"hosts:{_unique_url_host_count(bundle)}",
        f"path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"dominant_source:{_dominant_source_class(_source_class_counts(bundle))}",
    ]
    shared = _shared_url(bundle)
    if shared:
        evidence.append(
            "shared_url_listing_like"
            if _looks_listing_or_category_url(shared) or _title_shape(row) == "category_like"
            else "shared_url_detail_like"
        )
    return evidence


def _outlier_reason(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> str:
    if not bundle:
        return "unknown"
    source_classes = _source_class_counts(bundle)
    location_count = len(_meaningful_locations(row))
    has_strong_identity = _has_any_strong_identity(bundle)
    bundle_count = max(int(row.get("sourceBundleCount") or 0), len(bundle))

    if (
        source_classes["provider"] > 0
        and source_classes["static"] > 0
        and not _shared_primary_url(bundle)
    ):
        return "provider_static_disagreement"
    if location_count > 1 and has_strong_identity:
        return "multi_location_strong_identity"
    if location_count > 1:
        return "location_divergence_without_strong_identity"
    if source_classes["other"] >= max(source_classes.values()) and bundle_count >= 10:
        return "large_other_source_bundle"
    if len(bundle) > 1 and not has_strong_identity:
        return "sparse_title_company_bundle"
    return "unknown"
