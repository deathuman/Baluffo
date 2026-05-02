"""Read-only deduplication evidence for fetch reports."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.json_shapes import json_object_rows

TOP_MERGED_LIMIT = 10
RISKY_EXAMPLE_LIMIT = 10
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
REVIEW_QUEUE_ACTION_KEYS = (
    "review_many_urls_same_title",
    "review_listing_url_bundle",
    "review_category_title_bundle",
    "review_open_application_bundle",
    "review_provider_static_disagreement",
    "monitor",
)
REVIEW_QUEUE_CAUSE_KEYS = (
    "category_or_department_bucket",
    "open_application_family",
    "listing_page_bundle",
    "spreadsheet_role_bucket_needs_review",
    "google_sheets_role_bucket_needs_review",
    "parser_or_directory_text_pollution",
    "non_provider_url_identity_needs_review",
    "provider_static_disagreement",
    "likely_legitimate_multi_role_family",
    "unknown",
)
DEDUP_AUDIT_GATE_BLOCKER_CAUSES = frozenset(
    {
        "provider_static_disagreement",
        "unknown",
        "non_provider_url_identity_needs_review",
        "parser_or_directory_text_pollution",
        "spreadsheet_role_bucket_needs_review",
        "google_sheets_role_bucket_needs_review",
    }
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
GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS = (
    "role_category_bucket",
    "company_role_family",
    "single_location_many_urls",
    "multi_location_many_urls",
    "spreadsheet_row_collision",
    "not_google_sheets",
    "unknown",
)
GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS = (
    "likely_spreadsheet_category_bucket",
    "role_family_needs_manual_review",
    "job_detail_urls_same_role",
    "listing_or_search_url_bucket",
    "parser_normalized_role_title",
    "not_google_sheets_role_bucket",
    "unknown",
)
GOOGLE_SHEETS_BUCKET_INTENT_KEYS = (
    "likely_spreadsheet_taxonomy_bucket",
    "possible_role_family",
    "weak_title_company_grouping",
    "listing_or_search_bucket",
    "parser_normalized_bucket",
    "not_google_sheets_bucket",
    "unknown",
)
GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS = (
    "role_bucket_detail_url_grouping",
    "role_bucket_listing_grouping",
    "single_token_title_many_urls",
    "two_token_title_many_urls",
    "concrete_title_many_urls",
    "parser_pollution_grouping",
    "not_weak_google_sheets_grouping",
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
    values = {
        normalize_url(item.get(field)) if normalize_urls else clean_text(item.get(field))
        for item in items
    }
    return sorted(value for value in values if value)[:5]


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


def _google_sheets_bundle_shape(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets"
    ids = _non_provider_source_job_id_values(bundle)
    if not bundle:
        return "unknown"
    if len(ids) != len(set(ids)):
        return "spreadsheet_row_collision"
    if _title_shape(row) == "category_like" or _looks_role_bucket_title(row):
        return "role_category_bucket"
    if _unique_job_link_count(bundle) > 1 and len(_meaningful_locations(row)) > 1:
        return "multi_location_many_urls"
    if _unique_job_link_count(bundle) > 1:
        return "single_location_many_urls"
    if len(bundle) > 1:
        return "company_role_family"
    return "unknown"


def _google_sheets_role_bucket_audit(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets_role_bucket"
    if _google_sheets_bundle_shape(row, bundle) not in {
        "role_category_bucket",
        "single_location_many_urls",
        "multi_location_many_urls",
        "company_role_family",
    }:
        return "not_google_sheets_role_bucket"
    if _title_company_pollution_signals(row):
        return "parser_normalized_role_title"
    paths = _sample_url_paths(bundle)
    if paths and all(_url_path_looks_listing_or_search(path) for path in paths):
        return "listing_or_search_url_bucket"
    if (
        _url_paths_look_job_detail(bundle)
        and _google_sheets_bundle_shape(row, bundle) == "role_category_bucket"
    ):
        return "job_detail_urls_same_role"
    if _has_generic_role_bucket_title(row):
        return "likely_spreadsheet_category_bucket"
    if _unique_job_link_count(bundle) > 1:
        return "role_family_needs_manual_review"
    return "unknown"


def _google_sheets_role_bucket_audit_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    paths = _sample_url_paths(bundle)
    path_tokens = sorted({token for path in paths for token in _path_tokens(path)})
    evidence = [
        f"audit:{_google_sheets_role_bucket_audit(row, bundle)}",
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
        f"path_tokens:{len(path_tokens)}",
    ]
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:4]:
        evidence.append(f"title_token:{token}")
    for token in path_tokens[:4]:
        evidence.append(f"path_token:{token}")
    for shape in sorted({_source_job_id_shape(value) for value in ids})[:3]:
        evidence.append(f"id_shape:{shape}")
    return evidence[:16]


def _google_sheets_bucket_intent(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets_bucket"
    audit = _google_sheets_role_bucket_audit(row, bundle)
    shape = _google_sheets_bundle_shape(row, bundle)
    if audit == "parser_normalized_role_title":
        return "parser_normalized_bucket"
    if audit == "listing_or_search_url_bucket":
        return "listing_or_search_bucket"
    if shape == "role_category_bucket" or _has_generic_role_bucket_title(row):
        return "likely_spreadsheet_taxonomy_bucket"
    if audit == "role_family_needs_manual_review" and len(_title_tokens(row)) <= 2:
        return "weak_title_company_grouping"
    if audit == "role_family_needs_manual_review" or shape in {
        "company_role_family",
        "single_location_many_urls",
        "multi_location_many_urls",
    }:
        return "possible_role_family"
    if (
        _identity_shape(row, bundle) == "many_unique_urls_same_title"
        and _provider_source_job_id_count(bundle) == 0
        and _non_provider_source_job_id_count(bundle) > 0
    ):
        return "weak_title_company_grouping"
    return "unknown"


def _google_sheets_bucket_intent_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    paths = _sample_url_paths(bundle)
    evidence = [
        f"intent:{_google_sheets_bucket_intent(row, bundle)}",
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"audit:{_google_sheets_role_bucket_audit(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
    ]
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    if _looks_role_bucket_title(row):
        evidence.append("role_bucket_title")
    if _has_generic_role_bucket_title(row):
        evidence.append("generic_role_bucket_title")
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:4]:
        evidence.append(f"title_token:{token}")
    return evidence[:16]


def _google_sheets_weak_grouping_audit(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_weak_google_sheets_grouping"
    if _title_company_pollution_signals(row):
        return "parser_pollution_grouping"
    paths = _sample_url_paths(bundle)
    if paths and all(_url_path_looks_listing_or_search(path) for path in paths):
        return "role_bucket_listing_grouping"
    if _url_paths_look_job_detail(bundle) and (
        _looks_role_bucket_title(row) or _has_generic_role_bucket_title(row)
    ):
        return "role_bucket_detail_url_grouping"
    if _google_sheets_bucket_intent(row, bundle) != "weak_title_company_grouping":
        return "not_weak_google_sheets_grouping"
    title_token_count = len(_title_tokens(row))
    if title_token_count <= 1:
        return "single_token_title_many_urls"
    if title_token_count == 2:
        return "two_token_title_many_urls"
    if _unique_job_link_count(bundle) > 1:
        return "concrete_title_many_urls"
    return "unknown"


def _google_sheets_weak_grouping_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    indexes = _sheet_row_indexes(bundle)
    paths = _sample_url_paths(bundle)
    path_tokens = sorted({token for path in paths for token in _path_tokens(path)})
    evidence = [
        f"audit:{_google_sheets_weak_grouping_audit(row, bundle)}",
        f"intent:{_google_sheets_bucket_intent(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"sheet_rows:{len(indexes)}",
        f"sheet_row_span:{_sheet_row_span(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
    ]
    if indexes:
        evidence.append(f"sheet_row_min:{indexes[0]}")
        evidence.append(f"sheet_row_max:{indexes[-1]}")
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:3]:
        evidence.append(f"title_token:{token}")
    for token in path_tokens[:3]:
        evidence.append(f"path_token:{token}")
    for value in ids[:3]:
        evidence.append(f"source_id:{value}")
    return evidence[:20]


def _google_sheets_bundle_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    prefixes = sorted(
        {_source_job_id_prefix(value) for value in ids if _source_job_id_prefix(value)}
    )
    evidence = [
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_shape:{_title_shape(row)}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
        f"source_id_prefixes:{len(prefixes)}",
    ]
    for path in _sample_url_paths(bundle)[:3]:
        evidence.append(f"url_path:{path}")
    for shape in sorted({_source_job_id_shape(value) for value in ids})[:3]:
        evidence.append(f"id_shape:{shape}")
    if _looks_role_bucket_title(row):
        evidence.append("role_bucket_title")
    for caveat in _identity_caveats(row, bundle):
        evidence.append(f"caveat:{caveat}")
    return evidence[:16]


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


def _recommended_review_action(summary: Mapping[str, Any]) -> str:
    caveats = {str(caveat) for caveat in summary.get("identityCaveats") or []}
    identity_shape = str(summary.get("identityShape") or "")
    title_shape = str(summary.get("titleShape") or "")
    outlier_reason = str(summary.get("outlierReason") or "")
    if outlier_reason == "provider_static_disagreement":
        return "review_provider_static_disagreement"
    if identity_shape == "shared_listing_or_category_url":
        return "review_listing_url_bundle"
    if title_shape == "category_like" or "category_like_title" in caveats:
        return "review_category_title_bundle"
    if (
        title_shape == "speculative_or_open_application"
        or "speculative_or_open_application_title" in caveats
    ):
        return "review_open_application_bundle"
    if identity_shape == "many_unique_urls_same_title":
        return "review_many_urls_same_title"
    return "monitor"


def _suspected_cause(summary: Mapping[str, Any]) -> str:
    caveats = {str(caveat) for caveat in summary.get("identityCaveats") or []}
    pollution = {str(signal) for signal in summary.get("titleCompanyPollutionSignals") or []}
    identity_shape = str(summary.get("identityShape") or "")
    identity_quality = str(summary.get("identityQuality") or "")
    title_shape = str(summary.get("titleShape") or "")
    outlier_reason = str(summary.get("outlierReason") or "")
    dominant_source_class = str(summary.get("dominantSourceClass") or "")
    provenance = str(summary.get("nonProviderIdentityProvenance") or "")
    google_sheets_shape = str(summary.get("googleSheetsBundleShape") or "")
    google_sheets_audit = str(summary.get("googleSheetsRoleBucketAudit") or "")
    if outlier_reason == "provider_static_disagreement":
        return "provider_static_disagreement"
    if title_shape == "speculative_or_open_application":
        return "open_application_family"
    if provenance == "google_sheets_row_identity" and google_sheets_shape == "role_category_bucket":
        return "spreadsheet_role_bucket_needs_review"
    if google_sheets_audit in {
        "listing_or_search_url_bucket",
        "parser_normalized_role_title",
        "role_family_needs_manual_review",
    }:
        return "google_sheets_role_bucket_needs_review"
    if title_shape == "category_like" or "category_like_title" in caveats:
        return "category_or_department_bucket"
    if pollution and dominant_source_class == "other":
        return "parser_or_directory_text_pollution"
    if identity_shape == "shared_listing_or_category_url":
        return "listing_page_bundle"
    if (
        dominant_source_class == "other"
        and identity_shape == "many_unique_urls_same_title"
        and identity_quality
        in {
            "many_urls_same_host_weak",
            "many_urls_many_hosts_weak",
            "other_source_id_untrusted",
        }
    ):
        return "non_provider_url_identity_needs_review"
    if identity_quality in {"provider_id_strong", "shared_detail_url_strong"} and (
        identity_shape == "provider_id_backed" or outlier_reason == "multi_location_strong_identity"
    ):
        return "likely_legitimate_multi_role_family"
    return "unknown"


def _google_sheets_cause_evidence(summary: Mapping[str, Any]) -> list[str]:
    fields = (
        ("google_sheets_shape", "googleSheetsBundleShape", {"", "not_google_sheets", "unknown"}),
        (
            "google_sheets_audit",
            "googleSheetsRoleBucketAudit",
            {"", "not_google_sheets_role_bucket", "unknown"},
        ),
        (
            "google_sheets_intent",
            "googleSheetsBucketIntent",
            {"", "not_google_sheets_bucket", "unknown"},
        ),
        (
            "google_sheets_weak_audit",
            "googleSheetsWeakGroupingAudit",
            {"", "not_weak_google_sheets_grouping", "unknown"},
        ),
    )
    evidence: list[str] = []
    for label, key, ignored in fields:
        value = str(summary.get(key) or "")
        if value not in ignored:
            evidence.append(f"{label}:{value}")
    return evidence


def _cause_evidence(summary: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for label, key in (
        ("cause", "suspectedCause"),
        ("identity", "identityShape"),
        ("quality", "identityQuality"),
        ("title", "titleShape"),
        ("outlier", "outlierReason"),
        ("dominant_source", "dominantSourceClass"),
    ):
        value = str(summary.get(key) or "")
        if value:
            evidence.append(f"{label}:{value}")
    evidence.extend(_google_sheets_cause_evidence(summary))
    if summary.get("hasStrongIdentity"):
        evidence.append("strong_identity")
    for signal in summary.get("titleCompanyPollutionSignals") or []:
        evidence.append(f"pollution:{signal}")
    provenance = str(summary.get("nonProviderIdentityProvenance") or "")
    if provenance:
        evidence.append(f"provenance:{provenance}")
    for caveat in summary.get("identityCaveats") or []:
        evidence.append(f"caveat:{caveat}")
    return evidence[:10]


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


def _job_summary(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    source_names = sorted(
        {clean_text(item.get("source")) for item in bundle if clean_text(item.get("source"))}
    )
    source_classes = _source_class_counts(bundle)
    meaningful_locations = _meaningful_locations(row)
    shared_url = _shared_url(bundle)
    summary = {
        "id": clean_text(row.get("id")),
        "dedupKey": clean_text(row.get("dedupKey")),
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "jobLink": normalize_url(row.get("jobLink")),
        "locationSummary": clean_text(row.get("locationSummary")),
        "sourceBundleCount": max(int(row.get("sourceBundleCount") or 0), len(bundle)),
        "sourceClasses": source_classes,
        "sources": source_names[:8],
        "sampleSources": source_names[:8],
        "outlierReason": _outlier_reason(row, bundle),
        "distinctLocationCount": len(meaningful_locations),
        "sampleLocations": meaningful_locations[:5],
        "uniqueJobLinkCount": _unique_job_link_count(bundle),
        "sharedPrimaryUrl": _shared_primary_url(bundle),
        "sharedUrlHost": _url_host(shared_url) if shared_url else "",
        "sharedUrlPath": _url_path(shared_url) if shared_url else "",
        "uniqueUrlHostCount": _unique_url_host_count(bundle),
        "uniqueUrlPathPrefixCount": _unique_url_path_prefix_count(bundle),
        "urlHostDiversity": _unique_url_host_count(bundle),
        "urlPathPrefixDiversity": _unique_url_path_prefix_count(bundle),
        "providerSourceJobIdCount": _provider_source_job_id_count(bundle),
        "nonProviderSourceJobIdCount": _non_provider_source_job_id_count(bundle),
        "hasStrongIdentity": _has_any_strong_identity(bundle),
        "dominantSourceClass": _dominant_source_class(source_classes),
        "identityShape": _identity_shape(row, bundle),
        "identityQuality": _identity_quality(row, bundle),
        "identityQualityEvidence": _identity_quality_evidence(row, bundle),
        "nonProviderIdentityProvenance": _non_provider_identity_provenance(row, bundle),
        "nonProviderIdentityEvidence": _non_provider_identity_evidence(row, bundle),
        "googleSheetsBundleShape": _google_sheets_bundle_shape(row, bundle),
        "googleSheetsBundleEvidence": _google_sheets_bundle_evidence(row, bundle),
        "googleSheetsRoleBucketAudit": _google_sheets_role_bucket_audit(row, bundle),
        "googleSheetsRoleBucketAuditEvidence": _google_sheets_role_bucket_audit_evidence(
            row, bundle
        ),
        "googleSheetsBucketIntent": _google_sheets_bucket_intent(row, bundle),
        "googleSheetsBucketIntentEvidence": _google_sheets_bucket_intent_evidence(row, bundle),
        "googleSheetsWeakGroupingAudit": _google_sheets_weak_grouping_audit(row, bundle),
        "googleSheetsWeakGroupingEvidence": _google_sheets_weak_grouping_evidence(row, bundle),
        "titleShape": _title_shape(row),
        "identityCaveats": _identity_caveats(row, bundle),
        "titleCompanyPollutionSignals": _title_company_pollution_signals(row),
    }
    summary["suspectedCause"] = _suspected_cause(summary)
    summary["causeEvidence"] = _cause_evidence(summary)
    return summary


def _provider_static_disagreement_example(
    summary: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    provider_items = _items_for_source_class(bundle, "provider")
    static_items = _items_for_source_class(bundle, "static")
    provider_urls = _sample_clean_values(provider_items, "jobLink", normalize_urls=True)
    static_urls = _sample_clean_values(static_items, "jobLink", normalize_urls=True)
    provider_hosts = sorted({_url_host(url) for url in provider_urls if _url_host(url)})
    static_hosts = sorted({_url_host(url) for url in static_urls if _url_host(url)})
    provider_prefixes = sorted({_path_prefix(url) for url in provider_urls if _path_prefix(url)})
    static_prefixes = sorted({_path_prefix(url) for url in static_urls if _path_prefix(url)})
    evidence = [
        f"bundle_origin:{clean_text(summary.get('bundleEvidenceOrigin')) or 'unknown'}",
        f"provider_sources:{len(_sample_clean_values(provider_items, 'source'))}",
        f"static_sources:{len(_sample_clean_values(static_items, 'source'))}",
        f"provider_urls:{len(provider_urls)}",
        f"static_urls:{len(static_urls)}",
        f"provider_ids:{len(_sample_clean_values(provider_items, 'sourceJobId'))}",
        f"static_ids:{len(_sample_clean_values(static_items, 'sourceJobId'))}",
        f"shared_primary_url:{str(bool(summary.get('sharedPrimaryUrl'))).lower()}",
        f"identity_quality:{clean_text(summary.get('identityQuality')) or 'unknown'}",
    ]
    return {
        "title": clean_text(summary.get("title")),
        "company": clean_text(summary.get("company")),
        "dedupKey": clean_text(summary.get("dedupKey")),
        "bundleEvidenceOrigin": clean_text(summary.get("bundleEvidenceOrigin")),
        "sourceBundleCount": max(0, int(summary.get("sourceBundleCount") or 0)),
        "providerSources": _sample_clean_values(provider_items, "source"),
        "staticSources": _sample_clean_values(static_items, "source"),
        "providerSourceJobIds": _sample_clean_values(provider_items, "sourceJobId"),
        "staticSourceJobIds": _sample_clean_values(static_items, "sourceJobId"),
        "providerUrls": provider_urls,
        "staticUrls": static_urls,
        "providerUrlHosts": provider_hosts[:5],
        "staticUrlHosts": static_hosts[:5],
        "providerUrlPathPrefixes": provider_prefixes[:5],
        "staticUrlPathPrefixes": static_prefixes[:5],
        "identityQuality": clean_text(summary.get("identityQuality")),
        "outlierReason": clean_text(summary.get("outlierReason")),
        "disagreementEvidence": evidence,
    }


def _high_risk_origin_counts(summary: Mapping[str, Any], origin: str) -> tuple[int, int]:
    if summary.get("suspectedCause") not in DEDUP_AUDIT_GATE_BLOCKER_CAUSES:
        return 0, 0
    return (1, 0) if origin == "current_run" else (0, 1)


def _provider_static_disagreement_origin_update(
    summary: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> tuple[int, int, list[dict[str, Any]]]:
    if summary.get("outlierReason") != "provider_static_disagreement":
        return 0, 0, []
    origin = clean_text(summary.get("bundleEvidenceOrigin"))
    current_count, carried_count = (1, 0) if origin == "current_run" else (0, 1)
    return current_count, carried_count, [_provider_static_disagreement_example(summary, bundle)]


def _merge_reason_counts(dedup_stats: Mapping[str, Any]) -> dict[str, int]:
    primary = max(0, int(dedup_stats.get("mergedByPrimaryUrl") or 0))
    secondary = max(0, int(dedup_stats.get("mergedBySecondaryKey") or 0))
    social = max(0, int(dedup_stats.get("mergedBySocialKey") or 0))
    sparse_explicit = dedup_stats.get("mergedBySparseIdentity")
    total = max(0, int(dedup_stats.get("mergedCount") or 0))
    sparse = (
        max(0, int(sparse_explicit or 0))
        if sparse_explicit is not None
        else max(0, total - primary - secondary - social)
    )
    known = primary + secondary + social + sparse
    return {
        "primaryUrl": primary,
        "secondaryKey": secondary,
        "socialKey": social,
        "sparseIdentity": sparse,
        "unknown": max(0, total - known),
    }


def _nonzero_counts(counts: Mapping[str, Any]) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in counts.items()
        if isinstance(value, int | float) and int(value) > 0
    }


def _mapping_value(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _audit_gate_provider_static_disagreement_count(
    *,
    provider_static_disagreement_counts: Mapping[str, Any],
    review_queue_cause_counts: Mapping[str, Any],
    risk_reason_counts: Mapping[str, Any],
    outlier_reason_counts: Mapping[str, Any],
) -> int:
    if provider_static_disagreement_counts:
        return max(0, int(provider_static_disagreement_counts.get("total") or 0))
    return max(
        int(review_queue_cause_counts.get("provider_static_disagreement") or 0),
        int(risk_reason_counts.get("provider_static_duplicate_disagreement") or 0),
        int(outlier_reason_counts.get("provider_static_disagreement") or 0),
    )


def _audit_gate_high_risk_count(review_queue_cause_counts: Mapping[str, Any]) -> int:
    return sum(
        int(review_queue_cause_counts.get(cause) or 0) for cause in DEDUP_AUDIT_GATE_BLOCKER_CAUSES
    )


def _audit_gate_blockers_and_warnings(
    *,
    merged_count: int,
    current_run_non_primary_merges: int,
    provider_static_disagreement_count: int,
    current_run_high_risk_review_queue_count: int,
    carried_high_risk_review_queue_count: int,
    carried_collision_likely_historical_count: int,
    high_risk_review_queue_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if current_run_non_primary_merges > 0:
        blockers.append("current_run_non_primary_merges_need_review")
    elif merged_count > 0:
        warnings.append("current_run_primary_url_merges_present")
    if provider_static_disagreement_count > 0:
        blockers.append("provider_static_disagreement_needs_review")
    if current_run_high_risk_review_queue_count > 0:
        blockers.append("high_risk_review_queue_causes_need_review")
    elif high_risk_review_queue_count and not carried_high_risk_review_queue_count:
        blockers.append("high_risk_review_queue_causes_need_review")
    if carried_high_risk_review_queue_count > 0:
        warnings.append("carried_high_risk_review_queue_causes_present")
    if carried_collision_likely_historical_count > 0:
        warnings.append("carried_source_bundle_collisions_present")
    return blockers, warnings


def _audit_gate_examples(dedup_evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    provider_static_examples = json_object_rows(
        dedup_evidence.get("providerStaticDisagreementExamples")
    )
    if provider_static_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
            }
            for row in provider_static_examples[:5]
        ]
    examples = []
    for row in dedup_evidence.get("reviewQueue") or []:
        if not isinstance(row, Mapping):
            continue
        cause = str(row.get("suspectedCause") or "")
        if cause not in DEDUP_AUDIT_GATE_BLOCKER_CAUSES and len(examples) >= 3:
            continue
        examples.append(
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": cause,
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
            }
        )
        if len(examples) >= 5:
            break
    return examples


def build_dedup_audit_gate(dedup_evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize whether dedup evidence is ready for read-only lifecycle UX."""
    merged_count = max(0, int(dedup_evidence.get("mergedCount") or 0))
    source_bundle_collision_count = max(
        0, int(dedup_evidence.get("sourceBundleCollisionCount") or 0)
    )
    current_run_source_bundle_collision_count = max(
        0, int(dedup_evidence.get("currentRunSourceBundleCollisionCount") or 0)
    )
    carried_source_bundle_collision_count = max(
        0, int(dedup_evidence.get("carriedSourceBundleCollisionCount") or 0)
    )
    current_run_high_risk_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunHighRiskReviewQueueCount") or 0)
    )
    carried_high_risk_review_queue_count = max(
        0, int(dedup_evidence.get("carriedHighRiskReviewQueueCount") or 0)
    )
    merge_reason_counts = _mapping_value(dedup_evidence, "mergeReasonCounts")
    review_queue_cause_counts = _mapping_value(dedup_evidence, "reviewQueueCauseCounts")
    provider_static_disagreement_counts = _mapping_value(
        dedup_evidence, "providerStaticDisagreementCounts"
    )
    provider_static_disagreement_count = _audit_gate_provider_static_disagreement_count(
        provider_static_disagreement_counts=provider_static_disagreement_counts,
        review_queue_cause_counts=review_queue_cause_counts,
        risk_reason_counts=_mapping_value(dedup_evidence, "riskReasonCounts"),
        outlier_reason_counts=_mapping_value(dedup_evidence, "outlierReasonCounts"),
    )
    provider_static_current_run_count = max(
        0, int(provider_static_disagreement_counts.get("currentRun") or 0)
    )
    provider_static_carried_count = max(
        0, int(provider_static_disagreement_counts.get("carried") or 0)
    )
    high_risk_review_queue_count = _audit_gate_high_risk_count(review_queue_cause_counts)
    current_run_non_primary_merges = max(
        0,
        merged_count - int(merge_reason_counts.get("primaryUrl") or 0),
    )
    carried_collision_likely_historical_count = (
        carried_source_bundle_collision_count
        if carried_source_bundle_collision_count
        else source_bundle_collision_count
        if merged_count == 0
        else 0
    )
    blockers, warnings = _audit_gate_blockers_and_warnings(
        merged_count=merged_count,
        current_run_non_primary_merges=current_run_non_primary_merges,
        provider_static_disagreement_count=provider_static_disagreement_count,
        current_run_high_risk_review_queue_count=current_run_high_risk_review_queue_count,
        carried_high_risk_review_queue_count=carried_high_risk_review_queue_count,
        carried_collision_likely_historical_count=carried_collision_likely_historical_count,
        high_risk_review_queue_count=high_risk_review_queue_count,
    )

    status = "blocked" if blockers else "warning" if warnings else "pass"
    return {
        "status": status,
        "lifecycleUxReady": not blockers,
        "currentRunMergedCount": merged_count,
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "highRiskReviewQueueCount": high_risk_review_queue_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "providerStaticDisagreementCount": provider_static_disagreement_count,
        "providerStaticDisagreementCurrentRunCount": provider_static_current_run_count,
        "providerStaticDisagreementCarriedCount": provider_static_carried_count,
        "googleSheetsGenericRoleGuardActive": True,
        "carriedCollisionLikelyHistoricalCount": carried_collision_likely_historical_count,
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "blockers": blockers,
        "warnings": warnings,
        "examples": _audit_gate_examples(dedup_evidence),
        "nonzeroReviewQueueCauseCounts": _nonzero_counts(review_queue_cause_counts),
    }


def build_dedup_evidence(
    dedup_stats: Mapping[str, Any],
    canonical_rows: Sequence[CanonicalJob | Mapping[str, Any]],
    *,
    top_limit: int = TOP_MERGED_LIMIT,
    risky_limit: int = RISKY_EXAMPLE_LIMIT,
    seeded_from_existing_output: bool = False,
) -> dict[str, Any]:
    """Build compact diagnostics without changing dedup decisions."""
    rows = [_payload(row) for row in canonical_rows]
    composition: Counter[str] = Counter()
    risk_reason_counts: Counter[str] = Counter()
    outlier_reason_counts: Counter[str] = Counter()
    identity_shape_counts: Counter[str] = Counter()
    review_queue_counts: Counter[str] = Counter()
    review_queue_cause_counts: Counter[str] = Counter()
    identity_quality_counts: Counter[str] = Counter()
    non_provider_identity_provenance_counts: Counter[str] = Counter()
    google_sheets_bundle_shape_counts: Counter[str] = Counter()
    google_sheets_role_bucket_audit_counts: Counter[str] = Counter()
    google_sheets_bucket_intent_counts: Counter[str] = Counter()
    google_sheets_weak_grouping_audit_counts: Counter[str] = Counter()
    top_rows: list[dict[str, Any]] = []
    risky_rows: list[dict[str, Any]] = []
    location_divergence_rows: list[dict[str, Any]] = []
    review_queue_rows: list[dict[str, Any]] = []
    carried_bundle_rows: list[dict[str, Any]] = []
    provider_static_disagreement_rows: list[dict[str, Any]] = []
    source_bundle_collision_count = 0
    current_run_source_bundle_collision_count = 0
    carried_source_bundle_collision_count = 0
    current_run_high_risk_review_queue_count = 0
    carried_high_risk_review_queue_count = 0
    current_run_provider_static_disagreement_count = 0
    carried_provider_static_disagreement_count = 0
    current_run_merged_dedup_keys = {
        clean_text(value) for value in dedup_stats.get("currentRunMergedDedupKeys") or []
    }

    for row in rows:
        bundle = _source_bundle(row)
        for item in bundle:
            composition[_source_class(item)] += 1
        bundle_count = max(int(row.get("sourceBundleCount") or 0), len(bundle))
        if bundle_count > 1:
            source_bundle_collision_count += 1
            summary = _job_summary(row, bundle)
            dedup_key = clean_text(summary.get("dedupKey"))
            origin = (
                "current_run"
                if not seeded_from_existing_output or dedup_key in current_run_merged_dedup_keys
                else "carried_from_existing_output"
            )
            summary["bundleEvidenceOrigin"] = origin
            if origin == "current_run":
                current_run_source_bundle_collision_count += 1
            else:
                carried_source_bundle_collision_count += 1
                carried_bundle_rows.append(summary)
            top_rows.append(summary)
            outlier_reason_counts.update([summary["outlierReason"]])
            identity_shape_counts.update([summary["identityShape"]])
            identity_quality_counts.update([summary["identityQuality"]])
            non_provider_identity_provenance_counts.update(
                [summary["nonProviderIdentityProvenance"]]
            )
            google_sheets_bundle_shape_counts.update([summary["googleSheetsBundleShape"]])
            google_sheets_role_bucket_audit_counts.update([summary["googleSheetsRoleBucketAudit"]])
            google_sheets_bucket_intent_counts.update([summary["googleSheetsBucketIntent"]])
            google_sheets_weak_grouping_audit_counts.update(
                [summary["googleSheetsWeakGroupingAudit"]]
            )
            review_action = _recommended_review_action(summary)
            review_queue_counts.update([review_action])
            review_queue_cause_counts.update([summary["suspectedCause"]])
            current_high_risk, carried_high_risk = _high_risk_origin_counts(summary, origin)
            current_run_high_risk_review_queue_count += current_high_risk
            carried_high_risk_review_queue_count += carried_high_risk
            if review_action != "monitor":
                review_queue_rows.append({**summary, "recommendedReviewAction": review_action})
            if int(summary.get("distinctLocationCount") or 0) > 1:
                location_divergence_rows.append(summary)
            current_disagreement, carried_disagreement, disagreement_rows = (
                _provider_static_disagreement_origin_update(summary, bundle)
            )
            current_run_provider_static_disagreement_count += current_disagreement
            carried_provider_static_disagreement_count += carried_disagreement
            provider_static_disagreement_rows.extend(disagreement_rows)
            reasons = _risky_reasons(row, bundle)
            if reasons:
                risk_reason_counts.update(reasons)
                risky_rows.append({**summary, "riskReasons": reasons})

    top_rows.sort(
        key=lambda row: (
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    risky_rows.sort(
        key=lambda row: (
            ",".join(str(reason) for reason in row.get("riskReasons") or []),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    location_divergence_rows.sort(
        key=lambda row: (
            -int(row.get("distinctLocationCount") or 0),
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    action_order = {action: index for index, action in enumerate(REVIEW_QUEUE_ACTION_KEYS)}
    review_queue_rows.sort(
        key=lambda row: (
            action_order.get(str(row.get("recommendedReviewAction") or ""), len(action_order)),
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    carried_bundle_rows.sort(
        key=lambda row: (
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    provider_static_disagreement_rows.sort(
        key=lambda row: (
            norm_text(row.get("bundleEvidenceOrigin")),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    provider_static_disagreement_count = (
        current_run_provider_static_disagreement_count + carried_provider_static_disagreement_count
    )

    payload = {
        "schemaVersion": 1,
        "mergedCount": max(0, int(dedup_stats.get("mergedCount") or 0)),
        "collisionSamplesCount": max(0, int(dedup_stats.get("collisionSamplesCount") or 0)),
        "mergeReasonCounts": _merge_reason_counts(dedup_stats),
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "providerStaticDisagreementCounts": {
            "total": provider_static_disagreement_count,
            "currentRun": current_run_provider_static_disagreement_count,
            "carried": carried_provider_static_disagreement_count,
        },
        "sourceBundleComposition": {
            key: int(composition.get(key, 0)) for key in ("provider", "static", "social", "other")
        },
        "riskReasonCounts": {
            key: int(risk_reason_counts.get(key, 0))
            for key in (
                "same_title_company_different_location",
                "provider_static_duplicate_disagreement",
                "missing_provider_ids",
                "weak_title_company_only_evidence",
            )
        },
        "outlierReasonCounts": {
            key: int(outlier_reason_counts.get(key, 0)) for key in OUTLIER_REASON_KEYS
        },
        "identityShapeCounts": {
            key: int(identity_shape_counts.get(key, 0)) for key in IDENTITY_SHAPE_KEYS
        },
        "identityQualityCounts": {
            key: int(identity_quality_counts.get(key, 0)) for key in IDENTITY_QUALITY_KEYS
        },
        "nonProviderIdentityProvenanceCounts": {
            key: int(non_provider_identity_provenance_counts.get(key, 0))
            for key in NON_PROVIDER_IDENTITY_PROVENANCE_KEYS
        },
        "googleSheetsBundleShapeCounts": {
            key: int(google_sheets_bundle_shape_counts.get(key, 0))
            for key in GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS
        },
        "googleSheetsRoleBucketAuditCounts": {
            key: int(google_sheets_role_bucket_audit_counts.get(key, 0))
            for key in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS
        },
        "googleSheetsBucketIntentCounts": {
            key: int(google_sheets_bucket_intent_counts.get(key, 0))
            for key in GOOGLE_SHEETS_BUCKET_INTENT_KEYS
        },
        "googleSheetsWeakGroupingAuditCounts": {
            key: int(google_sheets_weak_grouping_audit_counts.get(key, 0))
            for key in GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS
        },
        "reviewQueueCounts": {
            key: int(review_queue_counts.get(key, 0)) for key in REVIEW_QUEUE_ACTION_KEYS
        },
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "reviewQueue": review_queue_rows[: max(0, int(risky_limit))],
        "providerStaticDisagreementExamples": provider_static_disagreement_rows[
            : max(0, int(risky_limit))
        ],
        "carriedBundleExamples": carried_bundle_rows[: max(0, int(risky_limit))],
        "carriedBundleReconciliationRecommendation": {
            "recommendedAction": "rebuild_carried_source_bundle_metadata",
            "destructiveActionAllowed": False,
            "requiresExplicitMaintenanceRun": True,
        }
        if carried_source_bundle_collision_count
        else {},
        "topMergedJobs": top_rows[: max(0, int(top_limit))],
        "topSourceBundleOutliers": top_rows[: max(0, int(top_limit))],
        "locationDivergenceExamples": location_divergence_rows[: max(0, int(risky_limit))],
        "riskyMergeExamples": risky_rows[: max(0, int(risky_limit))],
        "riskyMergeExampleCount": len(risky_rows),
    }
    payload["dedupAuditGate"] = build_dedup_audit_gate(payload)
    return payload
