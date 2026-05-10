"""Read-only deduplication evidence for fetch reports."""

from __future__ import annotations

import unicodedata
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.common.contracts_dedup_review_state import (
    dedup_disagreement_gate_disposition,
    dedup_operator_review_fields,
    dedup_review_pair_public_fields,
    find_dedup_review_pair,
)
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.json_shapes import json_object_rows

TOP_MERGED_LIMIT = 10
RISKY_EXAMPLE_LIMIT = 10
GRACKLEHQ_SOURCE_NAME = "gracklehq"
GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE = (
    "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-"
    "8608_guerrilla-games?page=1"
)
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
PROVIDER_STATIC_DISAGREEMENT_CLASSIFICATION_KEYS = (
    "same_job_different_urls",
    "provider_redirect_or_canonical_url",
    "static_parser_url_variant",
    "title_company_collision",
    "stale_carried_bundle",
    "needs_manual_review",
)
PROVIDER_STATIC_TITLE_COMPANY_COLLISION_AUDIT_KEYS = (
    "carried_location_pollution",
    "carried_location_variant",
    "carried_provider_identity_location_conflict",
    "possible_real_multi_location_conflict",
    "not_carried",
    "unknown",
)


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
GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS = (
    "fixed_by_generic_role_guard",
    "allowed_same_primary_url",
    "historical_carried_bundle",
    "unresolved_current_run_role_bucket",
    "parser_or_sheet_category_noise",
    "needs_narrow_dedup_guard",
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
    provider_urls_all = _clean_values(provider_items, "jobLink", normalize_urls=True)
    static_urls_all = _clean_values(static_items, "jobLink", normalize_urls=True)
    provider_urls = provider_urls_all[:5]
    static_urls = static_urls_all[:5]
    provider_hosts = sorted({_url_host(url) for url in provider_urls_all if _url_host(url)})
    static_hosts = sorted({_url_host(url) for url in static_urls_all if _url_host(url)})
    provider_prefixes = sorted(
        {_path_prefix(url) for url in provider_urls_all if _path_prefix(url)}
    )
    static_prefixes = sorted({_path_prefix(url) for url in static_urls_all if _path_prefix(url)})
    provider_ids_all = _clean_values(provider_items, "sourceJobId")
    static_ids_all = _clean_values(static_items, "sourceJobId")
    provider_ids = provider_ids_all[:5]
    static_ids = static_ids_all[:5]
    shared_tokens = sorted(
        _identifier_tokens([*provider_ids_all, *(_url_path(url) for url in provider_urls_all)])
        & _identifier_tokens([*static_ids_all, *(_url_path(url) for url in static_urls_all)])
    )
    concrete_shared_tokens = _concrete_shared_identifier_tokens(
        provider_ids=provider_ids_all,
        provider_urls=provider_urls_all,
        static_ids=static_ids_all,
        static_urls=static_urls_all,
    )
    source_classes = _source_class_counts(bundle)
    provider_static_only = bool(
        source_classes["provider"]
        and source_classes["static"]
        and not source_classes["social"]
        and not source_classes["other"]
    )
    classification, classification_evidence = _provider_static_disagreement_classification(
        summary=summary,
        provider_urls=provider_urls_all,
        static_urls=static_urls_all,
        provider_hosts=provider_hosts,
        static_hosts=static_hosts,
        provider_ids=provider_ids_all,
        static_ids=static_ids_all,
    )
    if _is_known_gracklehq_gamesjobsdirect_mirror_bundle(bundle):
        classification_evidence = [
            *classification_evidence,
            "known_gracklehq_gamesjobsdirect_mirror_pair",
        ]
    evidence = [
        f"bundle_origin:{clean_text(summary.get('bundleEvidenceOrigin')) or 'unknown'}",
        f"provider_sources:{len(_clean_values(provider_items, 'source'))}",
        f"static_sources:{len(_clean_values(static_items, 'source'))}",
        f"provider_urls:{len(provider_urls_all)}",
        f"static_urls:{len(static_urls_all)}",
        f"provider_ids:{len(provider_ids_all)}",
        f"static_ids:{len(static_ids_all)}",
        f"concrete_shared_tokens:{len(concrete_shared_tokens)}",
        f"provider_static_only:{str(provider_static_only).lower()}",
        f"shared_primary_url:{str(bool(summary.get('sharedPrimaryUrl'))).lower()}",
        f"identity_quality:{clean_text(summary.get('identityQuality')) or 'unknown'}",
        f"classification:{classification}",
    ]
    return {
        "title": clean_text(summary.get("title")),
        "company": clean_text(summary.get("company")),
        "dedupKey": clean_text(summary.get("dedupKey")),
        "bundleEvidenceOrigin": clean_text(summary.get("bundleEvidenceOrigin")),
        "sourceBundleCount": max(0, int(summary.get("sourceBundleCount") or 0)),
        "providerSources": _sample_clean_values(provider_items, "source"),
        "staticSources": _sample_clean_values(static_items, "source"),
        "providerSourceJobIds": provider_ids,
        "staticSourceJobIds": static_ids,
        "providerUrls": provider_urls,
        "staticUrls": static_urls,
        "providerUrlHosts": provider_hosts[:5],
        "staticUrlHosts": static_hosts[:5],
        "providerUrlPathPrefixes": provider_prefixes[:5],
        "staticUrlPathPrefixes": static_prefixes[:5],
        "sharedIdentifierTokens": shared_tokens[:5],
        "concreteSharedIdentifierTokens": concrete_shared_tokens[:5],
        "providerStaticOnly": provider_static_only,
        "distinctLocationCount": max(0, int(summary.get("distinctLocationCount") or 0)),
        "sampleLocations": [
            clean_text(value) for value in summary.get("sampleLocations") or [] if clean_text(value)
        ][:5],
        "identityQuality": clean_text(summary.get("identityQuality")),
        "outlierReason": clean_text(summary.get("outlierReason")),
        "disagreementClassification": classification,
        "disagreementClassificationEvidence": classification_evidence,
        "collisionReviewHint": _provider_static_collision_review_hint(
            classification=classification,
            summary=summary,
            provider_urls=provider_urls,
            static_urls=static_urls,
            provider_ids=provider_ids,
            static_ids=static_ids,
        ),
        "disagreementEvidence": evidence,
    }


def _provider_static_row_with_gate_fields(
    row: Mapping[str, Any], review_state: Any
) -> dict[str, Any]:
    review_pair = find_dedup_review_pair(review_state or {}, row)
    disposition, gate_evidence = dedup_disagreement_gate_disposition(row, review_pair)
    with_gate = {
        **row,
        **dedup_review_pair_public_fields(review_pair),
        "disagreementGateDisposition": disposition,
        "disagreementGateEvidence": gate_evidence,
    }
    return {**with_gate, **dedup_operator_review_fields(with_gate)}


def _is_known_gracklehq_gamesjobsdirect_mirror_bundle(
    bundle: Sequence[Mapping[str, Any]],
) -> bool:
    sources = {clean_text(item.get("source")) for item in bundle if clean_text(item.get("source"))}
    return bool(
        GRACKLEHQ_SOURCE_NAME in sources and GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE in sources
    )


def _provider_static_disagreement_classification(
    *,
    summary: Mapping[str, Any],
    provider_urls: Sequence[str],
    static_urls: Sequence[str],
    provider_hosts: Sequence[str],
    static_hosts: Sequence[str],
    provider_ids: Sequence[str],
    static_ids: Sequence[str],
) -> tuple[str, list[str]]:
    provider_tokens = _identifier_tokens(
        [*provider_ids, *(_url_path(url) for url in provider_urls)]
    )
    static_tokens = _identifier_tokens([*static_ids, *(_url_path(url) for url in static_urls)])
    shared_tokens = sorted(provider_tokens & static_tokens)
    concrete_shared_tokens = _concrete_shared_identifier_tokens(
        provider_ids=provider_ids,
        provider_urls=provider_urls,
        static_ids=static_ids,
        static_urls=static_urls,
    )
    same_host = bool(set(provider_hosts) & set(static_hosts))
    origin = clean_text(summary.get("bundleEvidenceOrigin"))
    location_count = max(0, int(summary.get("distinctLocationCount") or 0))
    evidence = [
        f"origin:{origin or 'unknown'}",
        f"provider_hosts:{len(provider_hosts)}",
        f"static_hosts:{len(static_hosts)}",
        f"shared_identifier_tokens:{len(shared_tokens)}",
        f"concrete_shared_identifier_tokens:{len(concrete_shared_tokens)}",
        f"locations:{location_count}",
    ]
    if shared_tokens:
        evidence.append(f"shared_token:{shared_tokens[0]}")
    if concrete_shared_tokens:
        evidence.append(f"concrete_shared_token:{concrete_shared_tokens[0]}")
    if (
        location_count > 1
        and origin != "carried_from_existing_output"
        and _provider_static_locations_are_single_effective_place(summary)
    ):
        evidence.append("single_effective_location_variant")
    elif location_count > 1:
        return "title_company_collision", evidence + ["multiple_locations"]
    if origin == "carried_from_existing_output" and (
        not provider_urls or not static_urls or not provider_ids or not static_ids
    ):
        return "stale_carried_bundle", evidence + ["missing_url_or_id_side"]
    if same_host:
        return "provider_redirect_or_canonical_url", evidence + ["same_host"]
    if shared_tokens and static_urls:
        return "static_parser_url_variant", evidence + ["provider_static_shared_identifier"]
    if provider_ids and static_ids and provider_urls and static_urls:
        return "same_job_different_urls", evidence + ["both_sides_have_ids_and_urls"]
    return "needs_manual_review", evidence


def _provider_static_locations_are_single_effective_place(summary: Mapping[str, Any]) -> bool:
    labels = [
        clean_text(value) for value in summary.get("sampleLocations") or [] if clean_text(value)
    ]
    if len(labels) <= 1:
        return False
    plausible_city_keys: set[str] = set()
    polluted_count = 0
    for label in labels:
        city, country = _location_label_parts(label)
        city_key = _location_city_key(label)
        if not city_key and not country:
            continue
        if country:
            plausible_city_keys.add(city_key or norm_text(country))
            continue
        if (city and classify_city_garbage(city)) or _location_token_overlaps_title_or_company(
            city, summary
        ):
            polluted_count += 1
            continue
        plausible_city_keys.add(city_key)
    return (
        bool(plausible_city_keys)
        and len(plausible_city_keys) == 1
        and (polluted_count > 0 or len(plausible_city_keys) < len(labels))
    )


def _provider_static_collision_review_hint(
    *,
    classification: str,
    summary: Mapping[str, Any],
    provider_urls: Sequence[str],
    static_urls: Sequence[str],
    provider_ids: Sequence[str],
    static_ids: Sequence[str],
) -> str:
    location_count = max(0, int(summary.get("distinctLocationCount") or 0))
    if classification == "title_company_collision" and location_count > 1:
        return "different_locations_same_title_company"
    if not provider_urls or not static_urls or not provider_ids or not static_ids:
        return "provider_static_location_missing"
    if max(0, int(summary.get("sourceBundleCount") or 0)) > 2:
        return "multiple_sources_need_manual_review"
    if location_count <= 1 and provider_urls and static_urls:
        return "same_location_different_provider_static_urls"
    return "unknown"


def _company_countryless_location_token_counts(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Counter[str]]:
    counts: dict[str, Counter[str]] = {}
    for row in rows:
        if clean_text(row.get("bundleEvidenceOrigin")) != "carried_from_existing_output":
            continue
        company = norm_text(row.get("company"))
        if not company:
            continue
        for label in row.get("sampleLocations") or []:
            city, country = _location_label_parts(clean_text(label))
            token = norm_text(city)
            if not token or country:
                continue
            counts.setdefault(company, Counter())[token] += 1
    return counts


def _location_token_overlaps_title_or_company(city: str, row: Mapping[str, Any]) -> bool:
    city_tokens = {
        token
        for token in norm_text(city).replace("-", " ").replace("_", " ").replace("/", " ").split()
        if token
    }
    if not city_tokens:
        return False
    return bool(city_tokens & (set(_title_tokens(row)) | set(_company_tokens(row))))


def _location_city_key(label: str) -> str:
    city = norm_text(_location_label_parts(label)[0])
    if not city:
        return ""
    return "".join(
        char for char in unicodedata.normalize("NFKD", city) if not unicodedata.combining(char)
    )


def _has_shared_provider_static_identity(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("sharedPrimaryUrl")
        or row.get("sharedIdentifierTokens")
        or set(row.get("providerUrlHosts") or []) & set(row.get("staticUrlHosts") or [])
    )


def _carried_location_variant_city(row: Mapping[str, Any], plausible_labels: Sequence[str]) -> str:
    city_keys = {
        _location_city_key(label) for label in plausible_labels if _location_city_key(label)
    }
    if (
        len(plausible_labels) > 1
        and len(city_keys) == 1
        and _has_shared_provider_static_identity(row)
    ):
        return next(iter(city_keys))
    return ""


def _has_carried_provider_identity_location_conflict(
    row: Mapping[str, Any], plausible_labels: Sequence[str], polluted_labels: Sequence[str]
) -> bool:
    return bool(
        len(plausible_labels) > 1
        and polluted_labels
        and row.get("sharedIdentifierTokens")
        and set(row.get("providerUrlHosts") or []) & set(row.get("staticUrlHosts") or [])
    )


def _carried_location_label_bucket(
    raw_label: Any, row: Mapping[str, Any], repeated_for_company: Counter[str]
) -> tuple[str, str, list[str]]:
    label = clean_text(raw_label)
    if not label:
        return "", "", []
    city, country = _location_label_parts(label)
    city_garbage = classify_city_garbage(city) if city else ""
    repeated_token = norm_text(city)
    repeated_pollution = (
        bool(repeated_token) and repeated_for_company.get(repeated_token, 0) >= 3 and not country
    )
    if country:
        return "plausible", label, []
    if city_garbage:
        return (
            "polluted",
            label,
            [
                f"garbage_category:{city_garbage}",
                f"sample_location:{norm_text(city)}",
            ],
        )
    if _location_token_overlaps_title_or_company(city, row):
        return (
            "polluted",
            label,
            [
                "location_token_overlaps_title",
                f"sample_location:{norm_text(city)}",
            ],
        )
    if repeated_pollution:
        return (
            "polluted",
            label,
            [
                f"repeated_company_location_token:{repeated_token}",
                f"sample_location:{repeated_token}",
            ],
        )
    return "plausible", label, []


def _provider_static_title_company_collision_audit(
    row: Mapping[str, Any],
    repeated_countryless_tokens: Mapping[str, Counter[str]],
) -> tuple[str, list[str]]:
    origin = clean_text(row.get("bundleEvidenceOrigin")) or "unknown"
    if origin != "carried_from_existing_output":
        return "not_carried", [f"origin:{origin}"]

    company_key = norm_text(row.get("company"))
    repeated_for_company = repeated_countryless_tokens.get(company_key, Counter())
    plausible_labels: list[str] = []
    polluted_labels: list[str] = []
    evidence = [f"origin:{origin}"]

    for raw_label in row.get("sampleLocations") or []:
        bucket, label, label_evidence = _carried_location_label_bucket(
            raw_label, row, repeated_for_company
        )
        if not bucket:
            continue
        if bucket == "plausible":
            plausible_labels.append(label)
            continue
        polluted_labels.append(label)
        evidence.extend(label_evidence)

    evidence.append(f"plausible_location_count:{len(plausible_labels)}")
    evidence.append(f"polluted_location_count:{len(polluted_labels)}")
    if polluted_labels and len(plausible_labels) == 1:
        return "carried_location_pollution", evidence[:8]
    variant_city = _carried_location_variant_city(row, plausible_labels)
    if variant_city:
        evidence.append(f"equivalent_city:{variant_city}")
        return "carried_location_variant", evidence[:8]
    if _has_carried_provider_identity_location_conflict(row, plausible_labels, polluted_labels):
        evidence.append("shared_provider_identity")
        return "carried_provider_identity_location_conflict", evidence[:8]
    if len(plausible_labels) > 1:
        return "possible_real_multi_location_conflict", evidence[:8]
    return "unknown", evidence[:8]


def _high_risk_origin_counts(
    summary: Mapping[str, Any], origin: str, current_run_known_mirror_pair_dedup_keys: set[str]
) -> tuple[int, int]:
    if (
        origin == "current_run"
        and clean_text(summary.get("dedupKey")) in current_run_known_mirror_pair_dedup_keys
    ):
        return 0, 0
    role_bucket_classification = _google_sheets_role_bucket_audit_classification(summary)
    if role_bucket_classification == "allowed_same_primary_url":
        return 0, 0
    if summary.get("suspectedCause") not in DEDUP_AUDIT_GATE_BLOCKER_CAUSES:
        return 0, 0
    return (1, 0) if origin == "current_run" else (0, 1)


def _review_pressure_origin_counts(
    *,
    summary: Mapping[str, Any],
    origin: str,
    current_run_known_mirror_pair_dedup_keys: set[str],
    review_action: str,
) -> tuple[int, int, int, int, int, int]:
    current_high_risk, carried_high_risk = _high_risk_origin_counts(
        summary, origin, current_run_known_mirror_pair_dedup_keys
    )
    current_blocking = 0
    carried_blocking = 0
    current_monitor = 0
    carried_monitor = 0
    if review_action == "monitor":
        current_monitor = current_high_risk
        carried_monitor = carried_high_risk
    else:
        current_blocking = current_high_risk
        carried_blocking = carried_high_risk
    return (
        current_high_risk,
        carried_high_risk,
        current_blocking,
        carried_blocking,
        current_monitor,
        carried_monitor,
    )


def _is_google_sheets_role_bucket_summary(summary: Mapping[str, Any]) -> bool:
    if clean_text(summary.get("nonProviderIdentityProvenance")) != "google_sheets_row_identity":
        return False
    if clean_text(summary.get("googleSheetsBundleShape")) in {
        "role_category_bucket",
        "single_location_many_urls",
        "multi_location_many_urls",
        "company_role_family",
    }:
        return True
    return clean_text(summary.get("suspectedCause")) in {
        "spreadsheet_role_bucket_needs_review",
        "google_sheets_role_bucket_needs_review",
    }


def _google_sheets_role_bucket_audit_classification(summary: Mapping[str, Any]) -> str:
    if not _is_google_sheets_role_bucket_summary(summary):
        return ""
    if summary.get("sharedPrimaryUrl"):
        return "allowed_same_primary_url"
    if clean_text(summary.get("bundleEvidenceOrigin")) != "current_run":
        return "historical_carried_bundle"
    audit = clean_text(summary.get("googleSheetsRoleBucketAudit"))
    intent = clean_text(summary.get("googleSheetsBucketIntent"))
    weak_audit = clean_text(summary.get("googleSheetsWeakGroupingAudit"))
    if (
        audit
        in {
            "likely_spreadsheet_category_bucket",
            "listing_or_search_url_bucket",
            "parser_normalized_role_title",
        }
        or intent
        in {
            "likely_spreadsheet_taxonomy_bucket",
            "listing_or_search_bucket",
            "parser_normalized_bucket",
        }
        or weak_audit
        in {
            "role_bucket_listing_grouping",
            "parser_pollution_grouping",
        }
    ):
        return "parser_or_sheet_category_noise"
    if audit == "role_family_needs_manual_review" or intent in {
        "possible_role_family",
        "weak_title_company_grouping",
    }:
        return "needs_narrow_dedup_guard"
    return "unresolved_current_run_role_bucket"


def _google_sheets_guard_audit_example(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": "fixed_by_generic_role_guard",
        "title": clean_text(row.get("incomingTitle")),
        "company": clean_text(row.get("incomingCompany")),
        "incomingSource": clean_text(row.get("incomingSource")),
        "incomingJobLink": normalize_url(row.get("incomingJobLink")),
        "incomingSourceJobId": clean_text(row.get("incomingSourceJobId")),
        "targetTitle": clean_text(row.get("targetTitle")),
        "targetCompany": clean_text(row.get("targetCompany")),
        "targetSource": clean_text(row.get("targetSource")),
        "targetJobLink": normalize_url(row.get("targetJobLink")),
        "targetSourceJobId": clean_text(row.get("targetSourceJobId")),
        "blockedMergeReason": clean_text(row.get("blockedMergeReason")) or "unknown",
        "guardReason": clean_text(row.get("guardReason")) or "unknown",
        "bundleEvidenceOrigin": "current_run",
        "evidence": [
            "different_concrete_primary_urls",
            f"blocked_merge_reason:{clean_text(row.get('blockedMergeReason')) or 'unknown'}",
            f"guard_reason:{clean_text(row.get('guardReason')) or 'unknown'}",
        ],
    }


def _google_sheets_role_bucket_audit_example(summary: Mapping[str, Any]) -> dict[str, Any]:
    classification = _google_sheets_role_bucket_audit_classification(summary)
    evidence = [
        f"classification:{classification or 'unknown'}",
        f"origin:{clean_text(summary.get('bundleEvidenceOrigin')) or 'unknown'}",
        f"shape:{clean_text(summary.get('googleSheetsBundleShape')) or 'unknown'}",
        f"audit:{clean_text(summary.get('googleSheetsRoleBucketAudit')) or 'unknown'}",
        f"intent:{clean_text(summary.get('googleSheetsBucketIntent')) or 'unknown'}",
        f"weak_audit:{clean_text(summary.get('googleSheetsWeakGroupingAudit')) or 'unknown'}",
        f"shared_primary_url:{str(bool(summary.get('sharedPrimaryUrl'))).lower()}",
        f"unique_urls:{int(summary.get('uniqueJobLinkCount') or 0)}",
        f"url_hosts:{int(summary.get('uniqueUrlHostCount') or 0)}",
        f"url_path_prefixes:{int(summary.get('uniqueUrlPathPrefixCount') or 0)}",
    ]
    evidence.extend(str(item) for item in summary.get("googleSheetsRoleBucketAuditEvidence") or [])
    evidence.extend(str(item) for item in summary.get("googleSheetsWeakGroupingEvidence") or [])
    return {
        "classification": classification or "unknown",
        "title": clean_text(summary.get("title")),
        "company": clean_text(summary.get("company")),
        "dedupKey": clean_text(summary.get("dedupKey")),
        "sourceBundleCount": max(0, int(summary.get("sourceBundleCount") or 0)),
        "bundleEvidenceOrigin": clean_text(summary.get("bundleEvidenceOrigin")) or "unknown",
        "suspectedCause": clean_text(summary.get("suspectedCause")),
        "googleSheetsBundleShape": clean_text(summary.get("googleSheetsBundleShape")),
        "googleSheetsRoleBucketAudit": clean_text(summary.get("googleSheetsRoleBucketAudit")),
        "googleSheetsBucketIntent": clean_text(summary.get("googleSheetsBucketIntent")),
        "googleSheetsWeakGroupingAudit": clean_text(summary.get("googleSheetsWeakGroupingAudit")),
        "sharedPrimaryUrl": bool(summary.get("sharedPrimaryUrl")),
        "uniqueJobLinkCount": max(0, int(summary.get("uniqueJobLinkCount") or 0)),
        "urlHostDiversity": max(0, int(summary.get("uniqueUrlHostCount") or 0)),
        "urlPathPrefixDiversity": max(0, int(summary.get("uniqueUrlPathPrefixCount") or 0)),
        "evidence": list(dict.fromkeys(evidence))[:16],
    }


def _google_sheets_role_bucket_audit_summary(
    *,
    role_bucket_rows: Sequence[Mapping[str, Any]],
    guard_samples: Sequence[Mapping[str, Any]],
    guard_blocked_count: int,
    limit: int = 10,
) -> dict[str, Any]:
    classification_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    current_run_count = 0
    carried_count = 0
    classification_counts["fixed_by_generic_role_guard"] += max(0, int(guard_blocked_count))
    for row in role_bucket_rows:
        classification = _google_sheets_role_bucket_audit_classification(row)
        if not classification:
            continue
        classification_counts.update([classification])
        if clean_text(row.get("bundleEvidenceOrigin")) == "current_run":
            current_run_count += 1
        else:
            carried_count += 1
        examples.append(_google_sheets_role_bucket_audit_example(row))
    for row in guard_samples:
        examples.append(_google_sheets_guard_audit_example(row))
    examples.sort(
        key=lambda row: (
            GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS.index(row.get("classification"))
            if row.get("classification") in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS
            else len(GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS),
            norm_text(row.get("company")),
            norm_text(row.get("title") or row.get("targetTitle")),
            norm_text(row.get("dedupKey")),
        )
    )
    unresolved_count = sum(
        classification_counts.get(key, 0)
        for key in (
            "unresolved_current_run_role_bucket",
            "parser_or_sheet_category_noise",
            "needs_narrow_dedup_guard",
        )
    )
    return {
        "totalRoleBucketCount": len(role_bucket_rows) + max(0, int(guard_blocked_count)),
        "currentRunRoleBucketCount": current_run_count,
        "carriedHistoricalRoleBucketCount": carried_count,
        "blockedByDifferentPrimaryUrlCount": max(0, int(guard_blocked_count)),
        "allowedSamePrimaryUrlCount": int(classification_counts.get("allowed_same_primary_url", 0)),
        "likelyHistoricalCollisionCount": int(
            classification_counts.get("historical_carried_bundle", 0)
        ),
        "likelyParserCategoryBucketCount": int(
            classification_counts.get("parser_or_sheet_category_noise", 0)
        ),
        "unresolvedRoleBucketCount": unresolved_count,
        "classificationCounts": {
            key: int(classification_counts.get(key, 0))
            for key in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS
        },
        "examples": examples[: max(0, int(limit))],
    }


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
    known_mirror_pair = max(0, int(dedup_stats.get("mergedByKnownMirrorPair") or 0))
    sparse_explicit = dedup_stats.get("mergedBySparseIdentity")
    total = max(0, int(dedup_stats.get("mergedCount") or 0))
    sparse = (
        max(0, int(sparse_explicit or 0))
        if sparse_explicit is not None
        else max(0, total - primary - secondary - social - known_mirror_pair)
    )
    known = primary + secondary + social + known_mirror_pair + sparse
    return {
        "primaryUrl": primary,
        "secondaryKey": secondary,
        "socialKey": social,
        "knownMirrorPair": known_mirror_pair,
        "sparseIdentity": sparse,
        "unknown": max(0, total - known),
    }


def _current_run_merge_examples(dedup_stats: Mapping[str, Any]) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for row in json_object_rows(dedup_stats.get("collisionSamples")):
        merge_reason = clean_text(row.get("reason")) or "unknown"
        blocks_lifecycle = merge_reason not in {"primary_url", "known_mirror_pair"}
        example = {
            "mergeReason": merge_reason,
            "existingDedupKey": clean_text(row.get("existingDedupKey")),
            "incomingSource": clean_text(row.get("incomingSource")),
            "title": clean_text(row.get("incomingTitle")),
            "company": clean_text(row.get("incomingCompany")),
            "incomingJobLink": normalize_url(row.get("incomingJobLink")),
            "bundleEvidenceOrigin": "current_run",
            "blocksLifecycle": blocks_lifecycle,
            "nonBlockingReason": "",
            "recommendedReviewAction": (
                "review_current_run_merge" if blocks_lifecycle else "monitor"
            ),
            "suspectedCause": (
                "current_run_non_primary_merge" if blocks_lifecycle else "known_mirror_pair"
            ),
        }
        if merge_reason == "known_mirror_pair":
            example["nonBlockingReason"] = "known_gracklehq_gamesjobsdirect_mirror_pair"
        examples.append(example)
    return examples


def _current_run_merge_examples_by_reason(
    dedup_stats: Mapping[str, Any], *, limit_per_reason: int = 5
) -> dict[str, list[dict[str, Any]]]:
    by_reason = {
        "secondaryKey": [],
        "sparseIdentity": [],
        "knownMirrorPair": [],
        "primaryUrl": [],
        "unknown": [],
    }
    reason_keys = {
        "secondary_key": "secondaryKey",
        "sparse_identity": "sparseIdentity",
        "known_mirror_pair": "knownMirrorPair",
        "primary_url": "primaryUrl",
    }
    for example in _current_run_merge_examples(dedup_stats):
        reason = clean_text(example.get("mergeReason"))
        key = reason_keys.get(reason, "unknown")
        if len(by_reason[key]) < max(0, int(limit_per_reason)):
            by_reason[key].append(example)
    return by_reason


def _current_run_non_primary_merge_counts(merge_reason_counts: Mapping[str, Any]) -> dict[str, int]:
    secondary = max(0, int(merge_reason_counts.get("secondaryKey") or 0))
    sparse = max(0, int(merge_reason_counts.get("sparseIdentity") or 0))
    social = max(0, int(merge_reason_counts.get("socialKey") or 0))
    unknown = max(0, int(merge_reason_counts.get("unknown") or 0))
    known_mirror_pair = max(0, int(merge_reason_counts.get("knownMirrorPair") or 0))
    return {
        "secondaryKey": secondary,
        "sparseIdentity": sparse,
        "socialKey": social,
        "unknown": unknown,
        "knownMirrorPair": known_mirror_pair,
        "blocking": secondary + sparse + social + unknown,
        "nonBlockingKnownMirrorPair": known_mirror_pair,
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


def _provider_static_gate_alerts(
    *,
    current_run_provider_static_disagreement_blocking_count: int,
    carried_provider_static_disagreement_blocking_count: int,
    provider_static_location_pollution_count: int,
    provider_static_auto_safe_warning_count: int,
    provider_static_reviewed_safe_warning_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if (
        current_run_provider_static_disagreement_blocking_count > 0
        or carried_provider_static_disagreement_blocking_count > 0
    ):
        blockers.append("provider_static_disagreement_needs_review")
    if provider_static_location_pollution_count > 0:
        warnings.append("carried_provider_static_location_pollution_present")
    if provider_static_auto_safe_warning_count > 0:
        warnings.append("carried_provider_static_auto_safe_variants_present")
    if provider_static_reviewed_safe_warning_count > 0:
        warnings.append("carried_provider_static_reviewed_safe_present")
    return blockers, warnings


def _audit_gate_blockers_and_warnings(
    *,
    primary_url_merge_count: int,
    current_run_non_primary_merges: int,
    current_run_provider_static_disagreement_blocking_count: int,
    carried_provider_static_disagreement_blocking_count: int,
    provider_static_location_pollution_count: int,
    provider_static_auto_safe_warning_count: int,
    provider_static_reviewed_safe_warning_count: int,
    current_run_blocking_review_queue_count: int,
    carried_blocking_review_queue_count: int,
    current_run_monitor_review_queue_count: int,
    carried_monitor_review_queue_count: int,
    carried_collision_likely_historical_count: int,
    blocking_review_queue_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if current_run_non_primary_merges > 0:
        blockers.append("current_run_non_primary_merges_need_review")
    elif primary_url_merge_count > 0:
        warnings.append("current_run_primary_url_merges_present")
    provider_static_blockers, provider_static_warnings = _provider_static_gate_alerts(
        current_run_provider_static_disagreement_blocking_count=(
            current_run_provider_static_disagreement_blocking_count
        ),
        carried_provider_static_disagreement_blocking_count=(
            carried_provider_static_disagreement_blocking_count
        ),
        provider_static_location_pollution_count=provider_static_location_pollution_count,
        provider_static_auto_safe_warning_count=provider_static_auto_safe_warning_count,
        provider_static_reviewed_safe_warning_count=provider_static_reviewed_safe_warning_count,
    )
    blockers.extend(provider_static_blockers)
    warnings.extend(provider_static_warnings)
    if current_run_blocking_review_queue_count > 0:
        blockers.append("high_risk_review_queue_causes_need_review")
    elif blocking_review_queue_count and not carried_blocking_review_queue_count:
        blockers.append("high_risk_review_queue_causes_need_review")
    if carried_blocking_review_queue_count > 0:
        warnings.append("carried_high_risk_review_queue_causes_present")
    if current_run_monitor_review_queue_count > 0 or carried_monitor_review_queue_count > 0:
        warnings.append("monitor_review_queue_diagnostics_present")
    if carried_collision_likely_historical_count > 0:
        warnings.append("carried_source_bundle_collisions_present")
    return blockers, warnings


def _audit_gate_examples(dedup_evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    provider_static_examples = json_object_rows(
        dedup_evidence.get("providerStaticDisagreementExamples")
    )
    blocking_provider_static_examples = [
        row
        for row in provider_static_examples
        if clean_text(row.get("disagreementGateDisposition")) == "blocked"
    ]
    if blocking_provider_static_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
                "disagreementClassification": clean_text(row.get("disagreementClassification")),
                "disagreementGateDisposition": clean_text(row.get("disagreementGateDisposition")),
                "dedupReviewStatus": clean_text(row.get("dedupReviewStatus")),
                "collisionReviewHint": clean_text(row.get("collisionReviewHint")),
                "carriedLocationPollutionAudit": clean_text(
                    row.get("carriedLocationPollutionAudit")
                ),
            }
            for row in blocking_provider_static_examples[:5]
        ]
    current_run_merge_examples = [
        row
        for row in json_object_rows(dedup_evidence.get("currentRunMergeExamples"))
        if row.get("blocksLifecycle") is True
    ]
    if current_run_merge_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": clean_text(row.get("recommendedReviewAction")),
                "suspectedCause": clean_text(row.get("suspectedCause")),
                "incomingSource": clean_text(row.get("incomingSource")),
                "mergeReason": clean_text(row.get("mergeReason")),
                "existingDedupKey": clean_text(row.get("existingDedupKey")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
            }
            for row in current_run_merge_examples[:5]
        ]
    warning_provider_static_examples = [
        row
        for row in provider_static_examples
        if clean_text(row.get("disagreementGateDisposition")) == "warning"
    ]
    if warning_provider_static_examples:
        return [
            {
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "recommendedReviewAction": "review_provider_static_disagreement",
                "suspectedCause": "provider_static_disagreement",
                "sourceBundleCount": max(0, int(row.get("sourceBundleCount") or 0)),
                "identityQuality": clean_text(row.get("identityQuality")),
                "bundleEvidenceOrigin": clean_text(row.get("bundleEvidenceOrigin")),
                "disagreementClassification": clean_text(row.get("disagreementClassification")),
                "disagreementGateDisposition": clean_text(row.get("disagreementGateDisposition")),
                "dedupReviewStatus": clean_text(row.get("dedupReviewStatus")),
                "collisionReviewHint": clean_text(row.get("collisionReviewHint")),
                "carriedLocationPollutionAudit": clean_text(
                    row.get("carriedLocationPollutionAudit")
                ),
            }
            for row in warning_provider_static_examples[:5]
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
    current_run_blocking_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunBlockingReviewQueueCount") or 0)
    )
    carried_blocking_review_queue_count = max(
        0, int(dedup_evidence.get("carriedBlockingReviewQueueCount") or 0)
    )
    current_run_monitor_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunMonitorReviewQueueCount") or 0)
    )
    carried_monitor_review_queue_count = max(
        0, int(dedup_evidence.get("carriedMonitorReviewQueueCount") or 0)
    )
    merge_reason_counts = _mapping_value(dedup_evidence, "mergeReasonCounts")
    review_queue_cause_counts = _mapping_value(dedup_evidence, "reviewQueueCauseCounts")
    provider_static_disagreement_counts = _mapping_value(
        dedup_evidence, "providerStaticDisagreementCounts"
    )
    provider_static_disagreement_gate_counts = _mapping_value(
        dedup_evidence, "providerStaticDisagreementGateCounts"
    )
    google_sheets_role_bucket_audit = _mapping_value(dedup_evidence, "googleSheetsRoleBucketAudit")
    title_company_collision_audit_counts = _mapping_value(
        dedup_evidence, "providerStaticTitleCompanyCollisionAuditCounts"
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
    current_run_provider_static_disagreement_blocking_count = max(
        0, int(provider_static_disagreement_gate_counts.get("currentRunBlocked") or 0)
    )
    carried_provider_static_disagreement_blocking_count = max(
        0, int(provider_static_disagreement_gate_counts.get("carriedBlocked") or 0)
    )
    provider_static_location_pollution_count = max(
        0, int(title_company_collision_audit_counts.get("carried_location_pollution") or 0)
    )
    provider_static_auto_safe_warning_count = max(
        0, int(provider_static_disagreement_gate_counts.get("autoSafeWarning") or 0)
    )
    provider_static_reviewed_safe_warning_count = max(
        0, int(provider_static_disagreement_gate_counts.get("reviewedSafeWarning") or 0)
    )
    if (
        "currentRunHighRiskReviewQueueCount" in dedup_evidence
        or "carriedHighRiskReviewQueueCount" in dedup_evidence
    ):
        high_risk_review_queue_count = (
            current_run_high_risk_review_queue_count + carried_high_risk_review_queue_count
        )
    else:
        high_risk_review_queue_count = _audit_gate_high_risk_count(review_queue_cause_counts)
    if (
        "currentRunBlockingReviewQueueCount" in dedup_evidence
        or "carriedBlockingReviewQueueCount" in dedup_evidence
    ):
        blocking_review_queue_count = (
            current_run_blocking_review_queue_count + carried_blocking_review_queue_count
        )
    else:
        current_run_blocking_review_queue_count = current_run_high_risk_review_queue_count
        carried_blocking_review_queue_count = carried_high_risk_review_queue_count
        blocking_review_queue_count = high_risk_review_queue_count
    current_run_non_primary_merges = max(
        0,
        merged_count - int(merge_reason_counts.get("primaryUrl") or 0),
    )
    current_run_non_primary_merges = max(
        0, current_run_non_primary_merges - int(merge_reason_counts.get("knownMirrorPair") or 0)
    )
    primary_url_merge_count = max(0, int(merge_reason_counts.get("primaryUrl") or 0))
    carried_collision_likely_historical_count = (
        carried_source_bundle_collision_count
        if carried_source_bundle_collision_count
        else source_bundle_collision_count
        if merged_count == 0
        else 0
    )
    blockers, warnings = _audit_gate_blockers_and_warnings(
        primary_url_merge_count=primary_url_merge_count,
        current_run_non_primary_merges=current_run_non_primary_merges,
        current_run_provider_static_disagreement_blocking_count=(
            current_run_provider_static_disagreement_blocking_count
        ),
        carried_provider_static_disagreement_blocking_count=(
            carried_provider_static_disagreement_blocking_count
        ),
        provider_static_location_pollution_count=provider_static_location_pollution_count,
        provider_static_auto_safe_warning_count=provider_static_auto_safe_warning_count,
        provider_static_reviewed_safe_warning_count=provider_static_reviewed_safe_warning_count,
        current_run_blocking_review_queue_count=current_run_blocking_review_queue_count,
        carried_blocking_review_queue_count=carried_blocking_review_queue_count,
        current_run_monitor_review_queue_count=current_run_monitor_review_queue_count,
        carried_monitor_review_queue_count=carried_monitor_review_queue_count,
        carried_collision_likely_historical_count=carried_collision_likely_historical_count,
        blocking_review_queue_count=blocking_review_queue_count,
    )

    status = "blocked" if blockers else "warning" if warnings else "pass"
    return {
        "status": status,
        "lifecycleUxReady": not blockers,
        "currentRunMergedCount": merged_count,
        "currentRunNonPrimaryMergeCounts": _current_run_non_primary_merge_counts(
            merge_reason_counts
        ),
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "highRiskReviewQueueCount": high_risk_review_queue_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "blockingReviewQueueCount": blocking_review_queue_count,
        "currentRunBlockingReviewQueueCount": current_run_blocking_review_queue_count,
        "carriedBlockingReviewQueueCount": carried_blocking_review_queue_count,
        "monitorReviewQueueCount": (
            current_run_monitor_review_queue_count + carried_monitor_review_queue_count
        ),
        "currentRunMonitorReviewQueueCount": current_run_monitor_review_queue_count,
        "carriedMonitorReviewQueueCount": carried_monitor_review_queue_count,
        "providerStaticDisagreementCount": provider_static_disagreement_count,
        "providerStaticDisagreementCurrentRunCount": provider_static_current_run_count,
        "providerStaticDisagreementCarriedCount": provider_static_carried_count,
        "providerStaticDisagreementBlockedCount": max(
            0,
            current_run_provider_static_disagreement_blocking_count
            + carried_provider_static_disagreement_blocking_count,
        ),
        "providerStaticDisagreementWarningCount": max(
            0, int(provider_static_disagreement_gate_counts.get("warning") or 0)
        ),
        "googleSheetsGenericRoleGuardActive": True,
        "googleSheetsRoleBucketUnresolvedCount": max(
            0, int(google_sheets_role_bucket_audit.get("unresolvedRoleBucketCount") or 0)
        ),
        "googleSheetsRoleBucketGuardBlockedCount": max(
            0,
            int(google_sheets_role_bucket_audit.get("blockedByDifferentPrimaryUrlCount") or 0),
        ),
        "googleSheetsRoleBucketHistoricalCount": max(
            0,
            int(google_sheets_role_bucket_audit.get("likelyHistoricalCollisionCount") or 0),
        ),
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
    review_state: Any = None,
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
    provider_static_disagreement_classification_counts: Counter[str] = Counter()
    top_rows: list[dict[str, Any]] = []
    risky_rows: list[dict[str, Any]] = []
    location_divergence_rows: list[dict[str, Any]] = []
    review_queue_rows: list[dict[str, Any]] = []
    carried_bundle_rows: list[dict[str, Any]] = []
    google_sheets_role_bucket_rows: list[dict[str, Any]] = []
    provider_static_disagreement_rows: list[dict[str, Any]] = []
    source_bundle_collision_count = 0
    current_run_source_bundle_collision_count = 0
    carried_source_bundle_collision_count = 0
    current_run_high_risk_review_queue_count = 0
    carried_high_risk_review_queue_count = 0
    current_run_blocking_review_queue_count = 0
    carried_blocking_review_queue_count = 0
    current_run_monitor_review_queue_count = 0
    carried_monitor_review_queue_count = 0
    current_run_provider_static_disagreement_count = 0
    carried_provider_static_disagreement_count = 0
    current_run_merged_dedup_keys = {
        clean_text(value) for value in dedup_stats.get("currentRunMergedDedupKeys") or []
    }
    current_run_known_mirror_pair_dedup_keys = {
        clean_text(value) for value in dedup_stats.get("currentRunKnownMirrorPairDedupKeys") or []
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
            if _is_google_sheets_role_bucket_summary(summary):
                google_sheets_role_bucket_rows.append(summary)
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
            (
                current_high_risk,
                carried_high_risk,
                current_blocking,
                carried_blocking,
                current_monitor,
                carried_monitor,
            ) = _review_pressure_origin_counts(
                summary=summary,
                origin=origin,
                current_run_known_mirror_pair_dedup_keys=current_run_known_mirror_pair_dedup_keys,
                review_action=review_action,
            )
            current_run_high_risk_review_queue_count += current_high_risk
            carried_high_risk_review_queue_count += carried_high_risk
            current_run_blocking_review_queue_count += current_blocking
            carried_blocking_review_queue_count += carried_blocking
            current_run_monitor_review_queue_count += current_monitor
            carried_monitor_review_queue_count += carried_monitor
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
            provider_static_disagreement_classification_counts.update(
                row.get("disagreementClassification", "needs_manual_review")
                for row in disagreement_rows
            )
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
    provider_static_title_company_collision_rows = [
        row
        for row in provider_static_disagreement_rows
        if row.get("disagreementClassification") == "title_company_collision"
    ]
    repeated_countryless_tokens = _company_countryless_location_token_counts(
        provider_static_title_company_collision_rows
    )
    provider_static_disagreement_rows = [
        {
            **row,
            **(
                {
                    "carriedLocationPollutionAudit": audit,
                    "carriedLocationPollutionEvidence": evidence,
                }
                if clean_text(row.get("disagreementClassification")) == "title_company_collision"
                else {
                    "carriedLocationPollutionAudit": "",
                    "carriedLocationPollutionEvidence": [],
                }
            ),
        }
        for row in provider_static_disagreement_rows
        for audit, evidence in [
            _provider_static_title_company_collision_audit(row, repeated_countryless_tokens)
            if clean_text(row.get("disagreementClassification")) == "title_company_collision"
            else ("", [])
        ]
    ]
    provider_static_disagreement_rows = [
        _provider_static_row_with_gate_fields(row, review_state or {})
        for row in provider_static_disagreement_rows
    ]
    disposition_order = {"blocked": 0, "warning": 1}
    provider_static_disagreement_rows.sort(
        key=lambda row: (
            disposition_order.get(clean_text(row.get("disagreementGateDisposition")), 9),
            norm_text(row.get("bundleEvidenceOrigin")),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    provider_static_title_company_collision_rows = [
        row
        for row in provider_static_disagreement_rows
        if row.get("disagreementClassification") == "title_company_collision"
    ]
    title_company_current_run_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if row.get("bundleEvidenceOrigin") == "current_run"
    )
    title_company_carried_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
    )
    provider_static_title_company_collision_audit_counts = Counter(
        clean_text(row.get("carriedLocationPollutionAudit")) or "unknown"
        for row in provider_static_title_company_collision_rows
    )
    provider_static_disagreement_gate_counts = Counter(
        clean_text(row.get("disagreementGateDisposition")) or "blocked"
        for row in provider_static_disagreement_rows
    )
    current_run_provider_static_disagreement_blocked_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") == "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    carried_provider_static_disagreement_blocked_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    carried_provider_static_disagreement_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_auto_safe_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("disagreementGateDisposition")) == "warning"
        and any(
            clean_text(item).startswith("auto_safe_")
            for item in row.get("disagreementGateEvidence") or []
        )
    )
    provider_static_reviewed_safe_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("dedupReviewStatus")) == "reviewed_safe"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_confirmed_blocking_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("dedupReviewStatus")) == "confirmed_blocking"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    provider_static_location_pollution_warning_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if clean_text(row.get("carriedLocationPollutionAudit")) == "carried_location_pollution"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_disagreement_count = (
        current_run_provider_static_disagreement_count + carried_provider_static_disagreement_count
    )
    sheet_guard_reason_counts = (
        dedup_stats.get("sheetRoleBucketGuardBlockedReasonCounts")
        or dedup_stats.get("googleSheetsGenericRoleGuardBlockedReasonCounts")
        or {}
    )
    google_sheets_guard_samples = json_object_rows(
        dedup_stats.get("sheetRoleBucketGuardBlockedSamples")
        or dedup_stats.get("googleSheetsGenericRoleGuardBlockedSamples")
    )
    google_sheets_guard_blocked_count = max(
        0,
        int(
            dedup_stats.get("sheetRoleBucketGuardBlockedCount")
            or dedup_stats.get("googleSheetsGenericRoleGuardBlockedCount")
            or 0
        ),
    )
    google_sheets_role_bucket_audit = _google_sheets_role_bucket_audit_summary(
        role_bucket_rows=google_sheets_role_bucket_rows,
        guard_samples=google_sheets_guard_samples,
        guard_blocked_count=google_sheets_guard_blocked_count,
        limit=risky_limit,
    )

    payload = {
        "schemaVersion": 1,
        "mergedCount": max(0, int(dedup_stats.get("mergedCount") or 0)),
        "collisionSamplesCount": max(0, int(dedup_stats.get("collisionSamplesCount") or 0)),
        "mergeReasonCounts": _merge_reason_counts(dedup_stats),
        "currentRunMergeExamples": _current_run_merge_examples(dedup_stats),
        "currentRunMergeExamplesByReason": _current_run_merge_examples_by_reason(dedup_stats),
        "sheetRoleBucketGuardBlockedCount": google_sheets_guard_blocked_count,
        "sheetRoleBucketGuardBlockedReasonCounts": {
            "secondaryKey": max(0, int(sheet_guard_reason_counts.get("secondaryKey") or 0)),
            "sparseIdentity": max(0, int(sheet_guard_reason_counts.get("sparseIdentity") or 0)),
        },
        "sheetRoleBucketGuardBlockedSamples": google_sheets_guard_samples,
        "googleSheetsGenericRoleGuardBlockedCount": google_sheets_guard_blocked_count,
        "googleSheetsGenericRoleGuardBlockedReasonCounts": {
            "secondaryKey": max(0, int(sheet_guard_reason_counts.get("secondaryKey") or 0)),
            "sparseIdentity": max(0, int(sheet_guard_reason_counts.get("sparseIdentity") or 0)),
        },
        "googleSheetsGenericRoleGuardBlockedSamples": google_sheets_guard_samples,
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "currentRunBlockingReviewQueueCount": current_run_blocking_review_queue_count,
        "carriedBlockingReviewQueueCount": carried_blocking_review_queue_count,
        "currentRunMonitorReviewQueueCount": current_run_monitor_review_queue_count,
        "carriedMonitorReviewQueueCount": carried_monitor_review_queue_count,
        "providerStaticDisagreementCounts": {
            "total": provider_static_disagreement_count,
            "currentRun": current_run_provider_static_disagreement_count,
            "carried": carried_provider_static_disagreement_count,
        },
        "providerStaticDisagreementGateCounts": {
            "blocked": int(provider_static_disagreement_gate_counts.get("blocked", 0)),
            "warning": int(provider_static_disagreement_gate_counts.get("warning", 0)),
            "currentRunBlocked": current_run_provider_static_disagreement_blocked_count,
            "carriedBlocked": carried_provider_static_disagreement_blocked_count,
            "carriedWarning": carried_provider_static_disagreement_warning_count,
            "autoSafeWarning": provider_static_auto_safe_warning_count,
            "locationPollutionWarning": provider_static_location_pollution_warning_count,
            "reviewedSafeWarning": provider_static_reviewed_safe_warning_count,
            "confirmedBlocking": provider_static_confirmed_blocking_count,
        },
        "providerStaticDisagreementClassificationCounts": {
            key: int(provider_static_disagreement_classification_counts.get(key, 0))
            for key in PROVIDER_STATIC_DISAGREEMENT_CLASSIFICATION_KEYS
        },
        "providerStaticTitleCompanyCollisionCounts": {
            "total": len(provider_static_title_company_collision_rows),
            "currentRun": title_company_current_run_count,
            "carried": title_company_carried_count,
        },
        "providerStaticTitleCompanyCollisionAuditCounts": {
            key: int(provider_static_title_company_collision_audit_counts.get(key, 0))
            for key in PROVIDER_STATIC_TITLE_COMPANY_COLLISION_AUDIT_KEYS
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
        "googleSheetsRoleBucketAudit": google_sheets_role_bucket_audit,
        "reviewQueueCounts": {
            key: int(review_queue_counts.get(key, 0)) for key in REVIEW_QUEUE_ACTION_KEYS
        },
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "reviewQueue": review_queue_rows[: max(0, int(risky_limit))],
        "providerStaticDisagreementExamples": _limit_provider_static_examples(
            provider_static_disagreement_rows, risky_limit
        ),
        "providerStaticTitleCompanyCollisionExamples": (
            _limit_provider_static_examples(
                provider_static_title_company_collision_rows, risky_limit
            )
        ),
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
