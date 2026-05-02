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
    "parser_or_directory_text_pollution",
    "non_provider_url_identity_needs_review",
    "provider_static_disagreement",
    "likely_legitimate_multi_role_family",
    "unknown",
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
    if outlier_reason == "provider_static_disagreement":
        return "provider_static_disagreement"
    if title_shape == "speculative_or_open_application":
        return "open_application_family"
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


def _cause_evidence(summary: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    cause = str(summary.get("suspectedCause") or "")
    identity_shape = str(summary.get("identityShape") or "")
    title_shape = str(summary.get("titleShape") or "")
    outlier_reason = str(summary.get("outlierReason") or "")
    dominant_source_class = str(summary.get("dominantSourceClass") or "")
    if cause:
        evidence.append(f"cause:{cause}")
    if identity_shape:
        evidence.append(f"identity:{identity_shape}")
    if summary.get("identityQuality"):
        evidence.append(f"quality:{summary.get('identityQuality')}")
    if title_shape:
        evidence.append(f"title:{title_shape}")
    if outlier_reason:
        evidence.append(f"outlier:{outlier_reason}")
    if dominant_source_class:
        evidence.append(f"dominant_source:{dominant_source_class}")
    if summary.get("hasStrongIdentity"):
        evidence.append("strong_identity")
    for signal in summary.get("titleCompanyPollutionSignals") or []:
        evidence.append(f"pollution:{signal}")
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
        "titleShape": _title_shape(row),
        "identityCaveats": _identity_caveats(row, bundle),
        "titleCompanyPollutionSignals": _title_company_pollution_signals(row),
    }
    summary["suspectedCause"] = _suspected_cause(summary)
    summary["causeEvidence"] = _cause_evidence(summary)
    return summary


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


def build_dedup_evidence(
    dedup_stats: Mapping[str, Any],
    canonical_rows: Sequence[CanonicalJob | Mapping[str, Any]],
    *,
    top_limit: int = TOP_MERGED_LIMIT,
    risky_limit: int = RISKY_EXAMPLE_LIMIT,
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
    top_rows: list[dict[str, Any]] = []
    risky_rows: list[dict[str, Any]] = []
    location_divergence_rows: list[dict[str, Any]] = []
    review_queue_rows: list[dict[str, Any]] = []
    source_bundle_collision_count = 0

    for row in rows:
        bundle = _source_bundle(row)
        for item in bundle:
            composition[_source_class(item)] += 1
        bundle_count = max(int(row.get("sourceBundleCount") or 0), len(bundle))
        if bundle_count > 1:
            source_bundle_collision_count += 1
            summary = _job_summary(row, bundle)
            top_rows.append(summary)
            outlier_reason_counts.update([summary["outlierReason"]])
            identity_shape_counts.update([summary["identityShape"]])
            identity_quality_counts.update([summary["identityQuality"]])
            review_action = _recommended_review_action(summary)
            review_queue_counts.update([review_action])
            review_queue_cause_counts.update([summary["suspectedCause"]])
            if review_action != "monitor":
                review_queue_rows.append({**summary, "recommendedReviewAction": review_action})
            if int(summary.get("distinctLocationCount") or 0) > 1:
                location_divergence_rows.append(summary)
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

    return {
        "schemaVersion": 1,
        "mergedCount": max(0, int(dedup_stats.get("mergedCount") or 0)),
        "collisionSamplesCount": max(0, int(dedup_stats.get("collisionSamplesCount") or 0)),
        "mergeReasonCounts": _merge_reason_counts(dedup_stats),
        "sourceBundleCollisionCount": source_bundle_collision_count,
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
        "reviewQueueCounts": {
            key: int(review_queue_counts.get(key, 0)) for key in REVIEW_QUEUE_ACTION_KEYS
        },
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "reviewQueue": review_queue_rows[: max(0, int(risky_limit))],
        "topMergedJobs": top_rows[: max(0, int(top_limit))],
        "topSourceBundleOutliers": top_rows[: max(0, int(top_limit))],
        "locationDivergenceExamples": location_divergence_rows[: max(0, int(risky_limit))],
        "riskyMergeExamples": risky_rows[: max(0, int(risky_limit))],
        "riskyMergeExampleCount": len(risky_rows),
    }
