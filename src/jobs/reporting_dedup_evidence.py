"""Read-only deduplication evidence for fetch reports."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

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


def _unique_job_link_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len(
        {
            normalize_url(item.get("jobLink"))
            for item in bundle
            if normalize_url(item.get("jobLink"))
        }
    )


def _provider_source_job_id_count(bundle: Sequence[Mapping[str, Any]]) -> int:
    return len(
        {
            clean_text(item.get("sourceJobId"))
            for item in bundle
            if _source_class(item) == "provider" and clean_text(item.get("sourceJobId"))
        }
    )


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
    return {
        "id": clean_text(row.get("id")),
        "dedupKey": clean_text(row.get("dedupKey")),
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "jobLink": normalize_url(row.get("jobLink")),
        "locationSummary": clean_text(row.get("locationSummary")),
        "sourceBundleCount": max(int(row.get("sourceBundleCount") or 0), len(bundle)),
        "sourceClasses": source_classes,
        "sources": source_names[:8],
        "outlierReason": _outlier_reason(row, bundle),
        "distinctLocationCount": len(meaningful_locations),
        "sampleLocations": meaningful_locations[:5],
        "uniqueJobLinkCount": _unique_job_link_count(bundle),
        "sharedPrimaryUrl": _shared_primary_url(bundle),
        "providerSourceJobIdCount": _provider_source_job_id_count(bundle),
        "hasStrongIdentity": _has_any_strong_identity(bundle),
        "dominantSourceClass": _dominant_source_class(source_classes),
    }


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
    top_rows: list[dict[str, Any]] = []
    risky_rows: list[dict[str, Any]] = []
    location_divergence_rows: list[dict[str, Any]] = []
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
        "topMergedJobs": top_rows[: max(0, int(top_limit))],
        "topSourceBundleOutliers": top_rows[: max(0, int(top_limit))],
        "locationDivergenceExamples": location_divergence_rows[: max(0, int(risky_limit))],
        "riskyMergeExamples": risky_rows[: max(0, int(risky_limit))],
        "riskyMergeExampleCount": len(risky_rows),
    }
