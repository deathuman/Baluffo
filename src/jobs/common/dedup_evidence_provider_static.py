"""Provider/static disagreement helpers for dedup evidence.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: provider/static disagreement evidence, overlap rows, and candidate comparison helpers.
AI boundary implement in: this file for provider/static evidence; contract shape stays in contracts_provider_static_overlap.
AI boundary search before contracts: dedup evidence bundle, provider coverage contracts, and provider/static tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider/static evidence tests.
"""

from __future__ import annotations

import unicodedata
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, cast

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.common.contracts_dedup_evidence import (
    ProviderStaticDisagreementRow,
)
from src.jobs.common.contracts_dedup_review_state import (
    dedup_disagreement_gate_disposition,
    dedup_operator_review_fields,
    dedup_review_pair_public_fields,
    find_dedup_review_pair,
)
from src.jobs.common.dedup_evidence_bundle import (
    _clean_values,
    _company_tokens,
    _concrete_shared_identifier_tokens,
    _identifier_tokens,
    _items_for_source_class,
    _location_label_parts,
    _path_prefix,
    _sample_clean_values,
    _source_class_counts,
    _title_tokens,
    _url_host,
    _url_path,
)
from src.jobs.common.dedup_evidence_google_sheets import (
    _google_sheets_role_bucket_audit_classification,
)
from src.jobs.common.smartrecruiters_identity import (
    smartrecruiters_company_slugs_from_values,
    smartrecruiters_title_aliases,
    smartrecruiters_title_has_alias_separator,
)
from src.jobs.text_utils import clean_text, norm_text
from src.url_hosts import host_matches_domain

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

GRACKLEHQ_SOURCE_NAME = "gracklehq"
GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE = (
    "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-"
    "8608_guerrilla-games?page=1"
)

DEDUP_AUDIT_GATE_BLOCKER_CAUSES = frozenset(
    {
        "unknown",
        "non_provider_url_identity_needs_review",
        "parser_or_directory_text_pollution",
        "spreadsheet_role_bucket_needs_review",
        "google_sheets_role_bucket_needs_review",
    }
)


def _provider_static_disagreement_example(
    summary: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> ProviderStaticDisagreementRow:
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
    classification_evidence = [
        *classification_evidence,
        *_smartrecruiters_same_board_title_location_alias_evidence(
            summary=summary,
            provider_urls=provider_urls_all,
            static_urls=static_urls_all,
            provider_ids=provider_ids_all,
            static_ids=static_ids_all,
        ),
    ]
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
) -> ProviderStaticDisagreementRow:
    review_pair = find_dedup_review_pair(review_state or {}, row)
    disposition, gate_evidence = dedup_disagreement_gate_disposition(row, review_pair)
    with_gate: dict[str, Any] = {
        **row,
        **dedup_review_pair_public_fields(review_pair),
        "disagreementGateDisposition": disposition,
        "disagreementGateEvidence": gate_evidence,
    }
    merged = {**with_gate, **dedup_operator_review_fields(with_gate)}
    return cast(ProviderStaticDisagreementRow, merged)


def _is_known_gracklehq_gamesjobsdirect_mirror_bundle(
    bundle: Sequence[Mapping[str, Any]],
) -> bool:
    sources = {clean_text(item.get("source")) for item in bundle if clean_text(item.get("source"))}
    return bool(
        GRACKLEHQ_SOURCE_NAME in sources and GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE in sources
    )


def _is_wargaming_greenhouse_careers_vacancy_alias(
    *,
    provider_urls: Sequence[str],
    static_urls: Sequence[str],
    provider_ids: Sequence[str],
) -> bool:
    provider_values = [*provider_ids, *provider_urls]
    has_wargaming_greenhouse = any(
        "wargamingen" in norm_text(value) for value in provider_values
    ) and any(host_matches_domain(_url_host(url), "greenhouse.io") for url in provider_urls)
    has_wargaming_vacancy_detail = any(
        _url_host(url) == "wargaming.com"
        and _url_path(url).lower().startswith("/en/careers/vacancy_")
        for url in static_urls
    )
    return has_wargaming_greenhouse and has_wargaming_vacancy_detail


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
        if _is_wargaming_greenhouse_careers_vacancy_alias(
            provider_urls=provider_urls,
            static_urls=static_urls,
            provider_ids=provider_ids,
        ):
            evidence.append("wargaming_greenhouse_careers_vacancy_alias")
        return "same_job_different_urls", evidence + ["both_sides_have_ids_and_urls"]
    return "needs_manual_review", evidence


def _smartrecruiters_same_board_title_location_alias_evidence(
    *,
    summary: Mapping[str, Any],
    provider_urls: Sequence[str],
    static_urls: Sequence[str],
    provider_ids: Sequence[str],
    static_ids: Sequence[str],
) -> list[str]:
    title_aliases = smartrecruiters_title_aliases(summary.get("title"))
    if not smartrecruiters_title_has_alias_separator(summary.get("title")):
        return []
    if not title_aliases:
        return []
    location_count = max(0, int(summary.get("distinctLocationCount") or 0))
    if location_count > 1 and not _provider_static_locations_are_single_effective_place(summary):
        return []
    provider_slugs = smartrecruiters_company_slugs_from_values([*provider_ids, *provider_urls])
    static_slugs = smartrecruiters_company_slugs_from_values([*static_ids, *static_urls])
    shared_slugs = sorted(provider_slugs & static_slugs)
    if not shared_slugs:
        return []
    return [
        "smartrecruiters_same_board_title_location_alias",
        f"smartrecruiters_board:{shared_slugs[0]}",
        f"title_aliases:{len(title_aliases)}",
        f"locations:{location_count}",
    ]


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


def _update_review_pressure_cause_counts(
    *,
    summary: Mapping[str, Any],
    current_blocking: int,
    carried_blocking: int,
    current_monitor: int,
    carried_monitor: int,
    current_run_blocking_review_queue_cause_counts: Counter[str],
    carried_blocking_review_queue_cause_counts: Counter[str],
    current_run_monitor_review_queue_cause_counts: Counter[str],
    carried_monitor_review_queue_cause_counts: Counter[str],
) -> None:
    cause = clean_text(summary["suspectedCause"])
    for count, counter in (
        (current_blocking, current_run_blocking_review_queue_cause_counts),
        (carried_blocking, carried_blocking_review_queue_cause_counts),
        (current_monitor, current_run_monitor_review_queue_cause_counts),
        (carried_monitor, carried_monitor_review_queue_cause_counts),
    ):
        if count:
            counter.update([cause])


def _provider_static_disagreement_origin_update(
    summary: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> tuple[int, int, list[ProviderStaticDisagreementRow]]:
    if summary.get("outlierReason") != "provider_static_disagreement":
        return 0, 0, []
    origin = clean_text(summary.get("bundleEvidenceOrigin"))
    current_count, carried_count = (1, 0) if origin == "current_run" else (0, 1)
    return current_count, carried_count, [_provider_static_disagreement_example(summary, bundle)]
