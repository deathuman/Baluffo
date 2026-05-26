from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from src.source_registry import source_identity, static_listing_url_aliases
from src.source_registry_policy import duplicate_family_conflict_cards
from src.url_hosts import url_host_matches_domain

SOURCE_HEALTH_FIELD_NAMES = (
    "healthScore",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastFetchedCount",
    "lastJobsFound",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_DIFF_FIELDS = (
    "name",
    "sourceId",
    "id",
    "registryState",
    "candidateState",
    "transitionReason",
    "pendingReason",
    "quarantineReason",
    "stateChangedAt",
    "stateChangedBy",
    "lastPromotedAt",
    "lastDemotedAt",
    "duplicateFamilyKey",
    "duplicateOfSourceId",
    "duplicateOfSourceName",
    "adapter",
    "jobsFound",
    "rankScore",
    "score",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastFetchedCount",
    "lastJobsFound",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_ACTIONS_BY_STATE = {
    "active": ({"action": "demote-active", "label": "Demote", "route": "/registry/demote-active"},),
    "pending": (
        {"action": "approve", "label": "Promote", "route": "/registry/approve"},
        {"action": "reject", "label": "Reject", "route": "/registry/reject"},
    ),
    "rejected": (
        {"action": "restore-rejected", "label": "Restore", "route": "/registry/restore-rejected"},
    ),
}

PROVIDER_ADAPTERS = {
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
    "workday",
}
INDEPENDENT_PROVIDER_BOARD_ADAPTERS = {"greenhouse"}
INDEPENDENT_PROVIDER_BOARD_OVERLAP_THRESHOLD = 0.5

PROVIDER_HOST_SUFFIX_ADAPTERS = {
    ".bamboohr.com": "bamboohr",
    ".jobs.ashbyhq.com": "ashby",
    ".jobs.personio.de": "personio",
    ".myworkdayjobs.com": "workday",
    ".pinpointhq.com": "pinpoint",
    ".recruitee.com": "recruitee",
    ".teamtailor.com": "teamtailor",
    ".workable.com": "workable",
    ".workday.com": "workday",
}

PROVIDER_HOST_EXACT_ADAPTERS = {
    "apply.workable.com": "workable",
    "bamboohr.com": "bamboohr",
    "boards.greenhouse.io": "greenhouse",
    "jobs.ashbyhq.com": "ashby",
    "jobs.greenhouse.io": "greenhouse",
    "jobs.smartrecruiters.com": "smartrecruiters",
}

TRIAGE_BUCKETS = (
    {
        "bucket": "exact_duplicate_auto_healable",
        "label": "Exact duplicate",
        "risk": "low",
        "description": "Rows share the same canonical source identity and are eligible for existing exact-duplicate repair.",
    },
    {
        "bucket": "active_active_likely_duplicate",
        "label": "Active-active likely duplicate",
        "risk": "high",
        "description": "More than one active row exists for the same source family.",
    },
    {
        "bucket": "pending_duplicate_of_active",
        "label": "Pending duplicate of active",
        "risk": "medium",
        "description": "A pending candidate belongs to a family that already has one active source.",
    },
    {
        "bucket": "rejected_historical_noise",
        "label": "Rejected historical noise",
        "risk": "low",
        "description": "Rejected rows are present without a higher-priority active/pending duplicate pattern.",
    },
    {
        "bucket": "ambiguous_manual_review",
        "label": "Manual review",
        "risk": "medium",
        "description": "The conflict shape is not safe to categorize more narrowly.",
    },
)

_TRIAGE_BY_BUCKET = {str(row["bucket"]): row for row in TRIAGE_BUCKETS}

REVIEW_QUEUES = (
    {
        "queue": "p0_multi_active_provider",
        "priority": 0,
        "label": "Multiple active providers",
        "description": "Multiple active API/provider rows exist for one source family.",
    },
    {
        "queue": "p1_active_provider_static",
        "priority": 1,
        "label": "Active provider + static",
        "description": "Active provider rows coexist with active static rows.",
    },
    {
        "queue": "p1_pending_provider_against_active",
        "priority": 1,
        "label": "Pending provider vs active",
        "description": "A pending API/provider candidate is competing with one active source.",
    },
    {
        "queue": "p2_same_adapter_active_variant",
        "priority": 2,
        "label": "Same-adapter active variant",
        "description": "Multiple active rows use the same non-static source type.",
    },
    {
        "queue": "p2_static_url_variant_active",
        "priority": 2,
        "label": "Active static URL variants",
        "description": "Multiple active static rows look like URL variants.",
    },
    {
        "queue": "p2_pending_static_variant",
        "priority": 2,
        "label": "Pending static variant",
        "description": "Pending static rows compete with one active source.",
    },
    {
        "queue": "p3_pending_only_intake",
        "priority": 3,
        "label": "Pending-only intake",
        "description": "Duplicate candidates are pending only, so they are not active fetch duplication.",
    },
    {
        "queue": "p3_low_signal_manual",
        "priority": 3,
        "label": "Low-signal manual review",
        "description": "The conflict does not match a higher-confidence review queue.",
    },
)

_REVIEW_BY_QUEUE = {str(row["queue"]): row for row in REVIEW_QUEUES}

SAFE_AUTO_DEMOTE_ACTION = "auto_demote_same_adapter_provider_alias"
SAFE_AUTO_DEMOTE_LABEL = "Auto-demote safe duplicate"
SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION = "auto_demote_static_normalized_url_alias"
SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL = "Auto-demote static URL alias"
SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION = "auto_demote_static_same_host_listing_variant"
SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL = "Auto-demote static listing variant"
SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION = "auto_demote_static_generated_listing_variants"
SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL = "Auto-demote generated static listing variants"
SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION = "auto_demote_provider_static_weaker_source"
SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL = "Auto-demote weaker static source"
SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION = "auto_demote_provider_redirect_static_aliases"
SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL = "Auto-demote redirect/static aliases"
SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION = "auto_promote_pending_static_jobs_fragment"
SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL = "Auto-promote static jobs-section alias"
SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION = "auto_reject_pending_static_bare_alias"
SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL = "Auto-reject pending bare static alias"
SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION = "auto_promote_pending_provider_higher_jobs"
SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL = "Auto-promote higher-yield provider"
SAFE_AUTO_DEMOTE_ACTIONS = {
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION,
}
SAFE_AUTO_DEMOTE_ROUTE = "/registry/conflicts/auto-demote-safe"
SAFE_AUTO_DEMOTE_REASON = "registry_conflict_safe_auto_demote"
ADJUDICATION_AUTO_DEMOTE_REASON = "registry_conflict_adjudication_auto_demote"
RESOLVED_PENDING_DEMOTION_REASONS = frozenset(
    {
        SAFE_AUTO_DEMOTE_REASON,
        ADJUDICATION_AUTO_DEMOTE_REASON,
    }
)

_FIELD_LABELS = {
    "name": "Name",
    "sourceId": "Source ID",
    "id": "ID",
    "registryState": "Registry state",
    "candidateState": "Candidate state",
    "transitionReason": "Transition reason",
    "pendingReason": "Pending reason",
    "quarantineReason": "Quarantine reason",
    "stateChangedAt": "State changed at",
    "stateChangedBy": "State changed by",
    "lastPromotedAt": "Last promoted at",
    "lastDemotedAt": "Last demoted at",
    "duplicateFamilyKey": "Duplicate family",
    "duplicateOfSourceId": "Duplicate of source ID",
    "duplicateOfSourceName": "Duplicate of source name",
    "adapter": "Adapter",
    "jobsFound": "Jobs found",
    "rankScore": "Rank score",
    "score": "Score",
    "lastStatus": "Last status",
    "lastRunAt": "Last run at",
    "lastCheckedAt": "Last checked at",
    "lastSuccessAt": "Last success at",
    "lastSuccessfulFetchAt": "Last successful fetch at",
    "lastSeenInFetchAt": "Last seen in fetch at",
    "lastKeptCount": "Last kept count",
    "lastJobsKept": "Last jobs kept",
    "consecutiveFailures": "Consecutive failures",
    "failureCount": "Failure count",
    "consecutiveZeroKept": "Consecutive zero-kept",
    "zeroJobStreak": "Zero-job streak",
    "health": "Health",
    "healthReason": "Health reason",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _row_identity(row: dict[str, Any]) -> str:
    return _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))


def _row_state(row: dict[str, Any]) -> str:
    return _clean_text(row.get("registryState") or row.get("candidateState")).lower()


def _row_adapter(row: dict[str, Any]) -> str:
    adapter = _clean_text(row.get("adapter") or row.get("sourceType")).lower()
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row)).lower()
    if not adapter and ":" in row_id:
        adapter = row_id.split(":", 1)[0]
    return adapter or "unknown"


def _is_static_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) == "static"


def _is_provider_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) in PROVIDER_ADAPTERS


def _provider_adapter_from_urls(row: dict[str, Any]) -> str:
    for url in _row_urls(row):
        try:
            parsed = urlparse(url)
        except ValueError:
            continue
        host = _clean_text(parsed.netloc).lower()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        exact = PROVIDER_HOST_EXACT_ADAPTERS.get(host)
        if exact:
            return exact
        for suffix, adapter in PROVIDER_HOST_SUFFIX_ADAPTERS.items():
            if host.endswith(suffix):
                return adapter
    return ""


def _effective_provider_adapter(row: dict[str, Any]) -> str:
    adapter = _row_adapter(row)
    if adapter in PROVIDER_ADAPTERS:
        return adapter
    if _is_static_row(row):
        return _provider_adapter_from_urls(row)
    return ""


def _is_provider_like_row(row: dict[str, Any]) -> bool:
    return bool(_effective_provider_adapter(row))


def _row_has_weak_job_signal(row: dict[str, Any]) -> bool:
    confidence = _clean_text(row.get("lastProbeCountConfidence")).lower()
    return any(bool(row.get(key)) for key in ("weakSignal", "lastProbeWeakSignal")) or (
        confidence and confidence != "high"
    )


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _row_jobs_evidence(row: dict[str, Any]) -> int:
    live_jobs = _count_from_key(row, "liveJobsFound")
    if live_jobs is not None:
        return live_jobs
    fresh_jobs = _fresh_jobs_found_count(row)
    if fresh_jobs is not None:
        return fresh_jobs
    if _is_static_row(row) and _row_has_weak_job_signal(row):
        reliable_jobs = _count_from_key(row, "lastReliableJobsFound")
        if reliable_jobs is not None:
            return reliable_jobs
        return 0
    for key in (
        "jobsFound",
        "jobs_found",
        "lastKeptCount",
        "lastJobsKept",
        "keptCount",
        "kept_count",
    ):
        value = _int_value(row.get(key))
        if value > 0:
            return value
    return 0


def _count_from_key(row: dict[str, Any], key: str) -> int | None:
    if key not in row:
        return None
    return max(0, _int_value(row.get(key)))


def _latest_fetch_failed(row: dict[str, Any]) -> bool:
    status = _clean_text(row.get("lastStatus")).lower()
    health = _clean_text(row.get("health")).lower()
    return status in {"error", "failed", "failure"} or health == "broken"


def _fresh_jobs_found_count(row: dict[str, Any]) -> int | None:
    if _latest_fetch_failed(row):
        return None
    for key in ("lastJobsFound", "lastJobsKept", "lastKeptCount"):
        value = _count_from_key(row, key)
        if value is not None:
            return value
    return None


def _positive_evidence_score(row: dict[str, Any]) -> int:
    return _row_jobs_evidence(row) + sum(
        max(0, _int_value(row.get(key)))
        for key in ("rankScore", "score", "lastJobsKept", "lastKeptCount")
    )


def _jobs_found_count(row: dict[str, Any]) -> int | None:
    live_jobs = _count_from_key(row, "liveJobsFound")
    if live_jobs is not None:
        return live_jobs
    fresh_jobs = _fresh_jobs_found_count(row)
    if fresh_jobs is not None:
        return fresh_jobs
    if _is_static_row(row) and _row_has_weak_job_signal(row):
        reliable_jobs = _count_from_key(row, "lastReliableJobsFound")
        return reliable_jobs if reliable_jobs is not None else 0
    for key in ("jobsFound", "sampleCount"):
        if key in row:
            return max(0, _int_value(row.get(key)))
    return None


def _has_fresh_or_healthy_signal(row: dict[str, Any]) -> bool:
    health = _clean_text(row.get("health") or row.get("lastStatus")).lower()
    return health in {"healthy", "ok", "success"}


def _row_urls(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "id",
            "sourceId",
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s]+", _clean_text(value)):
            urls.append(match.rstrip("),.;'\""))
    return urls


def _normalized_url_for_comparison(url: str) -> str:
    try:
        parsed = urlparse(_clean_text(url))
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _provider_slug(row: dict[str, Any]) -> str:
    slug = _clean_text(row.get("slug")).lower()
    if slug:
        return slug
    row_id = _row_identity(row).lower()
    prefix = f"{_row_adapter(row)}:slug:"
    if row_id.startswith(prefix):
        return row_id.removeprefix(prefix).split(":", 1)[0]
    return ""


def _provider_source_aliases(row: dict[str, Any]) -> set[str]:
    adapter = _row_adapter(row)
    aliases = {
        _clean_text(row.get("id")).lower(),
        _clean_text(row.get("sourceId")).lower(),
        _clean_text(row.get("sourceIdentity")).lower(),
        _row_identity(row).lower(),
    }
    slug = _provider_slug(row)
    if adapter and slug:
        aliases.update({slug, f"{adapter}:{slug}", f"{adapter}:slug:{slug}"})
    return {alias for alias in aliases if alias}


def _source_item_aliases(item: dict[str, Any]) -> set[str]:
    aliases = {
        _clean_text(item.get("source")).lower(),
        _clean_text(item.get("sourceId")).lower(),
        _clean_text(item.get("sourceIdentity")).lower(),
    }
    source_job_id = _clean_text(item.get("sourceJobId")).lower()
    parts = [part for part in source_job_id.split(":") if part]
    if len(parts) >= 2:
        aliases.update({f"{parts[0]}:{parts[1]}", f"{parts[0]}:slug:{parts[1]}"})
    return {alias for alias in aliases if alias}


def _job_identity_keys(item: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    source_job_id = _clean_text(item.get("sourceJobId"))
    job_link = _clean_text(item.get("jobLink") or item.get("url"))
    if source_job_id:
        keys.add(f"id:{source_job_id.lower()}")
        for token in re.findall(r"\d{5,}", source_job_id):
            keys.add(f"token:{token}")
    if job_link:
        normalized = _normalized_url_for_comparison(job_link)
        if normalized:
            keys.add(f"url:{normalized}")
        for token in re.findall(r"\d{5,}", job_link):
            keys.add(f"token:{token}")
    return keys


def _source_job_identity_index(job_rows: Any) -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    for row in _as_list(job_rows):
        if not isinstance(row, dict):
            continue
        items = [item for item in _as_list(row.get("sourceBundle")) if isinstance(item, dict)]
        if not items:
            items = [row]
        for item in items:
            keys = _job_identity_keys(item)
            if not keys:
                continue
            for alias in _source_item_aliases(item):
                index.setdefault(alias, set()).update(keys)
    return index


def _row_direct_job_identity_keys(row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in _as_list(row.get("jobIdentityKeys")):
        text = _clean_text(value)
        if text:
            keys.add(text.lower())
    for value in _as_list(row.get("sourceJobIds")):
        keys.update(_job_identity_keys({"sourceJobId": value}))
    for value in _as_list(row.get("jobLinks")):
        keys.update(_job_identity_keys({"jobLink": value}))
    return keys


def _row_job_identity_keys(row: dict[str, Any], job_index: dict[str, set[str]]) -> set[str]:
    keys = _row_direct_job_identity_keys(row)
    for alias in _provider_source_aliases(row):
        keys.update(job_index.get(alias, set()))
    return keys


def _identity_overlap_ratio(left: set[str], right: set[str]) -> float:
    shared = left & right
    if any(value.startswith("token:") for value in shared):
        return 1.0
    denominator = min(len(left), len(right))
    if not denominator:
        return 0.0
    return len(shared) / denominator


def _row_primary_url(row: dict[str, Any]) -> str:
    return next(iter(_row_urls(row)), "")


def _row_live_final_url(row: dict[str, Any]) -> str:
    return _clean_text(row.get("liveProbeFinalUrl") or row.get("finalUrl"))


def _static_row_current_jobs(row: dict[str, Any]) -> int:
    for key in ("liveJobsFound", "lastJobsKept", "lastKeptCount", "lastReliableJobsFound"):
        value = _count_from_key(row, key)
        if value is not None:
            return value
    return _row_jobs_evidence(row)


def _static_url_has_job_fragment(row: dict[str, Any]) -> bool:
    exact_job_fragments = {
        "jobs",
        "job",
        "positions",
        "position",
        "openings",
        "opening",
        "vacancies",
        "join",
        "join-us",
        "job-openings",
        "open-positions",
        "current-openings",
    }
    job_fragment_tokens = {
        "job",
        "jobs",
        "position",
        "positions",
        "opening",
        "openings",
        "vacancy",
        "vacancies",
        "role",
        "roles",
        "opportunity",
        "opportunities",
        "join",
    }
    for url in _row_urls(row):
        try:
            fragment = urlparse(url).fragment.strip().lower().strip("/")
        except ValueError:
            continue
        if fragment in exact_job_fragments:
            return True
        fragment_tokens = {token for token in re.split(r"[^a-z0-9]+", fragment) if token}
        if fragment_tokens & job_fragment_tokens:
            return True
    return False


def _provider_endpoint_shape(row: dict[str, Any]) -> str:
    for url in _row_urls(row):
        parsed = urlparse(url)
        path = parsed.path.strip().lower().rstrip("/")
        if path:
            return path
    return ""


def _normalized_static_url_aliases(row: dict[str, Any]) -> set[str]:
    return static_listing_url_aliases(row)


def _static_url_host_paths(row: dict[str, Any]) -> set[tuple[str, str]]:
    host_paths: set[tuple[str, str]] = set()
    for url in _row_urls(row):
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host:
            continue
        path = parsed.path.strip().lower().rstrip("/") or "/"
        host_paths.add((host, path))
    return host_paths


def _family_tokens(family_key: str) -> set[str]:
    stop_words = {
        "digital",
        "entertainment",
        "game",
        "games",
        "group",
        "interactive",
        "online",
        "software",
        "studio",
        "studios",
        "world",
    }
    return {
        token
        for token in re.split(r"[^a-z0-9]+", family_key.lower())
        if len(token) > 2 and token not in stop_words
    }


def _host_matches_family(host: str, family_key: str) -> bool:
    compact_host = host.replace("-", "").replace(".", "")
    return any(token in compact_host for token in _family_tokens(family_key))


def _is_parent_child_path(left: str, right: str) -> bool:
    if left == right:
        return True
    return left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _hosts_same_or_subdomain(left: str, right: str) -> bool:
    return left == right or left.endswith(f".{right}") or right.endswith(f".{left}")


def _is_careerish_path(path: str) -> bool:
    return bool(
        set(re.split(r"[^a-z0-9]+", path.lower()))
        & {
            "career",
            "careers",
            "hiring",
            "job",
            "jobs",
            "join",
            "opening",
            "openings",
            "position",
            "positions",
            "vacancies",
            "work",
        }
    )


def _is_homepage_path(path: str) -> bool:
    return path.strip().lower().rstrip("/") in {"", "/"}


def _json_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value or "").strip()


def _source_state_url_identity_keys(row: dict[str, Any]) -> list[str]:
    adapter = _clean_text(row.get("adapter")).lower()
    provider_url = _clean_text(
        row.get("providerUrl") or row.get("provider_url") or row.get("apiUrl") or row.get("api_url")
    )
    listing_url = _clean_text(
        row.get("listingUrl") or row.get("listing_url") or row.get("sourceUrl") or row.get("url")
    )
    keys: list[str] = []
    for url in (provider_url, listing_url):
        if not url:
            continue
        keys.append(url)
        if adapter == "recruitee" and url_host_matches_domain(url, "recruitee.com"):
            keys.append(f"recruitee:api_url:{url}")
        elif adapter == "teamtailor":
            keys.append(f"teamtailor:listing_url:{url}")
        elif adapter == "static":
            static_key = f"static:listing_url:{url}"
            keys.append(static_key)
            keys.append(f"static_source::{static_key}")
    return keys


def _source_state_index_keys(raw_key: Any, row: dict[str, Any]) -> list[str]:
    return [
        str(raw_key).strip(),
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("sourceIdentity")),
        *_source_state_url_identity_keys(row),
    ]


def _ambiguous_registry_row_names(rows: list[dict[str, Any]]) -> set[str]:
    counts = Counter(
        _clean_text(row.get("name")).lower() for row in rows if _clean_text(row.get("name"))
    )
    return {name for name, count in counts.items() if count > 1}


def _source_state_rows_by_name(source_state_payload: Any) -> dict[str, dict[str, Any]]:
    rows = _as_dict(_as_dict(source_state_payload).get("sources"))
    by_key: dict[str, dict[str, Any]] = {}
    for raw_key, row in rows.items():
        if not isinstance(row, dict):
            continue
        for key in _source_state_index_keys(raw_key, row):
            lookup = key.strip().lower()
            if lookup:
                by_key[lookup] = row
    return by_key


def _fetch_report_source_state_row(
    detail: dict[str, Any], parent: dict[str, Any], report: dict[str, Any]
) -> dict[str, Any]:
    status = _clean_text(detail.get("status") or parent.get("status")).lower()
    kept = _int_value(detail.get("keptCount") or detail.get("lastKeptCount"))
    fetched = _int_value(detail.get("fetchedCount") or detail.get("lastFetchedCount"))
    failure_count = _int_value(detail.get("failureCount") or parent.get("failureCount"))
    observed_at = _clean_text(
        detail.get("finishedAt")
        or detail.get("listingCheckedAt")
        or detail.get("lastCheckedAt")
        or detail.get("lastRunAt")
        or parent.get("lastRunAt")
        or parent.get("lastCheckedAt")
        or parent.get("lastSuccessfulFetchAt")
        or parent.get("lastSuccessAt")
        or report.get("finishedAt")
    )
    seen_at = _clean_text(
        detail.get("lastSeenInFetchAt")
        or detail.get("listingCheckedAt")
        or detail.get("lastCheckedAt")
        or detail.get("lastRunAt")
        or parent.get("lastRunAt")
        or parent.get("lastCheckedAt")
        or parent.get("lastSeenInFetchAt")
        or observed_at
    )
    if status == "ok" and kept > 0:
        health = "healthy"
        health_reason = "last fetch kept jobs"
        success_at = observed_at
    elif status == "ok":
        health = "warning"
        health_reason = "latest fetch kept no jobs"
        success_at = observed_at
    else:
        health = "broken"
        health_reason = "latest fetch failed"
        success_at = ""
    row = {
        "health": _clean_text(detail.get("health") or parent.get("health")) or health,
        "healthReason": _clean_text(detail.get("healthReason") or parent.get("healthReason"))
        or health_reason,
        "lastStatus": status or _clean_text(parent.get("lastStatus")),
        "lastRunAt": _clean_text(detail.get("lastRunAt") or parent.get("lastRunAt") or seen_at),
        "lastCheckedAt": _clean_text(
            detail.get("lastCheckedAt")
            or detail.get("listingCheckedAt")
            or parent.get("lastCheckedAt")
            or seen_at
        ),
        "lastSuccessAt": _clean_text(detail.get("lastSuccessAt") or success_at),
        "lastSuccessfulFetchAt": _clean_text(detail.get("lastSuccessfulFetchAt") or success_at),
        "lastSeenInFetchAt": seen_at,
        "lastKeptCount": kept,
        "lastJobsKept": kept,
        "lastJobsFound": fetched,
        "failureCount": failure_count,
        "consecutiveFailures": failure_count,
        "zeroJobStreak": 0 if kept > 0 else _int_value(parent.get("zeroJobStreak")),
        "consecutiveZeroKept": 0 if kept > 0 else _int_value(parent.get("consecutiveZeroKept")),
    }
    for key in ("sourceId", "name", "adapter", "studio", "providerUrl", "listingUrl"):
        value = _clean_text(detail.get(key) or parent.get(key))
        if value:
            row[key] = value
    return row


def _timestamp_is_newer(candidate: Any, current: Any) -> bool:
    candidate_text = _clean_text(candidate)
    current_text = _clean_text(current)
    return bool(candidate_text and (not current_text or candidate_text > current_text))


def _merge_source_state_row_from_fetch_report(
    existing: dict[str, Any], row: dict[str, Any]
) -> dict[str, Any]:
    if not (
        _timestamp_is_newer(row.get("lastRunAt"), existing.get("lastRunAt"))
        or _timestamp_is_newer(
            row.get("lastSuccessfulFetchAt"), existing.get("lastSuccessfulFetchAt")
        )
        or _timestamp_is_newer(row.get("lastSeenInFetchAt"), existing.get("lastSeenInFetchAt"))
    ):
        return existing
    merged = dict(existing)
    for key in (
        *SOURCE_HEALTH_FIELD_NAMES,
        "lastJobsFound",
        "sourceId",
        "name",
        "adapter",
        "studio",
        "providerUrl",
        "listingUrl",
    ):
        value = row.get(key)
        if value not in {"", None}:
            merged[key] = value
    return merged


def _merge_fetch_report_source_details(
    source_state_payload: Any, fetch_report_payload: Any
) -> dict[str, Any]:
    merged = dict(_as_dict(source_state_payload))
    sources = dict(_as_dict(merged.get("sources")))
    report = _as_dict(fetch_report_payload)
    for parent_value in _as_list(report.get("sources")):
        parent = _as_dict(parent_value)
        if not parent:
            continue
        details = _as_list(parent.get("details")) or [parent]
        for detail_value in details:
            detail = _as_dict(detail_value)
            if not detail:
                continue
            row = _fetch_report_source_state_row(detail, parent, report)
            candidate_keys = [
                _clean_text(row.get("sourceId")),
                _clean_text(row.get("sourceIdentity")),
                *_source_state_url_identity_keys(row),
                _clean_text(row.get("name")),
            ]
            for key in candidate_keys:
                if not key:
                    continue
                if key in sources:
                    sources[key] = _merge_source_state_row_from_fetch_report(sources[key], row)
                else:
                    sources[key] = row
    merged["sources"] = sources
    return merged


def _adjudication_families_by_key(adjudication_payload: Any) -> dict[str, dict[str, Any]]:
    payload = _as_dict(adjudication_payload)
    by_key: dict[str, dict[str, Any]] = {}
    observed_at = _clean_text(payload.get("finishedAt") or payload.get("startedAt"))
    for row in _as_list(payload.get("families")):
        if not isinstance(row, dict) or not _clean_text(row.get("familyKey")):
            continue
        family = dict(row)
        if observed_at:
            family["_observedAt"] = observed_at
        by_key[_clean_text(row.get("familyKey"))] = family
    return by_key


def _adjudication_probe_by_source_id(family: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _clean_text(row.get("sourceId")): row
        for row in _as_list(family.get("probes"))
        if isinstance(row, dict) and _clean_text(row.get("sourceId"))
    }


def _adjudication_probe_matches_for_rows(
    rows: list[dict[str, Any]], probes: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    matches: dict[str, dict[str, Any]] = {}
    unmatched_probe_ids = set(probes)
    row_aliases = {
        _row_identity(row): static_listing_url_aliases(row) for row in rows if _row_identity(row)
    }
    probe_aliases = {
        probe_id: static_listing_url_aliases(probe)
        for probe_id, probe in probes.items()
        if probe_id
    }
    for row in rows:
        row_id = _row_identity(row)
        if not row_id:
            continue
        if row_id in probes:
            matches[row_id] = probes[row_id]
            unmatched_probe_ids.discard(row_id)
            continue
        aliases = row_aliases.get(row_id) or set()
        if not aliases:
            continue
        alias_matches = [
            probe_id
            for probe_id in unmatched_probe_ids
            if aliases & (probe_aliases.get(probe_id) or set())
        ]
        if len(alias_matches) == 1:
            probe_id = alias_matches[0]
            matches[row_id] = probes[probe_id]
            unmatched_probe_ids.discard(probe_id)
    if unmatched_probe_ids:
        return {}
    return matches


def _adjudication_complete_for_rows(rows: list[dict[str, Any]], family: dict[str, Any]) -> bool:
    if _clean_text(family.get("status")).lower() in {"", "running", "failed"}:
        return False
    probes = _adjudication_probe_by_source_id(family)
    row_ids = {_row_identity(row) for row in rows if _row_identity(row)}
    probe_matches = _adjudication_probe_matches_for_rows(rows, probes)
    if not row_ids or set(probe_matches) != row_ids:
        return False
    return all(
        _int_value(probe.get("httpStatus")) > 0 and not _clean_text(probe.get("error"))
        for probe in probe_matches.values()
    )


def _adjudicated_independent_provider_loser_ids(
    rows: list[dict[str, Any]], family: dict[str, Any]
) -> set[str]:
    if not family:
        return set()
    active_provider_ids = {
        _row_identity(row)
        for row in rows
        if _row_state(row) == "active" and _is_provider_row(row) and _row_identity(row)
    }
    if len(active_provider_ids) < 2:
        return set()
    checked_ids = {
        _clean_text(source_id)
        for source_id in _as_list(family.get("checkedSourceIds"))
        if _clean_text(source_id)
    }
    if not active_provider_ids <= checked_ids:
        return set()
    independent_ids: set[str] = set()
    for decision in _as_list(family.get("decisions")):
        if not isinstance(decision, dict):
            continue
        source_id = _clean_text(decision.get("sourceId"))
        status = _clean_text(decision.get("status")).lower()
        reason = _clean_text(decision.get("reason")).lower()
        if (
            source_id in active_provider_ids
            and status == "keep_both"
            and "job sets differ" in reason
        ):
            independent_ids.add(source_id)
    return independent_ids


def _active_same_adapter_provider_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    if len(active_rows) < 2 or len(active_rows) != len(rows):
        return []
    if not all(_is_provider_row(row) for row in active_rows):
        return []
    adapters = {_row_adapter(row) for row in active_rows}
    if len(adapters) != 1 or next(iter(adapters)) not in INDEPENDENT_PROVIDER_BOARD_ADAPTERS:
        return []
    row_ids = [_row_identity(row) for row in active_rows]
    if any(not row_id for row_id in row_ids) or len(set(row_ids)) != len(row_ids):
        return []
    slugs = [_provider_slug(row) for row in active_rows]
    if any(not slug for slug in slugs) or len(set(slugs)) != len(slugs):
        return []
    return active_rows


def _adjudication_proves_independent_provider_boards(
    rows: list[dict[str, Any]], family: dict[str, Any] | None
) -> bool:
    if not family:
        return False
    row_ids = {_row_identity(row) for row in rows if _row_identity(row)}
    checked_ids = {
        _clean_text(source_id)
        for source_id in _as_list(family.get("checkedSourceIds"))
        if _clean_text(source_id)
    }
    if not row_ids or not row_ids <= checked_ids:
        return False
    keep_both_ids = set()
    for decision in _as_list(family.get("decisions")):
        if not isinstance(decision, dict):
            continue
        source_id = _clean_text(decision.get("sourceId"))
        status = _clean_text(decision.get("status")).lower()
        reason = _clean_text(decision.get("reason")).lower()
        if status == "keep_both" and "job sets differ" in reason and source_id:
            keep_both_ids.add(source_id)
    winner_id = _clean_text(family.get("winnerSourceId"))
    expected_decisions = row_ids - {winner_id}
    return bool(expected_decisions) and expected_decisions <= keep_both_ids


def _current_jobs_prove_independent_provider_boards(
    rows: list[dict[str, Any]], job_index: dict[str, set[str]]
) -> bool:
    row_job_keys = [_row_job_identity_keys(row, job_index) for row in rows]
    if any(not keys for keys in row_job_keys):
        return False
    for index, left in enumerate(row_job_keys):
        for right in row_job_keys[index + 1 :]:
            if _identity_overlap_ratio(left, right) >= INDEPENDENT_PROVIDER_BOARD_OVERLAP_THRESHOLD:
                return False
    return True


def _independent_provider_board_audit_row(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
    evidence_reason: str,
) -> dict[str, Any]:
    return {
        "familyKey": family_key,
        "rowCount": len(rows),
        "adapter": _row_adapter(rows[0]) if rows else "",
        "sourceIds": [_row_identity(row) for row in rows if _row_identity(row)],
        "evidenceReason": evidence_reason,
    }


def _row_with_live_adjudication(
    row: dict[str, Any], probe: dict[str, Any], *, observed_at: str = ""
) -> dict[str, Any]:
    next_row = dict(row)
    if "jobsFound" in next_row or "sampleCount" in next_row:
        next_row["registryJobsFound"] = _jobs_found_count(next_row)
    live_jobs = max(0, _int_value(probe.get("jobsFound")))
    next_row["liveJobsFound"] = live_jobs
    next_row["jobsFound"] = live_jobs
    next_row["sampleCount"] = live_jobs
    next_row["liveProbeOk"] = bool(probe.get("ok"))
    next_row["liveProbeHttpStatus"] = _int_value(probe.get("httpStatus"))
    next_row["liveProbeFinalUrl"] = _clean_text(probe.get("finalUrl"))
    if observed_at:
        next_row.setdefault("lastCheckedAt", observed_at)
        next_row.setdefault("lastSeenInFetchAt", observed_at)
        if probe.get("ok"):
            next_row.setdefault("lastSuccessAt", observed_at)
            next_row.setdefault("lastSuccessfulFetchAt", observed_at)
    if probe.get("ok") and live_jobs > 0:
        next_row.setdefault("lastStatus", "ok")
        next_row.setdefault("health", "healthy")
        next_row.setdefault("healthReason", "live adjudication found jobs")
    elif probe.get("ok"):
        next_row.setdefault("lastStatus", "ok")
        next_row.setdefault("health", "warning")
        next_row.setdefault("healthReason", "live adjudication found no jobs")
    else:
        next_row.setdefault("lastStatus", "error")
        next_row.setdefault("health", "broken")
        next_row.setdefault("healthReason", "live adjudication probe failed")
    return next_row


def _with_live_adjudication_card(
    card: dict[str, Any],
    *,
    family: dict[str, Any],
    source_state_payload: Any,
) -> dict[str, Any]:
    rows = [_as_dict(row) for row in _as_list(card.get("rows")) if isinstance(row, dict)]
    if not _adjudication_complete_for_rows(rows, family):
        return {**card, "effectiveWinnerSource": "registry"}
    probes = _adjudication_probe_by_source_id(family)
    probe_matches = _adjudication_probe_matches_for_rows(rows, probes)
    original_winner_id = _row_identity(_as_dict(card.get("winner")))
    observed_at = _clean_text(family.get("_observedAt"))
    live_rows = [
        _row_with_live_adjudication(row, probe_matches[_row_identity(row)], observed_at=observed_at)
        for row in rows
        if _row_identity(row) in probe_matches
    ]
    recalculated = duplicate_family_conflict_cards(
        live_rows,
        target_families=[_clean_text(card.get("familyKey"))],
        source_state=source_state_payload,
    )
    if not recalculated:
        return {**card, "effectiveWinnerSource": "registry"}
    next_card = dict(recalculated[0])
    next_winner_id = _row_identity(_as_dict(next_card.get("winner")))
    next_card["adjudication"] = family
    next_card["liveAdjudicationComplete"] = True
    next_card["effectiveWinnerSource"] = (
        "live_adjudication" if next_winner_id != original_winner_id else "registry"
    )
    return next_card


def _source_state_lookup_keys(
    row: dict[str, Any], ambiguous_names: set[str] | None = None
) -> list[str]:
    keys: list[str] = []
    for key in (
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("id")),
        source_identity(row),
    ):
        if key:
            keys.append(key)
            keys.append(f"static_source::{key}")
    aliases = row.get("sourceStateAliases")
    if isinstance(aliases, list):
        keys.extend(_clean_text(alias) for alias in aliases)
    keys.extend(_source_state_url_identity_keys(row))
    row_name = _clean_text(row.get("name"))
    if row_name and row_name.lower() not in (ambiguous_names or set()):
        keys.append(row_name)
    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        lookup = key.strip().lower()
        if lookup and lookup not in seen:
            seen.add(lookup)
            out.append(lookup)
    return out


def _source_state_row_for_registry_row(
    row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
    ambiguous_names: set[str] | None = None,
) -> tuple[dict[str, Any], str]:
    for lookup in _source_state_lookup_keys(row, ambiguous_names):
        if lookup in source_state_rows:
            return source_state_rows[lookup], lookup
    return {}, ""


def _row_actions(row: dict[str, Any]) -> list[dict[str, Any]]:
    state = _row_state(row)
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))
    actions = [dict(action) for action in CONFLICT_ACTIONS_BY_STATE.get(state, ())]
    if row_id:
        for action in actions:
            action["ids"] = [row_id]
    return actions


def _source_identity_counts(rows: list[dict[str, Any]]) -> Counter[str]:
    identities: Counter[str] = Counter()
    for row in rows:
        row_id = source_identity(row)
        if row_id:
            identities[row_id] += 1
    return identities


def _safe_auto_demoted_pending_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _row_identity(row),
        "name": _clean_text(row.get("name")),
        "registryState": _row_state(row),
        "pendingReason": _clean_text(row.get("pendingReason")),
        "stateChangedAt": _clean_text(row.get("stateChangedAt")),
        "stateChangedBy": _clean_text(row.get("stateChangedBy")),
    }


def _build_pending_audit_section(cards: list[dict[str, Any]]) -> dict[str, Any]:
    row_count = sum(len(_as_list(card.get("rows"))) for card in cards)
    return {
        "summary": {
            "familyCount": len(cards),
            "rowCount": row_count,
        },
        "families": cards,
    }


def _build_independent_provider_board_audit(cards: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "summary": {
            "familyCount": len(cards),
            "rowCount": sum(_int_value(card.get("rowCount")) for card in cards),
        },
        "families": cards,
    }


def _build_pending_conflict_audit(
    *,
    safe_auto_demoted_cards: list[dict[str, Any]],
    safe_static_alias_cards: list[dict[str, Any]],
    safe_pending_provider_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "safeAutoDemotedPending": _build_pending_audit_section(safe_auto_demoted_cards),
        "safePendingStaticAlias": _build_pending_audit_section(safe_static_alias_cards),
        "safePendingProviderLowerJobs": _build_pending_audit_section(safe_pending_provider_cards),
    }


def _unique_registry_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        row_id = source_identity(row)
        if row_id and row_id in seen:
            continue
        if row_id:
            seen.add(row_id)
        unique.append(row)
    return unique


def _has_parent_child_listing_path(
    winner_host_paths: set[tuple[str, str]], loser_host_paths: set[tuple[str, str]]
) -> bool:
    return any(
        _is_parent_child_path(winner_path, loser_path)
        for winner_host, winner_path in winner_host_paths
        for loser_host, loser_path in loser_host_paths
        if winner_host == loser_host
    )


def _has_homepage_to_career_site_path(
    *,
    family_key: str,
    winner_host_paths: set[tuple[str, str]],
    loser_host_paths: set[tuple[str, str]],
) -> bool:
    return any(
        _is_careerish_path(winner_path)
        and _is_homepage_path(loser_path)
        and _host_matches_family(winner_host, family_key)
        and _host_matches_family(loser_host, family_key)
        and _hosts_same_or_subdomain(winner_host, loser_host)
        for winner_host, winner_path in winner_host_paths
        for loser_host, loser_path in loser_host_paths
    )


def _single_static_host_path(row: dict[str, Any]) -> tuple[str, str]:
    host_paths = _static_url_host_paths(row)
    if len(host_paths) != 1:
        return "", ""
    return next(iter(host_paths))


def _row_has_fresh_count_evidence(row: dict[str, Any]) -> bool:
    if any(
        _count_from_key(row, key) is not None
        for key in ("liveJobsFound", "lastJobsKept", "lastKeptCount", "lastReliableJobsFound")
    ):
        return True
    return _has_fresh_or_healthy_signal(row)


def _join_source_health_aliases(
    row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
    ambiguous_names: set[str] | None = None,
) -> dict[str, Any]:
    merged = dict(row)
    source_state_row, source_state_name = _source_state_row_for_registry_row(
        row, source_state_rows, ambiguous_names
    )
    if source_state_name:
        merged["sourceStateName"] = source_state_name
    for key in SOURCE_HEALTH_FIELD_NAMES:
        value = source_state_row.get(key)
        if value not in {"", None}:
            merged[key] = value
    if not merged.get("lastSuccessfulFetchAt") and merged.get("lastSuccessAt"):
        merged["lastSuccessfulFetchAt"] = merged.get("lastSuccessAt")
    if not merged.get("lastSeenInFetchAt"):
        merged["lastSeenInFetchAt"] = merged.get("lastCheckedAt") or merged.get("lastRunAt") or ""
    if merged.get("lastJobsKept") in {"", None} and merged.get("lastKeptCount") not in {"", None}:
        merged["lastJobsKept"] = merged.get("lastKeptCount")
    if merged.get("failureCount") in {"", None} and merged.get("consecutiveFailures") not in {
        "",
        None,
    }:
        merged["failureCount"] = merged.get("consecutiveFailures")
    if merged.get("zeroJobStreak") in {"", None} and merged.get("consecutiveZeroKept") not in {
        "",
        None,
    }:
        merged["zeroJobStreak"] = merged.get("consecutiveZeroKept")
    transition_reason = _clean_text(
        merged.get("pendingReason")
        or merged.get("quarantineReason")
        or merged.get("reason")
        or merged.get("registryReason")
    )
    merged["transitionReason"] = transition_reason
    merged["actions"] = _row_actions(merged)
    return merged


def _compare_registry_rows(winner: dict[str, Any], loser: dict[str, Any]) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    for key in CONFLICT_DIFF_FIELDS:
        winner_value = winner.get(key)
        loser_value = loser.get(key)
        if _json_value(winner_value) == _json_value(loser_value):
            continue
        diffs.append(
            {
                "key": key,
                "label": _FIELD_LABELS.get(key, key.replace("_", " ").title()),
                "winnerValue": winner_value,
                "loserValue": loser_value,
            }
        )
    return diffs
