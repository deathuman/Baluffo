"""Provider migration advisory helpers.

AI boundary owns: provider migration readiness, advisory rows, and source replacement evidence.
AI boundary implement in: this file for advisory derivation; provider inference and source-policy contracts stay elsewhere.
AI boundary search before contracts: provider inference, source-policy recommendations, and migration advisory tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider advisory tests.
"""

from __future__ import annotations

from collections import Counter
from fnmatch import fnmatch
from typing import Any
from urllib.parse import urlparse

from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from src.shared.coerce import as_text as _text
from src.shared.utils import int_or_default as _as_int
from src.source_registry import source_identity
from src.url_hosts import host_matches_domain

from .config import SUPPORTED_PROVIDERS
from .core_identity import (
    PATH_TENANT_PROVIDER_FAMILIES,
    board_identity_key,
    multi_tenant_provider,
    root_domain,
    tenant_host_label,
    tenant_path,
)
from .provider_inference import infer_provider_adapter, provider_candidate
from .scoring import unique_string_list

_SUPPORTED_MIGRATION_PROVIDERS = {
    *SUPPORTED_PROVIDERS,
    "bamboohr",
    "breezy",
    "jazzhr",
    "workday",
}
_PROVIDER_ID_FIELDS = (
    "slug",
    "account",
    "company_id",
    "subdomain",
    "api_url",
    "feed_url",
    "board_url",
    "site_path",
    "listing_url",
    "base_url",
)
_COMPACT_KEYS = (
    "sourceIdentity",
    "name",
    "company",
    "studio",
    "currentAdapter",
    "currentUrl",
    "detectedProvider",
    "detectedProviderFamily",
    "detectedProviderUrl",
    "detectedProviderId",
    "createdFromAdvisory",
    "migrationSourceIdentity",
    "existingProviderSourceId",
    "existingProviderSourceState",
    "staticSourceState",
    "migrationConfidence",
    "migrationReasons",
    "recommendedAction",
    "jobsFound",
    "rankScore",
    "lastProbeStatus",
    "lastProbeError",
    "providerStagingDecision",
    "providerStagingBlockers",
    "providerStagingCandidateId",
    "providerStagingSourceIdentity",
    "providerStagingProviderFamily",
    "providerStagingProviderUrl",
    "providerStagingProviderId",
)
_STAGED_ACTOR = "provider_migration_advisory"
_STAGEABLE_ACTIONS = {"add_provider_source", "review_provider_migration"}
_STATIC_LIKE_ADAPTERS = {"static", "scrapy_static"}
_STATIC_LIKE_STAGES = {"generic_static", "seed_careers_page", "sheet_directory"}
_STRONG_PROVIDER_EVIDENCE_REASONS = {
    "provider_url_evidence",
    "provider_id:slug",
    "provider_id:account",
    "provider_id:company_id",
    "provider_id:api_url",
    "provider_id:feed_url",
    "provider_id:board_url",
    "provider_id:site_path",
    "provider_id:listing_url",
    "provider_id:base_url",
}


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _candidate_url(row: dict[str, Any]) -> str:
    for key in (
        "detectedProviderUrl",
        "providerUrl",
        "api_url",
        "feed_url",
        "board_url",
        "listing_url",
        "careersUrl",
    ):
        value = _text(row.get(key))
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = _text(value)
            if text:
                return text
    return ""


def _first_ats_link(row: dict[str, Any]) -> str:
    for key in ("atsLinks", "providerLinks", "outboundAtsLinks"):
        links = row.get(key)
        if isinstance(links, list):
            for value in links:
                text = _text(value)
                if text:
                    return text
    meta = row.get("_staticPluginMeta")
    if isinstance(meta, dict):
        return _first_ats_link(meta)
    return ""


def _current_url(row: dict[str, Any]) -> str:
    for key in ("listing_url", "careersUrl", "url", "api_url", "feed_url", "board_url"):
        value = _text(row.get(key))
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = _text(value)
            if text:
                return text
    return ""


def _host(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").strip().lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _host_matches(host: str, pattern: str) -> bool:
    clean_host = _lower(host)
    clean_pattern = _lower(pattern)
    if not clean_host or not clean_pattern:
        return False
    return (
        fnmatch(clean_host, clean_pattern) if "*" in clean_pattern else clean_host == clean_pattern
    )


def _redundant_rule_for_host(host: str) -> dict[str, Any]:
    for rule in REDUNDANT_STATIC_IF_PROVIDER:
        hosts = rule.get("hosts")
        if isinstance(hosts, list) and any(_host_matches(host, str(item)) for item in hosts):
            return dict(rule)
    return {}


def _unsupported_provider_family(url: str) -> str:
    host = _host(url)
    path = (urlparse(url).path or "").lower()
    if host_matches_domain(host, "oraclecloud.com") and "/hcmui/candidateexperience/" in path:
        return "oracle_hcm"
    if host_matches_domain(host, "icims.com"):
        return "icims"
    if host_matches_domain(host, "successfactors.com"):
        return "successfactors"
    if host_matches_domain(host, "csod.com"):
        return "cornerstone_csod"
    if host_matches_domain(host, "homerun.co"):
        return "homerun"
    if host_matches_domain(host, "hrmos.co"):
        return "hrmos"
    if host_matches_domain(host, "jobvite.com"):
        return "jobvite"
    return ""


def _provider_family(row: dict[str, Any], provider_url: str) -> str:
    explicit = _lower(
        row.get("detectedProviderFamily")
        or row.get("providerFamily")
        or row.get("provider")
        or row.get("providerAdapter")
        or row.get("detectedProvider")
    )
    if explicit and explicit != "static":
        return explicit
    adapter = _lower(row.get("adapter"))
    if adapter in _SUPPORTED_MIGRATION_PROVIDERS and adapter != "static":
        return adapter
    if provider_url:
        inferred = infer_provider_adapter(_host(provider_url), urlparse(provider_url).path or "")
        if inferred:
            return inferred
        unsupported = _unsupported_provider_family(provider_url)
        if unsupported:
            return unsupported
    rule = _redundant_rule_for_host(_host(_current_url(row)))
    return _lower(rule.get("adapter"))


def _provider_row_from_url(row: dict[str, Any], family: str, provider_url: str) -> dict[str, Any]:
    if family not in SUPPORTED_PROVIDERS or not provider_url:
        return {}
    inferred = provider_candidate(
        studio=_text(row.get("studio") or row.get("company") or row.get("name") or "Unknown"),
        adapter=family,
        url=provider_url,
        nl_priority=_as_bool(row.get("nlPriority")),
        discovery_method=_text(row.get("discoveryMethod")) or "provider_migration_advisory",
        evidence_types=["web_provider_url"],
        evidence_source="provider_migration_advisory",
        evidence_score=max(1, _as_int(row.get("evidenceScore") or row.get("score"))),
    )
    return dict(inferred or {})


def _provider_id(row: dict[str, Any], family: str, provider_url: str) -> tuple[str, str]:
    rule = _redundant_rule_for_host(_host(_current_url(row)))
    if _lower(rule.get("adapter")) == family:
        field = _text(rule.get("provider_id_field"))
        value = _text(rule.get("provider_id_value"))
        if field and value:
            return field, value
    merged = {**_provider_row_from_url(row, family, provider_url), **row}
    for key in _PROVIDER_ID_FIELDS:
        value = _text(merged.get(key))
        if value:
            return key, value
    return "", ""


def _provider_row_from_evidence(
    row: dict[str, Any], *, family: str, provider_url: str
) -> dict[str, Any]:
    provider_row = _provider_row_from_url(row, family, provider_url)
    if not provider_row:
        return {}
    for key in _PROVIDER_ID_FIELDS:
        value = _text(row.get(key))
        if value and not provider_row.get(key):
            provider_row[key] = value
    return provider_row


def _is_static_like_advisory(row: dict[str, Any]) -> bool:
    current_adapter = _lower(row.get("currentAdapter") or row.get("adapter"))
    discovery_stage = _lower(row.get("discoveryStage"))
    if current_adapter in SUPPORTED_PROVIDERS and not _text(row.get("migrationSourceIdentity")):
        return False
    if current_adapter in _STATIC_LIKE_ADAPTERS or discovery_stage in _STATIC_LIKE_STAGES:
        return True
    return bool(_text(row.get("migrationSourceIdentity")))


def _has_strong_provider_evidence(row: dict[str, Any]) -> bool:
    reasons = {_lower(item) for item in row.get("migrationReasons") or []}
    return bool(reasons & _STRONG_PROVIDER_EVIDENCE_REASONS)


def _tenant_scoped_lookup_key(
    row: dict[str, Any], family: str, provider_id: str
) -> tuple[str, str] | None:
    """A tenant-scoped key for multi-tenant providers, or None to use the family key.

    ``REDUNDANT_STATIC_IF_PROVIDER`` declares Workday and BambooHR with
    ``provider_id_field: adapter`` / ``provider_id_value: workday``. That value is the
    *adapter name*, not a tenant, so every board on the platform hashed to one lookup key
    and the duplicate index held a single arbitrary entry: NVIDIA resolved to whichever
    Workday board was indexed first, was flagged ``already_covered_by_provider``, and was
    never registered. 64 curated boards / 1,257 openings were suppressed this way.

    The redundancy rule still means what it should -- "this static board is served by a
    provider" -- so it is left alone. What changes is that identity resolution asks which
    tenant, and only falls back to the platform-wide key when no tenant can be resolved.
    """
    if not family or not provider_id:
        return None
    if provider_id.lower() not in {family.lower(), ""}:
        return None
    identity = board_identity_key(row)
    host, _, tenant = identity.partition("|")
    if "|" not in identity:
        host = _host(_current_url(row))
        tenant = ""
    if not multi_tenant_provider(host):
        return None
    label = tenant_host_label(host) or tenant
    path = tenant_path(_current_url(row)) or tenant
    if not label and not path:
        return None
    # A tenant on the same host but a different board path is a different board. Keying on
    # host alone would re-merge the two tenants this exists to separate.
    return family.lower(), f"{label}|{path}".lower()


def _provider_seen_identities(rows: list[dict[str, Any]] | None) -> set[str]:
    return {
        source_identity(row)
        for row in rows or []
        if isinstance(row, dict)
        and (
            _lower(row.get("adapter")) in SUPPORTED_PROVIDERS
            or bool(row.get("createdFromAdvisory"))
        )
        and source_identity(row)
    }


def _provider_lookup_keys(
    row: dict[str, Any], *, family: str = "", provider_id: str = ""
) -> list[tuple[str, str]]:
    """Every key this board may be found under, most specific first.

    ``family``/``provider_id`` are passed in rather than recomputed because the provider
    URL is not always the candidate URL: a static board advertising its ATS in
    ``atsLinks`` resolves its provider from that link, not from its own careers page.
    Recomputing from ``_candidate_url`` alone silently dropped those boards out of the
    index, which is how a real Greenhouse match stopped being found.

    A tenant-scoped key is authoritative when the board resolves to one, because that is
    the only key that can tell two tenants apart. The platform-wide key is kept as a
    fallback so a registry row that carries no resolvable tenant is still reachable.
    """
    if not family or not provider_id:
        provider_url = (
            _text(row.get("detectedProviderUrl")) or _first_ats_link(row) or _candidate_url(row)
        )
        family = family or _provider_family(row, provider_url)
        provider_id = provider_id or _provider_id(row, family, provider_url)[1]
    if not family or not provider_id:
        return []
    keys: list[tuple[str, str]] = []
    scoped = _tenant_scoped_lookup_key(row, family, provider_id)
    if scoped is not None:
        keys.append(scoped)
    wide = (family, provider_id.lower())
    if wide not in keys:
        keys.append(wide)
    return keys


def _provider_registry_index(
    active_rows: list[dict[str, Any]] | None,
    pending_rows: list[dict[str, Any]] | None,
) -> dict[tuple[str, str], tuple[str, str]]:
    """Registry rows indexed by every key they answer to.

    ``setdefault`` over most-specific-first means a tenant-scoped row wins its own key
    rather than being shadowed by whichever unrelated tenant happened to be indexed first.
    """
    index: dict[tuple[str, str], tuple[str, str]] = {}
    for state, rows in (("active", active_rows or []), ("pending", pending_rows or [])):
        for row in rows:
            if not isinstance(row, dict):
                continue
            identity, row_state = source_identity(row), state
            for key in _provider_lookup_keys(row):
                index.setdefault(key, (identity, row_state))
    return index


def _static_state(row: dict[str, Any]) -> str:
    if _as_bool(row.get("hiddenFromDefault")) or _lower(row.get("candidateState")) == "hidden":
        return "hidden"
    if _as_bool(row.get("duplicateOfActiveSource")):
        return "active_duplicate"
    if _as_bool(row.get("duplicateOfPendingSource")):
        return "pending_duplicate"
    if _as_bool(row.get("deferred")):
        return "deferred"
    return _lower(row.get("candidateState")) or "candidate"


def _existing_provider_match(
    row: dict[str, Any],
    *,
    family: str,
    provider_id: str,
    provider_index: dict[tuple[str, str], tuple[str, str]],
) -> tuple[str, str]:
    """The registry row that already serves this board, if any.

    Looked up tenant-scoped first. A match is only accepted when it is this board: a
    cross-tenant hit means the row belongs to a different studio, and reporting it as
    coverage is how 64 curated boards were suppressed.
    """
    for key in _provider_lookup_keys(row, family=family, provider_id=provider_id):
        existing_id, existing_state = provider_index.get(key, ("", ""))
        if existing_id and _row_serves_board(existing_id, row):
            return existing_id, existing_state
    # No indexed match. A carried-over value is only trusted when it is this board too.
    carried = _text(row.get("existingProviderSourceId"))
    if carried and _row_serves_board(carried, row):
        return carried, _text(row.get("existingProviderSourceState"))
    return "", ""


def _row_serves_board(registry_id: str, candidate: dict[str, Any]) -> bool:
    """Whether a registry id identifies the same board as the candidate.

    Both sides resolve to ``(host, tenant)``. A shared platform domain is not identity:
    ``nvidia.wd5.myworkdayjobs.com`` and ``aristocrat.wd3.myworkdayjobs.com`` are two
    studios on one platform, not one board.
    """
    registry_host, registry_tenant = _id_host_tenant(registry_id)
    if not registry_host:
        return False
    candidate_host = _board_host(candidate)
    if not candidate_host:
        # No board host to compare. The registry row was found under this candidate's own
        # provider key, which is the strongest evidence available; the key carried the
        # tenant, so accept it rather than rejecting a correct match.
        return registry_id == (_text(candidate.get("sourceIdentity")) or source_identity(candidate))
    if _same_host(registry_host, candidate_host):
        pass
    elif registry_tenant and registry_tenant == _candidate_tenant(candidate):
        # A provider row is keyed by tenant on the platform's API host, while the board is
        # on the studio's own host. Same tenant is the same board.
        return True
    else:
        return False
    if not multi_tenant_provider(candidate_host):
        return True
    # Same platform host: the tenant decides, read the way that platform keeps it
    # (subdomain label for workday/bamboohr/teamtailor/breezy/..., path slug for
    # greenhouse/ashby/lever/workable/smartrecruiters). Both resolvers return the
    # platform's own service label for path-tenant hosts, which is never a studio.
    candidate_tenant = _candidate_tenant(candidate)
    if registry_tenant and candidate_tenant:
        return registry_tenant == candidate_tenant
    return registry_id == (_text(candidate.get("sourceIdentity")) or source_identity(candidate))


def _board_host(candidate: dict[str, Any]) -> str:
    """The candidate's own board host.

    The ATS link is preferred over the careers page: a static board whose provider row
    lives on the platform API host is the same board, and the careers host is the studio's
    marketing site. Neither is wrong; they are different roles.
    """
    for url in (_first_ats_link(candidate), _current_url(candidate), _candidate_url(candidate)):
        host = _host(url)
        if host:
            return host
    return ""


def _same_host(left: str, right: str) -> bool:
    """Host equality that tolerates ``www.`` and the API/page split on one domain."""

    def bare(value: str) -> str:
        label = str(value or "").strip().lower().split(":")[0]
        return label[4:] if label.startswith("www.") else label

    left_bare, right_bare = bare(left), bare(right)
    if left_bare == right_bare:
        return True
    return root_domain(left_bare) == root_domain(right_bare) and bool(root_domain(left_bare))


def _candidate_tenant(candidate: dict[str, Any]) -> str:
    """The candidate's tenant, from whichever of its URLs carries one.

    The ATS link is consulted before the board's own URL: the board link is the studio's
    marketing site, and on the path-tenant platforms only the ATS link carries the slug.
    """
    urls = (_first_ats_link(candidate), _current_url(candidate), _candidate_url(candidate))
    for url in urls:
        host = _host(url)
        if host and multi_tenant_provider(host) in PATH_TENANT_PROVIDER_FAMILIES:
            path = tenant_path(url)
            if path:
                return path.split("/")[-1]
    for url in urls:
        host = _host(url)
        label = tenant_host_label(host) if host else ""
        if label:
            return label
    for url in (_current_url(candidate), _candidate_url(candidate)):
        path = tenant_path(url)
        if path:
            return path.split("/")[-1]
    return _text(candidate.get("slug") or candidate.get("account") or candidate.get("company_id"))


def _id_host_tenant(registry_id: str) -> tuple[str, str]:
    try:
        from tools.coverage_board_identity import registry_identity
    except ImportError:  # pragma: no cover - tools/ is absent in a slimmed install
        parsed = _fallback_registry_identity(registry_id)
        if parsed is None:
            return "", ""
    else:
        parsed = registry_identity(registry_id)
        if parsed is None:
            return "", ""
    _adapter, host, tenant = parsed
    if multi_tenant_provider(host) in PATH_TENANT_PROVIDER_FAMILIES:
        # ``job-boards``/``boards`` are the platform's own service names, not a studio.
        return host, tenant
    return host, tenant_host_label(host) or tenant


def _fallback_registry_identity(registry_id: str) -> tuple[str, str, str] | None:
    """``(adapter, host, tenant)`` for a registry id, without the tools package.

    The tools module owns the canonical rule for splitting an id; this mirrors it closely
    enough for a host/tenant comparison and is only reached when it cannot be imported.
    """
    parts = str(registry_id or "").split(":", 2)
    if len(parts) < 3:
        return None
    adapter, value = parts[0].lower(), parts[2]
    host = _host(value if "/" in value else "")
    if not host:
        host = _host(f"https://{value}")
    if not host:
        return "", "", ""
    path = tenant_path(value)
    return adapter, host, path.split("/")[-1] if path else ""


def _migration_reasons(
    row: dict[str, Any],
    *,
    provider_url: str,
    family: str,
    provider_id_field: str,
    provider_id: str,
    existing_state: str,
    unsupported: bool,
) -> list[str]:
    reasons: list[str] = []
    if provider_url:
        reasons.append("provider_url_evidence")
    if family:
        reasons.append(f"provider_family:{family}")
    if provider_id:
        reasons.append(f"provider_id:{provider_id_field}")
    if existing_state:
        reasons.append(f"existing_provider:{existing_state}")
    if unsupported:
        reasons.append("unsupported_provider_evidence")
    if _as_bool(row.get("browserFallbackRecommended")):
        reasons.append("needs_probe")
    return unique_string_list([_text(reason) for reason in reasons])


def _recommended_action(
    row: dict[str, Any],
    *,
    current_adapter: str,
    family: str,
    supported: bool,
    unsupported: bool,
    provider_id: str,
    existing_id: str,
) -> tuple[str, int]:
    is_static_review_target = current_adapter in {"static", "scrapy_static"} or _lower(
        row.get("discoveryStage")
    ) in {"generic_static", "seed_careers_page", "sheet_directory"}
    if existing_id:
        return "already_covered_by_provider", 95
    if unsupported:
        return "unsupported_provider", 45
    if supported and provider_id:
        return "add_provider_source", 85
    if supported and family:
        return "review_provider_migration", 60
    if _as_bool(row.get("browserFallbackRecommended")) or _text(row.get("lastProbeError")):
        return "needs_probe", 35
    if is_static_review_target and _as_int(row.get("jobsFound")) > 0:
        return "keep_static", 30
    return "insufficient_evidence", 15


def enrich_provider_migration_metadata(
    row: dict[str, Any],
    *,
    provider_index: dict[tuple[str, str], tuple[str, str]] | None = None,
) -> dict[str, Any]:
    updated = dict(row)
    current_adapter = _lower(updated.get("adapter")) or "unknown"
    current_url = _current_url(updated)
    provider_url = (
        _text(updated.get("detectedProviderUrl"))
        or _first_ats_link(updated)
        or _candidate_url(updated)
    )
    family = _provider_family(updated, provider_url)
    provider_row_from_url = _provider_row_from_url(updated, family, provider_url)
    supported = family in _SUPPORTED_MIGRATION_PROVIDERS and (
        family != "oracle_hcm" or bool(provider_row_from_url)
    )
    unsupported = bool(family and not supported)
    id_field, provider_id = _provider_id(updated, family, provider_url)
    existing_id, existing_state = _existing_provider_match(
        updated,
        family=family,
        provider_id=provider_id,
        provider_index=provider_index or {},
    )
    action, confidence = _recommended_action(
        updated,
        current_adapter=current_adapter,
        family=family,
        supported=supported,
        unsupported=unsupported,
        provider_id=provider_id,
        existing_id=existing_id,
    )

    updated["currentAdapter"] = current_adapter
    updated["currentUrl"] = current_url
    updated["detectedProvider"] = bool(family)
    updated["detectedProviderFamily"] = family
    updated["detectedProviderUrl"] = provider_url
    updated["detectedProviderId"] = provider_id
    updated["existingProviderSourceId"] = existing_id
    updated["existingProviderSourceState"] = existing_state
    updated["staticSourceState"] = _static_state(updated)
    updated["migrationConfidence"] = int(confidence)
    updated["migrationReasons"] = _migration_reasons(
        updated,
        provider_url=provider_url,
        family=family,
        provider_id_field=id_field,
        provider_id=provider_id,
        existing_state=existing_state or ("known" if existing_id else ""),
        unsupported=unsupported,
    )
    updated["recommendedAction"] = action
    return updated


def _staged_provider_row_from_enriched_advisory(row: dict[str, Any], *, at: str) -> dict[str, Any]:
    family = _lower(row.get("detectedProviderFamily"))
    provider_url = _text(row.get("detectedProviderUrl"))
    provider_row = _provider_row_from_evidence(row, family=family, provider_url=provider_url)
    if not provider_row:
        return {}
    provider_row["createdFromAdvisory"] = True
    provider_row["migrationSourceIdentity"] = _text(row.get("sourceIdentity")) or source_identity(
        row
    )
    provider_row["migrationReasons"] = list(row.get("migrationReasons") or [])
    provider_row["migrationConfidence"] = _as_int(row.get("migrationConfidence"))
    provider_row["detectedProviderFamily"] = family
    provider_row["detectedProviderUrl"] = provider_url
    provider_row["detectedProviderId"] = _text(row.get("detectedProviderId"))
    provider_row["candidateState"] = "staged_provider_candidate"
    provider_row["stagedAt"] = str(at)
    provider_row["stagedBy"] = _STAGED_ACTOR
    provider_row["discoveryMethod"] = "provider_migration_advisory"
    provider_row["discoveryStage"] = "provider_migration_advisory"
    provider_row["sourceIdentity"] = source_identity(provider_row)
    provider_row["jobsFound"] = 0
    provider_row["sampleCount"] = 0
    return provider_row


def _staged_advisory_diagnostic(advisory: dict[str, Any]) -> dict[str, Any]:
    candidate_id = source_identity(advisory)
    return {
        **advisory,
        "providerStagingDecision": "staged",
        "providerStagingBlockers": [],
        "providerStagingCandidateId": candidate_id,
        "providerStagingSourceIdentity": _text(advisory.get("migrationSourceIdentity")),
        "providerStagingProviderFamily": _lower(
            advisory.get("detectedProviderFamily") or advisory.get("adapter")
        ),
        "providerStagingProviderUrl": _text(advisory.get("detectedProviderUrl")),
        "providerStagingProviderId": _text(advisory.get("detectedProviderId")),
    }


def _non_stageable_action_blocker(action: str) -> str:
    if action == "unsupported_provider":
        return "unsupported_provider"
    if action == "needs_probe":
        return "needs_probe"
    if action == "insufficient_evidence":
        return "insufficient_evidence"
    if action == "already_covered_by_provider":
        return "existing_provider"
    return "non_stageable_action"


def _provider_staging_blockers(
    advisory: dict[str, Any],
    *,
    action: str,
    family: str,
    provider_url: str,
    provider_id: str,
) -> list[str]:
    blockers: list[str] = []
    stageable_action = action in _STAGEABLE_ACTIONS
    if not stageable_action:
        blockers.append(_non_stageable_action_blocker(action))
    if stageable_action and not _is_static_like_advisory(advisory):
        blockers.append("adapter_mismatch")
    if family and family not in SUPPORTED_PROVIDERS:
        blockers.append("unsupported_provider")
    if bool(advisory.get("duplicateOfActiveSource")):
        blockers.append("duplicate_active")
    if bool(advisory.get("duplicateOfPendingSource")):
        blockers.append("duplicate_pending")
    if _text(advisory.get("existingProviderSourceId")):
        blockers.append("existing_provider")
    if stageable_action and not (provider_url or provider_id):
        blockers.append("missing_provider_evidence")
    if stageable_action and not _has_strong_provider_evidence(advisory):
        blockers.append("insufficient_evidence")
    return unique_string_list(blockers)


def _provider_row_for_decision(
    advisory: dict[str, Any],
    *,
    at: str,
    blockers: list[str],
    seen_provider_ids: set[str] | None,
) -> tuple[dict[str, Any], str, list[str]]:
    if blockers:
        return {}, "", blockers
    provider_row = _staged_provider_row_from_enriched_advisory(advisory, at=at)
    if not provider_row:
        return {}, "", [*blockers, "provider_row_build_failure"]
    candidate_id = source_identity(provider_row)
    if candidate_id in (seen_provider_ids or set()):
        return {}, candidate_id, [*blockers, "identity_collision"]
    return provider_row, candidate_id, blockers


def _provider_staging_diagnostic(
    advisory: dict[str, Any],
    *,
    blockers: list[str],
    candidate_id: str,
    family: str,
    provider_url: str,
    provider_id: str,
    provider_row: dict[str, Any],
) -> dict[str, Any]:
    diagnostic = {
        **advisory,
        "providerStagingDecision": "skipped" if blockers else "staged",
        "providerStagingBlockers": unique_string_list(blockers),
        "providerStagingCandidateId": candidate_id,
        "providerStagingSourceIdentity": _text(advisory.get("sourceIdentity"))
        or source_identity(advisory),
        "providerStagingProviderFamily": family,
        "providerStagingProviderUrl": provider_url,
        "providerStagingProviderId": provider_id,
    }
    if provider_row:
        diagnostic["providerStagingCandidateId"] = source_identity(provider_row)
        diagnostic["providerStagingSourceIdentity"] = provider_row.get("migrationSourceIdentity")
    return diagnostic


def provider_staging_decision_for_advisory(
    row: dict[str, Any],
    *,
    at: str,
    provider_index: dict[tuple[str, str], tuple[str, str]] | None = None,
    seen_provider_ids: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    advisory = enrich_provider_migration_metadata(row, provider_index=provider_index or {})
    if bool(advisory.get("createdFromAdvisory")):
        return {}, _staged_advisory_diagnostic(advisory)

    action = _text(advisory.get("recommendedAction"))
    family = _lower(advisory.get("detectedProviderFamily"))
    provider_url = _text(advisory.get("detectedProviderUrl"))
    provider_id = _text(advisory.get("detectedProviderId"))
    blockers = _provider_staging_blockers(
        advisory,
        action=action,
        family=family,
        provider_url=provider_url,
        provider_id=provider_id,
    )
    provider_row, candidate_id, blockers = _provider_row_for_decision(
        advisory,
        at=at,
        blockers=blockers,
        seen_provider_ids=seen_provider_ids,
    )
    return provider_row, _provider_staging_diagnostic(
        advisory,
        blockers=blockers,
        candidate_id=candidate_id,
        family=family,
        provider_url=provider_url,
        provider_id=provider_id,
        provider_row=provider_row,
    )


def stage_provider_candidates_with_diagnostics(
    rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    pending_rows: list[dict[str, Any]] | None = None,
    seen_rows: list[dict[str, Any]] | None = None,
    at: str,
) -> dict[str, Any]:
    provider_index = _provider_registry_index(active_rows, pending_rows)
    seen_ids = {
        *_provider_seen_identities(active_rows),
        *_provider_seen_identities(pending_rows),
        *_provider_seen_identities(seen_rows),
    }
    staged: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate, diagnostic = provider_staging_decision_for_advisory(
            row,
            at=at,
            provider_index=provider_index,
            seen_provider_ids=seen_ids,
        )
        diagnostics.append(diagnostic)
        if not candidate:
            continue
        candidate_id = source_identity(candidate)
        seen_ids.add(candidate_id)
        staged.append(candidate)
    return {"staged": staged, "diagnostics": diagnostics}


def stage_provider_candidates_from_advisories(
    rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    pending_rows: list[dict[str, Any]] | None = None,
    at: str,
) -> list[dict[str, Any]]:
    result = stage_provider_candidates_with_diagnostics(
        rows,
        active_rows=active_rows,
        pending_rows=pending_rows,
        at=at,
    )
    return [dict(row) for row in result.get("staged", []) if isinstance(row, dict)]


def enrich_provider_migration_rows(
    rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    pending_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    provider_index = _provider_registry_index(active_rows, pending_rows)
    return [
        enrich_provider_migration_metadata(row, provider_index=provider_index)
        for row in rows
        if isinstance(row, dict)
    ]


def _compact(row: dict[str, Any]) -> dict[str, Any]:
    compact = {key: row.get(key) for key in _COMPACT_KEYS if row.get(key) not in (None, "")}
    compact["migrationConfidence"] = _as_int(compact.get("migrationConfidence"))
    compact["jobsFound"] = _as_int(compact.get("jobsFound"))
    compact["rankScore"] = _as_int(compact.get("rankScore"))
    return compact


def _top_rows(
    rows: list[dict[str, Any]], actions: set[str], *, limit: int = 8
) -> list[dict[str, Any]]:
    selected = [row for row in rows if _text(row.get("recommendedAction")) in actions]
    selected.sort(
        key=lambda row: (_as_int(row.get("migrationConfidence")), _as_int(row.get("jobsFound"))),
        reverse=True,
    )
    return [_compact(row) for row in selected[:limit]]


def _top_blocker_rows(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    selected = [
        row
        for row in rows
        if row.get("providerStagingDecision") == "skipped" and row.get("providerStagingBlockers")
    ]
    selected.sort(
        key=lambda row: (_as_int(row.get("migrationConfidence")), _as_int(row.get("jobsFound"))),
        reverse=True,
    )
    return [_compact(row) for row in selected[:limit]]


def _staging_blocker_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        blockers = row.get("providerStagingBlockers")
        if not isinstance(blockers, list):
            continue
        for blocker in blockers:
            token = _text(blocker)
            if token:
                counts[token] += 1
    return dict(sorted(counts.items()))


def _blocker_count(counts: dict[str, int], key: str) -> int:
    return int(counts.get(key) or 0)


def build_provider_migration_payload(
    rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    pending_rows: list[dict[str, Any]] | None = None,
    at: str = "",
) -> dict[str, Any]:
    result = stage_provider_candidates_with_diagnostics(
        rows,
        active_rows=active_rows,
        pending_rows=pending_rows,
        at=at,
    )
    candidates = [
        dict(row)
        for row in result.get("diagnostics", [])
        if isinstance(row, dict) and _text(row.get("recommendedAction"))
    ]
    counts = Counter(_text(row.get("recommendedAction")) for row in candidates)
    counts.pop("", None)
    blocker_counts = _staging_blocker_counts(candidates)
    staged_candidate_ids = {
        _text(row.get("providerStagingCandidateId"))
        for row in candidates
        if row.get("providerStagingDecision") == "staged"
        and _text(row.get("providerStagingCandidateId"))
    }
    staged_count = len(staged_candidate_ids)
    stageable_count = sum(
        1
        for row in candidates
        if row.get("providerStagingDecision") == "staged"
        and not bool(row.get("createdFromAdvisory"))
    )
    return {
        "totalCandidates": len(candidates),
        "stagedProviderCount": sum(1 for row in candidates if bool(row.get("createdFromAdvisory"))),
        "stageableProviderCandidateCount": stageable_count,
        "stagedProviderCandidateCount": staged_count,
        "stagingSkippedCount": sum(
            1 for row in candidates if row.get("providerStagingDecision") == "skipped"
        ),
        "stagingBlockedByDuplicateActiveCount": _blocker_count(blocker_counts, "duplicate_active"),
        "stagingBlockedByDuplicatePendingCount": _blocker_count(
            blocker_counts, "duplicate_pending"
        ),
        "stagingBlockedByUnsupportedProviderCount": _blocker_count(
            blocker_counts, "unsupported_provider"
        ),
        "stagingBlockedByInsufficientEvidenceCount": _blocker_count(
            blocker_counts, "insufficient_evidence"
        ),
        "stagingBlockedByNeedsProbeCount": _blocker_count(blocker_counts, "needs_probe"),
        "stagingBlockedByProviderRowBuildFailureCount": _blocker_count(
            blocker_counts, "provider_row_build_failure"
        ),
        "stagingBlockedByIdentityCollisionCount": _blocker_count(
            blocker_counts, "identity_collision"
        ),
        "stagingBlockedByAdapterMismatchCount": _blocker_count(blocker_counts, "adapter_mismatch"),
        "stagingBlockerCounts": blocker_counts,
        "stagingBlockerExamples": _top_blocker_rows(candidates),
        "actionCounts": dict(sorted(counts.items())),
        "stagedProviderCandidates": _top_rows(
            [row for row in candidates if row.get("providerStagingDecision") == "staged"],
            {
                "add_provider_source",
                "review_provider_migration",
                "already_covered_by_provider",
            },
        ),
        "providerMigrationCandidates": _top_rows(
            candidates, {"review_provider_migration", "add_provider_source"}
        ),
        "alreadyCoveredByProvider": _top_rows(candidates, {"already_covered_by_provider"}),
        "addProviderSourceCandidates": _top_rows(candidates, {"add_provider_source"}),
        "unsupportedProviderCandidates": _top_rows(candidates, {"unsupported_provider"}),
        "needsProbeCandidates": _top_rows(candidates, {"needs_probe"}),
        "keepStaticOrInsufficientEvidence": _top_rows(
            candidates, {"keep_static", "insufficient_evidence"}
        ),
    }
