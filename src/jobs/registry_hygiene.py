"""Advisory registry-hygiene audit for configured source rows.

The monitor is deliberately read-only. It reports bounded evidence for the
registry defects that previously required one-off hunts: asset pages in a
configured window, canonical-URL duplicate candidates, unreachable evidence
from the previous source state, and host drift between a row's identity URL and
its configured fetch URLs. It never demotes, deletes, or rewrites a row.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.jobs.common.contracts_registry_repair_review import (
    registry_finding_fingerprint,
    registry_repair_review_status,
)
from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.page_gating import looks_like_asset_url
from src.jobs.text_utils import clean_text
from src.source_registry_identity import canonicalize_careers_url

REGISTRY_HYGIENE_SOURCE_LIMIT = 20
REGISTRY_HYGIENE_SAMPLE_LIMIT = 3
REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT = 200

# Repeated-evidence bar for promoting a permanently failing row from an advisory
# "unreachable" finding to a "repair candidate" -- a row worth a human-approved
# repoint/demote/retire decision.
#
# Repetition is accepted in *either* of two forms, and requiring both would
# under-report exactly the worst rows. `consecutiveFailures` is incremented by
# the error applier but the success applier resets it, and a circuit-broken or
# cadence-skipped source simply stops running -- so a source dead for five
# months can sit at two recorded failures. The outage span, measured from the
# last success, is the surviving evidence for those rows. Requiring
# `min_failures AND min_outage_days` therefore misses precisely the dead
# domains this is meant to find.
#
# Either form alone is too weak, so a third condition is required: the last
# observation must be recent. Without it the promotion would fire on ancient
# bookkeeping -- a source last checked months ago is unproven, not dead.
REGISTRY_REACHABILITY_MIN_FAILURES = 3
REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS = 7
REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS = 30

REGISTRY_HYGIENE_FLAGS = (
    "asset_pages",
    "duplicate_candidate",
    "host_drift_candidate",
    "unreachable_page",
    "repair_candidate",
)
_STATIC_LISTING_URL_RE = re.compile(
    r"^(?:static_source::)?static:listing_url:(?P<url>https?://\S+)$", re.IGNORECASE
)
_PERMANENT_ERROR_PATTERNS = (
    ("http_404", re.compile(r"(?:http|status[^\d])\s*404|\b404\b", re.IGNORECASE)),
    ("not_found", re.compile(r"\bnot found\b|\bno such host\b", re.IGNORECASE)),
    (
        "dns_or_tls",
        re.compile(r"getaddrinfo|name or service not known|certificate|ssl|tls", re.IGNORECASE),
    ),
    (
        "connection",
        re.compile(r"connection refused|network is unreachable|host not found", re.IGNORECASE),
    ),
)


def _rows(value: Any) -> list[dict[str, Any]]:
    try:
        values = list(value)
    except TypeError:
        return []
    return [row for row in values if isinstance(row, dict)]


def _dedupe(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            output.append(text)
    return output


def _host(value: Any) -> str:
    try:
        return (urlparse(clean_text(value)).hostname or "").lower()
    except ValueError:
        return ""


def _configured_urls(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in ("pages", "listing_url", "listingUrl", "careersUrl", "currentUrl"):
        raw = row.get(field)
        if isinstance(raw, list):
            values.extend(str(item or "") for item in raw)
        elif isinstance(raw, str):
            values.append(raw)
    return _dedupe(
        value for value in values if clean_text(value).startswith(("http://", "https://"))
    )


def _identity_url(row: dict[str, Any]) -> str:
    source_id = clean_text(row.get("id"))
    match = _STATIC_LISTING_URL_RE.match(source_id)
    if match:
        return match.group("url")
    for field in ("listing_url", "listingUrl", "careersUrl", "currentUrl", "board_url", "url"):
        value = clean_text(row.get(field))
        if value.startswith(("http://", "https://")):
            return value
    return ""


def _canonical_url(row: dict[str, Any]) -> str:
    for field in ("board_url", "listing_url", "listingUrl", "url"):
        value = clean_text(row.get(field))
        if not value:
            continue
        try:
            return canonicalize_careers_url(value)
        except (TypeError, ValueError):
            continue
    return ""


def _state_for_row(row: dict[str, Any], state_rows: Any) -> dict[str, Any]:
    """Resolve a registry row to its source-state entry.

    Static rows are keyed by id (optionally with the ``static_source::``
    loader prefix), while provider rows are keyed by their human-readable
    registry ``name`` (for example ``workable:account:hutch`` →
    ``Hutch (Workable)``). Both forms are tried so reachability evidence is
    attributed to the row that actually failed.
    """
    states = state_rows if isinstance(state_rows, dict) else {}
    source_id = clean_text(row.get("id"))
    for key in (
        source_id,
        f"static_source::{source_id}",
        source_id.removeprefix("static_source::"),
        clean_text(row.get("name")),
    ):
        if not key:
            continue
        value = states.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _unreachable_evidence(state: dict[str, Any]) -> str:
    if clean_text(state.get("lastStatus")).lower() != "error":
        return ""
    try:
        failures = int(state.get("consecutiveFailures") or 0)
    except (TypeError, ValueError):
        failures = 0
    if failures < 2:
        return ""
    error = clean_text(state.get("lastError") or state.get("error"))
    try:
        http_status = int(state.get("lastHttpStatus") or 0)
    except (TypeError, ValueError):
        http_status = 0
    if http_status in {404, 410}:
        return f"http_{http_status}"
    for name, pattern in _PERMANENT_ERROR_PATTERNS:
        if pattern.search(error):
            return name
    return ""


def _counter(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _days_between(earlier: datetime | None, reference: datetime | None) -> int | None:
    """Whole days from ``earlier`` to ``reference``, or None when unparseable."""
    if earlier is None or reference is None:
        return None
    return max(0, (reference - earlier).days)


def _outage_days(state: dict[str, Any], reference: datetime | None) -> int:
    """Whole days a row has been continuously failing, or 0 when unknowable.

    ``consecutiveFailures`` is a cross-run counter -- the success applier resets
    it to zero and drops ``lastError``/``lastFailureAt``, so a non-zero count
    means the row has failed every run since it last succeeded.
    ``lastSuccessAt`` is *not* cleared on failure, so it marks the start of the
    current outage. A row that has never succeeded has no such anchor and
    returns 0: the outage length is genuinely unknown, and guessing would
    manufacture repair evidence out of nothing.
    """
    days = _days_between(
        parse_datetime(state.get("lastSuccessAt") or state.get("lastSuccessfulFetchAt")),
        reference,
    )
    return days or 0


def _observation_age_days(state: dict[str, Any], reference: datetime | None) -> int | None:
    """Days since the row was last actually looked at, or None when unrecorded.

    The most recent observation is the last failure while it is failing, else
    the last run. This is what separates "we just checked and it is dead" from
    "we have not looked in months", which are very different claims.
    """
    last_observed = parse_datetime(state.get("lastFailureAt")) or parse_datetime(
        state.get("lastRunAt") or state.get("lastCheckedAt")
    )
    return _days_between(last_observed, reference)


def _reachability(state: dict[str, Any], reference: datetime | None) -> dict[str, Any]:
    """Repeated-evidence reachability verdict for one source-state row.

    Two thresholds, deliberately different:

    * ``unreachable_page`` -- 2+ consecutive runs with a permanent error. An
      early warning that something may be wrong.
    * ``repair_candidate`` -- a permanent error plus repetition in *either*
      form (recorded failures **or** a multi-day outage since the last success)
      plus a recent observation. See the threshold constants for why the two
      repetition forms are alternatives rather than a conjunction.

    Both are advisory. A repair candidate only means a row is *ready for a
    human-approved repair decision*; nothing here mutates the registry.
    """
    evidence = _unreachable_evidence(state)
    failures = _counter(state.get("consecutiveFailures"))
    outage_days = _outage_days(state, reference) if evidence else 0
    observation_age = _observation_age_days(state, reference) if evidence else None
    repeated = (
        failures >= REGISTRY_REACHABILITY_MIN_FAILURES
        or outage_days >= REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS
    )
    recently_observed = observation_age is not None and (
        observation_age <= REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS
    )
    return {
        "evidence": evidence,
        "consecutiveFailures": failures,
        "outageDays": outage_days,
        "observationAgeDays": observation_age,
        "isRepairCandidate": bool(evidence and repeated and recently_observed),
    }


def _sample(values: list[str]) -> list[str]:
    return values[:REGISTRY_HYGIENE_SAMPLE_LIMIT]


# One row can carry several independent findings. Each gets its own review, and
# the most severe wins the row's summary state so a row is never reported as
# "acknowledged" while a dead-domain repair candidate sits unreviewed on it.
_REVIEW_PRIORITY = ("stale", "new", "repair_approved", "snoozed", "acknowledged")


def _review_state_for_entry(entry: dict[str, Any], repair_review: Any) -> dict[str, str]:
    """Resolve the strongest recorded review decision for one flagged row.

    Reads the review artifact only. A recorded decision whose evidence
    fingerprint no longer matches is surfaced as ``stale`` with a reason rather
    than silently honored, so changed evidence visibly returns to the operator's
    queue instead of inheriting an old approval.
    """
    best: dict[str, str] = {
        "reviewState": "new",
        "reviewDecisionAt": "",
        "reviewDecidedBy": "",
        "approvedAction": "",
        "reviewStaleReason": "",
    }
    best_rank = len(_REVIEW_PRIORITY)
    for finding_kind in entry["flags"]:
        urls = _review_urls_for_flag(finding_kind, entry)
        fingerprint = registry_finding_fingerprint(
            source_id=entry["sourceId"],
            finding_kind=finding_kind,
            registry_state=entry["registryState"],
            unreachable_evidence=entry["unreachableEvidence"],
            affected_urls=urls,
        )
        recorded = registry_repair_review_status(
            repair_review,
            source_id=entry["sourceId"],
            finding_kind=finding_kind,
            evidence_fingerprint=fingerprint,
        )
        state = "stale" if recorded["isStale"] else recorded["decision"]
        if state == "new":
            continue
        rank = _REVIEW_PRIORITY.index(state)
        if rank < best_rank:
            best_rank = rank
            best = {
                "reviewState": state,
                "reviewDecisionAt": recorded["decisionAt"],
                "reviewDecidedBy": recorded["decidedBy"],
                "approvedAction": recorded["approvedAction"],
                "reviewStaleReason": (
                    "recorded decision no longer matches the observed evidence"
                    if state == "stale"
                    else ""
                ),
            }
    return best


def _review_urls_for_flag(finding_kind: str, entry: dict[str, Any]) -> list[str]:
    if finding_kind in {"unreachable_page", "repair_candidate"}:
        return list(entry["sampleUnreachablePages"])
    if finding_kind == "host_drift_candidate":
        return list(entry["sampleHostDriftUrls"])
    if finding_kind == "duplicate_candidate":
        return list(entry["sampleDuplicateUrls"])
    if finding_kind == "asset_pages":
        return list(entry["sampleAssetPages"])
    return []


def registry_hygiene_audit(
    rows: Any,
    *,
    source_state_rows: Any = None,
    known_collision_urls: Any = None,
    observed_at: Any = None,
    repair_review: Any = None,
) -> dict[str, Any]:
    """Return a bounded advisory hygiene report for registry rows.

    Counts remain exact even when the flagged-source and sample lists are
    capped. All findings are candidates for review; this function performs no
    registry mutation and treats host drift as advisory because provider and
    redirect relationships can legitimately cross hosts.

    ``repair_review`` is the human-approval artifact
    (``contracts_registry_repair_review``). It is read, never written: a
    recorded decision annotates a finding so an already-adjudicated row stops
    re-reporting as fresh work, and a decision whose evidence has since changed
    is reported as ``stale`` rather than honored. This function applies nothing
    and mutates nothing.

    ``known_collision_urls`` is the reviewed-collision allowlist from
    ``source-registry-known-url-collisions.json``. When supplied, duplicate
    groups are split by ``knownCollisionGroupCount`` (already reviewed and
    grandfathered) and ``uncoveredDuplicateGroupCount`` (new drift). When it is
    ``None`` or empty, every duplicate is reported as uncovered so drift is never
    silently hidden by a baseline that failed to load.

    ``observed_at`` is the reference instant for outage-length arithmetic;
    it defaults to now. Pass the run's start time so a report is reproducible
    from its own payload.
    """
    known: set[str] = set()
    if isinstance(known_collision_urls, (set, frozenset, list, tuple)):
        known = {url for url in (clean_text(item) for item in known_collision_urls) if url}
    reference = parse_datetime(observed_at) or datetime.now(UTC)
    source_rows = _rows(rows)
    duplicate_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    findings: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        source_id = clean_text(row.get("id"))
        if not source_id:
            continue
        urls = _configured_urls(row)
        entry = findings.setdefault(
            source_id,
            {
                "sourceId": source_id,
                "registryState": clean_text(row.get("registryState")),
                "flags": set(),
                "assetPageCount": 0,
                "sampleAssetPages": [],
                "duplicateGroupCount": 0,
                "uncoveredDuplicateGroupCount": 0,
                "sampleDuplicateUrls": [],
                "unreachableEvidence": "",
                "sampleUnreachablePages": [],
                "consecutiveFailures": 0,
                "outageDays": 0,
                "observationAgeDays": -1,
                "lastErrorSample": "",
                "hostDriftCount": 0,
                "sampleHostDriftUrls": [],
            },
        )
        assets = [url for url in urls if looks_like_asset_url(url)]
        if assets:
            entry["flags"].add("asset_pages")
            entry["assetPageCount"] = len(assets)
            entry["sampleAssetPages"] = _sample(assets)
        canonical = _canonical_url(row)
        if canonical:
            duplicate_groups[canonical].append(row)
        identity_host = _host(_identity_url(row))
        if identity_host:
            foreign = [url for url in urls if _host(url) and _host(url) != identity_host]
            if foreign:
                entry["flags"].add("host_drift_candidate")
                entry["hostDriftCount"] = len(foreign)
                entry["sampleHostDriftUrls"] = _sample(foreign)
        state = _state_for_row(row, source_state_rows)
        reachability = _reachability(state, reference)
        evidence = reachability["evidence"]
        if evidence:
            entry["flags"].add("unreachable_page")
            entry["unreachableEvidence"] = evidence
            entry["consecutiveFailures"] = reachability["consecutiveFailures"]
            entry["outageDays"] = reachability["outageDays"]
            age = reachability["observationAgeDays"]
            entry["observationAgeDays"] = age if age is not None else -1
            # Provider rows often carry no careers page, so fall back to the API
            # endpoint that actually failed rather than reporting an empty sample.
            entry["sampleUnreachablePages"] = _sample(
                _dedupe([*urls, clean_text(row.get("api_url")), _identity_url(row)])
            )
        if reachability["isRepairCandidate"]:
            entry["flags"].add("repair_candidate")
            # Provenance for the repair decision: what actually failed, verbatim
            # and bounded, so a reviewer does not have to re-run the source.
            entry["lastErrorSample"] = clean_text(state.get("lastError") or state.get("error"))[
                :REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT
            ]

    duplicate_row_count = 0
    duplicate_group_count = 0
    known_group_count = 0
    uncovered_group_count = 0
    uncovered_row_count = 0
    for canonical, group in duplicate_groups.items():
        if len(group) < 2:
            continue
        duplicate_group_count += 1
        duplicate_row_count += len(group)
        reviewed = canonical in known
        if reviewed:
            known_group_count += 1
        else:
            uncovered_group_count += 1
            uncovered_row_count += len(group)
        for row in group:
            source_id = clean_text(row.get("id"))
            candidate = findings.get(source_id)
            if candidate is None:
                continue
            candidate["flags"].add("duplicate_candidate")
            candidate["duplicateGroupCount"] += 1
            if not reviewed:
                candidate["uncoveredDuplicateGroupCount"] += 1
            candidate["sampleDuplicateUrls"] = _sample(
                [*candidate["sampleDuplicateUrls"], canonical]
            )

    ordered = []
    stale_review_count = 0
    reviewed_count = 0
    for source_id in sorted(findings):
        entry = findings[source_id]
        flags = [flag for flag in REGISTRY_HYGIENE_FLAGS if flag in entry["flags"]]
        if not flags:
            continue
        review = _review_state_for_entry(entry, repair_review)
        review_state = review["reviewState"]
        if review_state == "stale":
            stale_review_count += 1
        elif review_state != "new":
            reviewed_count += 1
        ordered.append(
            {
                "sourceId": entry["sourceId"],
                "registryState": entry["registryState"],
                "flags": flags,
                "assetPageCount": entry["assetPageCount"],
                "sampleAssetPages": entry["sampleAssetPages"],
                "duplicateGroupCount": entry["duplicateGroupCount"],
                "uncoveredDuplicateGroupCount": entry["uncoveredDuplicateGroupCount"],
                "sampleDuplicateUrls": entry["sampleDuplicateUrls"],
                "unreachableEvidence": entry["unreachableEvidence"],
                "sampleUnreachablePages": entry["sampleUnreachablePages"],
                "consecutiveFailures": entry["consecutiveFailures"],
                "outageDays": entry["outageDays"],
                "observationAgeDays": entry["observationAgeDays"],
                "lastErrorSample": entry["lastErrorSample"],
                "hostDriftCount": entry["hostDriftCount"],
                "sampleHostDriftUrls": entry["sampleHostDriftUrls"],
                "reviewState": review_state,
                "reviewDecisionAt": review["reviewDecisionAt"],
                "reviewDecidedBy": review["reviewDecidedBy"],
                "approvedAction": review["approvedAction"],
                "reviewStaleReason": review["reviewStaleReason"],
            }
        )
    return {
        "sourceCount": len(ordered),
        "assetPageCount": sum(item["assetPageCount"] for item in ordered),
        "duplicateGroupCount": duplicate_group_count,
        "duplicateRowCount": duplicate_row_count,
        "knownCollisionGroupCount": known_group_count,
        "uncoveredDuplicateGroupCount": uncovered_group_count,
        "uncoveredDuplicateRowCount": uncovered_row_count,
        "unreachablePageCount": sum("unreachable_page" in item["flags"] for item in ordered),
        "repairCandidateCount": sum("repair_candidate" in item["flags"] for item in ordered),
        "repairCandidateMinFailures": REGISTRY_REACHABILITY_MIN_FAILURES,
        "repairCandidateMinOutageDays": REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS,
        "repairCandidateMaxObservationAgeDays": REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS,
        "reviewedFindingCount": reviewed_count,
        "staleReviewCount": stale_review_count,
        "hostDriftCount": sum("host_drift_candidate" in item["flags"] for item in ordered),
        "sources": ordered[:REGISTRY_HYGIENE_SOURCE_LIMIT],
    }


__all__ = [
    "REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT",
    "REGISTRY_HYGIENE_FLAGS",
    "REGISTRY_HYGIENE_SAMPLE_LIMIT",
    "REGISTRY_HYGIENE_SOURCE_LIMIT",
    "REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS",
    "REGISTRY_REACHABILITY_MIN_FAILURES",
    "REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS",
    "registry_hygiene_audit",
]
