from __future__ import annotations

"""Shared runtime mechanics for active-source audit batches."""

from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso

from . import audit_ledger


@dataclass
class HomepagePageOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    found_candidates: bool = False


@dataclass
class NoCandidateOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveHomepageBatchResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)
    homepages_fetched: int = 0


@dataclass
class ActiveAuditArtifactCounts:
    rejected_rows: list[dict[str, Any]]
    active_rows: list[dict[str, Any]]
    all_candidates: list[dict[str, Any]]
    recovered_candidates: list[dict[str, Any]]
    recovered_active: list[dict[str, Any]]
    technical_failures: list[dict[str, Any]]
    coverage_misses: list[dict[str, Any]]
    reason_counts: dict[str, int]
    detail_counts: dict[str, int]
    active_adapter_counts: dict[str, int]
    zero_job_count: int
    failure_count: int
    failure_sample_count: int
    browser_recovery_candidate_count: int
    browser_recovery_processed_count: int
    browser_recovered_active_count: int
    lost_recovered_active_count: int


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in _as_list(value) if isinstance(row, dict)]


def create_active_audit_artifact(
    *,
    schema_version: int,
    run_id: str,
    started_at: str,
    mode: str,
    progress: dict[str, Any],
    runtime: dict[str, Any],
    list_keys: list[str],
    dict_keys: list[str],
) -> dict[str, Any]:
    artifact: dict[str, Any] = {
        "schemaVersion": int(schema_version),
        "runId": str(run_id or ""),
        "startedAt": str(started_at or now_iso()),
        "updatedAt": "",
        "finishedAt": "",
        "mode": str(mode or ""),
        "summary": {},
        "progress": dict(progress),
        "runtime": dict(runtime),
        "timings": {"batches": [], "totalsMs": {}},
    }
    for key in dict_keys:
        artifact[str(key)] = {}
    for key in list_keys:
        artifact[str(key)] = []
    return artifact


def load_or_initialize_active_audit_artifact(
    output_path: Path,
    *,
    reset: bool,
    schema_version: int,
    initial_artifact: dict[str, Any],
    runtime_updates: dict[str, Any],
    progress_updates: dict[str, Any],
    list_keys: list[str],
    dict_keys: list[str],
    failure_sample_limit: int,
    load_json_object: Callable[[Path, dict[str, Any]], Any],
) -> dict[str, Any]:
    if reset:
        with suppress(FileNotFoundError, PermissionError, OSError):
            output_path.unlink()
    existing = load_json_object(output_path, {})
    if isinstance(existing, dict) and int(existing.get("schemaVersion") or 0) == int(
        schema_version
    ):
        artifact = dict(existing)
        artifact["runtime"] = {
            **_as_dict(artifact.get("runtime")),
            **dict(runtime_updates),
        }
        artifact["progress"] = {
            **_as_dict(artifact.get("progress")),
            **dict(progress_updates),
        }
        artifact.setdefault("timings", {"batches": [], "totalsMs": {}})
        for key in dict_keys:
            artifact.setdefault(str(key), {})
        for key in list_keys:
            if str(key) == "failureSamples":
                continue
            artifact.setdefault(str(key), [])
        artifact.setdefault(
            "failureSamples", _as_list(artifact.get("failures"))[:failure_sample_limit]
        )
        return artifact
    return dict(initial_artifact)


def finalize_active_audit_artifact(
    artifact: dict[str, Any],
    output_path: Path,
    *,
    completed_identities: set[str],
    complete: bool,
    completed_cursor_position: int,
    completed_key: str,
    summarize: Callable[[dict[str, Any], set[str]], None],
) -> None:
    progress = _as_dict(artifact.get("progress"))
    progress["complete"] = bool(complete)
    progress["completedUrlsCount"] = len(completed_identities)
    if complete:
        progress["cursorPosition"] = int(completed_cursor_position)
        artifact["finishedAt"] = now_iso()
    artifact["progress"] = progress
    artifact[completed_key] = sorted(completed_identities)
    artifact["updatedAt"] = now_iso()
    summarize(artifact, completed_identities)
    audit_ledger.save_artifact_atomic(artifact, output_path)


def save_updated_active_audit_artifact(
    artifact: dict[str, Any],
    output_path: Path,
    *,
    completed_identities: set[str],
    summarize: Callable[[dict[str, Any], set[str]], None],
) -> None:
    summarize(artifact, completed_identities)
    artifact["updatedAt"] = now_iso()
    audit_ledger.save_artifact_atomic(artifact, output_path)


def merge_unique_candidate_rows(
    existing: Any,
    incoming: list[dict[str, Any]],
    *,
    unique_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    return unique_rows([*_dict_rows(existing), *[dict(row) for row in incoming]])


def merge_rows_by_identity(
    existing: Any,
    incoming: list[dict[str, Any]],
    *,
    identity_fn: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in [*_as_list(existing), *incoming]:
        if not isinstance(row, dict):
            continue
        row_id = identity_fn(row)
        if row_id:
            rows[row_id] = dict(row)
        else:
            passthrough.append(dict(row))
    return [*passthrough, *rows.values()]


def append_artifact_rows(
    artifact: dict[str, Any],
    field_name: str,
    rows: list[dict[str, Any]],
) -> None:
    artifact[field_name] = [*_as_list(artifact.get(field_name)), *[dict(row) for row in rows]]


def append_batch_timing(artifact: dict[str, Any], timing: dict[str, Any]) -> None:
    audit_ledger.append_batch_timing(artifact, timing)


def record_failure_rows(
    artifact: dict[str, Any],
    failures: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> None:
    audit_ledger.record_failures(artifact, failures, sample_limit=sample_limit)


def failure_count(artifact: dict[str, Any]) -> int:
    return audit_ledger.failure_count(artifact)


def active_audit_artifact_counts(
    artifact: dict[str, Any],
    *,
    all_candidates_key: str,
    active_candidates_key: str,
    zero_candidates_key: str,
    rejected_key: str,
    browser_candidates_key: str,
    recovered_predicate: Callable[[dict[str, Any]], bool],
    failure_bucket_fn: Callable[[dict[str, Any]], str],
) -> ActiveAuditArtifactCounts:
    rejected = _dict_rows(artifact.get(rejected_key))
    reason_counts = Counter(str(row.get("reason") or "unknown") for row in rejected)
    detail_counts = Counter(str(row.get("reasonDetail") or "unknown") for row in rejected)
    active = _dict_rows(artifact.get(active_candidates_key))
    all_candidates = _dict_rows(artifact.get(all_candidates_key))
    recovered_candidates = [row for row in all_candidates if recovered_predicate(row)]
    recovered_active = [row for row in active if recovered_predicate(row)]
    technical_failures = [row for row in rejected if failure_bucket_fn(row) == "technical_failure"]
    coverage_misses = [row for row in rejected if failure_bucket_fn(row) == "coverage_miss"]
    adapter_counts = Counter(str(row.get("adapter") or "unknown") for row in active)
    browser_recovery = _as_dict(artifact.get("browserRecovery"))
    lost_recovery = _as_dict(artifact.get("lostRecoveryAudit"))
    return ActiveAuditArtifactCounts(
        rejected_rows=rejected,
        active_rows=active,
        all_candidates=all_candidates,
        recovered_candidates=recovered_candidates,
        recovered_active=recovered_active,
        technical_failures=technical_failures,
        coverage_misses=coverage_misses,
        reason_counts=dict(reason_counts),
        detail_counts=dict(detail_counts),
        active_adapter_counts=dict(adapter_counts),
        zero_job_count=len(_as_list(artifact.get(zero_candidates_key))),
        failure_count=failure_count(artifact),
        failure_sample_count=len(_as_list(artifact.get("failureSamples"))),
        browser_recovery_candidate_count=len(_as_list(artifact.get(browser_candidates_key))),
        browser_recovery_processed_count=_safe_int(browser_recovery.get("processedCount")),
        browser_recovered_active_count=_safe_int(browser_recovery.get("activeCandidates")),
        lost_recovered_active_count=_safe_int(lost_recovery.get("lostCount")),
    )


def row_identity_keys(
    row: dict[str, Any],
    *,
    url: str,
    entry_url: str,
) -> set[str]:
    keys: set[str] = set()
    if url:
        keys.add(f"url:{url}")
    if entry_url:
        keys.add(f"entry:{entry_url}")
    return keys


def rejection_rerun_key(
    rejection: dict[str, Any],
    *,
    candidate_url_fields: tuple[str, ...] = ("careersUrl", "listing_url"),
) -> str:
    url = str(rejection.get("url") or "").strip()
    if url:
        return f"url:{url}"
    candidate = _as_dict(rejection.get("candidate"))
    entry_url = str(candidate.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        return f"entry:{entry_url}"
    for field_name in candidate_url_fields:
        candidate_url = str(candidate.get(field_name) or "").strip()
        if candidate_url:
            return f"url:{candidate_url}"
    return ""


def select_rerun_rows(
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    rerun_reasons: set[str],
    *,
    rejected_key: str,
    rejection_key_fn: Callable[[dict[str, Any]], str],
    row_keys_fn: Callable[[dict[str, Any]], set[str]],
) -> tuple[list[dict[str, Any]], set[str]]:
    if not rerun_reasons:
        return representative_rows, set()
    requested_keys = {
        key
        for rejection in _as_list(artifact.get(rejected_key))
        if isinstance(rejection, dict)
        and str(rejection.get("reason") or "").strip() in rerun_reasons
        for key in [rejection_key_fn(rejection)]
        if key
    }
    if not requested_keys:
        return [], set()
    rows = [row for row in representative_rows if row_keys_fn(row) & requested_keys]
    return rows, requested_keys


def prune_rerun_rejections(
    artifact: dict[str, Any],
    *,
    rejected_key: str,
    rerun_reasons: set[str],
    rerun_row_keys: set[str],
    rejection_key_fn: Callable[[dict[str, Any]], str],
) -> None:
    if not rerun_reasons or not rerun_row_keys:
        return
    kept: list[dict[str, Any]] = []
    for rejection in _as_list(artifact.get(rejected_key)):
        if not isinstance(rejection, dict):
            continue
        reason = str(rejection.get("reason") or "").strip()
        key = rejection_key_fn(rejection)
        if reason in rerun_reasons and key in rerun_row_keys:
            continue
        kept.append(dict(rejection))
    artifact[rejected_key] = kept


def rejection_lookup_keys(
    rejection: dict[str, Any],
    *,
    candidate_identity_fn: Callable[[dict[str, Any]], str],
    candidate_url_key_fn: Callable[[dict[str, Any]], str],
) -> set[str]:
    candidate = _as_dict(rejection.get("candidate"))
    return {
        str(rejection.get("sourceId") or "").strip(),
        candidate_identity_fn(candidate),
        candidate_url_key_fn(candidate),
        f"url:{str(rejection.get('url') or '').strip()}",
        f"entry:{str(rejection.get('sourceDirectoryEntryUrl') or '').strip()}",
        f"entry:{str(candidate.get('sourceDirectoryEntryUrl') or '').strip()}",
    }


def index_rejections_by_identity(
    artifact: dict[str, Any],
    *,
    rejected_key: str,
    lookup_keys_fn: Callable[[dict[str, Any]], set[str]],
) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for rejection in _as_list(artifact.get(rejected_key)):
        if not isinstance(rejection, dict):
            continue
        for key in lookup_keys_fn(rejection):
            if key and key not in {"url:", "entry:"}:
                indexed.setdefault(key, []).append(dict(rejection))
    return indexed


def recovered_active_by_identity(
    artifact: dict[str, Any],
    *,
    active_key: str,
    recovered_predicate: Callable[[dict[str, Any]], bool],
    identity_fn: Callable[[dict[str, Any]], str],
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _as_list(artifact.get(active_key)):
        if not isinstance(row, dict) or not recovered_predicate(row):
            continue
        row_id = identity_fn(row)
        if row_id:
            rows[row_id] = dict(row)
    return rows


def compare_recovered_active_maps(
    *,
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    current_rejections: dict[str, list[dict[str, Any]]],
    classify_lost: Callable[
        [dict[str, Any], dict[str, list[dict[str, Any]]]], tuple[str, dict[str, Any]]
    ],
    lost_row_builder: Callable[[str, str, dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    lost_rows: list[dict[str, Any]] = []
    cause_counts: Counter[str] = Counter()
    for row_id, previous_candidate in previous.items():
        if row_id in current:
            continue
        cause, matched_rejection = classify_lost(previous_candidate, current_rejections)
        cause_counts[cause] += 1
        lost_rows.append(lost_row_builder(row_id, cause, previous_candidate, matched_rejection))
    return {
        "previousRecoveredActiveCount": len(previous),
        "currentRecoveredActiveCount": len(current),
        "lostCount": len(lost_rows),
        "lossCauseCounts": dict(cause_counts),
        "lostCandidates": sorted(lost_rows, key=lambda row: str(row.get("sourceId") or "")),
    }


def run_active_homepage_batch(
    *,
    batch_rows: list[dict[str, Any]],
    homepage_fetch_results: list[dict[str, Any]],
    row_url: Callable[[dict[str, Any]], str],
    infer_direct_provider: Callable[[dict[str, Any]], dict[str, Any] | None],
    fetch_failure_rejection: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    analyze_homepage: Callable[[dict[str, Any], str, str], HomepagePageOutcome],
    handle_no_candidate: Callable[[dict[str, Any], str, str], NoCandidateOutcome],
) -> ActiveHomepageBatchResult:
    result = ActiveHomepageBatchResult()
    fetched_urls = {str(row.get("url") or "").strip() for row in homepage_fetch_results}
    direct_rows = [row for row in batch_rows if row_url(row) not in fetched_urls]

    for row in direct_rows:
        inferred = infer_direct_provider(row)
        if inferred:
            result.provider_candidates.append(inferred)

    for fetch_result in homepage_fetch_results:
        row = dict(fetch_result.get("payload") or {})
        target_url = str(fetch_result.get("url") or row_url(row)).strip()
        if not bool(fetch_result.get("ok")):
            failure = fetch_result.get("failure")
            if isinstance(failure, dict):
                result.failures.append(dict(failure))
            result.rejected_rows.append(fetch_failure_rejection(row, fetch_result))
            continue

        result.homepages_fetched += 1
        html = str(fetch_result.get("text") or "")
        page_outcome = analyze_homepage(row, target_url, html)
        if page_outcome.found_candidates:
            result.provider_candidates.extend(page_outcome.provider_candidates)
            result.static_candidates.extend(page_outcome.static_candidates)
            continue

        no_candidate = handle_no_candidate(row, target_url, html)
        result.provider_candidates.extend(no_candidate.provider_candidates)
        result.static_candidates.extend(no_candidate.static_candidates)
        result.primary_recovery_jobs.extend(no_candidate.primary_recovery_jobs)
        result.secondary_recovery_jobs.extend(no_candidate.secondary_recovery_jobs)
        result.browser_recovery_candidates.extend(no_candidate.browser_recovery_candidates)
        result.rejected_rows.extend(no_candidate.rejected_rows)

    return result
