"""Active audit runtime helpers for source discovery.

Shared runtime mechanics for active-source audit batches.

AI boundary owns: active-audit task runtime, batching, evidence rows, and audit progress state.
AI boundary implement in: this file for active audit execution; GameDevMap-specific dry-run behavior stays in its leaf.
AI boundary search before contracts: gamedevmap active dry run, audit reports, and active audit tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused active audit tests.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from inspect import Parameter, signature
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso

from . import audit_ledger
from . import directory_page_recovery as directory_recovery_helpers


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


@dataclass
class ActiveAuditPreparedRows:
    direct_provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    homepage_rows: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveAuditRecoveryFetchResult:
    results: list[dict[str, Any]] = field(default_factory=list)
    unique_jobs: int = 0
    network_jobs: int = 0


@dataclass
class ActiveAuditRecoveryApplicationResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    pages_fetched: int = 0
    grouped_state: dict[str, Any] = field(default_factory=dict)
    recovered_homepages: set[str] = field(default_factory=set)


@dataclass
class ActiveAuditCandidateMergeResult:
    candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveAuditBatchResult:
    timing: dict[str, Any]
    candidates: list[dict[str, Any]]
    recovery_jobs: list[dict[str, Any]]
    recovered_homepages: set[str]


@dataclass
class ActiveAuditBatchStrategy:
    prepare_rows: Callable[[list[dict[str, Any]]], ActiveAuditPreparedRows]
    fetch_homepages: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    analyze_homepages: Callable[[list[dict[str, Any]]], ActiveHomepageBatchResult]
    fetch_recovery: Callable[[list[dict[str, Any]], str], ActiveAuditRecoveryFetchResult]
    apply_recovery: Callable[
        ...,
        ActiveAuditRecoveryApplicationResult,
    ]
    recovery_homepage_key: Callable[[dict[str, Any]], str]
    merge_candidates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        ActiveAuditCandidateMergeResult,
    ]
    merge_artifact_updates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        None,
    ]
    update_summary: Callable[[dict[str, Any]], None]
    probe_candidates: Callable[[list[dict[str, Any]]], Any]
    apply_probe_results: Callable[[Any], None]
    row_identity: Callable[[dict[str, Any]], str]
    append_timing: Callable[[dict[str, Any]], None]
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass
class ActiveAuditLoopResult:
    batches_run: int
    completed_identities: set[str]
    complete: bool


@dataclass
class ActiveAuditLoopStrategy:
    row_identity: Callable[[dict[str, Any]], str]
    emit_batch_log: Callable[[int, int, int], None]
    run_batch: Callable[[list[dict[str, Any]], int, int], None]
    before_write: Callable[[], None]
    write_artifact: Callable[[bool], None]
    progress_callback: Callable[[dict[str, Any]], None] | None = None


def build_active_audit_batch_strategy(
    *,
    prepare_rows: Callable[[list[dict[str, Any]]], ActiveAuditPreparedRows],
    fetch_homepages: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    analyze_homepages: Callable[[list[dict[str, Any]]], ActiveHomepageBatchResult],
    fetch_recovery: Callable[[list[dict[str, Any]], str], ActiveAuditRecoveryFetchResult],
    apply_recovery: Callable[
        ...,
        ActiveAuditRecoveryApplicationResult,
    ],
    recovery_homepage_key: Callable[[dict[str, Any]], str],
    merge_candidates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        ActiveAuditCandidateMergeResult,
    ],
    merge_artifact_updates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        None,
    ],
    update_summary: Callable[[dict[str, Any]], None],
    probe_candidates: Callable[[list[dict[str, Any]]], Any],
    apply_probe_results: Callable[[Any], None],
    row_identity: Callable[[dict[str, Any]], str],
    append_timing: Callable[[dict[str, Any]], None],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditBatchStrategy:
    return ActiveAuditBatchStrategy(
        prepare_rows=prepare_rows,
        fetch_homepages=fetch_homepages,
        analyze_homepages=analyze_homepages,
        fetch_recovery=fetch_recovery,
        apply_recovery=apply_recovery,
        recovery_homepage_key=recovery_homepage_key,
        merge_candidates=merge_candidates,
        merge_artifact_updates=merge_artifact_updates,
        update_summary=update_summary,
        probe_candidates=probe_candidates,
        apply_probe_results=apply_probe_results,
        row_identity=row_identity,
        append_timing=append_timing,
        progress_callback=progress_callback,
    )


def _emit_progress(
    callback: Callable[[dict[str, Any]], None] | None,
    event: dict[str, Any],
    *,
    force: bool = False,
) -> None:
    if callback is None:
        return
    callback({**event, "force": bool(force)})


def _apply_recovery_with_progress(
    strategy: ActiveAuditBatchStrategy,
    results: list[dict[str, Any]],
    grouped: dict[str, Any] | None,
    finalize: bool,
    progress_label: str,
) -> ActiveAuditRecoveryApplicationResult:
    use_progress_label = False
    with suppress(TypeError, ValueError):
        params = list(signature(strategy.apply_recovery).parameters.values())
        accepts_varargs = any(param.kind == Parameter.VAR_POSITIONAL for param in params)
        positional_params = [
            param
            for param in params
            if param.kind in {Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD}
        ]
        use_progress_label = accepts_varargs or len(positional_params) >= 4
    if use_progress_label:
        return strategy.apply_recovery(results, grouped, finalize, progress_label)
    return strategy.apply_recovery(results, grouped, finalize)


def build_active_audit_loop_strategy(
    *,
    artifact: dict[str, Any],
    row_identity: Callable[[dict[str, Any]], str],
    batch_strategy: ActiveAuditBatchStrategy,
    completed_identities: set[str],
    emit_batch_log: Callable[[int, int, int], None],
    before_write: Callable[[], None],
    write_artifact: Callable[[bool], None],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditLoopStrategy:
    def _run_batch(
        batch_rows: list[dict[str, Any]],
        cursor: int,
        _batch_number: int,
    ) -> None:
        progress = _as_dict(artifact.get("progress"))
        run_active_audit_batch(
            artifact=artifact,
            batch_rows=batch_rows,
            cursor=cursor,
            batch_number=_safe_int(progress.get("batchesCompleted")) + 1,
            strategy=batch_strategy,
            completed_identities=completed_identities,
        )

    return ActiveAuditLoopStrategy(
        row_identity=row_identity,
        emit_batch_log=emit_batch_log,
        run_batch=_run_batch,
        before_write=before_write,
        write_artifact=write_artifact,
        progress_callback=progress_callback,
    )


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


def _duration_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _next_unprocessed_batch(
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    row_identity: Callable[[dict[str, Any]], str],
) -> tuple[list[dict[str, Any]], int]:
    batch: list[dict[str, Any]] = []
    cursor = len(source_rows)
    for index, row in enumerate(source_rows):
        row_id = row_identity(row)
        if row_id and row_id in completed_identities:
            continue
        if not batch:
            cursor = index
        batch.append(row)
        if len(batch) >= batch_size:
            break
    return batch, cursor


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
        return dict(initial_artifact)
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


def run_active_audit_cache(
    *,
    reset: bool,
    has_rerun_reasons: bool,
    load_artifact: Callable[[], Any],
    signature_matches: Callable[[dict[str, Any]], bool],
    is_fresh: Callable[[dict[str, Any]], bool],
    refresh: Callable[[bool], dict[str, Any]],
    cache_hit_log: Callable[[dict[str, Any]], str],
    emit_log_fn: Callable[[str], None],
    signature_mismatch_log: Callable[[dict[str, Any]], str] | None = None,
) -> tuple[dict[str, Any], bool]:
    effective_reset = bool(reset)
    if not effective_reset and not has_rerun_reasons:
        existing = load_artifact()
        existing_artifact = dict(existing) if isinstance(existing, dict) else {}
        if existing_artifact and not signature_matches(existing_artifact):
            effective_reset = True
            if signature_mismatch_log is not None:
                emit_log_fn(signature_mismatch_log(existing_artifact))
        fresh = existing_artifact if is_fresh(existing_artifact) else None
        if fresh is not None:
            emit_log_fn(cache_hit_log(fresh))
            return fresh, True
    return refresh(effective_reset), False


def validated_active_candidates_from_artifact(
    artifact: dict[str, Any],
    *,
    active_key: str,
    identity_fn: Callable[[dict[str, Any]], str],
    validation_metadata: dict[str, Any],
    source_directory: str,
    static_transform: Callable[[dict[str, Any]], dict[str, Any] | None],
    unique_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    for item in _as_list(artifact.get(active_key)):
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("probeStatus") or "").strip().lower() != "ok":
            continue
        if _safe_int(row.get("jobsFound") or row.get("sampleCount")) <= 0:
            continue
        if not identity_fn(row):
            continue
        row.update(dict(validation_metadata))
        row["sourceDirectory"] = str(row.get("sourceDirectory") or source_directory)
        adapter = str(row.get("adapter") or "").strip().lower()
        if adapter == "static":
            static_row = static_transform(row)
            if static_row is not None:
                static_candidates.append(static_row)
        elif adapter:
            provider_candidates.append(row)
    return unique_rows(provider_candidates), unique_rows(static_candidates)


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


def merge_active_audit_batch_artifact_updates(
    artifact: dict[str, Any],
    *,
    all_candidates: list[dict[str, Any]],
    browser_recovery_rows: list[dict[str, Any]],
    homepage_failures: list[dict[str, Any]],
    recovery_failures: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    all_candidates_key: str,
    browser_candidates_key: str,
    rejected_key: str,
    unique_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    failure_sample_limit: int,
) -> None:
    artifact[all_candidates_key] = merge_unique_candidate_rows(
        artifact.get(all_candidates_key),
        all_candidates,
        unique_rows=unique_rows,
    )
    artifact[browser_candidates_key] = merge_unique_candidate_rows(
        artifact.get(browser_candidates_key),
        browser_recovery_rows,
        unique_rows=unique_rows,
    )
    record_failure_rows(artifact, homepage_failures, sample_limit=failure_sample_limit)
    record_failure_rows(artifact, recovery_failures, sample_limit=failure_sample_limit)
    append_artifact_rows(artifact, rejected_key, rejected_rows)


def increment_active_audit_summary(
    artifact: dict[str, Any],
    batch_counts: dict[str, Any],
) -> None:
    summary = _as_dict(artifact.get("summary"))
    for key, value in batch_counts.items():
        summary[key] = _safe_int(summary.get(key)) + int(value or 0)
    artifact["summary"] = summary


def apply_active_audit_probe_results(
    artifact: dict[str, Any],
    probe_results: Any,
    *,
    classify_probe_results: Callable[..., Any],
    probe_failed_rejection: Callable[..., dict[str, Any]],
    zero_jobs_rejection: Callable[..., dict[str, Any]],
    active_key: str,
    zero_candidates_key: str,
    rejected_key: str,
    identity_fn: Callable[[dict[str, Any]], str],
) -> None:
    classification = classify_probe_results(
        probe_results,
        probe_failed_rejection=probe_failed_rejection,
        zero_jobs_rejection=zero_jobs_rejection,
    )
    artifact[active_key] = merge_rows_by_identity(
        artifact.get(active_key),
        list(getattr(classification, "positive_candidates", []) or []),
        identity_fn=identity_fn,
    )
    artifact[zero_candidates_key] = merge_rows_by_identity(
        artifact.get(zero_candidates_key),
        list(getattr(classification, "zero_job_candidates", []) or []),
        identity_fn=identity_fn,
    )
    append_artifact_rows(
        artifact,
        rejected_key,
        list(getattr(classification, "rejected_rows", []) or []),
    )


def apply_active_audit_recovery_fetch_results(
    recovery_fetch_results: list[dict[str, Any]],
    *,
    grouped: dict[str, dict[str, Any]] | None = None,
    finalize: bool = True,
    apply_payload: directory_recovery_helpers.RecoveryPayloadApplier,
    finalize_group: directory_recovery_helpers.RecoveryGroupFinalizer,
    progress_label: str = "",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditRecoveryApplicationResult:
    output = directory_recovery_helpers.apply_recovery_fetch_results(
        recovery_fetch_results,
        grouped=grouped,
        finalize=finalize,
        apply_payload=apply_payload,
        finalize_group=finalize_group,
        progress_label=progress_label,
        progress_callback=progress_callback,
    )
    return ActiveAuditRecoveryApplicationResult(
        provider_candidates=list(output.provider_candidates),
        static_candidates=list(output.static_candidates),
        rejected_rows=list(output.rejected_rows),
        failures=list(output.failures),
        pages_fetched=int(output.pages_fetched),
        grouped_state=dict(output.grouped),
        recovered_homepages=set(output.recovered_homepages),
    )


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


def run_active_audit_batch(
    *,
    artifact: dict[str, Any],
    batch_rows: list[dict[str, Any]],
    cursor: int,
    batch_number: int,
    strategy: ActiveAuditBatchStrategy,
    completed_identities: set[str],
) -> ActiveAuditBatchResult:
    batch_started = time.perf_counter()
    batch_timing: dict[str, Any] = {
        "batch": int(batch_number),
        "rows": len(batch_rows),
        "cursor": int(cursor),
    }
    progress_base = {
        "batch": int(batch_number),
        "batchRows": len(batch_rows),
        "cursor": int(cursor),
    }

    _emit_progress(
        strategy.progress_callback,
        {**progress_base, "phase": "batch_start", "phaseLabel": "Starting active audit batch"},
        force=True,
    )
    direct_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "direct_inference",
            "phaseLabel": "Checking direct provider URLs",
        },
        force=True,
    )
    prepared = strategy.prepare_rows(batch_rows)
    batch_timing["directInferenceMs"] = _duration_ms(direct_started)

    homepage_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_fetch",
            "phaseLabel": "Fetching studio homepages",
            "phaseTotal": len(prepared.homepage_rows),
        },
        force=True,
    )
    homepage_fetch_results = strategy.fetch_homepages(prepared.homepage_rows)
    batch_timing["homepageFetchMs"] = _duration_ms(homepage_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_fetch",
            "phaseLabel": "Fetched studio homepages",
            "phaseCompleted": len(homepage_fetch_results),
            "phaseTotal": len(prepared.homepage_rows),
        },
        force=True,
    )

    homepage_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "homepage_analysis",
            "phaseLabel": "Analyzing studio homepages",
            "phaseTotal": len(homepage_fetch_results),
        },
        force=True,
    )
    homepage_result = strategy.analyze_homepages(homepage_fetch_results)
    batch_timing["homepageAnalysisMs"] = _duration_ms(homepage_analysis_started)

    recovery_wave1_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_fetch",
            "phaseLabel": "Fetching recovery pages wave 1",
            "phaseTotal": len(homepage_result.primary_recovery_jobs),
        },
        force=True,
    )
    wave1_fetch = strategy.fetch_recovery(
        homepage_result.primary_recovery_jobs,
        "GameDevMap active dry run careers recovery fetch wave 1",
    )
    batch_timing["recoveryWave1FetchMs"] = _duration_ms(recovery_wave1_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_fetch",
            "phaseLabel": "Fetched recovery pages wave 1",
            "phaseCompleted": len(wave1_fetch.results),
            "phaseTotal": len(homepage_result.primary_recovery_jobs),
        },
        force=True,
    )

    recovery_wave1_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave1_analysis",
            "phaseLabel": "Analyzing recovery pages wave 1",
            "phaseTotal": len(wave1_fetch.results),
        },
        force=True,
    )
    wave1_apply = _apply_recovery_with_progress(
        strategy,
        wave1_fetch.results,
        None,
        False,
        "active recovery analysis wave 1",
    )
    batch_timing["recoveryWave1AnalysisMs"] = _duration_ms(recovery_wave1_analysis_started)

    generated_not_found_homepages = (
        directory_recovery_helpers.generated_common_path_not_found_homepages(wave1_fetch.results)
    )
    secondary_jobs_to_fetch = [
        job
        for job in homepage_result.secondary_recovery_jobs
        if strategy.recovery_homepage_key(job) not in wave1_apply.recovered_homepages
        and not (
            strategy.recovery_homepage_key(job) in generated_not_found_homepages
            and directory_recovery_helpers.recovery_job_is_generated_common_path(job)
        )
    ]

    recovery_wave2_fetch_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_fetch",
            "phaseLabel": "Fetching recovery pages wave 2",
            "phaseTotal": len(secondary_jobs_to_fetch),
        },
        force=True,
    )
    wave2_fetch = strategy.fetch_recovery(
        secondary_jobs_to_fetch,
        "GameDevMap active dry run careers recovery fetch wave 2",
    )
    batch_timing["recoveryWave2FetchMs"] = _duration_ms(recovery_wave2_fetch_started)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_fetch",
            "phaseLabel": "Fetched recovery pages wave 2",
            "phaseCompleted": len(wave2_fetch.results),
            "phaseTotal": len(secondary_jobs_to_fetch),
        },
        force=True,
    )

    recovery_wave2_analysis_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "recovery_wave2_analysis",
            "phaseLabel": "Analyzing recovery pages wave 2",
            "phaseTotal": len(wave2_fetch.results),
        },
        force=True,
    )
    wave2_apply = _apply_recovery_with_progress(
        strategy,
        wave2_fetch.results,
        wave1_apply.grouped_state,
        True,
        "active recovery analysis wave 2",
    )
    batch_timing["recoveryWave2AnalysisMs"] = _duration_ms(recovery_wave2_analysis_started)

    recovery_provider_rows = [
        *wave1_apply.provider_candidates,
        *wave2_apply.provider_candidates,
    ]
    recovery_static_rows = [
        *wave1_apply.static_candidates,
        *wave2_apply.static_candidates,
    ]
    recovery_failures = [*wave1_apply.failures, *wave2_apply.failures]
    recovery_pages_fetched = wave1_apply.pages_fetched + wave2_apply.pages_fetched
    recovery_jobs = [*homepage_result.primary_recovery_jobs, *secondary_jobs_to_fetch]
    recovered_homepages = wave1_apply.recovered_homepages | wave2_apply.recovered_homepages

    batch_timing["primaryRecoveryJobs"] = len(homepage_result.primary_recovery_jobs)
    batch_timing["secondaryRecoveryJobs"] = len(secondary_jobs_to_fetch)
    batch_timing["recoveryUniqueJobs"] = wave1_fetch.unique_jobs + wave2_fetch.unique_jobs
    batch_timing["recoveryNetworkJobs"] = wave1_fetch.network_jobs + wave2_fetch.network_jobs
    batch_timing["recoverySkippedByWave1"] = len(homepage_result.secondary_recovery_jobs) - len(
        secondary_jobs_to_fetch
    )
    batch_timing["recoverySkippedByGeneratedNotFound"] = sum(
        1
        for job in homepage_result.secondary_recovery_jobs
        if strategy.recovery_homepage_key(job) in generated_not_found_homepages
        and directory_recovery_helpers.recovery_job_is_generated_common_path(job)
    )
    batch_timing["recoveryRecoveredHomepages"] = len(recovered_homepages)

    merge_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {**progress_base, "phase": "merge_candidates", "phaseLabel": "Merging audit candidates"},
        force=True,
    )
    merged = strategy.merge_candidates(
        prepared.direct_provider_candidates,
        homepage_result.provider_candidates,
        homepage_result.static_candidates,
        recovery_provider_rows,
        recovery_static_rows,
    )
    homepage_failures: list[dict[str, Any]] = []
    for result in homepage_fetch_results:
        failure = result.get("failure")
        if isinstance(failure, dict):
            homepage_failures.append(dict(failure))
    strategy.merge_artifact_updates(
        merged.candidates,
        homepage_result.browser_recovery_candidates,
        homepage_failures,
        recovery_failures,
        [
            *prepared.rejected_rows,
            *homepage_result.rejected_rows,
            *wave2_apply.rejected_rows,
            *merged.rejected_rows,
        ],
    )
    strategy.update_summary(
        {
            "homepageFetchAttempts": len(prepared.homepage_rows),
            "homepagesFetched": int(homepage_result.homepages_fetched),
            "recoveryFetchAttempts": len(recovery_jobs),
            "recoveryUniqueFetchAttempts": int(wave1_fetch.unique_jobs + wave2_fetch.unique_jobs),
            "recoveryNetworkFetchAttempts": int(
                wave1_fetch.network_jobs + wave2_fetch.network_jobs
            ),
            "recoveryPagesFetched": int(recovery_pages_fetched),
        }
    )
    batch_timing["mergeMs"] = _duration_ms(merge_started)

    probe_started = time.perf_counter()
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "probe_candidates",
            "phaseLabel": "Probing audit candidates",
            "phaseTotal": len(merged.candidates),
        },
        force=True,
    )
    probe_results = strategy.probe_candidates(merged.candidates)
    strategy.apply_probe_results(probe_results)
    batch_timing["probeMs"] = _duration_ms(probe_started)

    completed_identities.update(
        strategy.row_identity(row) for row in batch_rows if strategy.row_identity(row)
    )
    progress = _as_dict(artifact.get("progress"))
    progress["batchesCompleted"] = _safe_int(progress.get("batchesCompleted")) + 1
    artifact["progress"] = progress
    batch_timing["totalMs"] = _duration_ms(batch_started)
    batch_timing["artifactWriteMs"] = 0
    strategy.append_timing(batch_timing)
    _emit_progress(
        strategy.progress_callback,
        {
            **progress_base,
            "phase": "batch_complete",
            "phaseLabel": "Completed active audit batch",
            "candidates": len(merged.candidates),
            "failures": len(homepage_failures) + len(recovery_failures),
        },
        force=True,
    )

    return ActiveAuditBatchResult(
        timing=batch_timing,
        candidates=merged.candidates,
        recovery_jobs=recovery_jobs,
        recovered_homepages=recovered_homepages,
    )


def run_active_audit_loop(
    *,
    artifact: dict[str, Any],
    source_rows: list[dict[str, Any]],
    completed_identities: set[str],
    batch_size: int,
    max_batches: int,
    strategy: ActiveAuditLoopStrategy,
) -> ActiveAuditLoopResult:
    batches_run = 0
    effective_batch_size = max(1, int(batch_size or 1))
    effective_max_batches = max(0, int(max_batches or 0))
    _emit_progress(
        strategy.progress_callback,
        {
            "phase": "audit_setup",
            "phaseLabel": "Preparing active audit queue",
            "completed": len(completed_identities),
            "total": len(source_rows),
            "batchSize": effective_batch_size,
        },
        force=True,
    )

    while True:
        batch_rows, cursor = _next_unprocessed_batch(
            source_rows,
            completed_identities,
            effective_batch_size,
            strategy.row_identity,
        )
        progress = _as_dict(artifact.get("progress"))
        progress["cursorPosition"] = int(cursor)
        artifact["progress"] = progress

        if not batch_rows:
            strategy.before_write()
            strategy.write_artifact(True)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "audit_complete",
                    "phaseLabel": "Completed active audit",
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": effective_batch_size,
                },
                force=True,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=True,
            )

        if effective_max_batches and batches_run >= effective_max_batches:
            strategy.before_write()
            strategy.write_artifact(False)
            _emit_progress(
                strategy.progress_callback,
                {
                    "phase": "audit_paused",
                    "phaseLabel": "Paused active audit",
                    "completed": len(completed_identities),
                    "total": len(source_rows),
                    "batchSize": effective_batch_size,
                },
                force=True,
            )
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=False,
            )

        batch_number = batches_run + 1
        strategy.emit_batch_log(batch_number, len(batch_rows), cursor)
        _emit_progress(
            strategy.progress_callback,
            {
                "phase": "batch_queued",
                "phaseLabel": "Queued active audit batch",
                "batch": batch_number,
                "batchRows": len(batch_rows),
                "cursor": int(cursor),
                "completed": len(completed_identities),
                "total": len(source_rows),
                "batchSize": effective_batch_size,
            },
            force=True,
        )
        strategy.run_batch(batch_rows, cursor, batch_number)
        batches_run += 1

        strategy.before_write()
        complete = len(completed_identities) >= len(source_rows)
        strategy.write_artifact(complete)
        _emit_progress(
            strategy.progress_callback,
            {
                "phase": "batch_written",
                "phaseLabel": "Wrote active audit batch",
                "batch": batch_number,
                "batchRows": len(batch_rows),
                "cursor": int(cursor),
                "completed": len(completed_identities),
                "total": len(source_rows),
                "batchSize": effective_batch_size,
            },
            force=True,
        )
        if effective_max_batches and batches_run >= effective_max_batches:
            return ActiveAuditLoopResult(
                batches_run=batches_run,
                completed_identities=set(completed_identities),
                complete=complete,
            )
