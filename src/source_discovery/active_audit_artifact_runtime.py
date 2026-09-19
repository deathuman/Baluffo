"""Active audit artifact lifecycle, caching, and merge helpers.

AI boundary owns: active-audit artifact creation/load/finalize, cache resolution, row merging, and summary increments.
AI boundary implement in: this file for artifact persistence and merge semantics; batch execution stays in active_audit_batch_runtime.
AI boundary search before contracts: artifact schema keys, audit ledger helpers, and active audit artifact tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from src.shared.json_shapes import (
    as_json_list as _as_list,
)
from src.shared.json_shapes import (
    as_json_object as _as_dict,
)
from src.shared.utils import int_or_default as _safe_int
from src.shared.utils import now_iso

from . import audit_ledger
from .active_audit_candidate_rows import merge_rows_by_identity as merge_rows_by_identity
from .active_audit_candidate_rows import merge_unique_candidate_rows as merge_unique_candidate_rows


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


def append_batch_timing(artifact: dict[str, Any], timing: dict[str, Any]) -> None:
    audit_ledger.append_batch_timing(artifact, timing)


def record_failure_rows(
    artifact: dict[str, Any],
    failures: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> None:
    audit_ledger.record_failures(artifact, failures, sample_limit=sample_limit)
