from __future__ import annotations

"""Shared opt-in audit runner for directory-style source discovery adapters."""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import audit_ledger, audit_report_summary, candidate_collections

DirectoryAuditRows = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]
DirectoryAuditScan = Callable[[int], dict[str, Any]]
_LATEST_DIRECTORY_AUDIT_SUMMARIES: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class DirectoryAuditRunSpec:
    adapter: str
    schema_version: int
    output_path: Path
    ttl_minutes: int
    signature: Any
    timeout_s: int
    scan: DirectoryAuditScan
    runtime: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    sample_limit: int = 100
    emit_log: Callable[[str], None] | None = None


def load_directory_audit_artifact(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def directory_audit_rows(artifact: dict[str, Any]) -> DirectoryAuditRows:
    provider_rows, static_rows = candidate_collections.provider_static_rows_from_payload(artifact)
    failures = artifact.get("failures")
    if not isinstance(failures, list):
        failures = []
    return provider_rows, static_rows, list(failures)


def directory_scan_result_rows(scan_result: dict[str, Any]) -> DirectoryAuditRows:
    return (
        list(scan_result.get("providerCandidates") or []),
        list(scan_result.get("staticCandidates") or []),
        list(scan_result.get("failures") or []),
    )


def directory_audit_rows_for_method(
    artifact: dict[str, Any],
    discovery_method: str,
) -> DirectoryAuditRows:
    provider_rows, static_rows, failures = directory_audit_rows(artifact)
    method = str(discovery_method)
    return (
        [row for row in provider_rows if str(row.get("discoveryMethod") or "") == method],
        [row for row in static_rows if str(row.get("discoveryMethod") or "") == method],
        [row for row in failures if str(row.get("adapter") or "") == method],
    )


def directory_audit_report_summary(
    artifact: dict[str, Any],
    *,
    adapter: str,
    cache_hit: bool,
    output_path: Path | None = None,
) -> dict[str, Any]:
    summary = audit_report_summary.as_dict(artifact.get("summary"))
    runtime = audit_report_summary.as_dict(artifact.get("runtime"))
    progress = audit_report_summary.as_dict(artifact.get("progress"))
    timings = audit_report_summary.as_dict(artifact.get("timings"))
    timing_totals = {
        str(key): audit_report_summary.safe_int(value)
        for key, value in audit_report_summary.as_dict(timings.get("totalsMs")).items()
    }
    report_summary: dict[str, Any] = {
        "adapter": str(adapter),
        "cacheHit": bool(cache_hit),
        "complete": bool(progress.get("complete")),
        "auditDurationMs": audit_report_summary.safe_int(timing_totals.get("totalMs")),
        "providerCandidates": audit_report_summary.safe_int(summary.get("providerCandidates")),
        "staticCandidates": audit_report_summary.safe_int(summary.get("staticCandidates")),
        "failures": audit_report_summary.safe_int(summary.get("failures")),
        "artifactSizeBytes": audit_report_summary.artifact_size_bytes(
            summary=summary,
            runtime=runtime,
        ),
        "timingTotalsMs": timing_totals,
        "topFailureBuckets": audit_report_summary.top_failure_buckets(
            rejected_reason_detail_counts=None,
            failure_counts=artifact.get("failureCounts"),
        ),
    }
    if output_path is not None:
        report_summary["outputPath"] = str(output_path)
    for key, value in summary.items():
        if key in report_summary:
            continue
        if isinstance(value, bool):
            report_summary[str(key)] = bool(value)
        elif isinstance(value, int | float | str) or value is None:
            report_summary[str(key)] = audit_report_summary.safe_int(value)
        elif isinstance(value, dict):
            report_summary[str(key)] = dict(value)
        elif isinstance(value, list):
            report_summary[str(key)] = list(value)
        else:
            report_summary[str(key)] = value
    return report_summary


def latest_directory_audit_summaries() -> dict[str, dict[str, Any]]:
    return {
        adapter: dict(summary) for adapter, summary in _LATEST_DIRECTORY_AUDIT_SUMMARIES.items()
    }


def clear_directory_audit_summaries() -> None:
    _LATEST_DIRECTORY_AUDIT_SUMMARIES.clear()


def initial_directory_audit_artifact(
    *,
    adapter: str,
    schema_version: int,
    timeout_s: int,
    signature: Any,
    runtime: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "schemaVersion": int(schema_version),
        "adapter": str(adapter),
        "startedAt": now,
        "updatedAt": now,
        "runtime": {
            "configSignature": signature,
            "timeoutSeconds": int(timeout_s),
            **dict(runtime or {}),
        },
        "progress": {"complete": False, "cursor": 0, "completedUrlIdentities": []},
        "summary": {
            **dict(summary or {}),
            "providerCandidates": 0,
            "staticCandidates": 0,
            "failures": 0,
        },
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": [],
        "failureCounts": {},
        "failureErrorCounts": {},
        "failureSamples": [],
        "timings": {"batches": [], "totalsMs": {}},
    }


def run_directory_audit(
    *,
    adapter: str,
    schema_version: int,
    output_path: Path,
    ttl_minutes: int,
    signature: Any,
    timeout_s: int,
    scan: DirectoryAuditScan,
    runtime: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    sample_limit: int = 100,
    emit_log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], bool]:
    existing = load_directory_audit_artifact(output_path)
    if existing is not None and audit_ledger.artifact_is_fresh(
        existing,
        schema_version=schema_version,
        expected_signature=signature,
        ttl_minutes=ttl_minutes,
    ):
        if emit_log is not None:
            emit_log(f"{adapter} directory audit cache hit: {output_path}.")
        _LATEST_DIRECTORY_AUDIT_SUMMARIES[str(adapter)] = directory_audit_report_summary(
            existing,
            adapter=adapter,
            cache_hit=True,
            output_path=output_path,
        )
        return existing, True

    artifact = initial_directory_audit_artifact(
        adapter=adapter,
        schema_version=schema_version,
        timeout_s=timeout_s,
        signature=signature,
        runtime=runtime,
        summary=summary,
    )
    scan_started = time.perf_counter()
    scan_result = scan(timeout_s)
    provider_candidates, static_candidates, failures = directory_scan_result_rows(scan_result)
    artifact["providerCandidates"] = provider_candidates
    artifact["staticCandidates"] = static_candidates
    for key in ("browserRecoveryCandidates",):
        if key in scan_result:
            artifact[key] = list(scan_result.get(key) or [])
    audit_ledger.record_failures(artifact, failures, sample_limit=sample_limit)

    artifact_summary = dict(artifact.get("summary") or {})
    artifact_summary.update(dict(scan_result.get("summary") or {}))
    artifact_summary["providerCandidates"] = len(artifact["providerCandidates"])
    artifact_summary["staticCandidates"] = len(artifact["staticCandidates"])
    artifact_summary["failures"] = audit_ledger.failure_count(artifact)
    artifact["summary"] = artifact_summary

    batch_timing = dict(scan_result.get("batchTiming") or {})
    batch_timing["totalMs"] = audit_ledger.duration_ms(scan_started)
    audit_ledger.append_batch_timing(artifact, batch_timing)

    progress = dict(scan_result.get("progress") or {})
    progress.setdefault("complete", True)
    progress.setdefault("cursor", int(artifact_summary.get("eligibleRows") or 0))
    progress.setdefault("completedUrlIdentities", [])
    artifact["progress"] = progress

    artifact["finishedAt"] = datetime.now(UTC).isoformat()
    artifact["updatedAt"] = artifact["finishedAt"]
    audit_ledger.save_artifact_atomic(artifact, output_path)
    _LATEST_DIRECTORY_AUDIT_SUMMARIES[str(adapter)] = directory_audit_report_summary(
        artifact,
        adapter=adapter,
        cache_hit=False,
        output_path=output_path,
    )
    return artifact, False


def run_directory_audit_spec(spec: DirectoryAuditRunSpec) -> tuple[dict[str, Any], bool]:
    return run_directory_audit(
        adapter=spec.adapter,
        schema_version=spec.schema_version,
        output_path=spec.output_path,
        ttl_minutes=spec.ttl_minutes,
        signature=spec.signature,
        timeout_s=spec.timeout_s,
        scan=spec.scan,
        runtime=spec.runtime,
        summary=spec.summary,
        sample_limit=spec.sample_limit,
        emit_log=spec.emit_log,
    )
