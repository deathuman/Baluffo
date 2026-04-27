from __future__ import annotations

"""Shared opt-in audit runner for directory-style source discovery adapters."""

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

from . import audit_ledger

DirectoryAuditRows = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]
DirectoryAuditScan = Callable[[int], dict[str, Any]]


def load_directory_audit_artifact(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def directory_audit_rows(artifact: dict[str, Any]) -> DirectoryAuditRows:
    provider_rows = artifact.get("providerCandidates")
    static_rows = artifact.get("staticCandidates")
    failures = artifact.get("failures")
    if not isinstance(provider_rows, list):
        provider_rows = []
    if not isinstance(static_rows, list):
        static_rows = []
    if not isinstance(failures, list):
        failures = []
    return unique_sources(provider_rows), unique_sources(static_rows), list(failures)


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
    artifact["providerCandidates"] = list(scan_result.get("providerCandidates") or [])
    artifact["staticCandidates"] = list(scan_result.get("staticCandidates") or [])
    failures = list(scan_result.get("failures") or [])
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
    return artifact, False
