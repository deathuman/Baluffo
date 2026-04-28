from __future__ import annotations

"""Shared internal helpers for source-discovery audit artifacts."""

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def duration_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def ensure_timings(artifact: dict[str, Any]) -> dict[str, Any]:
    timings = _as_dict(artifact.get("timings"))
    timings.setdefault("batches", [])
    timings.setdefault("totalsMs", {})
    artifact["timings"] = timings
    return timings


def add_timing_total(artifact: dict[str, Any], key: str, duration_ms_value: int) -> None:
    timings = ensure_timings(artifact)
    totals = _as_dict(timings.get("totalsMs"))
    totals[key] = _safe_int(totals.get(key)) + max(0, int(duration_ms_value or 0))
    timings["totalsMs"] = totals


def append_batch_timing(artifact: dict[str, Any], timing: dict[str, Any]) -> None:
    timings = ensure_timings(artifact)
    batches = _as_list(timings.get("batches"))
    batches.append(dict(timing))
    timings["batches"] = batches
    for key, value in timing.items():
        if key.endswith("Ms"):
            add_timing_total(artifact, key, _safe_int(value))


def failure_error_key(failure: dict[str, Any]) -> str:
    error = str(failure.get("error") or "").strip()
    if not error:
        return "(blank)"
    return error[:160]


def record_failures(
    artifact: dict[str, Any],
    failures: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> None:
    if not failures:
        return
    counts = _as_dict(artifact.get("failureCounts"))
    error_counts = _as_dict(artifact.get("failureErrorCounts"))
    samples = _as_list(artifact.get("failureSamples"))
    for failure in failures:
        if not isinstance(failure, dict):
            continue
        stage = str(failure.get("stage") or "unknown")
        counts[stage] = _safe_int(counts.get(stage)) + 1
        error_key = f"{stage}|{failure_error_key(failure)}"
        error_counts[error_key] = _safe_int(error_counts.get(error_key)) + 1
        if len(samples) < sample_limit:
            samples.append(dict(failure))
    artifact["failureCounts"] = counts
    artifact["failureErrorCounts"] = error_counts
    artifact["failureSamples"] = samples[:sample_limit]
    artifact["failures"] = artifact["failureSamples"]


def failure_count(artifact: dict[str, Any]) -> int:
    counts = _as_dict(artifact.get("failureCounts"))
    if counts:
        return sum(_safe_int(value) for value in counts.values())
    return len(_as_list(artifact.get("failures")))


def stamp_artifact_size(artifact: dict[str, Any]) -> int:
    runtime = _as_dict(artifact.get("runtime"))
    size_bytes = len(json.dumps(artifact, ensure_ascii=False).encode("utf-8"))
    runtime["artifactSizeBytes"] = size_bytes
    artifact["runtime"] = runtime
    summary = _as_dict(artifact.get("summary"))
    summary["artifactSizeBytes"] = size_bytes
    artifact["summary"] = summary
    return size_bytes


def save_artifact_atomic(artifact: dict[str, Any], output_path: Path) -> None:
    stamp_artifact_size(artifact)
    source_registry_module.save_json_atomic(output_path, artifact)


def parse_artifact_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def artifact_signature_matches(
    artifact: dict[str, Any],
    *,
    schema_version: int,
    expected_signature: Any,
) -> bool:
    if int(artifact.get("schemaVersion") or 0) != int(schema_version):
        return False
    return bool(_as_dict(artifact.get("runtime")).get("configSignature") == expected_signature)


def artifact_is_fresh(
    artifact: dict[str, Any],
    *,
    schema_version: int,
    expected_signature: Any,
    ttl_minutes: int,
) -> bool:
    if int(artifact.get("schemaVersion") or 0) != int(schema_version):
        return False
    if not bool(_as_dict(artifact.get("progress")).get("complete")):
        return False
    if not artifact_signature_matches(
        artifact,
        schema_version=schema_version,
        expected_signature=expected_signature,
    ):
        return False
    if ttl_minutes <= 0:
        return False
    updated_at = parse_artifact_time(artifact.get("updatedAt") or artifact.get("finishedAt"))
    return bool(updated_at and datetime.now(UTC) - updated_at <= timedelta(minutes=ttl_minutes))
