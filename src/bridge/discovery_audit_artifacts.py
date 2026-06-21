"""Discovery audit artifact payload helpers.

AI boundary owns: admin-facing discovery audit artifact summaries and bounded artifact reads.
AI boundary implement in: this file for artifact payloads; discovery execution stays in source_discovery and DiscoveryService.
AI boundary search before contracts: ops diagnostics routes, discovery service, and source-discovery artifact tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery artifact tests.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MAX_SUMMARY_BYTES = 5 * 1024 * 1024
MAX_TOP_LEVEL_KEYS = 40
MAX_NESTED_SUMMARY_KEYS = 20

SCALAR_TYPES = (str, int, float, bool)
COUNTED_ARRAY_KEYS = (
    "activeSources",
    "browserRecoveryCandidates",
    "candidates",
    "failures",
    "pages",
    "providerCandidates",
    "recoveredProviderCandidates",
    "recoveredStaticCandidates",
    "rows",
    "staticCandidates",
    "topFailures",
)
SCALAR_SUMMARY_KEYS = (
    "adapter",
    "auditDurationMs",
    "cacheHit",
    "completed",
    "candidateCount",
    "failureCount",
    "generatedCandidateCount",
    "providerCandidateCount",
    "recoveredProviderCandidates",
    "recoveredStaticCandidates",
    "recoveryFailures",
    "staticCandidateCount",
    "status",
    "version",
)


@dataclass(frozen=True)
class DiscoveryAuditArtifactSpec:
    name: str
    filename: str


DISCOVERY_AUDIT_ARTIFACTS: tuple[DiscoveryAuditArtifactSpec, ...] = (
    DiscoveryAuditArtifactSpec("sheet-directory", "sheet-directory-discovery-audit.json"),
    DiscoveryAuditArtifactSpec("web-search", "web-search-discovery-audit.json"),
    DiscoveryAuditArtifactSpec("gamedevmap", "gamedevmap-active-source-dry-run.json"),
    DiscoveryAuditArtifactSpec("gameprog", "gameprog-discovery-audit.json"),
    DiscoveryAuditArtifactSpec("gamesmap", "gamesmap-discovery-audit.json"),
)


def _active_data_dir(api: Any) -> Path:
    runtime_data_dir = getattr(getattr(api, "runtime_config", None), "data_dir", None)
    if runtime_data_dir:
        return Path(runtime_data_dir)
    for attr in ("DISCOVERY_REPORT_PATH", "JOBS_FETCH_REPORT_PATH", "DISCOVERY_LOG_PATH"):
        path = getattr(api, attr, None)
        if path:
            return Path(path).parent
    return Path("data")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def _safe_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, SCALAR_TYPES):
        return value
    return None


def _compact_scalar_dict(value: Any, *, max_items: int = MAX_NESTED_SUMMARY_KEYS) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for key in sorted(value):
        scalar = _safe_scalar(value.get(key))
        if scalar is None:
            continue
        result[str(key)] = scalar
        if len(result) >= max_items:
            break
    return result


def _bounded_json_summary(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        summary: dict[str, Any] = {
            "jsonType": "object",
            "topLevelKeyCount": len(value),
        }
        for key in SCALAR_SUMMARY_KEYS:
            scalar = _safe_scalar(value.get(key))
            if scalar is not None:
                summary[key] = scalar
        for key in COUNTED_ARRAY_KEYS:
            rows = value.get(key)
            if isinstance(rows, list):
                summary[f"{key}Count"] = len(rows)
        for key in ("summary", "metadata", "runtime", "stats"):
            nested = _compact_scalar_dict(value.get(key))
            if nested:
                summary[key] = nested
        return summary
    if isinstance(value, list):
        return {"jsonType": "array", "itemCount": len(value)}
    scalar = _safe_scalar(value)
    return (
        {"jsonType": type(value).__name__, "value": scalar}
        if scalar is not None
        else {"jsonType": type(value).__name__}
    )


def _summarize_json_artifact(
    path: Path, size_bytes: int, warnings: list[str]
) -> tuple[list[str], dict[str, Any]]:
    if size_bytes > MAX_SUMMARY_BYTES:
        warnings.append("summary_skipped_large_file")
        return [], {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        warnings.append("invalid_json")
        return [], {}
    except OSError as exc:
        warnings.append(f"read_failed:{type(exc).__name__}")
        return [], {}
    if isinstance(payload, dict):
        top_level_keys = sorted(str(key) for key in payload)[:MAX_TOP_LEVEL_KEYS]
    else:
        top_level_keys = []
    return top_level_keys, _bounded_json_summary(payload)


def _artifact_path(data_dir: Path, filename: str) -> Path:
    base = Path(data_dir).resolve()
    candidate = (base / filename).resolve()
    candidate.relative_to(base)
    return candidate


def get_discovery_audit_artifacts_payload(api: Any) -> dict[str, Any]:
    data_dir = _active_data_dir(api)
    artifacts: list[dict[str, Any]] = []
    for spec in DISCOVERY_AUDIT_ARTIFACTS:
        warnings: list[str] = []
        path = _artifact_path(data_dir, spec.filename)
        row: dict[str, Any] = {
            "name": spec.name,
            "exists": path.is_file(),
            "relativePath": spec.filename,
            "pathDisplay": f"/data/{spec.filename}"
            if str(data_dir) == "/data"
            else f"data/{spec.filename}",
            "sizeBytes": 0,
            "modifiedAt": "",
            "sha256": "",
            "topLevelKeys": [],
            "summary": {},
            "warnings": warnings,
        }
        if not row["exists"]:
            warnings.append("missing")
            artifacts.append(row)
            continue
        try:
            stat = path.stat()
            row["sizeBytes"] = int(stat.st_size)
            row["modifiedAt"] = _iso_from_timestamp(stat.st_mtime)
            row["sha256"] = _sha256(path)
            top_level_keys, summary = _summarize_json_artifact(path, int(stat.st_size), warnings)
            row["topLevelKeys"] = top_level_keys
            row["summary"] = summary
        except (OSError, ValueError) as exc:
            warnings.append(f"stat_failed:{type(exc).__name__}")
        artifacts.append(row)
    return {"ok": True, "artifacts": artifacts}


__all__ = [
    "DISCOVERY_AUDIT_ARTIFACTS",
    "get_discovery_audit_artifacts_payload",
]
