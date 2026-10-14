"""Discovery audit artifact payload helpers.

AI boundary owns: admin-facing discovery audit artifact summaries and bounded artifact reads.
AI boundary implement in: this file for artifact payloads; discovery execution stays in source_discovery and DiscoveryService.
AI boundary search before contracts: ops diagnostics routes, discovery service, and source-discovery artifact tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery artifact tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.shared.hashing import sha256_file

MAX_SUMMARY_BYTES = 5 * 1024 * 1024
MAX_TOP_LEVEL_KEYS = 40
MAX_NESTED_SUMMARY_KEYS = 20
# The probe-wave receipt is published as its own small file precisely so a poll can read it. The
# bound is generous for that job and refuses anything that is not the receipt.
MAX_PROBE_RECEIPT_BYTES = 64 * 1024
PROBE_LIFECYCLE_RECEIPT_FILENAME = "gamedevmap-audit-receipt.json"
# The page-fetch lifecycle receipt is published beside each directory/web-search artifact for the
# same reason, and carries the same 256 KiB envelope bound the producer enforces.
MAX_PAGE_FETCH_SIDECAR_BYTES = 262_144
PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME = "page-fetch-lifecycle.v1.json"
# Above this size a full-file digest stops being a reasonable thing to do on every poll, so the row
# identity becomes the cheap stat tuple instead. The threshold matches MAX_SUMMARY_BYTES: it is the
# point past which the row's own summary is skipped anyway, so a digest there buys a caller nothing
# the stat identity does not already give it.
MAX_FINGERPRINT_HASH_BYTES = MAX_SUMMARY_BYTES
FINGERPRINT_KIND_STAT = "stat"
FINGERPRINT_KIND_SHA256 = "sha256"
# The published counters, in the order the audit's rollup defines them (see
# `gamedevmap_artifact_store._probe_lifecycle_rollup`). Anything else the file carries stays out.
PROBE_LIFECYCLE_KEYS = (
    "batches",
    "fetchesStarted",
    "fetchesBoundedOut",
    "fetchesOutstandingAtExit",
    "maxFetchesOutstandingAtExit",
    "clientFetchesOutstandingAtExit",
    "clientsRetiredByBatch",
    "clientsRetiredByLastFetch",
    "clientsUnclosed",
    "maxClientCloseDeferredMs",
    "poolThreadsAliveNow",
    "deadlineExits",
)
# The per-source receipt fields the Ops view renders, and nothing else the envelope might carry.
PAGE_FETCH_LIFECYCLE_SOURCE_KEYS = (
    "source",
    "runId",
    "artifactName",
    "state",
    "coverage",
    "truncated",
    "omittedCount",
    "receiptCount",
)
# The rollup counters of one source's page-fetch batches, in the producer's own field order.
PAGE_FETCH_LIFECYCLE_ROLLUP_KEYS = (
    "batchCount",
    "submitted",
    "started",
    "finished",
    "failed",
    "cancelledBeforeStart",
    "queuedNow",
    "outstandingNow",
    "asyncCancellationRequested",
    "asyncCleanupFinished",
    "pendingAsyncCleanup",
    "state",
)

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
    DiscoveryAuditArtifactSpec("gamedevmap-receipt", PROBE_LIFECYCLE_RECEIPT_FILENAME),
    DiscoveryAuditArtifactSpec("gameprog", "gameprog-discovery-audit.json"),
    DiscoveryAuditArtifactSpec("gamesmap", "gamesmap-discovery-audit.json"),
    # One row for the whole envelope: it is one file carrying one entry per source, so a row per
    # source would triple the table and break the present/total accounting the header renders.
    DiscoveryAuditArtifactSpec("page-fetch-lifecycle", PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME),
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
    return sha256_file(path)


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


def _probe_lifecycle_counters(rollup: Any) -> dict[str, Any]:
    """The receipt's published counters, and nothing else the file might carry."""
    if not isinstance(rollup, dict):
        return {}
    counters: dict[str, Any] = {}
    for key in PROBE_LIFECYCLE_KEYS:
        if key not in rollup:
            continue
        value = _safe_scalar(rollup.get(key))
        if value is not None:
            counters[key] = value
    return counters


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
) -> tuple[list[str], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if size_bytes > MAX_SUMMARY_BYTES:
        warnings.append("summary_skipped_large_file")
        return [], {}, {}, {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        warnings.append("invalid_json")
        return [], {}, {}, {}
    except OSError as exc:
        warnings.append(f"read_failed:{type(exc).__name__}")
        return [], {}, {}, {}
    if isinstance(payload, dict):
        top_level_keys = sorted(str(key) for key in payload)[:MAX_TOP_LEVEL_KEYS]
    else:
        top_level_keys = []
    probe_lifecycle = _probe_lifecycle_payload(payload, warnings)
    # A large artifact is not parsed at all above, so its page-fetch receipts are only ever read
    # from the published sidecar; this projection covers the case where a small artifact carries an
    # inline `pageFetchLifecycle` list and no sidecar has been written for it yet.
    page_fetch_lifecycle = _page_fetch_lifecycle_payload(payload, warnings)
    return top_level_keys, _bounded_json_summary(payload), probe_lifecycle, page_fetch_lifecycle


def _probe_lifecycle_payload(payload: Any, warnings: list[str]) -> dict[str, Any]:
    """A receipt carried by an artifact: `summary.probeLifecycle`, or the receipt file's own shape."""
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary")
    rollup = summary.get("probeLifecycle") if isinstance(summary, dict) else None
    if rollup is None:
        rollup = payload.get("probeLifecycle")
    if rollup is None:
        return {}
    counters = _probe_lifecycle_counters(rollup)
    if not counters:
        warnings.append("probe_lifecycle_unreadable")
    return counters


def load_probe_lifecycle_receipt(data_dir: Path) -> dict[str, Any]:
    """The published probe-wave receipt for the active data dir, or `{}` when none exists.

    A stat-then-read of a file measured in hundreds of bytes, because the caller is the Ops health
    endpoint and the artifact this summarizes is tens of MB. `fetchesBoundedOut` counts the probe
    fetches that outlived their own per-call bound (the abandoned ones); `clientsUnclosed` counts the
    waves whose shared client was never closed. Never raises: an unreadable or oversized receipt
    reads as absent.
    """
    try:
        path = _artifact_path(data_dir, PROBE_LIFECYCLE_RECEIPT_FILENAME)
        stat = path.stat()
        if not path.is_file() or int(stat.st_size) > MAX_PROBE_RECEIPT_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    counters = _probe_lifecycle_payload(payload, [])
    if not counters:
        return {}
    return {
        "runId": str(payload.get("runId") or "") if isinstance(payload, dict) else "",
        "finishedAt": str(payload.get("finishedAt") or "") if isinstance(payload, dict) else "",
        "modifiedAt": _iso_from_timestamp(stat.st_mtime),
        **counters,
    }


def _artifact_path(data_dir: Path, filename: str) -> Path:
    base = Path(data_dir).resolve()
    candidate = (base / filename).resolve()
    candidate.relative_to(base)
    return candidate


def _page_fetch_lifecycle_source_entry(entry: Any) -> dict[str, Any]:
    """One source's published receipt fields and rollup, and nothing else the envelope carries."""
    if not isinstance(entry, dict):
        return {}
    projected: dict[str, Any] = {}
    for key in PAGE_FETCH_LIFECYCLE_SOURCE_KEYS:
        if key not in entry:
            continue
        value = _safe_scalar(entry.get(key))
        if value is not None:
            projected[key] = value
    rollup = entry.get("rollup")
    if isinstance(rollup, dict):
        counters: dict[str, Any] = {}
        for key in PAGE_FETCH_LIFECYCLE_ROLLUP_KEYS:
            if key not in rollup:
                continue
            value = _safe_scalar(rollup.get(key))
            if value is not None:
                counters[key] = value
        if counters:
            projected["rollup"] = counters
    return projected


def _page_fetch_lifecycle_payload(payload: Any, warnings: list[str]) -> dict[str, Any]:
    """The per-source page-fetch receipts an envelope carries, keyed by source.

    Every source the envelope published is projected, including sources whose last run was
    truncated: `coverage`, `truncated` and `omittedCount` travel with each entry so a reader can
    tell a complete reading from a bounded one. A sidecar that carries sources but no readable
    entry is a warning, not silence, because the difference between "nothing published" and "this
    was published but could not be read" is exactly what the Ops view has to render distinctly.
    """
    if not isinstance(payload, dict):
        return {}
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        if "receiptType" in payload or "runId" in payload:
            warnings.append("page_fetch_lifecycle_unreadable")
        return {}
    projected: dict[str, Any] = {}
    for source, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        fields = _page_fetch_lifecycle_source_entry(entry)
        if fields:
            projected[str(source)] = fields
    if sources and not projected:
        warnings.append("page_fetch_lifecycle_unreadable")
    envelope: dict[str, Any] = {}
    for key in (
        "schemaVersion",
        "runId",
        "generatedAt",
        "artifactName",
        "coverage",
        "truncated",
        "omittedCount",
    ):
        if key not in payload:
            continue
        value = _safe_scalar(payload.get(key))
        if value is not None:
            envelope[key] = value
    if projected:
        envelope["sources"] = projected
    return envelope


def load_page_fetch_lifecycle_receipt(data_dir: Path) -> dict[str, Any]:
    """The published page-fetch receipt envelope for the active data dir, or `{}` when none exists.

    A stat-then-read of a file bounded at the producer's own 256 KiB envelope cap, because the
    caller is the Ops health endpoint and the artifacts this summarizes are tens of MB. Never
    raises: an unreadable, malformed or oversized receipt reads as absent, so an absent sidecar is
    never mistaken for a clean one.
    """
    try:
        path = _artifact_path(data_dir, PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME)
        stat = path.stat()
        if not path.is_file() or int(stat.st_size) > MAX_PAGE_FETCH_SIDECAR_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    envelope = _page_fetch_lifecycle_payload(payload, [])
    if not envelope.get("sources"):
        return {}
    return {
        "runId": str(payload.get("runId") or "") if isinstance(payload, dict) else "",
        "modifiedAt": _iso_from_timestamp(stat.st_mtime),
        **envelope,
    }


def _artifact_fingerprint(path: Path, size_bytes: int) -> tuple[str, str]:
    """Cache a large artifact's identity as `(size, mtime_ns)` instead of hashing it every poll.

    `get_discovery_audit_artifacts_payload` used to stream a SHA-256 over every existing row on
    every poll — including artifacts past `MAX_SUMMARY_BYTES`, whose summary is skipped anyway, so
    the digest bought a caller nothing the cheap identity does not already give it. Above the
    threshold the identity is the stat tuple and `sha256` is left empty for the caller to request
    on demand; at or below it the digest is still computed, because a small artifact's content hash
    is a cheap and genuinely useful identity.

    The returned fingerprint string is `"<size>:<mtime_ns>"` for the stat kind, which changes
    whenever either component changes, so a caller can key a digest cache on it and never reuse a
    digest for different bytes.
    """
    if size_bytes > MAX_FINGERPRINT_HASH_BYTES:
        return FINGERPRINT_KIND_STAT, f"{int(size_bytes)}:{int(path.stat().st_mtime_ns)}"
    return FINGERPRINT_KIND_SHA256, _sha256(path)


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
            "fingerprintKind": "",
            "fingerprint": "",
            "topLevelKeys": [],
            "summary": {},
            "probeLifecycle": {},
            "pageFetchLifecycle": {},
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
            fingerprint_kind, fingerprint = _artifact_fingerprint(path, int(stat.st_size))
            row["fingerprintKind"] = fingerprint_kind
            row["fingerprint"] = fingerprint
            # Above the threshold the digest is deliberately not computed on a poll: an empty
            # `sha256` with `fingerprintKind: "stat"` is the signal to request it on demand.
            row["sha256"] = fingerprint if fingerprint_kind == FINGERPRINT_KIND_SHA256 else ""
            top_level_keys, summary, probe_lifecycle, page_fetch_lifecycle = (
                _summarize_json_artifact(path, int(stat.st_size), warnings)
            )
            row["topLevelKeys"] = top_level_keys
            row["summary"] = summary
            row["probeLifecycle"] = probe_lifecycle
            row["pageFetchLifecycle"] = page_fetch_lifecycle
        except (OSError, ValueError) as exc:
            warnings.append(f"stat_failed:{type(exc).__name__}")
        artifacts.append(row)
    return {"ok": True, "artifacts": artifacts}


__all__ = [
    "DISCOVERY_AUDIT_ARTIFACTS",
    "FINGERPRINT_KIND_SHA256",
    "FINGERPRINT_KIND_STAT",
    "PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME",
    "PROBE_LIFECYCLE_KEYS",
    "get_discovery_audit_artifacts_payload",
    "load_page_fetch_lifecycle_receipt",
    "load_probe_lifecycle_receipt",
]
