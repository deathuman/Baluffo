"""Bounded probe-failure memory for source discovery probes.

AI boundary owns: probe-failure classification, per-identity consecutive-failure
tracking, and the bounded quarantine window that stops re-probing certain
certainties every cycle.
AI boundary implement in: this leaf; probe orchestration stays in
orchestrator_probe.py and candidate gating stays in orchestrator_generation.py.
AI boundary search before contracts: probe orchestration, candidate gating,
config thresholds, and probe failure tests.
AI boundary verify: ``npm run lint:repo-guardrails`` plus
``tests/source_discovery/test_probe_failure_memory.py``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso

from . import config as discovery_config_module

PROBE_FAILURE_MEMORY_FILENAME = "source-discovery-probe-failures.json"

_HTTP_4XX_ERROR_RE = re.compile(r"client error '4\d\d|http error 4\d\d", re.IGNORECASE)
_DNS_ERROR_RE = re.compile(
    r"name or service not known|no address associated|errno -[235]", re.IGNORECASE
)
_SSL_ERROR_RE = re.compile(r"certificate verify failed|sslv3[-_ ]?alert|ssl:", re.IGNORECASE)

# Only deterministic failure classes (DNS/SSL) may quarantine a candidate.
# Timeouts, 5xx, and resets are transient by nature and keep probing every run.
_QUARANTINE_CLASSES = frozenset({"dns", "ssl"})

_STORE_MAX_ENTRIES = 5000


def classify_probe_failure_class(error: str) -> str:
    """Classify a probe error into a small stable class token.

    ``probe_miss`` and ``probe`` stage tokens from
    :func:`src.source_discovery.core_scoring.classify_probe_failure_stage`
    already distinguish URL-shape misses; this classifier adds the
    transport-level classes used for quarantine decisions.
    """
    text = str(error or "")
    if _DNS_ERROR_RE.search(text):
        return "dns"
    if _SSL_ERROR_RE.search(text):
        return "ssl"
    if _HTTP_4XX_ERROR_RE.search(text[:120]):
        return "4xx"
    return "other"


def is_quarantine_class(failure_class: str) -> bool:
    """Whether a failure class is deterministic enough to quarantine a candidate."""
    return str(failure_class or "").strip().lower() in _QUARANTINE_CLASSES


def quarantine_threshold() -> int:
    """Consecutive same-class failures before a quarantine starts (config-backed)."""
    try:
        return max(1, int(discovery_config_module.PROBE_FAILURE_QUARANTINE_THRESHOLD))
    except (AttributeError, TypeError, ValueError):
        return 3


def memory_retention_days() -> int:
    """Quarantine/record retention window in days (config-backed)."""
    try:
        return max(1, int(discovery_config_module.PROBE_FAILURE_MEMORY_RETENTION_DAYS))
    except (AttributeError, TypeError, ValueError):
        return 45


def memory_filename() -> str:
    """Store file name inside the data dir (config-backed)."""
    return str(
        getattr(
            discovery_config_module,
            "PROBE_FAILURE_MEMORY_FILENAME",
            PROBE_FAILURE_MEMORY_FILENAME,
        )
    )


def store_path(data_dir: Path) -> Path:
    """Store location for a data dir (resolves config-backed file name)."""
    return Path(data_dir) / memory_filename()


def _parse_iso_ts(value: Any) -> float:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _parse_retention_days(raw: Any) -> int:
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return memory_retention_days()


def _retention_cutoff_s(retention_days: int) -> float:
    return datetime.now(UTC).timestamp() - max(1, int(retention_days)) * 86400.0


def _quarantine_expired(record: dict[str, Any], *, cutoff_s: float) -> bool:
    last_failure_s = _parse_iso_ts(record.get("lastFailureAt"))
    return last_failure_s > 0 and last_failure_s < cutoff_s


def prune_store(
    store: dict[str, Any],
    *,
    retention_days: int | None = None,
) -> dict[str, dict[str, Any]]:
    """Prune stale/malformed records from a raw loaded store, bounded.

    Args:
        store: raw loaded store dict keyed by identity.
        retention_days: optional retention window override (tests).

    Returns:
        A pruned copy of the store keyed by identity.
    """
    days = _parse_retention_days(retention_days)
    cutoff_s = _retention_cutoff_s(days)
    kept: dict[str, dict[str, Any]] = {}
    for identity, record in (store or {}).items():
        if not isinstance(record, dict):
            continue
        if not str(record.get("failureClass") or ""):
            continue
        if _quarantine_expired(record, cutoff_s=cutoff_s):
            continue
        kept[str(identity)] = record
    return kept


def load_store(path: Path, *, retention_days: int | None = None) -> dict[str, dict[str, Any]]:
    """Load the bounded failure-memory store from disk, pruning stale records."""
    from src.source_registry import load_json_object

    raw = load_json_object(Path(path), default={})
    if not isinstance(raw, dict):
        return {}
    return prune_store(raw, retention_days=retention_days)


def _quarantine_active(record: dict[str, Any], *, at: str) -> bool:
    until = str(record.get("quarantinedUntil") or "")
    return bool(until) and _parse_iso_ts(until) >= _parse_iso_ts(at)


class ProbeFailureMemory:
    """Run-scoped probe-failure memory with a single flush at finalize time.

    The store loads once when the run starts and persists once when the run
    finalizes; probe-time recording stays in-memory to avoid per-candidate disk
    writes on slow storage.
    """

    def __init__(self, path: Path, *, retention_days: int | None = None) -> None:
        self.path = Path(path)
        self._retention_days = _parse_retention_days(retention_days)
        self._records: dict[str, dict[str, Any]] = load_store(
            self.path, retention_days=self._retention_days
        )

    def quarantine_index(self) -> dict[str, dict[str, Any]]:
        """Active quarantine records at load time, for candidate gating."""
        now = now_iso()
        return {
            str(identity): record
            for identity, record in self._records.items()
            if is_quarantine_class(str(record.get("failureClass") or ""))
            and _quarantine_active(record, at=now)
        }

    def record_failure(
        self, *, identity: str, error: str, at: str | None = None
    ) -> dict[str, Any] | None:
        """Record one probe failure; returns the record when a quarantine starts.

        The consecutive counter resets on success or class change, so a
        candidate with a *different* problem is always probed again. When the
        count reaches ``quarantine_threshold()`` for a quarantine class (DNS or
        SSL), the record is stamped ``quarantinedUntil`` (last failure plus the
        retention window) and returned so the caller can log it.
        """
        identity = str(identity or "").strip()
        if not identity:
            return None
        failure_class = classify_probe_failure_class(error)
        prior = self._records.get(identity)
        count = 1
        if isinstance(prior, dict) and str(prior.get("failureClass") or "") == failure_class:
            count = max(1, int(prior.get("consecutiveCount") or 0)) + 1
        record: dict[str, Any] = {
            "failureClass": failure_class,
            "consecutiveCount": count,
            "lastError": str(error or "")[:300],
            "lastFailureAt": at or now_iso(),
        }
        if count >= quarantine_threshold() and is_quarantine_class(failure_class):
            record["quarantinedUntil"] = _quarantine_until_ts(
                record["lastFailureAt"], self._retention_days
            )
        self._records[identity] = record
        return record if record.get("quarantinedUntil") else None

    def clear_identity(self, identity: str) -> None:
        """Drop one identity's failure record (after a successful probe)."""
        self._records.pop(str(identity or "").strip(), None)

    def flush(self) -> dict[str, int]:
        """Persist the store atomically, pruned and bounded; returns write stats."""
        from src.source_registry import save_json_atomic

        pruned = prune_store(self._records, retention_days=self._retention_days)
        bounded_items = sorted(
            pruned.items(),
            key=lambda item: str(item[1].get("lastFailureAt") or ""),
            reverse=True,
        )[:_STORE_MAX_ENTRIES]
        payload = dict(bounded_items)
        save_json_atomic(self.path, payload)
        return {"entries": len(payload), "droppedOverQuota": max(0, len(pruned) - len(payload))}


def _quarantine_until_ts(failure_at: str, retention_days: int) -> str:
    base_s = _parse_iso_ts(failure_at)
    if base_s <= 0:
        return ""
    until = datetime.fromtimestamp(base_s + max(1, int(retention_days)) * 86400.0, tz=UTC)
    return until.isoformat()
