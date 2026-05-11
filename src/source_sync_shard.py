"""Deterministic source-sync shard construction."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from src.source_registry_identity import source_identity

SHARD_SCHEMA_VERSION = 3
DEFAULT_PREFIX_LENGTH = 2
PREFIX_LENGTH_STEP = 2
MAX_PREFIX_LENGTH = 64
DEFAULT_BASE_PATH = "baluffo/source-sync/shards"
_SAFE_PATH_COMPONENT = re.compile(r"^[A-Za-z0-9._=-]+$")


class SourceSyncShardError(ValueError):
    """Raised when rows cannot be represented as bounded source-sync shards."""


@dataclass(frozen=True)
class Shard:
    bucket: str
    key: str
    path: str
    row_count: int
    size_bytes: int
    sha256: str
    payload_bytes: bytes = field(repr=False, compare=False)

    def manifest_entry(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "path": self.path,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
            "sha256": self.sha256,
        }


def shard_key(source: dict[str, Any], *, prefix_length: int = DEFAULT_PREFIX_LENGTH) -> str:
    if prefix_length < 1 or prefix_length > MAX_PREFIX_LENGTH:
        raise ValueError(
            f"prefix_length must be between 1 and {MAX_PREFIX_LENGTH}: {prefix_length}"
        )
    identity = source_identity(source)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:prefix_length]


def build_shards(
    rows: list[dict[str, Any]],
    max_size: int,
    *,
    bucket: str = "active",
    base_path: str = DEFAULT_BASE_PATH,
) -> list[Shard]:
    if max_size <= 0:
        raise ValueError(f"max_size must be positive: {max_size}")
    normalized_bucket = _safe_path_component(bucket, field_name="bucket")
    normalized_base_path = _normalize_base_path(base_path)
    grouped = _group_by_prefix(rows, prefix_length=DEFAULT_PREFIX_LENGTH)
    shards: list[Shard] = []
    for key in sorted(grouped):
        shards.extend(
            _build_bounded_shards(
                grouped[key],
                max_size=max_size,
                bucket=normalized_bucket,
                base_path=normalized_base_path,
                prefix_length=DEFAULT_PREFIX_LENGTH,
            )
        )
    return sorted(shards, key=lambda shard: (shard.bucket, shard.key, shard.path))


def _build_bounded_shards(
    rows: list[dict[str, Any]],
    *,
    max_size: int,
    bucket: str,
    base_path: str,
    prefix_length: int,
) -> list[Shard]:
    canonical_rows = _canonical_rows(rows)
    key = shard_key(canonical_rows[0], prefix_length=prefix_length) if canonical_rows else "empty"
    shard = _build_shard(canonical_rows, bucket=bucket, base_path=base_path, key=key)
    if shard.size_bytes <= max_size:
        return [shard]
    if len(canonical_rows) <= 1 or prefix_length >= MAX_PREFIX_LENGTH:
        raise SourceSyncShardError(
            f"source-sync shard {bucket}/{key} is {shard.size_bytes} bytes, "
            f"exceeding {max_size} bytes and cannot be split further"
        )
    next_prefix_length = min(prefix_length + PREFIX_LENGTH_STEP, MAX_PREFIX_LENGTH)
    grouped = _group_by_prefix(canonical_rows, prefix_length=next_prefix_length)
    if len(grouped) == 1 and next(iter(grouped)) == key:
        raise SourceSyncShardError(
            f"source-sync shard {bucket}/{key} did not split at prefix length {next_prefix_length}"
        )
    shards: list[Shard] = []
    for next_key in sorted(grouped):
        shards.extend(
            _build_bounded_shards(
                grouped[next_key],
                max_size=max_size,
                bucket=bucket,
                base_path=base_path,
                prefix_length=next_prefix_length,
            )
        )
    return shards


def _build_shard(rows: list[dict[str, Any]], *, bucket: str, base_path: str, key: str) -> Shard:
    payload = _serialize_shard(bucket=bucket, key=key, rows=rows)
    return Shard(
        bucket=bucket,
        key=key,
        path=f"{base_path}/{bucket}/{key}.json.gz",
        row_count=len(rows),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        payload_bytes=payload,
    )


def _serialize_shard(*, bucket: str, key: str, rows: list[dict[str, Any]]) -> bytes:
    payload = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "bucket": bucket,
        "key": key,
        "rows": rows,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return gzip.compress(raw, mtime=0)


def _group_by_prefix(
    rows: list[dict[str, Any]], *, prefix_length: int
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError(f"source-sync rows must be dictionaries: {type(row).__name__}")
        grouped.setdefault(shard_key(row, prefix_length=prefix_length), []).append(dict(row))
    return grouped


def _canonical_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=lambda row: source_identity(row))


def _safe_path_component(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or not _SAFE_PATH_COMPONENT.match(normalized):
        raise ValueError(f"invalid source-sync shard {field_name}: {value!r}")
    return normalized


def _normalize_base_path(value: str) -> str:
    normalized = str(value or "").replace("\\", "/").strip()
    if not normalized or normalized != normalized.strip("/") or "//" in normalized:
        raise ValueError("base_path must not be empty")
    parts = normalized.split("/")
    for part in parts:
        _safe_path_component(part, field_name="base_path")
    return "/".join(parts)
